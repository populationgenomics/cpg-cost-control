#!/usr/bin/env python

"""
Python Pulumi program for creating Google Cloud Functions.

TODO: Modify this program to create all our aggregate cloud functions.

Create a single Google Cloud Function. The deployed application will calculate
the estimated travel time to a given location, sending the results via SMS.
"""

import os
import ast
import time
import pulumi

import pulumi_gcp as gcp

from base64 import b64encode

# Disable rule for that module-level exports be ALL_CAPS, for legibility.
# pylint: disable=C0103

# File path to where the Cloud Function's source code is located.
PATH_TO_SOURCE_CODE = './'

# Get values from Pulumi config to use as environment variables in our Cloud Function.
gcp_opts = pulumi.Config(name='gcp')
opts = pulumi.Config(name='opts')

config_values = {
    'PROJECT': gcp_opts.get('project'),
    'REGION': gcp_opts.get('region'),
    'GCP_AGGREGATE_DEST_TABLE': opts.get('destination'),
    'FUNCTIONS': opts.get('functions'),
}

name = 'aggregate-cloud-functions'
bucket_name = f'{config_values["PROJECT"]}-{name}'

# Start by enabling all cloud function services
gcp.projects.Service(
    'cloudfunctions-service',
    service='cloudfunctions.googleapis.com',
    disable_on_destroy=False,
)

# We will store the source code to the Cloud Function in a Google Cloud Storage bucket.
function_bucket = gcp.storage.Bucket(
    bucket_name,
    name=bucket_name,
    location=config_values['REGION'],
    project=config_values['PROJECT'],
    uniform_bucket_level_access=True,
)

# The Cloud Function source code itself needs to be zipped up into an
# archive, which we create using the pulumi.AssetArchive primitive.
def archive_folder(path: str) -> pulumi.AssetArchive:
    assets = {}
    for file in os.listdir(path):
        location = os.path.join(path, file)
        if os.path.isdir(location):
            asset = pulumi.FileArchive(location)
        elif location.endswith('.py') or location.endswith('.txt'):
            asset = pulumi.FileAsset(path=location)

        assets[file] = asset

    return pulumi.AssetArchive(assets)


archive = archive_folder(PATH_TO_SOURCE_CODE)

# Create the single Cloud Storage object, which contains all of the function's
# source code. ('main.py' and 'requirements.txt'.)
source_archive_object = gcp.storage.BucketObject(
    bucket_name,
    name=f'{name}-source-archive-{time.time()}',
    bucket=function_bucket.name,
    source=archive,
)


def create_cloud_function(
    name: str,
    pubsub_topic: gcp.pubsub.Topic,
    function_bucket: gcp.storage.Bucket,
    source_archive_object: gcp.storage.BucketObject,
):
    trigger = gcp.cloudfunctions.FunctionEventTriggerArgs(
        event_type="google.pubsub.topic.publish", resource=pubsub_topic.name
    )

    env = {
        'GCP_AGGREGATE_DEST_TABLE': config_values['GCP_AGGREGATE_DEST_TABLE'],
    }
    fxn = gcp.cloudfunctions.Function(
        f"{name}-billing-function",
        entry_point=f'{name}',
        runtime='python39',
        event_trigger=trigger,
        source_archive_bucket=function_bucket.name,
        source_archive_object=source_archive_object.name,
        project=config_values['PROJECT'],
        region=config_values['REGION'],
        build_environment_variables=env,
        environment_variables=env,
        opts=pulumi.ResourceOptions(
            depends_on=[function_bucket, source_archive_object, pubsub_topic]
        ),
    )

    return fxn, trigger


# Cloud scheduler -> cron
# Cloud scheduler -> pubsub -> function
# Set the timezone to australia
# Compare raw dates to UTC dates

# Create the Cloud Function, deploying the source we just uploaded to Google
# Cloud Storage.
functions = ast.literal_eval(config_values['FUNCTIONS'])

# Deploy all functions
pulumi.export('bucket_name', function_bucket.url)

# Create one pubsub to be triggered by the cloud scheduler
pubsub = gcp.pubsub.Topic(f"{name}-topic", project=config_values['PROJECT'])

for function in functions:
    # Create the function and it's corresponding pubsub and subscription.
    fxn, trigger = create_cloud_function(
        function, pubsub, function_bucket, source_archive_object
    )

    pulumi.export(f'{function}_fxn_name', fxn.name)


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')


# Create a cron job to run the function every day at midnight.
job = gcp.cloudscheduler.Job(
    f"{name}-job",
    pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
        topic_name=pubsub.id,
        data=b64encode_str('Run the functions'),
    ),
    schedule='every 24 hours',
    project=config_values['PROJECT'],
    region=config_values['REGION'],
    opts=pulumi.ResourceOptions(depends_on=[pubsub]),
)

pulumi.export('cron_job', job.id)
