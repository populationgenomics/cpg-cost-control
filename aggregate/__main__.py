#!/usr/bin/env python

"""
Python Pulumi program for creating Google Cloud Functions.

TODO: Modify this program to create all our aggregate cloud functions.

Create a single Google Cloud Function. The deployed application will calculate
the estimated travel time to a given location, sending the results via SMS.
"""

import os
import sys
import time
import pulumi

from pulumi_gcp import storage
from pulumi_gcp import cloudfunctions

# Disable rule for that module-level exports be ALL_CAPS, for legibility.
# pylint: disable=C0103

# File path to where the Cloud Function's source code is located.
PATH_TO_SOURCE_CODE = './'

# Get values from Pulumi config to use as environment variables in our Cloud Function.
config = pulumi.Config(name='gcp')

config_values = {
    'PROJECT': config.get('project'),
    'REGION': config.get('region'),
}

name = 'aggregate-cloud-functions'
bucket_name = f'{config_values["PROJECT"]}-{name}'

# We will store the source code to the Cloud Function in a Google Cloud Storage bucket.
bucket = storage.Bucket(bucket_name, location=config_values['REGION'])

# The Cloud Function source code itself needs to be zipped up into an
# archive, which we create using the pulumi.AssetArchive primitive.


def archive_folder(path: str) -> pulumi.AssetArchive:
    assets = {}
    for file in os.listdir(path):
        if file.startswith('_'):
            continue

        location = os.path.join(path, file)
        if os.path.isdir(location):
            asset = archive_folder(location)
        else:
            asset = pulumi.FileAsset(path=location)

        assets[file] = asset

    return pulumi.AssetArchive(assets)


archive = archive_folder(PATH_TO_SOURCE_CODE)

# Create the single Cloud Storage object, which contains all of the function's
# source code. ('main.py' and 'requirements.txt'.)
source_archive_object = storage.BucketObject(
    bucket_name,
    name=f'{name}-{time.time()}',
    bucket=bucket.name,
    source=archive,
)

# Create the Cloud Function, deploying the source we just uploaded to Google
# Cloud Storage.
functions = pulumi.Config(name='opts').get('functions')


def create_cloud_function(
    name: str,
    function_bucket: storage.Bucket,
    source_archive_object: storage.BucketObject,
):
    fxn = cloudfunctions.Function(
        name,
        entry_point='main.py',
        region=config_values['REGION'],
        runtime='python3',
        source_archive_bucket=function_bucket.name,
        source_archive_object=source_archive_object.name,
    )

    invoker = cloudfunctions.FunctionIamMember(
        'invoker',
        project=fxn.project,
        region=fxn.region,
        cloud_function=fxn.name,
        role='roles/cloudfunctions.invoker',
        member='allUsers',
    )

    return fxn


# Deploy all functions
pulumi.export('bucket_name', bucket.url)
for function in functions:
    # Export the DNS name of the bucket and the cloud function URL.
    fxn = create_cloud_function(function, bucket, source_archive_object)
    pulumi.export('fxn_url', fxn.https_trigger_url)
