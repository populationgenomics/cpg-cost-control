#!/usr/bin/env python
# Disable rule for that module-level exports be ALL_CAPS, for legibility.
# pylint: disable=C0103,missing-function-docstring
"""
Python Pulumi program for creating Google Cloud Functions.

TODO: Modify this program to create all our aggregate cloud functions.

Create a single Google Cloud Function. The deployed application will calculate
the estimated travel time to a given location, sending the results via SMS.
"""

import os
import ast
import time
from base64 import b64encode

import pulumi
import pulumi_gcp as gcp


# NOTE: Uncomment the below code when launching pulumi locally
# after running `pulumi up` or an equivalent command, then hit F5 to connect the
# vscode debugger. Helpful for finding hidden pulumi errors

# import debugpy

# debugpy.listen(("0.0.0.0", 5678))
# print("debugpy is listening, attach by pressing F5 or ►")

# debugpy.wait_for_client()
# print("Attached to debugpy!")


# File path to where the Cloud Function's source code is located.
PATH_TO_SOURCE_CODE = './'


def main():
    """
    Get values from Pulumi config to use as
    environment variables in our Cloud Function.
    """
    gcp_opts = pulumi.Config(name='gcp')
    opts = pulumi.Config(name='opts')

    config_values = {
        'REGION': gcp_opts.get('region'),
        'PROJECT': gcp_opts.get('project'),
        'FUNCTIONS': opts.get('functions'),
        'SLACK_CHANNEL': opts.get('slack_channel'),
        'GCP_SERVICE_ACCOUNT': opts.get('service_account'),
        'GCP_AGGREGATE_DEST_TABLE': opts.get('destination'),
        'SLACK_AUTH_TOKEN': os.getenv('SLACK_AUTH_TOKEN'),
    }

    # Set environment variable to the correct project
    name = 'aggregate-billing'
    bucket_name = f'{name}-{config_values["PROJECT"]}'

    # Start by enabling all cloud function services
    gcp.projects.Service(
        'cloudfunctions-service',
        service='cloudfunctions.googleapis.com',
        disable_on_destroy=False,
    )

    # We will store the source code to the Cloud Function
    # in a Google Cloud Storage bucket.
    function_bucket = gcp.storage.Bucket(
        bucket_name,
        name=bucket_name,
        location=config_values['REGION'],
        project=config_values['PROJECT'],
        uniform_bucket_level_access=True,
    )

    # Deploy all functions
    pulumi.export('bucket_name', function_bucket.url)

    # The Cloud Function source code itself needs to be zipped up into an
    # archive, which we create using the pulumi.AssetArchive primitive.
    archive = archive_folder(PATH_TO_SOURCE_CODE)

    # Create the single Cloud Storage object, which contains all of the function's
    # source code. ('main.py' and 'requirements.txt'.)
    source_archive_object = gcp.storage.BucketObject(
        bucket_name,
        name=f'{name}-source-archive-{time.time()}',
        bucket=function_bucket.name,
        source=archive,
    )

    # Create Service Account
    service_account = config_values['GCP_SERVICE_ACCOUNT']

    # Create the Cloud Function, deploying the source we just uploaded to Google
    # Cloud Storage.
    functions = ast.literal_eval(config_values['FUNCTIONS'])

    # Create one pubsub to be triggered by the cloud scheduler
    pubsub = gcp.pubsub.Topic(f'{name}-topic', project=config_values['PROJECT'])

    # Create a cron job to run the function every day at midnight.s
    job = gcp.cloudscheduler.Job(
        f'{name}-job',
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

    # Create slack notificaiton channel for all functions
    # Use cli command below to retrieve the required 'labels'
    # $ gcloud beta monitoring channel-descriptors describe slack
    slack_notification_channel = gcp.monitoring.NotificationChannel(
        f'{name}-slack-notification-channel',
        display_name=f'{name.capitalize()} Slack Notification Channel',
        type='slack',
        labels={
            'auth_token': config_values['SLACK_AUTH_TOKEN'],
            'channel_name': config_values['SLACK_CHANNEL'],
        },
        description='Slack notification channel for all gcp cost aggregator functions',
        project=config_values['PROJECT'],
    )

    for function in functions:
        # Create the function and it's corresponding pubsub and subscription.
        fxn, _, _ = create_cloud_function(
            name=function,
            config_values=config_values,
            pubsub_topic=pubsub,
            function_bucket=function_bucket,
            source_archive_object=source_archive_object,
            service_account=service_account,
            slack_notification_channel=slack_notification_channel,
        )

        pulumi.export(f'{function}_fxn_name', fxn.name)


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')


def create_cloud_function(
    name: str = '',
    config_values: dict = None,
    service_account: str = None,
    pubsub_topic: gcp.pubsub.Topic = None,
    function_bucket: gcp.storage.Bucket = None,
    source_archive_object: gcp.storage.BucketObject = None,
    slack_notification_channel: gcp.monitoring.NotificationChannel = None,
):
    """
    Create a single Cloud Function. Include the pubsub trigger and event alerts
    """

    # Trigger for the function, subscribe to the pubusub topic
    trigger = gcp.cloudfunctions.FunctionEventTriggerArgs(
        event_type='google.pubsub.topic.publish', resource=pubsub_topic.name
    )

    # Create the Cloud Function
    env = {
        'GCP_AGGREGATE_DEST_TABLE': config_values['GCP_AGGREGATE_DEST_TABLE'],
    }
    fxn = gcp.cloudfunctions.Function(
        f'{name}-billing-function',
        entry_point=f'{name}',
        runtime='python39',
        event_trigger=trigger,
        source_archive_bucket=function_bucket.name,
        source_archive_object=source_archive_object.name,
        project=config_values['PROJECT'],
        region=config_values['REGION'],
        build_environment_variables=env,
        environment_variables=env,
        service_account_email=service_account,
        available_memory_mb=1024,
        timeout=540,
        opts=pulumi.ResourceOptions(
            depends_on=[
                function_bucket,
                source_archive_object,
                pubsub_topic,
            ]
        ),
    )

    filter_string = fxn.name.apply(
        lambda fxn_name: f"""
            resource.type="cloud_function"
            AND resource.labels.function_name="{fxn_name}"
            AND severity >= WARNING
        """
    )

    # Create the Cloud Function's event alert
    alert_condition = gcp.monitoring.AlertPolicyConditionArgs(
        condition_matched_log=(
            gcp.monitoring.AlertPolicyConditionConditionMatchedLogArgs(
                filter=filter_string,
            )
        ),
        display_name='Function warning/error',
    )
    alert_rate = gcp.monitoring.AlertPolicyAlertStrategyArgs(
        notification_rate_limit=(
            gcp.monitoring.AlertPolicyAlertStrategyNotificationRateLimitArgs(
                period='300s'
            )
        ),
    )
    alert_policy = gcp.monitoring.AlertPolicy(
        f'{name}-billing-function-error-alert',
        display_name=f'{name.capitalize()} Billing Function Error Alert',
        combiner='OR',
        notification_channels=[slack_notification_channel],
        conditions=[alert_condition],
        alert_strategy=alert_rate,
        opts=pulumi.ResourceOptions(depends_on=[fxn]),
    )

    return fxn, trigger, alert_policy


def archive_folder(path: str) -> pulumi.AssetArchive:
    assets = {}
    for file in os.listdir(path):
        location = os.path.join(path, file)
        if os.path.isdir(location) and not location.startswith('__'):
            asset = pulumi.FileArchive(location)
        elif location.endswith('.py') or location.endswith('.txt'):
            asset = pulumi.FileAsset(path=location)

        assets[file] = asset

    return pulumi.AssetArchive(assets)


if __name__ == '__main__':
    main()
