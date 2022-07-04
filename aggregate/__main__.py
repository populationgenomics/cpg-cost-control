#!/usr/bin/env python
# Disable rule for that module-level exports be ALL_CAPS, for legibility.
# pylint: disable=C0103,missing-function-docstring,W0613
"""
Python Pulumi program for creating Aggregate Billing Function Stack.

Requires:
    - Service Account in the gcp project of deployment
        (see .github/workflows/deploy-aggregate.yml for details)
    - Pulumi.billing_aggregate.yaml file configured with correct values
        * gcp:project (gcp project name)
        * gcp:region (e.g. us-central1)
        * opts:service_account (with permissions already added)
        * opts:functions (list of functions in aggregate/ folder to deploy)
        * opts:destination (BQ table to write to)
        * opts:slack_channel (slack channel to post to)

Creates the following:
    - Enable Cloud Function Service
    - Create a bucket for the function source code
    - Create bucket object for the function source code and put it in the bucket
    - Create a pubsub topic and cloud scheduler for the functions
    - Create a slack notification channel for all functions
    - Create a cloud function for each function

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
        'NAME': opts.get('name'),
        'CRON': opts.get('cron'),
        'MEMORY': opts.get('memory'),
        'TIMEOUT': opts.get('timeout'),
        'FUNCTIONS': opts.get('functions'),
        'SLACK_CHANNEL': opts.get('slack_channel'),
        'GCP_SERVICE_ACCOUNT': opts.get('service_account'),
        'GCP_AGGREGATE_DEST_TABLE': opts.get('destination'),
        'SLACK_AUTH_TOKEN': os.getenv('SLACK_AUTH_TOKEN'),
    }

    # Set environment variable to the correct project
    name = config_values['NAME']
    bucket_name = f'{name}-{config_values["PROJECT"]}'

    # Start by enabling all cloud function services
    cloud_service = gcp.projects.Service(
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
        schedule=config_values['CRON'],
        project=config_values['PROJECT'],
        region=config_values['REGION'],
        time_zone='Australia/Sydney',
        opts=pulumi.ResourceOptions(depends_on=[pubsub]),
    )

    pulumi.export('cron_job', job.id)

    # Create slack notificaiton channel for all functions
    # Use cli command below to retrieve the required 'labels'
    # $ gcloud beta monitoring channel-descriptors describe slack
    slack_channel = gcp.monitoring.NotificationChannel(
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
            service_account=config_values['GCP_SERVICE_ACCOUNT'],
            pubsub_topic=pubsub,
            cloud_service=cloud_service,
            function_bucket=function_bucket,
            source_archive_object=source_archive_object,
            slack_channel=slack_channel,
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
    cloud_service: gcp.projects.Service = None,
    source_archive_object: gcp.storage.BucketObject = None,
    slack_channel: gcp.monitoring.NotificationChannel = None,
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
        runtime='python310',
        event_trigger=trigger,
        source_archive_bucket=function_bucket.name,
        source_archive_object=source_archive_object.name,
        project=config_values['PROJECT'],
        region=config_values['REGION'],
        build_environment_variables=env,
        environment_variables=env,
        service_account_email=service_account,
        available_memory_mb=config_values['MEMORY'],
        timeout=config_values['TIMEOUT'],
        opts=pulumi.ResourceOptions(
            depends_on=[
                pubsub_topic,
                cloud_service,
                function_bucket,
                source_archive_object,
            ]
        ),
    )

    # Slack notifications
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
        notification_channels=[slack_channel],
        conditions=[alert_condition],
        alert_strategy=alert_rate,
        opts=pulumi.ResourceOptions(depends_on=[fxn]),
    )
    alert_policy = None

    return fxn, trigger, alert_policy


def archive_folder(path: str) -> pulumi.AssetArchive:
    assets = {}
    allowed_extensions = ['.py', '.txt']
    for file in os.listdir(path):
        location = os.path.join(path, file)
        if os.path.isdir(location) and not location.startswith('__'):
            assets[file] = pulumi.FileArchive(location)
        elif any(location.endswith(ext) for ext in allowed_extensions):
            assets[file] = pulumi.FileAsset(path=location)
        # skip any other files,

    return pulumi.AssetArchive(assets)


if __name__ == '__main__':
    main()
