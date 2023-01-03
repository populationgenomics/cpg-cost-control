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
import subprocess as sp
from base64 import b64encode

import pulumi
import pulumi_gcp as gcp
import pulumi_docker as docker


# NOTE: Uncomment the below code when launching pulumi locally
# after running `pulumi up` or an equivalent command, then hit F5 to connect the
# vscode debugger. Helpful for finding hidden pulumi errors

# import debugpy

# debugpy.listen(('0.0.0.0', 5678))
# print('debugpy is listening, attach by pressing F5 or ►')

# debugpy.wait_for_client()
# print('Attached to debugpy!')


# File path to where the Cloud Function's source code is located.
PATH_TO_SOURCE_CODE = './'
DOCKER_IMAGE = 'billing-aggregate'
DOCKER_IMAGE_REGISTRY = 'australia-southeast1-docker.pkg.dev/cpg-common/images'
GCP_SERVICE_ACCOUNT = 'billing-admin-290403@appspot.gserviceaccount.com	'


def main():
    """
    Get values from Pulumi config to use as
    environment variables in our Cloud Function.
    """
    gcp_opts = pulumi.Config(name='gcp')
    opts = pulumi.Config(name='opts')
    bq_opts = pulumi.Config(name='bq')

    config_values = {
        'NAME': opts.get('name'),
        'TYPE': opts.get('type'),
        'DOCKERFILE': opts.get('dockerfile'),
        'DOCKERIMAGE': opts.get('imagename'),
        'CRON': opts.get('cron'),
        'MEMORY': int(opts.get('memory')),
        'TIMEOUT': int(opts.get('timeout')),
        'FUNCTIONS': opts.get('functions'),
        'SLACK_CHANNEL': opts.get('slack_channel'),
        'GCP_SERVICE_ACCOUNT': opts.get('service_account'),
        'SLACK_AUTH_TOKEN': os.getenv('SLACK_AUTH_TOKEN'),
        'REGION': gcp_opts.get('region'),
        'PROJECT': gcp_opts.get('project'),
        'GCP_AGGREGATE_DEST_TABLE': bq_opts.get('destination'),
    }

    # Set environment variable to the correct project
    name = config_values['NAME']
    bucket_name = f"{name}-{config_values['PROJECT']}"

    # Configure docker properly. Only needs to be run once, but is essential
    configure_docker()

    # Start by enabling all cloud function services
    cloud_service = gcp.projects.Service(
        'cloudfunctions-service',
        service='cloudfunctions.googleapis.com',
        disable_on_destroy=False,
    )
    cloudrun_service = gcp.projects.Service(
        'cloudrun-service',
        service='run.googleapis.com',
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
        bucket=bucket_name,
        source=archive,
    )

    # Create the Cloud Function, deploying the source we just uploaded to Google
    # Cloud Storage.
    functions = ast.literal_eval(config_values['FUNCTIONS'])

    # Create one pubsub to be triggered by the cloud scheduler
    pubsub = gcp.pubsub.Topic(f'{name}-topic', project=config_values['PROJECT'])
    pubsub_dead_letters = gcp.pubsub.Topic(
        f'{name}-dead-letters', project=config_values['PROJECT']
    )

    # Create a cron job to run the function every day at midnight.
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

    # Create slack notification channel for all functions
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

    images = {}
    for function in functions:
        if config_values.get('TYPE') == 'function':
            # Create the function and it's corresponding pubsub and subscription.
            create_cloud_function(
                name=function,
                config=config_values,
                service_account=config_values['GCP_SERVICE_ACCOUNT'],
                pubsub_topic=pubsub,
                pubsub_dead_topic=pubsub_dead_letters,
                cloud_service=cloud_service,
                function_bucket=function_bucket,
                source_archive_object=source_archive_object,
                slack_channel=slack_channel,
            )
        else:
            _, image, full_image_name, _, _ = create_cloudrun_job(
                name=function,
                config=config_values,
                service_account=config_values['GCP_SERVICE_ACCOUNT'],
                pubsub_topic=pubsub,
                pubsub_dead_topic=pubsub_dead_letters,
                cloudrun_service=cloudrun_service,
                slack_channel=slack_channel,
                prebuilt_images=images,
            )
            images[config_values['DOCKERIMAGE']] = (image, full_image_name)


def configure_docker():
    sp.check_output(
        ['gcloud', 'auth', 'configure-docker', DOCKER_IMAGE_REGISTRY], stderr=sp.DEVNULL
    )


def setup_docker_image(name, dockerfile, project):
    # Get dockerfile folder
    path = os.path.dirname(os.path.abspath(dockerfile))

    image_name = f'{DOCKER_IMAGE_REGISTRY}/{project}/{name}'

    # Dockerfile is assumed to be in the root folder of the build context
    image = docker.Image(
        name,
        image_name=image_name,
        build=docker.DockerBuild(
            context=path,
            extra_options=[
                '--platform',
                'linux/amd64',  # enforce linux in case building on apple silicon
                '--quiet',  # see https://github.com/pulumi/pulumi-docker/issues/289
            ],
        ),
        skip_push=False,
    )

    pulumi.export('base_image_name', image.base_image_name)
    pulumi.export('full_image_name', image.image_name)

    return image, image_name


def get_image(config: dict, prebuilt_images: dict[str, docker.Image]):
    image_name = config['DOCKERIMAGE']
    image: docker.Image = None
    if image_name in prebuilt_images.keys():
        image, full_image_name = prebuilt_images[image_name]
    else:
        image, full_image_name = setup_docker_image(
            config['DOCKERIMAGE'], config['DOCKERFILE'], config['PROJECT']
        )
    return image, full_image_name


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')


def create_cloud_function(
    name: str = '',
    config: dict = None,
    service_account: str = None,
    pubsub_topic: gcp.pubsub.Topic = None,
    pubsub_dead_topic: gcp.pubsub.Topic = None,
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
        'GCP_AGGREGATE_DEST_TABLE': config['GCP_AGGREGATE_DEST_TABLE'],
    }
    fxn = gcp.cloudfunctions.Function(
        f'{name}-billing-function',
        entry_point=name,
        runtime='python310',
        event_trigger=trigger,
        source_archive_bucket=function_bucket.name,
        source_archive_object=source_archive_object.name,
        project=config['PROJECT'],
        region=config['REGION'],
        build_environment_variables=env,
        environment_variables=env,
        service_account_email=service_account,
        available_memory_mb=config['MEMORY'],
        timeout=config['TIMEOUT'],
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
            resource.type='cloud_function'
            AND resource.labels.function_name='{fxn_name}'
            AND severity >= WARNING
        """
    )

    alert_policy = create_alert_policy(
        name, fxn, 'function', filter_string, slack_channel
    )

    pulumi.export(f'{name}_function_name', fxn.name)

    return fxn, trigger, alert_policy


def create_cloudrun_job(
    name: str = '',
    config: dict = None,
    service_account: str = None,
    pubsub_topic: gcp.pubsub.Topic = None,
    pubsub_dead_topic: gcp.pubsub.Topic = None,
    cloudrun_service: gcp.projects.Service = None,
    slack_channel: gcp.monitoring.NotificationChannel = None,
    prebuilt_images: dict[str, docker.Image] = None,
):
    """
    Create a single Cloud Run Job. Include the pubsub trigger and event alerts
    """

    # Get prebuilt image, or build the new one
    image, full_image_name = get_image(config, prebuilt_images)

    # Create the Cloud Function
    envs = [
        {
            'name': 'GCP_AGGREGATE_DEST_TABLE',
            'value': config['GCP_AGGREGATE_DEST_TABLE'],
        }
    ]

    container = gcp.cloudrun.ServiceTemplateSpecContainerArgs(
        image=full_image_name,
        envs=envs,
    )

    fxn = gcp.cloudrun.Service(
        f'{name}-billing-cloudrun',
        location=config['REGION'],
        template=gcp.cloudrun.ServiceTemplateArgs(
            spec=gcp.cloudrun.ServiceTemplateSpecArgs(
                containers=[container],
                timeout_seconds=config['TIMEOUT'],
                service_account_name=service_account,
            ),
        ),
        opts=pulumi.ResourceOptions(depends_on=[image, pubsub_topic, cloudrun_service]),
    )

    # Make this service account an invoker
    gcp.cloudrun.IamMember(
        f'{name}-cloudrun-iam-member',
        location=fxn.location,
        project=fxn.project,
        service=fxn.name,
        role='roles/run.invoker',
        member=f'serviceAccount:{service_account}',
    )

    # Create a subscription to trigger this cloudrun job
    # The function name to run is in the attributes
    subscription = gcp.pubsub.Subscription(
        f'billing-aggregate-{name}-cloudrun-subscription',
        topic=pubsub_topic.name,
        push_config=gcp.pubsub.SubscriptionPushConfigArgs(
            push_endpoint=fxn.statuses[0].url,
            attributes={
                'x-goog-version': 'v1',
                'function': name,
            },
            oidc_token=gcp.pubsub.SubscriptionPushConfigOidcTokenArgs(
                service_account_email=service_account,
            ),
        ),
        dead_letter_policy=gcp.pubsub.SubscriptionDeadLetterPolicyArgs(
            dead_letter_topic=pubsub_dead_topic, max_delivery_attempts=5
        ),
        opts=pulumi.ResourceOptions(depends_on=[fxn, pubsub_topic]),
    )

    # Slack notifications
    filter_string = fxn.name.apply(
        lambda fxn_name: f"""
            resource.type='cloud_run_job'
            AND resource.labels.job_name='{fxn_name}'
            AND severity >= WARNING
        """
    )

    alert_policy = create_alert_policy(
        name, fxn, 'cloudrun', filter_string, slack_channel
    )

    pulumi.export(f'{name}_cloudrun_name', fxn.name)

    return fxn, image, full_image_name, subscription, alert_policy


def create_alert_policy(
    name,
    fxn,
    run_type: str = '',
    filter_string: str = '',
    slack_channel: gcp.monitoring.NotificationChannel = None,
):
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
        f'{name}-billing-{run_type}-error-alert',
        display_name=f'{name.capitalize()} Billing Cloud Run Error Alert',
        combiner='OR',
        notification_channels=[slack_channel],
        conditions=[alert_condition],
        alert_strategy=alert_rate,
        opts=pulumi.ResourceOptions(depends_on=[fxn]),
    )

    return alert_policy


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
    if os.getenv('DEBUG'):
        import debugpy

        debugpy.listen(('localhost', 5678))
        print('debugpy is listening, attach by pressing F5 or ►')

        debugpy.wait_for_client()
        print('Attached to debugpy!')

    main()
