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
import json
import time
import subprocess as sp
from base64 import b64encode

import pulumi
import pulumi_gcp as gcp
import pulumi_docker as docker

from billing_functions import utils

# File path to where the Cloud Function's source code is located.
PATH_TO_SOURCE_CODE = './'
BIGQUERY_DEADLETTERS_TABLE = 'billing-aggregate-deadletters'
DOCKER_IMAGE = 'billing-aggregate'
DOCKER_IMAGE_REGISTRY = 'australia-southeast1-docker.pkg.dev/cpg-common/images'
GCP_SERVICE_ACCOUNT = 'billing-admin-290403@appspot.gserviceaccount.com'

# See docs regarding expiration policy and ack deadline:
# https://cloud.google.com/run/docs/triggering/pubsub-push#ack-deadline
# https://www.pulumi.com/registry/packages/gcp/api-docs/pubsub/subscription/#inputs
ACK_DEADLINE = 600
SUBSCRIPTION_EXPIRY = gcp.pubsub.SubscriptionExpirationPolicyArgs(ttl='')

DEAD_LETTERS_SCHEMA = json.dumps(utils.get_schema_json('dead_letters_schema.json'))


def main():
    """
    Get values from Pulumi config to use as
    environment variables in our Cloud Function.
    """
    gcp_opts = pulumi.Config(name='gcp')
    opts = pulumi.Config(name='opts')
    bq_opts = pulumi.Config(name='bq')

    config = {
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
    name = config['NAME']
    bucket_name = f"{name}-{config['PROJECT']}"

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
        location=config['REGION'],
        project=config['PROJECT'],
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
    functions = ast.literal_eval(config['FUNCTIONS'])

    # Create one pubsub to be triggered by the cloud scheduler
    pubsub = gcp.pubsub.Topic(f'{name}-topic', project=config['PROJECT'])

    # Dead lettering setup
    pubsub_dead_letters = dead_letters(name, config)

    # Create a cron job to run the function every day at midnight.
    job = gcp.cloudscheduler.Job(
        f'{name}-job',
        pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
            topic_name=pubsub.id, data=b64encode_str('Run aggregate functions')
        ),
        schedule=config['CRON'],
        project=config['PROJECT'],
        region=config['REGION'],
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
            'auth_token': config['SLACK_AUTH_TOKEN'],
            'channel_name': config['SLACK_CHANNEL'],
        },
        description='Slack notification channel for all gcp cost aggregator functions',
        project=config['PROJECT'],
    )

    images = {}
    for function in functions:
        if config.get('TYPE') == 'function':
            # Create the function and it's corresponding pubsub and subscription.
            create_cloud_function(
                name=function,
                config=config,
                service_account=config['GCP_SERVICE_ACCOUNT'],
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
                config=config,
                service_account=config['GCP_SERVICE_ACCOUNT'],
                pubsub_topic=pubsub,
                pubsub_dead_topic=pubsub_dead_letters,
                cloudrun_service=cloudrun_service,
                slack_channel=slack_channel,
                prebuilt_images=images,
            )
            images[config['DOCKERIMAGE']] = (image, full_image_name)


def dead_letters(name: str, config: dict) -> gcp.pubsub.Topic:
    project_name, dataset_id, table = config['GCP_AGGREGATE_DEST_TABLE'].rsplit('.', 2)

    projects = gcp.projects.get_project(filter=f'name:{project_name}')
    project = projects.projects[0]

    # Create dataset for aggregate billing if one not already in bigquery
    dataset = gcp.bigquery.Dataset(
        'aggregate-billing-destination-dataset',
        accesses=[
            gcp.bigquery.DatasetAccessArgs(
                role='READER',
                special_group='projectReaders',
            ),
            gcp.bigquery.DatasetAccessArgs(
                role='WRITER',
                special_group='projectWriters',
            ),
            gcp.bigquery.DatasetAccessArgs(
                role='OWNER',
                special_group='projectOwners',
            ),
        ],
        dataset_id=dataset_id,
        location=config['REGION'],
        project=project.project_id,
        opts=pulumi.ResourceOptions(protect=True),
    )

    # Make sure the sa can view and edit
    sa = (
        f'serviceAccount:service-{project.number}@gcp-sa-pubsub.iam.gserviceaccount.com'
    )
    viewer = gcp.projects.IAMMember(
        'dead-letters-sa-bq-viewer',
        project=project.project_id,
        role='roles/bigquery.metadataViewer',
        member=sa,
    )
    editor = gcp.projects.IAMMember(
        'dead-letters-sa-bq-editor',
        project=project.project_id,
        role='roles/bigquery.dataEditor',
        member=sa,
    )
    publisher = gcp.projects.IAMMember(
        'dead-letters-sa-publisher',
        project=project.project_id,
        role='roles/pubsub.publisher',
        member=sa,
    )
    subscriber = gcp.projects.IAMMember(
        'dead-letters-sa-subscriber',
        project=project.project_id,
        role='roles/pubsub.subscriber',
        member=sa,
    )
    create_token = gcp.projects.IAMMember(
        'dead-letters-sa-create_token',
        project=project.project_id,
        role='roles/iam.serviceAccountTokenCreator',
        member=sa,
    )
    iam = [viewer, editor, publisher, subscriber, create_token]

    # Create table
    deadletters_table = gcp.bigquery.Table(
        f'{table}-deadletters-table',
        project=project.project_id,
        dataset_id=dataset.dataset_id,
        table_id=f'{table}_deadletters',
        schema=DEAD_LETTERS_SCHEMA,
        opts=pulumi.ResourceOptions(depends_on=[dataset, *iam]),
    )
    table_id = pulumi.Output.all(
        deadletters_table.project,
        deadletters_table.dataset_id,
        deadletters_table.table_id,
    ).apply(lambda x: f'{x[0]}:{x[1]}.{x[2]}')

    # Create topic and subscription
    pubsub_dead_letters = gcp.pubsub.Topic(
        f'{name}-dead-letters', project=project.project_id
    )

    gcp.pubsub.Subscription(
        f'{name}-dead-letters-subscription',
        topic=pubsub_dead_letters.name,
        project=project.project_id,
        expiration_policy=SUBSCRIPTION_EXPIRY,
        bigquery_config=gcp.pubsub.SubscriptionBigqueryConfigArgs(
            table=table_id,
            use_topic_schema=True,
            write_metadata=True,
        ),
        opts=pulumi.ResourceOptions(
            depends_on=[pubsub_dead_letters, deadletters_table, *iam]
        ),
    )

    return pubsub_dead_letters


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
                service_account_name=service_account,
                # timeout_seconds=config['TIMEOUT'],
            ),
        ),
        traffics=[
            gcp.cloudrun.ServiceTrafficArgs(
                percent=100,
                latest_revision=True,
            )
        ],
        opts=pulumi.ResourceOptions(depends_on=[image, pubsub_topic, cloudrun_service]),
    )

    # Make this service account an invoker
    invoker = gcp.projects.IAMMember(
        f'{name}-sa-invoker',
        project=fxn.project,
        role='roles/run.invoker',
        member=f'serviceAccount:{service_account}',
    )
    binding = gcp.cloudrun.IamBinding(
        f'{name}-sa-binding',
        project=fxn.project,
        location=fxn.location,
        service=fxn.name,
        role='roles/run.invoker',
        members=[f'serviceAccount:{service_account}'],
    )
    project_token_creator = gcp.projects.IAMMember(
        f'{name}-sa-project_token_creator',
        project=fxn.project,
        role='roles/iam.serviceAccountTokenCreator',
        member=f'serviceAccount:{service_account}',
    )
    subscriber = gcp.projects.IAMMember(
        f'{name}-sa-subscriber',
        project=fxn.project,
        role='roles/pubsub.subscriber',
        member=f'serviceAccount:{service_account}',
    )
    publisher = gcp.projects.IAMMember(
        f'{name}-sa-publisher',
        project=fxn.project,
        role='roles/pubsub.publisher',
        member=f'serviceAccount:{service_account}',
    )
    iam = [invoker, binding, project_token_creator, subscriber, publisher]

    # Create a subscription to trigger this cloudrun job
    # The function name to run is in the attributes
    subscription = gcp.pubsub.Subscription(
        f'billing-aggregate-{name}-cloudrun-subscription',
        topic=pubsub_topic.name,
        ack_deadline_seconds=ACK_DEADLINE,
        expiration_policy=SUBSCRIPTION_EXPIRY,
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
        )
        if pubsub_dead_topic
        else None,
        opts=pulumi.ResourceOptions(depends_on=[fxn, pubsub_topic, *iam]),
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
