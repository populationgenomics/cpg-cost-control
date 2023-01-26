#!/usr/bin/env python
"""
Python Pulumi program for monitoring billing on Azure.

Requires:
    - Service Principal in the azure billing "project"
        * That's the cpg subscription and the billing resource group
        * Give it the following permissions through IAM on the subscription scope
            - Reader
            - Resource Policy Contributor
        * Give it the following permissions through IAM on the billing resource group
            - Owner
    - Pulumi.azure-cost-control.yaml file configured with correct values
        * See the file for required inputs
        * Requires Azure location, SP details and the resource groups to manage

- Creates an Azure web app to disable billing once triggered
"""

import pulumi
import pulumi_azure_native as az

import helpers


def main():
    """
    Get values from Pulumi config to use as
    environment variables in our Cloud Function.
    """
    opts = pulumi.Config(name='opts')
    az_opts = pulumi.Config(name='azure-native')
    create_billing_stop(opts, az_opts)


def create_billing_stop(opts: dict, az_opts: dict):
    """
    Creates the stop compute policy and the function that given a resource group
    will stop all compute on that resource group
    """

    resource_group = az.resources.get_resource_group(opts.get('billing_group'))

    # Create a the policy definitions
    stop_policy_definition = az.authorization.PolicyDefinition(
        'stop-compute-policy',
        policy_rule={
            'if': {
                'allOf': [
                    {'field': 'type', 'equals': 'Microsoft.Compute/virtualMachines'},
                ]
            },
            'then': {'effect': 'deny'},
        },
        mode='all',
    )
    start_policy_definition = az.authorization.PolicyDefinition(
        'start-compute-policy',
        policy_rule={
            'if': {
                'allOf': [
                    {'field': 'type', 'equals': 'Microsoft.Compute/virtualMachines'},
                    {'field': 'location', 'notEquals': 'australiaeast'},
                ]
            },
            'then': {'effect': 'deny'},
        },
        mode='all',
    )

    policies = [stop_policy_definition, start_policy_definition]

    # Create an Azure Function App
    func_name = 'stop-compute'

    # Storage account, code container and blob all required to host the code
    storage_account = az.storage.StorageAccount(
        f'{resource_group.name}-storage-account',
        account_name='billingstorage2975',
        resource_group_name=resource_group.name,
        sku=az.storage.SkuArgs(name='Standard_LRS'),
        kind='StorageV2',
    )

    code_container = az.storage.BlobContainer(
        f'container-{func_name}',
        resource_group_name=resource_group.name,
        account_name=storage_account.name,
    )

    code_blob = az.storage.Blob(
        f'zip-{func_name}',
        resource_group_name=resource_group.name,
        account_name=storage_account.name,
        container_name=code_container.name,
        source=pulumi.asset.FileArchive('./code'),
    )

    # Create the app service host plan as well as sign the blob url for the WebApp
    plan = az.web.AppServicePlan(
        f'plan-{func_name}',
        resource_group_name=resource_group.name,
        kind='Linux',
        sku=az.web.SkuDescriptionArgs(name='Y1', tier='Dynamic'),
        reserved=True,
    )

    sa_connection_url = helpers.get_connection_string(resource_group, storage_account)
    code_blob_url = helpers.signed_blob_read_url(
        code_blob, code_container, storage_account, resource_group
    )

    # Create the WebApp
    # Passes through the policy ids, subscription and location of the billing rg
    app = az.web.WebApp(
        func_name,
        resource_group_name=resource_group.name,
        server_farm_id=plan.id,
        kind='functionapp',
        site_config=az.web.SiteConfigArgs(
            app_settings=[
                {'name': 'runtime', 'value': 'python'},
                {'name': 'AzureWebJobsStorage', 'value': sa_connection_url},
                {'name': 'FUNCTIONS_WORKER_RUNTIME', 'value': 'python'},
                {'name': 'WEBSITE_RUN_FROM_PACKAGE', 'value': code_blob_url},
                {'name': 'FUNCTIONS_EXTENSION_VERSION', 'value': '~3'},
                {'name': 'LOCATION', 'value': az_opts.get('location')},
                {'name': 'SUBSCRIPTION_ID', 'value': az_opts.get('subscriptionId')},
                {'name': 'STOP_POLICY_ID', 'value': stop_policy_definition.id},
                {'name': 'START_POLICY_ID', 'value': start_policy_definition.id},
            ],
            http20_enabled=True,
            python_version='3.10',
        ),
        opts=pulumi.ResourceOptions(depends_on=policies),
    )

    pulumi.export('stop_policy_id', stop_policy_definition.id)
    pulumi.export('start_policy_id', start_policy_definition.id)
    pulumi.export(
        'endpoint', app.default_host_name.apply(lambda url: f'https://{url}/')
    )


if __name__ == '__main__':
    main()
