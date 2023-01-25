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
    # az_opts = pulumi.Config(name='azure-native')
    create_billing_stop(opts)


def create_billing_stop(opts: dict):  # , az_opts: dict):
    """
    Creates the stop compute policy and the function that given a resource group
    will stop all compute on that resource group
    """

    resource_group = az.resources.get_resource_group(opts.get('billing_group'))
    # subscription_id = az_opts.get('subscriptionId')
    # sub_scope = f'/subscriptions/{subscription_id}'

    # # Create a policy definition
    # policy_definition = az.authorization.PolicyDefinition(
    #     'compute-stop-policy',
    #     policy_rule={
    #         'if': {
    #             'allOf': [
    #                 {'field': 'type', 'equals': 'Microsoft.Compute/virtualMachines'},
    #                 # {
    #                 #     'field': 'Microsoft.Compute/virtualMachines[*].powerState',
    #                 #     'contains': 'running',
    #                 # },
    #             ]
    #         },
    #         'then': {'effect': 'deny'},
    #     },
    #     mode='all',
    # )

    # # Create a policy assignment to the resource group
    # policy_assignment = az.authorization.PolicyAssignment(
    #     'compute-stop-assignment',
    #     policy_definition_id=policy_definition.id,
    #     scope=sub_scope,
    # )

    # Create an Azure Function App
    func_name = 'stop-compute'

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

    plan = az.web.AppServicePlan(
        f'plan-{func_name}',
        resource_group_name=resource_group.name,
        kind='Linux',
        sku=az.web.SkuDescriptionArgs(name='S1', tier='Standard'),
        reserved=True,
    )

    code_blob_url = helpers.signed_blob_read_url(
        code_blob, code_container, storage_account, resource_group
    )

    app = az.web.WebApp(
        func_name,
        resource_group_name=resource_group.name,
        server_farm_id=plan.id,
        kind='functionapp',
        site_config=az.web.SiteConfigArgs(
            app_settings=[
                {'name': 'runtime', 'value': 'python'},
                {'name': 'FUNCTIONS_WORKER_RUNTIME', 'value': 'python'},
                {'name': 'WEBSITE_RUN_FROM_PACKAGE', 'value': code_blob_url},
                {'name': 'FUNCTIONS_EXTENSION_VERSION', 'value': '~3'},
            ],
            http20_enabled=True,
            python_version='3.10',
        ),
    )

    # pulumi.export('policy_definition', policy_definition)
    # pulumi.export('policy_assignment', policy_assignment)
    pulumi.export('endpoint', app.default_host_name)


if __name__ == '__main__':
    main()
