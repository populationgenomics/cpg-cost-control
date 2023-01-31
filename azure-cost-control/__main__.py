"""
Python Pulumi program for monitoring billing on Azure.

Requires:
#!/usr/bin/env python
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

import os
import pulumi
import pulumi_azuread as azuread
import pulumi_azure_native as az

import helpers

FUNCTION_APP_CODE = os.path.abspath('./azure-function-app')
RETENTION_DAYS = 30


def main():
    """
    Get values from Pulumi config to use as
    environment variables in our Cloud Function.
    """
    opts = pulumi.Config(name='opts')
    az_opts = pulumi.Config(name='azure-native')
    # create_service_principal(opts, az_opts)
    create_billing_stop(opts, az_opts)


def sub_scope(sid: str):
    """Reutrn subscription scope string"""
    return f'/subscriptions/{sid}'


def role_scope(sub_id: str, role_id: str):
    """Return role scope string"""
    role_def_str = 'providers/Microsoft.Authorization/roleDefinitions'
    return f'{sub_scope(sub_id)}/{role_def_str}/{role_id}'


def get_role(sub_id: str, role: str):
    """Return specific role scope string"""
    role_id = {
        'reader': 'acdd72a7-3385-48ef-bd42-f606fba81ae7',
        'policy_contributer': '36243c78-bf99-498c-9df9-86d9f8d28608',
        'owner': '8e3af657-a8ff-443c-a75c-2fe8c4bcb635',
    }.get(role)

    return role_scope(sub_id, role_id)


def create_service_principal(opts: dict, az_opts: dict):
    """Create a service principal"""
    name = opts.get('name')
    sub_id = az_opts.get('subscriptionId')
    resource_group = az.resources.get_resource_group(opts.get('billing_group'))

    # Create application
    application = azuread.Application(f'application-{name}')

    # Assign permissions
    # Reader and policy contributer on subscription
    az.authorization.RoleAssignment(
        f'app-{name}-subscription-reader',
        principal_type='ServicePrincipal',
        principal_id=application.id,
        role_definition_id=get_role(sub_id, 'reader'),
        scope=f'/subscriptions/{sub_id}',
    )
    az.authorization.RoleAssignment(
        f'app-{name}-subscription-policy-contributer',
        principal_type='ServicePrincipal',
        principal_id=application.id,
        role_definition_id=get_role(sub_id, 'policy_contributer'),
        scope=f'/subscriptions/{sub_id}',
    )

    # Owner on billing resource group
    az.authorization.RoleAssignment(
        f'app-{name}-{resource_group.name}-owner',
        principal_type='ServicePrincipal',
        principal_id=application.id,
        role_definition_id=get_role(sub_id, 'owner'),
        scope=resource_group.id,
    )

    return application


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
        source=pulumi.asset.FileArchive(FUNCTION_APP_CODE),
    )

    # Create the app service host plan as well as sign the blob url for the WebApp
    plan = az.web.AppServicePlan(
        f'plan-{func_name}',
        resource_group_name=resource_group.name,
        kind='Linux',
        sku=az.web.SkuDescriptionArgs(name='Y1', tier='Dynamic'),
        reserved=True,
    )

    # Insights connection
    insights = az.insights.Component(
        f'insights-{func_name}',
        application_type='web',
        kind='web',
        location=az_opts.get('LOCATION'),
        resource_group_name=resource_group.name,
    )

    insights_connection = insights.instrumentation_key.apply(
        lambda key: f'InstrumentKey={key}'
    )
    sa_connection_url = helpers.get_connection_string(resource_group, storage_account)
    code_blob_url = helpers.signed_blob_read_url(
        code_blob, code_container, storage_account, resource_group
    )
    pulumi.export('sa_connection_string', sa_connection_url)

    # Create the WebApp
    # Passes through the policy ids, subscription and location of the billing rg
    app = az.web.WebApp(
        func_name,
        resource_group_name=resource_group.name,
        server_farm_id=plan.id,
        kind='functionapp',
        site_config=az.web.SiteConfigArgs(
            app_settings=[
                {'name': 'AzureWebJobsStorage', 'value': sa_connection_url},
                {'name': 'AzureWebJobsFeatureFlags', 'value': 'EnableProxies'},
                {
                    'name': 'APPINSIGHTS_INSTRUMENTATIONKEY',
                    'value': insights.instrumentation_key,
                },
                {
                    'name': 'APPLICATIONINSIGHTS_CONNECTION_STRING',
                    'value': insights_connection,
                },
                {'name': 'ApplicationInsightsAgent_EXTENSION_VERSION', 'value': '~2'},
                {'name': 'FUNCTIONS_EXTENSION_VERSION', 'value': '~4'},
                {'name': 'FUNCTIONS_WORKER_RUNTIME', 'value': 'python'},
                {'name': 'WEBSITE_RUN_FROM_PACKAGE', 'value': code_blob_url},
                {'name': 'LOCATION', 'value': az_opts.get('location')},
                {'name': 'SUBSCRIPTION_ID', 'value': az_opts.get('subscriptionId')},
                {'name': 'STOP_POLICY_ID', 'value': stop_policy_definition.id},
                {'name': 'START_POLICY_ID', 'value': start_policy_definition.id},
            ],
            http20_enabled=True,
            python_version='3.9',
            linux_fx_version='Python|3.9',
        ),
        opts=pulumi.ResourceOptions(depends_on=policies),
    )

    pulumi.export('stop_policy_id', stop_policy_definition.id)
    pulumi.export('start_policy_id', start_policy_definition.id)
    pulumi.export('function_app', app.name)
    pulumi.export(
        'endpoint', app.default_host_name.apply(lambda url: f'https://{url}/')
    )


if __name__ == '__main__':
    main()
