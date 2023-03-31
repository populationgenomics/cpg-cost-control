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

from datetime import datetime, timedelta

import os
import pulumi
import pulumi_docker as docker
import pulumi_azure_native as az

# import helpers

FUNCTION_APP_CODE = os.path.abspath('./azure-function-app')
RETENTION_DAYS = 30
BUDGET_START_DATE = datetime.today().replace(day=1)
BUDGET_END_DATE = BUDGET_START_DATE + timedelta(weeks=52)
BUDGET_TIME_GRAIN = 'Monthly'


def main():
    """
    Get values from Pulumi config to use as
    environment variables in our Cloud Function.
    """
    opts = pulumi.Config(name='opts')
    az_opts = pulumi.Config(name='azure-native')

    # Create billing resource_group
    billing_rg_name = opts.get('billing_group')
    resource_group = az.resources.ResourceGroup(
        f'{billing_rg_name}-resource-group',
        location=az_opts.get('location'),
        resource_group_name=billing_rg_name,
    )

    give_service_principal_roles(resource_group, opts, az_opts)
    app = create_billing_stop(resource_group, opts, az_opts)

    set_resource_group_budget('billing-test', 0.000001, app, az_opts)


def get_url(app: az.web.WebApp, function: str = 'stop-compute'):
    """Returns the url to the app function"""
    return app.default_host_name.apply(lambda url: f'https://{url}/api/{function}')


def sub_scope(sid: str):
    """Return subscription scope string"""
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


def give_service_principal_roles(
    resource_group: az.resources.ResourceGroup, opts: dict, az_opts: dict
):
    """Create a service principal"""
    name = opts.get('name')
    sub_id = az_opts.get('subscriptionId')

    # Get application used as pulumi service principal for Azure
    application_id = az_opts.get('clientId')

    # Assign permissions
    # Reader and policy contributer on subscription
    az.authorization.RoleAssignment(
        f'app-{name}-subscription-reader',
        principal_type='ServicePrincipal',
        principal_id=application_id,
        role_definition_id=get_role(sub_id, 'reader'),
        scope=f'/subscriptions/{sub_id}',
    )
    az.authorization.RoleAssignment(
        f'app-{name}-subscription-policy-contributer',
        principal_type='ServicePrincipal',
        principal_id=application_id,
        role_definition_id=get_role(sub_id, 'policy_contributer'),
        scope=f'/subscriptions/{sub_id}',
    )

    # Owner on billing resource group
    az.authorization.RoleAssignment(
        f'app-{name}-billing-resource-group-owner',
        principal_type='ServicePrincipal',
        principal_id=application_id,
        role_definition_id=get_role(sub_id, 'owner'),
        scope=resource_group.id,
    )


def build_and_push_image(opts: dict, az_opts: dict):
    """Builds and pushes the docker to the container regsitry"""
    registry_info = docker.ImageRegistry(
        server=opts.get('container_registry'),
        username=az_opts.get('clientId'),
        password=az_opts.get('clientSecret'),
    )

    image = docker.Image(
        'image-azure-cost-control',
        build=docker.DockerBuild(
            context='azure-function-app',
        ),
        image_name=opts.get('name'),
        registry=registry_info,
    )

    return image.image_name


def create_billing_stop(
    resource_group: az.resources.ResourceGroup, opts: dict, az_opts: dict
):
    """
    Creates the stop compute policy and the function that given a resource group
    will stop all compute on that resource group
    """

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
    rg_name = opts.get('billing_group')
    storage_account = az.storage.StorageAccount(
        f'{rg_name}-storage-account',
        account_name=f'{rg_name}storage817497',
        resource_group_name=resource_group.name,
        sku=az.storage.SkuArgs(name='Standard_LRS'),
        kind='StorageV2',
        opts=pulumi.ResourceOptions(depends_on=[resource_group]),
    )

    code_container = az.storage.BlobContainer(
        f'container-{func_name}',
        resource_group_name=resource_group.name,
        account_name=storage_account.name,
    )

    # code_blob = az.storage.Blob(
    #     f'zip-{func_name}',
    #     resource_group_name=resource_group.name,
    #     account_name=storage_account.name,
    #     container_name=code_container.name,
    #     source=pulumi.asset.FileArchive(FUNCTION_APP_CODE),
    # )

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

    deps = [storage_account, code_container, plan, insights]

    insights_connection = insights.instrumentation_key.apply(
        lambda key: f'InstrumentationKey={key}'
    )
    # sa_connection_url = helpers.get_connection_string(resource_group, storage_account)
    # code_blob_url = helpers.signed_blob_read_url(
    #     code_blob, code_container, storage_account, resource_group
    # )

    # Create the Dockerfile
    docker_image = build_and_push_image(opts, az_opts)

    # Create the WebApp
    # Passes through the policy ids, subscription and location of the billing rg
    settings = {
        # 'AzureWebJobsStorage': sa_connection_url,
        # 'WEBSITE_RUN_FROM_PACKAGE': code_blob_url,
        'AzureWebJobsFeatureFlags': 'EnableProxies',
        'APPINSIGHTS_INSTRUMENTATIONKEY': insights.instrumentation_key,
        'APPLICATIONINSIGHTS_CONNECTION_STRING': insights_connection,
        'ApplicationInsightsAgent_EXTENSION_VERSION': '~2',
        'FUNCTIONS_EXTENSION_VERSION': '~4',
        'FUNCTIONS_WORKER_RUNTIME': 'python',
        'SCM_DO_BUILD_DURING_DEPLOYMENT': True,
        'LOCATION': az_opts.get('location'),
        'SUBSCRIPTION_ID': az_opts.get('subscriptionId'),
        'STOP_POLICY_ID': stop_policy_definition.id,
        'START_POLICY_ID': start_policy_definition.id,
    }

    app = az.web.WebApp(
        func_name,
        resource_group_name=resource_group.name,
        server_farm_id=plan.id,
        kind='functionapp',
        site_config=az.web.SiteConfigArgs(
            app_settings=[{'name': k, 'value': v} for k, v in settings.items()],
            http20_enabled=True,
            python_version='3.9',
            linux_fx_version=f'DOCKER|{docker_image}',
        ),
        opts=pulumi.ResourceOptions(depends_on=[*deps, *policies]),
    )
    url = get_url(app)

    pulumi.export('stop_policy_id', stop_policy_definition.id)
    pulumi.export('start_policy_id', start_policy_definition.id)
    pulumi.export('function_app', app.name)
    pulumi.export('url', url)

    return app


def set_resource_group_budget(
    resource_group_name: str,
    amount: float,
    stop_compute_app: az.web.WebApp,
    az_opts: dict,
):
    """Creates the budget, and the alerting system to trigger stop-compute"""

    # Create the resource group
    resource_group = az.resources.ResourceGroup(
        f'{resource_group_name}-resource-group',
        location=az_opts.get('location'),
        resource_group_name=resource_group_name,
    )

    # Create action group for this resource group
    # australiaeast not valid
    # The provided location 'australiaeast' is not available for resource type
    # 'microsoft.insights/actiongroups'.
    # List of available regions for the resource type is
    # 'global,swedencentral,germanywestcentral,northcentralus,southcentralus'."
    action_group = az.insights.ActionGroup(
        f'{resource_group_name}-action-group',
        action_group_name=f'{resource_group_name}-action-group',
        group_short_name=f'budget-ctrl',
        resource_group_name=resource_group.name,
        azure_function_receivers=[
            {
                'functionAppResourceId': stop_compute_app.id,
                'functionName': stop_compute_app.name,
                'httpTriggerUrl': get_url(stop_compute_app),
                'name': stop_compute_app.name,
                'useCommonAlertSchema': True,
            }
        ],
        email_receivers=[
            {
                'emailAddress': 'sabrina.yan@populationgenomics.org.au',
                'name': 'Sabrina Yan\'s email',
                'useCommonAlertSchema': False,
            }
        ],
        enabled=True,
        location='global',
    )

    # Set the budget
    # Enable the notification of the action group
    az.consumption.Budget(
        f'{resource_group_name}-budget',
        budget_name=f'{resource_group_name}-budget',
        category='Cost',
        time_grain=BUDGET_TIME_GRAIN,
        amount=amount,
        notifications={
            'Actual_GreaterThanOrEqualTo_100_Percent': az.consumption.NotificationArgs(
                contact_emails=[],
                contact_groups=[action_group.id],
                enabled=True,
                operator='GreaterThanOrEqualTo',
                threshold=100,
            )
        },
        scope=resource_group.id,
        time_period=az.consumption.BudgetTimePeriodArgs(
            start_date=str(BUDGET_START_DATE),
        ),
        opts=pulumi.ResourceOptions(
            delete_before_replace=True, replace_on_changes=['time_period']
        ),
    )


if __name__ == '__main__':
    main()
