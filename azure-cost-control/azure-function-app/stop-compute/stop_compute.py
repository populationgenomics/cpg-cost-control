"""
Azure function code to enable/disable compute in a provided Azure resource group
"""

import os
import logging
from enum import Enum

import azure.functions as func
from azure.identity import AzureCliCredential
from azure.mgmt.policyinsights._policy_insights_client import PolicyInsightsClient
from azure.mgmt.policyinsights.models import QueryOptions
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.resource.policy import PolicyClient
from azure.mgmt.resource.policy.models import (
    Identity,
    PolicyAssignment,
    # PolicyAssignmentUpdate,
    # UserAssignedIdentitiesValue,
)

# Constants
LOCATION = os.getenv('LOCATION')
SUBSCRIPTION_ID = os.getenv('SUBSCRIPTION_ID')
STOP_POLICY_ID = os.getenv('STOP_POLICY_ID')
START_POLICY_ID = os.getenv('START_POLICY_ID')

# Clients
CREDS = AzureCliCredential()
POLICY_CLIENT = PolicyClient(CREDS, SUBSCRIPTION_ID)
POLICY_INSIGHTS_CLIENT = PolicyInsightsClient(CREDS, SUBSCRIPTION_ID)
RESOURCE_CLIENT = ResourceManagementClient(CREDS, SUBSCRIPTION_ID)

# Classes


class Policy(Enum):
    """Class for the start/stop policy enum"""

    START = START_POLICY_ID
    STOP = STOP_POLICY_ID

    def policy_name(self):
        """Returns the policy name"""
        return f'{self.name.lower()} compute policy'.capitalize()

    @staticmethod
    def list():
        """Returns a list of all Policies"""
        return list(Policy)

    @staticmethod
    def names():
        """Retuns a list of all policy names"""
        return list(map(lambda r: r.name.lower(), Policy))

    @staticmethod
    def get(name: str):
        """Returns the policy given the name"""
        return Policy({r.name: r.value for r in Policy}.get(name.upper()))


# MAIN CODE #


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Handle http request"""
    logging.info('Python HTTP Trigger function processed a request.')
    logging.info(f'Running with creds {CREDS}')

    if req.method == 'GET':
        msg = (
            'Please submit a post request with response body in the form '
            '{"resource_group_name": <rg_name>, "request": <stop|start>}'
        )
        return func.HttpResponse(msg, status_code=200)

    request_body = req.get_json()

    logging.info(f'Request body is: {request_body}')

    resource_group_name = request_body.get('resource_group_name')
    policy = request_body.get('request', '')

    # Input validation
    if not resource_group_name:
        return func.HttpResponse(
            'Please specify a resource group by name in the request body',
            status_code=400,
        )

    if policy not in Policy.names():
        return func.HttpResponse(
            f'Policy type must be one of ({Policy.names()}). Found "{policy}".',
            status_code=400,
        )
    policy = Policy.get(policy)

    resource_group = get_resource_group(resource_group_name)
    if not resource_group:
        return func.HttpResponse(
            f'Resource group "{resource_group_name}" was not found', status_code=404
        )

    logging.info(f'Inputs are valid! Running {policy} on group {resource_group_name}')

    # Run the request!
    return apply_policy(resource_group, policy)


def apply_policy(resource_group: ResourceGroup, policy: Policy) -> func.HttpResponse:
    """Apply the correct policies and return a HttpResponse"""
    update_policy_assignments(resource_group, policy)
    noncompliant = find_noncompliant_resource(resource_group, policy)
    return func.HttpResponse(
        (
            f'Assigning of policy "{policy.policy_name()}" on '
            f'"{resource_group.name}" complete. '
            f'Non-compliant resources: {list(noncompliant)}'
        )
    )


def get_resource_group(resource_group_name: str) -> ResourceGroup | None:
    """Gets the resource group from the given name"""
    return RESOURCE_CLIENT.resource_groups.get(resource_group_name)


def remove_policies(resource_group: ResourceGroup):
    """Removes exiting compute policies on this resource"""
    for policy in Policy.list():
        logging.info(f'Removing policy {policy.policy_name()}')
        POLICY_CLIENT.policy_assignments.delete(resource_group.id, policy.policy_name())


def update_policy_assignments(resource_group: ResourceGroup, policy: Policy):
    """Assigns a policy to a given resource group"""

    # Setup the details of the policy assignment
    policy_assignment_identity = Identity(type='SystemAssigned')
    policy_assignment_details = PolicyAssignment(
        display_name=policy.policy_name(),
        description=f'{policy.policy_name()} in {resource_group.name}',
        policy_definition_id=policy.value,
        identity=policy_assignment_identity,
        location=LOCATION,
    )

    # Remove all existing policies 'compute' policies
    remove_policies(resource_group)

    # Create new policy assignment
    policy_assignment = POLICY_CLIENT.policy_assignments.create(
        resource_group.id,
        policy.policy_name(),
        policy_assignment_details,
    )

    return policy_assignment


def find_noncompliant_resource(resource_group: ResourceGroup, policy: Policy):
    """Finds resources in the resource group that don't comply with the policy"""

    query_filter = (
        f"IsCompliant eq false and PolicyAssignmentId eq '{policy.policy_name()}'"
    )
    query_opts = QueryOptions(
        filter=query_filter,
        apply='groupby((ResourceId))',
    )
    results = (
        POLICY_INSIGHTS_CLIENT.policy_states.list_query_results_for_resource_group(
            policy_states_resource='latest',
            subscription_id=SUBSCRIPTION_ID,
            resource_group_name=resource_group.name,
            query_options=query_opts,
        )
    )

    return results
