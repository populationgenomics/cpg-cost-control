"""
Azure function code to enable/disable compute in a provided Azure resource group
"""

import logging

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Handle http request"""
    logging.info('Python HTTP Trigger function processed a request.')

    request_body = req.get_json()
    resource_group_name = request_body.get('resource_group_name')
    request_type = request_body.get('request')

    if request_type == 'stop':
        return stop(resource_group_name)

    return start(resource_group_name)


def stop(resource_group_name: str):
    """Stop running compute in that resource group"""
    func.HttpResponse(
        f'Stop running compute resources in resource group {resource_group_name}...'
    )


def start(resource_group_name: str):
    """Enable compute in that resource group"""
    func.HttpResponse(
        f'Allowing new compute resources in resource group {resource_group_name}...'
    )
