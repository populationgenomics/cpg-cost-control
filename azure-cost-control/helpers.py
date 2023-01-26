"""
Helper functions required for pulumi deployment
"""

from datetime import datetime, timedelta

import pulumi
import pulumi_azure_native as az


def get_connection_string(
    resource_group: az.resources.ResourceGroup, account: az.storage.StorageAccount
):
    """Returns the storage account's connection string"""
    sa_keys = az.storage.list_storage_account_keys_output(
        account.name, resource_group_name=resource_group.name
    )
    return pulumi.Output.all(account.name, sa_keys).apply(
        lambda x: (
            f'DefaultEndpointsProtocol=https;AccountName={x};AccountKey={sa_keys[0]}'
        )
    )


def signed_blob_read_url(
    blob: az.storage.Blob,
    container: az.storage.BlobContainer,
    account: az.storage.StorageAccount,
    resource_group: az.resources.ResourceGroup,
) -> pulumi.Output:
    """Constructe a signed blob url with token"""

    start_time = datetime.today() - timedelta(weeks=1 * 52)
    end_time = datetime.today() + timedelta(weeks=5 * 52)

    token = az.storage.list_storage_account_service_sas_output(
        account_name=account.name,
        protocols=az.storage.HttpProtocol.HTTPS,
        shared_access_expiry_time=end_time.strftime('%Y-%m-%d'),
        shared_access_start_time=start_time.strftime('%Y-%m-%d'),
        resource_group_name=resource_group.name,
        resource=az.storage.SignedResource.C,
        permissions=az.storage.Permissions.R,
        canonicalized_resource=pulumi.Output.all(account.name, container.name).apply(
            lambda args: f'/blob/{args[0]}/{args[1]}'
        ),
        content_type='application/json',
        cache_control='max-age=5',
        content_disposition='inline',
        content_encoding='deflate',
    )

    return pulumi.Output.all(account.name, container.name, blob.name, token).apply(
        lambda args: (
            f'https://{args[0]}.blob.core.windows.net/{args[1]}/{args[2]}?{args[3]}'
        )
    )
