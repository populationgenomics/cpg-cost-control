"""
Cloud function that runs {TBA} that synchronises a portion of data from:

FROM:   billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343 
TO:     billing-admin-290403.billing_aggregate.aggregate

Tasks:

- Needs to convert {billing_project_id} into DATSET
- Only want to transfer data from the projects in the server-config
- Can't duplicate rows (so maybe just grab only settled data within START + END of previous time period)
- Service ID should be faithfully handed over
"""


def main(request):

    start_period = None
    finish_period = None
    pass


def get_dataset_to_gcp_map() -> dict:
    """Get the server-config from the secret manager"""
    m = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))

    return {k: v['projectId'] for k, v in m.items()}
