"""
A cloud function that synchronises HAIL billing data to BigQuery

TO:     billing-admin-290403.billing_aggregate.aggregate


Notes:

- The service.id should be "hail"


Tasks:

- Only want to transfer data from the projects in the server-config
- Need to build an endpoint in Hail to service this metadata
- Transform into new generic format
- Can't duplicate rows, so determine some ID:
    - And maybe only sync "settled" jobs within datetimes (ie: finished between START + END of previous time period)
"""

SERVICE_ID = 'hail'


def main(request):
    pass
