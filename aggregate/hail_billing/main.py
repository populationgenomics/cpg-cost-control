"""
A cloud function that synchronises HAIL billing data to BigQuery

TO:     billing-admin-290403.billing_aggregate.aggregate


Notes:

- The service.id should be "hail"


Tasks:

- Only want to transfer data from the projects in the server-config
- Need to build an endpoint in Hail to service this metadata
    - Take in a project and a date range in the endpoint
    - Result should be of a form like this
        [{
            batch_id: 0,
            jobs: [{"job_id": 0, "mseccpu": 100, "msecgb": 100}]
        }]

    - Should be able to filter on the following properties:
        - search by attribute
        - search by project
- Transform into new generic format
    - One batch per row

- Can't duplicate rows, so determine some ID:
    - And maybe only sync "settled" jobs within datetimes (ie: finished between START + END of previous time period)
"""

SERVICE_ID = 'hail'


def main(request):
    pass
