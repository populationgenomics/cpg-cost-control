"""
This cloud function runs WEEKLY, and distributes the cost of SEQR on the sample size within SEQR.

- It first pulls the cost of the seqr project (relevant components within it):
    - Elasticsearch, instance cost
    - Cost of loading data into seqr might be difficult:
        - At the moment this in dataproc, so covered by previous result
        - Soon to move to Hail Query, which means we have to query hail to get the "cost of loading"
- It determines the relative heuristic (of all projects loaded into seqr):
    - EG: "relative size of GVCFs by project", eg:
        - $DATASET has 12GB of GVCFs of a total 86GB of all seqr GVCFs, 
        - therefore it's relative heuristic is 12/86 = 0.1395, and share of seqr cost is 13.95%

- Insert rows in aggregate cost table for each of these costs:
    - The service.id should be "seqr" (or something similar)
    - Maybe the label could include the relative heuristic
"""

SERVICE_ID = 'seqr'


def main(request):
    pass
