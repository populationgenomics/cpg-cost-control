# Billing Cloud Functions

## Setup and running locally

To run we recommend a local.py python script in the aggregate/ folder like so:

```python
from billing_functions import gcp, hail, seqr

args = {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
gcp(args, None)
hail(args, None)
seqr(args, None)
```

Set your DEBUG environment variable to true and the other required tokens

```shell
export DEBUG=1
export HAIL_TOKEN=<aggregate_billing_hail_token>
unset SM_ENVIRONMENT
```

Then simply run the following:

```shell
pip install -r requirements.txt -r requirements-dev.txt
python local.py
```
