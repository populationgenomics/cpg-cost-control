# Billing Cloud Run Jobs

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
cat > .env <<EOF
DEBUG=1
HAIL_TOKEN=<aggregate_billing_hail_token>
SM_ENVIRONMENT=
EOF
source .env
```

Then simply run the following:

```shell
pip install -r requirements.txt -r requirements-dev.txt
python local.py
```


## Cloud Run Job

To run the cloud function on gcp simply use the following command:

```shell
 gcloud functions call seqr-billing-cloudrun-9ecc5d0 \
    --data '{"start": "2022-12-14 00:00", "end": "2022-12-16 09:00"}'
```

Alternatively, trigger the topic with a message using the following:

```shell
  gcloud pubsub topics publish aggregate-billing-topic-9c00d84 --message "hello"
```
