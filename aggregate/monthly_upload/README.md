# Update sample status

This repository contains a Cloud Function that lets you update the processing
status for a genomic sample in Airtable.

It's just a very lightweight wrapper around an Airtable API call. The motivation
for this indirection is Airtable's coarse permission model: it's not possible to
only share a subset of columns of a base with collaborators. While automation
should be able to update metadata fields like status, other fields should not
even be exposed for reading.

Consequently this Cloud Function uses its own Airtable credentials and only
permits status updates. Permissions to call this Cloud Function are granted
through a service account key.

## Set up the Cloud Function

1. Create a GCP project named `sample-metadata`.
1. Add a service account for _running_ the Cloud Function.
1. In the Secret Manager, create a secret `update-sample-status-config`, with
   a JSON config as follows, where `apiKey` corresponds to the Airtable account
   that should be used for the updates. The corresponding tables must have
   "Sample ID" and "Status" columns.

   ```json
   {
     "project1": {
       "baseKey": "baseasdf42",
       "tableName": "Sample table",
       "apiKey": "keyadjlas323"
     },
     "project2": {
       "baseKey": "baseasdlj3",
       "tableName": "Another table",
       "apiKey": "keyajksdl231"
     }
   }
   ```

1. Grant the Cloud Function service account the
   _Secret Manager Secret Accessor_ role for the secret.
1. Deploy the Cloud Function, replacing `$PROJECT`, `$REGION`, and
   `$SERVICE_ACCOUNT` accordingly. Do _not_ allow unauthenticated
   invocations.

   ```shell
   gcloud config set project $PROJECT
   gcloud functions deploy update_sample_status --runtime python37 \
     --region=$REGION --trigger-http --service-account $SERVICE_ACCOUNT
   ```

1. Create a service account for _invoking_ the Cloud Function.
1. Under the newly created Cloud Function, grant the invoker service account
   the _Cloud Functions Invoker_ role.

## Invoking the Cloud Function

Download a JSON key for the invoker service account. Then run:

```shell
gcloud auth activate-service-account --key-file=invoker-service-account.json

TOKEN=$(gcloud auth print-identity-token)

curl -X PUT https://$REGION-$PROJECT.cloudfunctions.net/update_sample_status \
    -H "Authorization: bearer $TOKEN" \
    -H "Content-Type:application/json" \
    -d '{"project": "project1", "sample": "HG00123", "status": "Sequencing started"}'
```
