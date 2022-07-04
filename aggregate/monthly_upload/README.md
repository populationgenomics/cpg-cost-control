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

1. Select the GCP billing project.
1. Add a service account for _running_ the Cloud Function.
1. In the Secret Manager, create a secret `billing-airtable-monthly-upload-apikeys`,
   with a JSON config as follows, where `apiKey` corresponds to the Airtable account
   that should be used for the updates. The corresponding tables must have
   format matching [2022 Example](https://airtable.com/app62isJvsSz0ziWT/tblqQkcgXt6fdsgj5/viwtxRpnYQNwXPVUw?blocks=hide)

   ```json
   {
     "2022": {
       "baseKey": "baseasdf42",
       "tableName": "2022 Invoice By Topic",
       "apiKey": "keyadjlas323"
     },
     "2023": {
       "baseKey": "baseasdlj3",
       "tableName": "2023 Invoice By Topic",
       "apiKey": "keyajksdl231"
     }
   }
   ```

1. Grant the Cloud Function service account the
   _Secret Manager Secret Accessor_ role for the secret.

## Invoking the Cloud Function

Download a JSON key for the invoker service account. Then run:

```shell
gcloud auth activate-service-account --key-file=invoker-service-account.json

TOKEN=$(gcloud auth print-identity-token)

curl -X PUT https://$REGION-$PROJECT.cloudfunctions.net/update_sample_status \
    -H "Authorization: bearer $TOKEN" \
    -H "Content-Type:application/json" \
    -d '{"year": "2022", "month": "05"}'
```
