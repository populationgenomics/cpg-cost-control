# GCP cost control

This repository contains a Cloud Function that handles GCP billing budget
notifications, inspired by the
[official documentation](https://cloud.google.com/billing/docs/how-to/notify#cap_disable_billing_to_stop_usage),
[an example](https://torbjornzetterlund.com/disable-billing-for-google-cloud-projet-when-billing-exceeds-the-budget-limit/)
using the Secret Manager for storing the Slack bot token, and
[another example](https://medium.com/faun/capping-costs-on-gcp-for-many-projects-with-a-budget-for-many-months-without-paying-a-penny-dc461525c2d2)
describing how to share the same Cloud Function instance across multiple
projects.

Whenever the budget threshold for a project is reached, the Cloud Function
disables billing for the project and posts a message to a Slack channel. The
Cloud Function only needs to be installed once and it will handle Pub/Sub budget
notifications for _all_ projects.

## Set up the Cloud Function

1. Create a GCP project named `billing-admin`.
1. Enable the
   [Cloud Billing API](https://console.developers.google.com/apis/library/cloudbilling.googleapis.com)
   for the project.
1. Create a Pub/Sub topic called `budget-notifications`.
1. Add a service account for running the Cloud Function and grant it _Project
   Billing Manager_ and _Browser_ roles at the **organization level**, to allow
   [checking the current billing information](https://cloud.google.com/billing/v1/how-tos/access-control)
   and
   [disabling billing](https://cloud.google.com/billing/docs/how-to/modify-project#disable_billing_for_a_project)
   for all projects.
1. Create a Slack app called `gcp-cost-control` with a `chat:write` scope bot
   token and install the app on your Slack workspace.
1. Invite the bot to the channel that you want to receive messages on:
   `/invite @gcp-cost-control`
1. Back in the `billing-admin` GCP project, store the bot user OAuth access
   token in the Secret Manager as a secret using the name
   `slack-gcp-cost-control`.
1. Grant the previously created service account access to the secret by granting
   the _Secret Manager Secret Accessor_ role at the project level.
1. Deploy the Cloud Function, replacing `$BILLING_ADMIN_PROJECT`, `$REGION`,
   `$SERVICE_ACCOUNT`, and `$SLACK_CHANNEL` accordingly:
   ```
   gcloud config set project $BILLING_ADMIN_PROJECT
   gcloud functions deploy gcp_cost_control --runtime python37 \
     --region=$REGION \
     --trigger-topic budget-notifications \
     --service-account $SERVICE_ACCOUNT \
     --set-env-vars SLACK_CHANNEL=$SLACK_CHANNEL
   ```

## Add billing budgets

Create a separate budget for each project that you'd like to cap billing for.

- It's important to **set the budget name to the project ID** (not the project
  name). That's how the Cloud Function can determine which project a
  notification corresponds to.
- Connect the budget to the shared Pub/Sub `budget-notifications` topic of the
  `billing-admin` project.

## Testing

To test the full setup, you can publish the following Pub/Sub message to the
`budget-notifications` topic in the `billing-admin` project, replacing
\$TEST_PROJECT accordingly. However, make sure that it's not a problem to shut
down the whole project when billing gets disabled temporarily. If there are any
issues, check the logs for the `gcp-cost-control` Cloud Function.

```
{
    "budgetDisplayName": "$TEST_PROJECT",
    "alertThresholdExceeded": 1.0,
    "costAmount": 110.01,
    "costIntervalStart": "2020-01-01T00:00:00Z",
    "budgetAmount": 100.00,
    "budgetAmountType": "SPECIFIED_AMOUNT",
    "currencyCode": "USD"
}
```
