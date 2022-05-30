[![Billing Aggregation Linter](https://github.com/populationgenomics/gcp-cost-control/actions/workflows/lint.yaml/badge.svg)](https://github.com/populationgenomics/gcp-cost-control/actions/workflows/lint.yaml)
[![Billing Aggregation Deploy](https://github.com/populationgenomics/gcp-cost-control/actions/workflows/deploy-aggregate.yaml/badge.svg)](https://github.com/populationgenomics/gcp-cost-control/actions/workflows/deploy-aggregate.yaml)

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

1. Create a GCP project named for billing administration, e.g. called `billing-admin-290403` below.
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
1. Back in the `billing-admin-290403` GCP project, store the bot user OAuth access
   token in the Secret Manager as a secret using the name
   `slack-gcp-cost-control`.
1. Grant the previously created service account access to the secret by granting
   the _Secret Manager Secret Accessor_ role at the project level.
1. Deploy the Cloud Function, replacing `$BILLING_ADMIN_PROJECT`, `$REGION`,
   `$SERVICE_ACCOUNT`, and `$SLACK_CHANNEL` accordingly:

   ```bash
   cd gcp_cost_control
   gcloud config set project $BILLING_ADMIN_PROJECT
   gcloud functions deploy gcp_cost_control --runtime python37 \
     --region=$REGION \
     --trigger-topic budget-notifications \
     --service-account $SERVICE_ACCOUNT \
     --set-env-vars SLACK_CHANNEL=$SLACK_CHANNEL
   ```

## Add billing budgets

Create a separate budget for each project that you'd like to cap billing for:

1. Go to "Billing".
1. Go to "Budgets & Alerts".
1. Click "Create Budget".
1. Set the name of the budget identical to the _project ID_ (not the project name!) of your project.
1. Select your new project from the drop-down "Projects".
1. In the "Amount" section, set a non-zero target amount.
1. In the "Actions" section, select "Connect a Pub/Sub topic to this budget". In the dropdown menu, select the topic `projects/billing-admin-290403/topics/budget-notifications`. If you can't see the topic, click on "Switch project" in that dropdown menu and select `billing-admin-290403`, and you should be able to see the topic.
1. Click Finish.

## Testing

To test the full setup, you can publish the following Pub/Sub message to the
`budget-notifications` topic in the `billing-admin-290403` project, replacing
`$TEST_PROJECT` accordingly. However, make sure that it's not a problem to shut
down the whole project when billing gets disabled temporarily. If there are any
issues, check the logs for the `gcp-cost-control` Cloud Function.

```json
{
  "budgetDisplayName": "$TEST_PROJECT",
  "alertThresholdExceeded": 1.0,
  "costAmount": 110.01,
  "costIntervalStart": "2020-01-01T00:00:00Z",
  "budgetAmount": 100.0,
  "budgetAmountType": "SPECIFIED_AMOUNT",
  "currencyCode": "USD"
}
```

## Daily cost reports

The [gcp_cost_report](gcp_cost_report/main.py) Cloud Function can be used to get a
daily per-project cost report in Slack.

1. Set up [Cloud Billing data export to BigQuery](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
   in the `billing-admin-290403` project. Replace `$BIGQUERY_BILLING_TABLE` below
   with the corresponding table name, e.g.
   `billing-admin-290403.billing.gcp_billing_export_v1_012345_ABCDEF_123456`.
1. Grant the service account _BigQuery Job User_ and _BigQuery Data Viewer_ role
   permissions.
1. At the organization level, grant the service account _Billing Viewer_ permissions.
   Replace `$BILLING_ACCOUNT_ID` below with your billing ID, e.g. `01D123-234567-CBDEFA`.
1. Create a new Pub/Sub topic in the `billing-admin-290403` project, named
   `cost-report`.
1. Create a Cloud Scheduler job that posts a Pub/Sub message to the
   `cost-report` topic, e.g. using a daily schedule like `0 9 * * *`.
   The payload can be abitrary, as it is ignored in the Cloud Function.
1. Install the Cloud Function that gets triggered when a message to the
   `cost-report` Pub/Sub topic is posted. Set `$QUERY_TIME_ZONE` to your local
   time zone, e.g. `Australia/Sydney`.

   ```bash
   cd gcp_cost_report
   gcloud config set project $BILLING_ADMIN_PROJECT
   gcloud functions deploy gcp_cost_report --runtime python37 \
     --region=$REGION \
     --trigger-topic cost-report \
     --service-account $SERVICE_ACCOUNT \
     --set-env-vars SLACK_CHANNEL=$SLACK_CHANNEL \
     --set-env-vars BIGQUERY_BILLING_TABLE=$BIGQUERY_BILLING_TABLE \
     --set-env-vars QUERY_TIME_ZONE=$QUERY_TIME_ZONE \
     --set-env-vars BILLING_ACCOUNT_ID=$BILLING_ACCOUNT_ID
   ```

## Individiual billing items

To drill down on the recent cost incurred by a particular `$PROJECT`, the
following query can be helpful:

```sql
SELECT
  *
FROM
  (
    SELECT
      FORMAT_TIMESTAMP("%F", export_time, "$QUERY_TIME_ZONE") as day,
      service.description as service,
      sku.description as sku,
      ROUND(sum(cost), 2) as cost,
      currency
    FROM
      `$BIGQUERY_BILLING_TABLE`
    WHERE
      _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)
      AND project.id = "$PROJECT"
    GROUP BY
      day,
      service,
      sku,
      currency
  )
WHERE
  cost > 0.1
ORDER BY
  day DESC,
  cost DESC;
```
