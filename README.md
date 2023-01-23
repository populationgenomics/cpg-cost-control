# CPG Cost Control

[![Billing Aggregation Deploy](https://github.com/populationgenomics/gcp-cost-control/actions/workflows/deploy-aggregate.yaml/badge.svg)](https://github.com/populationgenomics/gcp-cost-control/actions/workflows/deploy-aggregate.yaml)
[![Billing Aggregation Codecov](https://codecov.io/github/populationgenomics/gcp-cost-control/branch/main/graph/badge.svg?token=Q2YZYCUD1M)](https://codecov.io/github/populationgenomics/gcp-cost-control)

This repo contains the cloud functions/services running on various cloud
infrastructures in order to manage the CPGs budgets and monitoring.

## GCP Cost Management

The `gcp_cost_control` and `gcp_cost_report` are used exclusively for
GCP cost controls. See [the readme](gcp_cost_control/README.md) for more
details.

## Azure Cost Managment

*In Progress*
Addition of Azure cost management is still underway. Updates to follow.

## Aggregate

Hail Batch: *Complete*
GCP: *Complete*
Azure: *To Do*

The aggregate code currently is hosted and running on GCP aggregates our
billing data across the centre into a bigquery table.

Currently, only GCP aggregation is implemented. Azure and any other cloud
platforms with billing information is to be added later.

Note that also the billing information from CPG's deployment of Hail Batch
on GCP is also aggregated.
