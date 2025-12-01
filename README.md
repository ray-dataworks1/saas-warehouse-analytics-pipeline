# RavenStack SaaS Analytics (BigQuery)

RavenStack is a fictional AI-powered collaboration SaaS. This repo builds an analytics stack on top of a synthetic multi-table dataset:
- accounts (customer metadata)
- subscriptions (subscription lifecycle + revenue)
- feature_usage (daily product interaction)
- support_tickets (support ops + CSAT)
- churn_events (churn dates + reasons + refunds)

Goal: a production-style AE artefact: **raw → staging → core → marts → BI** with documented metrics and data quality checks.

## Stack
- Warehouse: **BigQuery**
- Transformations: **SQL + dbt**
- Ingestion: **Airflow**
- BI: **Looker Studio**
- CI: **GitHub Actions**

## Repo structure

.
├─ bi/ # dashboards, screenshots, links
├─ data/ # optional local copies / samples 
│ ├─ raw/ # raw CSVs if stored locally 
│ └─ samples/ # small samples for docs/tests
├─ docs/ # profiling notes, metric dictionary, decisions
├─ scripts/ # one-off helper scripts (loading, profiling)
├─ sql/ # if not using dbt, keep views/models here
└─ dbt/ # dbt project (recommended)
