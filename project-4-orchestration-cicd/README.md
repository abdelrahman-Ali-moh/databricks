# Orchestration and CI/CD for the Capital Projects Lakehouse

Databricks Asset Bundle that deploys and schedules the NYC Capital Projects medallion pipeline.

## What it does
- Job with 5 dependent tasks: bronze -> silver -> gold -> quality_checks -> log_run
- Daily schedule (06:00 Asia/Riyadh), 1 retry on bronze, failure email alert
- Quality checks task fails the whole job if any check fails
- Run history written to `workspace.gold.pipeline_runs`
- GitHub Actions: syntax check + `bundle validate` on pull requests, `bundle deploy` on merge to main

## Structure
databricks.yml | resources/capital_projects_job.yml | notebooks/ | .github/workflows/deploy.yml

## Deploy
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run capital_projects_pipeline -t dev

## Notes
- Built on Databricks Free Edition (serverless only, max 5 concurrent job tasks).
- Free Edition has one workspace, so dev and prod write to the same tables.
- Source data is a static public snapshot; the point of this project is orchestration, alerting and CI/CD.
