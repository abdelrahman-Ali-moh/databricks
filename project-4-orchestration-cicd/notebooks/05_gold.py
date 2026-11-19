# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per project, with KPIs and risk flag
# MAGIC CREATE OR REPLACE TABLE workspace.gold.project_kpis AS
# MAGIC WITH base AS (
# MAGIC   SELECT *,
# MAGIC          ROUND(spend_usd / NULLIF(budget_usd, 0), 4)                    AS spend_pct,
# MAGIC          budget_usd - orig_budget_usd                                    AS budget_growth_usd,
# MAGIC          ROUND((budget_usd - orig_budget_usd) / NULLIF(orig_budget_usd, 0), 4) AS budget_growth_pct,
# MAGIC          DATEDIFF(fcst_end_date, orig_end_date)                          AS delay_days
# MAGIC   FROM workspace.silver.projects
# MAGIC )
# MAGIC SELECT *,
# MAGIC        CASE WHEN COALESCE(budget_growth_pct, 0) > 0.20 OR COALESCE(delay_days, 0) > 365 THEN 'Red'
# MAGIC             WHEN COALESCE(budget_growth_pct, 0) > 0.10 OR COALESCE(delay_days, 0) > 180 THEN 'Amber'
# MAGIC             ELSE 'Green' END AS risk_status
# MAGIC FROM base;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agency summary
# MAGIC CREATE OR REPLACE TABLE workspace.gold.agency_summary AS
# MAGIC SELECT agency,
# MAGIC        COUNT(*)                                        AS projects,
# MAGIC        ROUND(SUM(budget_usd) / 1e6, 1)                 AS budget_m,
# MAGIC        ROUND(SUM(spend_usd) / 1e6, 1)                  AS spend_m,
# MAGIC        ROUND(SUM(spend_usd) / NULLIF(SUM(budget_usd), 0), 3) AS spend_pct,
# MAGIC        ROUND(AVG(budget_growth_pct), 3)                AS avg_budget_growth_pct,
# MAGIC        ROUND(AVG(delay_days), 0)                       AS avg_delay_days,
# MAGIC        SUM(CASE WHEN risk_status = 'Red' THEN 1 ELSE 0 END) AS red_projects
# MAGIC FROM workspace.gold.project_kpis
# MAGIC GROUP BY agency;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Phase summary
# MAGIC CREATE OR REPLACE TABLE workspace.gold.phase_summary AS
# MAGIC SELECT phase,
# MAGIC        COUNT(*)                         AS projects,
# MAGIC        ROUND(SUM(budget_usd) / 1e6, 1)  AS budget_m,
# MAGIC        ROUND(SUM(spend_usd) / 1e6, 1)   AS spend_m,
# MAGIC        ROUND(AVG(delay_days), 0)        AS avg_delay_days
# MAGIC FROM workspace.gold.project_kpis
# MAGIC GROUP BY phase;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly trend from spend history snapshots
# MAGIC CREATE OR REPLACE TABLE workspace.gold.monthly_trend AS
# MAGIC SELECT month,
# MAGIC        COUNT(DISTINCT fms_id)                         AS projects,
# MAGIC        ROUND(SUM(budget_usd) / 1e6, 1)                AS total_budget_m,
# MAGIC        ROUND(SUM(spend_usd) / 1e6, 1)                 AS total_spend_m,
# MAGIC        ROUND(SUM(budget_variance_usd) / 1e6, 2)       AS budget_variance_m
# MAGIC FROM workspace.silver.spend_history
# MAGIC GROUP BY month
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick look
# MAGIC SELECT risk_status, COUNT(*) AS projects, ROUND(SUM(budget_usd) / 1e9, 2) AS budget_billion
# MAGIC FROM workspace.gold.project_kpis GROUP BY risk_status;
