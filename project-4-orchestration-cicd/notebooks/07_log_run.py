# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("run_label", "manual")
run_id = dbutils.widgets.get("run_id")
run_label = dbutils.widgets.get("run_label")

# COMMAND ----------

row = [(
    run_id,
    run_label,
    "SUCCESS",
    spark.table("workspace.silver.projects").count(),
    spark.table("workspace.silver.projects_rejected").count(),
    spark.table("workspace.silver.spend_history").count(),
    spark.table("workspace.gold.project_kpis").count(),
)]
cols = ["run_id", "run_label", "status", "silver_projects", "rejected_projects",
        "spend_history_rows", "gold_projects"]

(spark.createDataFrame(row, cols)
    .withColumn("logged_at", F.current_timestamp())
    .write.mode("append").option("mergeSchema", "true")
    .saveAsTable("workspace.gold.pipeline_runs"))

display(spark.sql("SELECT * FROM workspace.gold.pipeline_runs ORDER BY logged_at DESC LIMIT 10"))