from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="One row per validated project with Red/Amber/Green risk status")
@dp.expect("risk_status_assigned", "risk_status IS NOT NULL")
def project_kpis():
    growth = F.coalesce(F.col("budget_growth_pct"), F.lit(0))
    delay = F.coalesce(F.col("delay_days"), F.lit(0))
    return (
        spark.read.table("projects_validated")
        .withColumn("risk_status",
            F.when((growth > 0.20) | (delay > 365), "Red")
             .when((growth > 0.10) | (delay > 180), "Amber")
             .otherwise("Green"))
    )


@dp.materialized_view(comment="Budget, spend, delay and risk by managing agency")
def agency_summary():
    return (
        spark.read.table("project_kpis")
        .groupBy("agency")
        .agg(
            F.count("*").alias("projects"),
            F.round(F.sum("budget_usd") / 1e6, 1).alias("budget_m"),
            F.round(F.sum("spend_usd") / 1e6, 1).alias("spend_m"),
            F.round(F.avg("budget_growth_pct"), 3).alias("avg_budget_growth_pct"),
            F.round(F.avg("delay_days"), 0).alias("avg_delay_days"),
            F.sum(F.when(F.col("risk_status") == "Red", 1).otherwise(0)).alias("red_projects"),
        )
    )


@dp.materialized_view(comment="Budget, spend and delay by project phase")
def phase_summary():
    return (
        spark.read.table("project_kpis")
        .groupBy("phase")
        .agg(
            F.count("*").alias("projects"),
            F.round(F.sum("budget_usd") / 1e6, 1).alias("budget_m"),
            F.round(F.sum("spend_usd") / 1e6, 1).alias("spend_m"),
            F.round(F.avg("delay_days"), 0).alias("avg_delay_days"),
        )
    )


@dp.materialized_view(comment="Monthly budget and spend totals from spend-history snapshots")
@dp.expect_or_drop("valid_month", "month IS NOT NULL")
@dp.expect("budget_present", "total_budget_m IS NOT NULL")
def monthly_trend():
    return (
        spark.read.table("workspace.silver.spend_history")
        .groupBy("month")
        .agg(
            F.countDistinct("fms_id").alias("projects"),
            F.round(F.sum("budget_usd") / 1e6, 1).alias("total_budget_m"),
            F.round(F.sum("spend_usd") / 1e6, 1).alias("total_spend_m"),
            F.round(F.sum("budget_variance_usd") / 1e6, 2).alias("budget_variance_m"),
        )
    )