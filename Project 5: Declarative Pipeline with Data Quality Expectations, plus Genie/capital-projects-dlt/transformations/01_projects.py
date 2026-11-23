from pyspark import pipelines as dp
from pyspark.sql import functions as F

SRC = "workspace.silver.projects"


@dp.materialized_view(comment="Projects with delay and spend metrics, validated by expectations")
@dp.expect_or_fail("has_fms_id", "fms_id IS NOT NULL AND fms_id != ''")
@dp.expect_or_drop("budget_positive", "budget_usd > 0")
@dp.expect_or_drop("plausible_delay", "delay_days IS NULL OR delay_days BETWEEN -3650 AND 7300")
@dp.expect("spend_not_negative", "spend_usd IS NULL OR spend_usd >= 0")
@dp.expect("spend_within_3x_budget", "spend_usd IS NULL OR spend_usd <= budget_usd * 3")
@dp.expect("has_agency", "agency IS NOT NULL")
@dp.expect("has_end_dates", "orig_end_date IS NOT NULL AND fcst_end_date IS NOT NULL")
def projects_validated():
    return (
        spark.read.table(SRC)
        .withColumn("delay_days", F.datediff("fcst_end_date", "orig_end_date"))
        .withColumn("spend_pct",
            F.when(F.col("budget_usd") > 0, F.round(F.col("spend_usd") / F.col("budget_usd"), 4)))
        .withColumn("budget_growth_usd", F.col("budget_usd") - F.col("orig_budget_usd"))
        .withColumn("budget_growth_pct",
            F.when(F.col("orig_budget_usd") > 0,
                   F.round((F.col("budget_usd") - F.col("orig_budget_usd")) / F.col("orig_budget_usd"), 4)))
    )


@dp.materialized_view(comment="Rows that violate the drop rules, kept for review")
def projects_quarantine():
    return (
        spark.read.table(SRC)
        .withColumn("delay_days", F.datediff("fcst_end_date", "orig_end_date"))
        .filter("budget_usd <= 0 OR (delay_days IS NOT NULL AND delay_days NOT BETWEEN -3650 AND 7300)")
        .withColumn("reject_reason",
            F.when(F.col("budget_usd") <= 0, "budget_not_positive").otherwise("implausible_delay"))
    )