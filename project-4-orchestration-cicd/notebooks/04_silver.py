# Databricks notebook source
import re
from pyspark.sql import functions as F, Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# ---------- helpers ----------
def find(cols, *patterns):
    for p in patterns:
        for c in cols:
            if re.search(p, c):
                return c
    return None

def num(c):
    return F.expr(f"try_cast(regexp_replace(`{c}`, '[$,% ]', '') as double)")

def dt(c):
    return F.coalesce(
        F.expr(f"try_to_date(`{c}`, 'MM/dd/yyyy')"),
        F.expr(f"try_to_date(`{c}`, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(substring(`{c}`, 1, 10), 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(concat(`{c}`, '-01'), 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(concat(`{c}`, '01'), 'yyyyMMdd')"),
        F.expr(f"try_to_date(`{c}`, 'MMM yyyy')"))

def txt(c):
    return F.trim(F.col(c)) if c else F.lit(None).cast("string")

def numc(c):
    return num(c) if c else F.lit(None).cast("double")

def datec(c):
    return dt(c) if c else F.lit(None).cast("date")

# ================= PROJECTS =================
bs = spark.table("workspace.bronze.nyc_budget_schedule")
cols = bs.columns

M = {
 "fms_id":       find(cols, r"^fms_id$", r"fms"),
 "project_name": find(cols, r"^project_name$", r"project_desc", r"^project$", r"name"),
 "agency":       find(cols, r"^managing_agency$", r"managing", r"agency"),
 "phase":        find(cols, r"^current_phase$", r"phase"),
 "orig_budget":  find(cols, r"original_budget", r"orig.*budget"),
 "budget":       find(cols, r"^total_budget$", r"budget_forecast", r"current_budget", r"^budget$"),
 "spend":        find(cols, r"^spend_to_date$", r"spend"),
 "orig_end":     find(cols, r"(original|baseline|planned).*(end|complet|finish)"),
 "fcst_end":     find(cols, r"(forecast|current|latest).*(end|complet|finish)", r"^(end|completion)"),
}

# Manual fixes if the mapping above is wrong, e.g. OVERRIDES = {"budget": "budget_forecast"}
OVERRIDES = {}
M.update(OVERRIDES)

print("PROJECT COLUMN MAPPING:")
for k, v in M.items():
    print(f"  {k:13s} -> {v}")
for must in ["fms_id", "budget", "spend"]:
    if not M[must]:
        raise ValueError(f"Missing column for '{must}'. Available columns: {cols}")

proj = bs.select(
    txt(M["fms_id"]).alias("fms_id"),
    txt(M["project_name"]).alias("project_name"),
    F.initcap(txt(M["agency"])).alias("agency"),
    F.initcap(txt(M["phase"])).alias("phase"),
    numc(M["orig_budget"]).alias("orig_budget_usd"),
    numc(M["budget"]).alias("budget_usd"),
    numc(M["spend"]).alias("spend_usd"),
    datec(M["orig_end"]).alias("orig_end_date"),
    datec(M["fcst_end"]).alias("fcst_end_date"),
)

# One row per project: keep the row with the highest spend, then highest budget
w = Window.partitionBy("fms_id").orderBy(F.col("spend_usd").desc_nulls_last(),
                                         F.col("budget_usd").desc_nulls_last())
ranked = proj.withColumn("_rn", F.row_number().over(w))
dups = ranked.filter("_rn > 1").count()
dedup = ranked.filter("_rn = 1").drop("_rn")

good_cond = F.col("fms_id").isNotNull() & (F.col("fms_id") != "") & \
            F.col("budget_usd").isNotNull() & (F.col("budget_usd") > 0) & \
            (F.col("spend_usd").isNull() | (F.col("spend_usd") >= 0))

valid = dedup.filter(good_cond)
rejected = (dedup.filter(~good_cond)
    .withColumn("reject_reason",
        F.when(F.col("fms_id").isNull() | (F.col("fms_id") == ""), "missing_fms_id")
         .when(F.col("budget_usd").isNull() | (F.col("budget_usd") <= 0), "invalid_budget")
         .otherwise("negative_spend"))
    .withColumn("_rejected_at", F.current_timestamp()))

valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.projects")
rejected.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.projects_rejected")
print(f"projects -> valid: {valid.count()} | rejected: {rejected.count()} | duplicate rows removed: {dups}")

# ================= SPEND HISTORY =================
sh = spark.table("workspace.bronze.nyc_spend_history")
sc = sh.columns
H = {
 "fms_id":   find(sc, r"^fms_id$", r"fms"),
 "month":    find(sc, r"year_month", r"reported", r"month", r"date"),
 "budget":   find(sc, r"^total_budget$", r"total_budget", r"budget$"),
 "spend":    find(sc, r"^spend_to_date$", r"spend"),
 "variance": find(sc, r"^budget_variance$", r"variance"),
}
H_OVERRIDES = {}
H.update(H_OVERRIDES)

print("\nSPEND HISTORY COLUMN MAPPING:")
for k, v in H.items():
    print(f"  {k:9s} -> {v}")
for must in ["fms_id", "month"]:
    if not H[must]:
        raise ValueError(f"Missing column for '{must}'. Available columns: {sc}")

hist = (sh.select(
        txt(H["fms_id"]).alias("fms_id"),
        F.trunc(datec(H["month"]), "month").alias("month"),
        numc(H["budget"]).alias("budget_usd"),
        numc(H["spend"]).alias("spend_usd"),
        numc(H["variance"]).alias("budget_variance_usd"))
    .filter(F.col("fms_id").isNotNull() & F.col("month").isNotNull())
    .dropDuplicates(["fms_id", "month"]))

hist.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.spend_history")
print("spend_history rows:", hist.count())
