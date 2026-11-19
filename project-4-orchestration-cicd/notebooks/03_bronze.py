# Databricks notebook source
import re
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/construction/raw"
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

def clean(c):
    c = c.replace("%", " pct ")
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-zA-Z]+", "_", c.strip().lower())).strip("_") or "col"

def unique_names(cols):
    seen, out = {}, []
    for c in cols:
        n = clean(c)
        if n in seen:
            seen[n] += 1
            new = f"{n}_{seen[n]}"
            print(f"  duplicate: '{c}' -> {new}")
        else:
            seen[n] = 1
            new = n
        out.append(new)
    return out

for name in ["budget_schedule", "budget_fy", "spend_history"]:
    print(name)
    df = (spark.read.option("header", True).option("multiLine", True)
          .option("escape", '"').csv(f"{BASE}/{name}/"))
    df = df.toDF(*unique_names(df.columns))
    df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"workspace.bronze.nyc_{name}"))
    print("  rows:", df.count(), "| columns:", len(df.columns))
