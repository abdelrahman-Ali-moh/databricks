# Databricks notebook source
q = lambda sql: spark.sql(sql).first()[0]
checks = {
 "silver projects not empty":      q("SELECT COUNT(*) FROM workspace.silver.projects") > 0,
 "no duplicate fms_id":            q("SELECT COUNT(*) - COUNT(DISTINCT fms_id) FROM workspace.silver.projects") == 0,
 "no null fms_id":                 q("SELECT COUNT(*) FROM workspace.silver.projects WHERE fms_id IS NULL") == 0,
 "budget > 0 everywhere":          q("SELECT COUNT(*) FROM workspace.silver.projects WHERE budget_usd <= 0") == 0,
 "no negative spend":              q("SELECT COUNT(*) FROM workspace.silver.projects WHERE spend_usd < 0") == 0,
 "gold rows = silver rows":        q("SELECT COUNT(*) FROM workspace.gold.project_kpis") == q("SELECT COUNT(*) FROM workspace.silver.projects"),
 "gold budget = silver budget":    abs(q("SELECT SUM(budget_usd) FROM workspace.gold.project_kpis") - q("SELECT SUM(budget_usd) FROM workspace.silver.projects")) < 1,
 "agency summary projects = gold": q("SELECT SUM(projects) FROM workspace.gold.agency_summary") == q("SELECT COUNT(*) FROM workspace.gold.project_kpis"),
 "history has no dup (id, month)": q("SELECT COUNT(*) - COUNT(DISTINCT fms_id, month) FROM workspace.silver.spend_history") == 0,
}
for k, v in checks.items():
    print("PASS" if v else "FAIL", "-", k)
assert all(checks.values()), "Some checks failed"
