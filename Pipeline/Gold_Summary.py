# Databricks notebook source
# LAYER  : Gold
# PURPOSE: Build one consolidated summary table for the Report Tracking Dashboard
# INPUT  : workspace.silver.report_execution_clean
# OUTPUT : workspace.gold.report_execution_summary
# ============================================================
# NOTE: All KPI datasets will be built on top of this single
#       table using SQL queries in the Databricks Dashboard UI
#       — exactly like the Application Usage Dashboard.
# ============================================================

# COMMAND ----------

# CELL 1 — Read from Silver

SILVER_TABLE = "workspace.silver.report_execution_clean"
silver_df    = spark.table(SILVER_TABLE)

print(f"Rows read from Silver : {silver_df.count()}")
print(f"Columns               : {len(silver_df.columns)}")
print()
silver_df.printSchema()

# COMMAND ----------

# CELL 2 — Build the consolidated Gold summary table
# ============================================================
# This table keeps one row per unique combination of:
#   Entity + Month + ReportName + UserName_Clean
#
# All execution counts and performance metrics are aggregated
# at this level. This gives the dashboard SQL full flexibility
# to slice by any dimension — lab, user, report, domain, etc.

from pyspark.sql import functions as F

gold_df = (silver_df
    .groupBy(
        "Entity",
        "Month",
        "ReportName",
        "ItemPath",
        "UserName_Clean",
        "UserName_Raw",
        "Domain",
        "IsSystemAccount"
    )
    .agg(
        # ── Execution counts ──────────────────────────────
        F.sum("UserExecCount")                        .alias("Total_Executions")

        # ── Duration metrics ──────────────────────────────
        F.round(F.avg("Avg_TotalDuration_Sec"),  2)   .alias("Avg_Duration_Sec"),
        F.round(F.max("Max_TotalDuration_Sec"),  2)   .alias("Max_Duration_Sec"),
        F.round(F.avg("P95_TotalDuration_Sec"),  2)   .alias("P95_Duration_Sec"),

        # ── Component breakdown ───────────────────────────
        F.round(F.avg("Avg_DataRetrieval_Sec"),  2)   .alias("Avg_DataRetrieval_Sec"),
        F.round(F.avg("Avg_Processing_Sec"),     2)   .alias("Avg_Processing_Sec"),
        F.round(F.avg("Avg_Rendering_Sec"),      2)   .alias("Avg_Rendering_Sec"),

        # ── Volume metrics ────────────────────────────────
        F.round(F.avg("Avg_RowCount"),           0)   .alias("Avg_Row_Count"),
        F.round(F.avg("Avg_ByteCount"),          0)   .alias("Avg_Byte_Count"),
    )
    .orderBy("Entity", "ReportName", "UserName_Clean")
)

print(f"Gold summary table — rows    : {gold_df.count()}")
print(f"Gold summary table — columns : {len(gold_df.columns)}")
print()
print("Columns in Gold table:")
for c in gold_df.columns:
    print(f"  - {c}")

# COMMAND ----------

# CELL 3 — Preview the Gold table

print("Sample rows (5):")
gold_df.show(5, truncate=True)

# COMMAND ----------

# CELL 4 — Save as Gold Delta table

GOLD_TABLE = "workspace.gold.report_execution_summary"

(gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE))

print(f"Saved : {GOLD_TABLE}")
print(f"Rows  : {gold_df.count()}")

# COMMAND ----------

# CELL 5 — Sanity check on saved Gold table

verify_df = spark.table(GOLD_TABLE)

print(f"Rows in saved Gold table: {verify_df.count()}")
print()

print("Row counts per Entity:")
(verify_df
    .groupBy("Entity")
    .agg(
        F.sum("Total_Executions")      .alias("Total_Executions"),
        F.countDistinct("ReportName")  .alias("Unique_Reports"),
        F.countDistinct(
            F.when(~verify_df.IsSystemAccount,
                   verify_df.UserName_Clean)).alias("Active_Users")
    )
    .orderBy(F.desc("Total_Executions"))
    .show(20, truncate=False))

print()
print("Human vs System split:")
(verify_df
    .groupBy("IsSystemAccount")
    .agg(F.sum("Total_Executions").alias("Total_Executions"))
    .show())

print()
print("=" * 60)
print("Gold complete — 1 summary table ready for dashboard.")
print()
print(f"Table : {GOLD_TABLE}")
print()
print("Next  : Build KPI datasets in Databricks Dashboard UI")
print("        using SQL queries on top of this Gold table.")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from workspace.gold.report_execution_summary
# MAGIC where month like "2026-05"

# COMMAND ----------

# CELL 6 — Reference SQL queries for Dashboard UI
# ============================================================
# Copy these queries into the Databricks Dashboard UI
# as individual datasets — one per KPI.
# ============================================================

reference_sql = """
-- ── KPI 1: Top-level headline numbers ─────────────────────
SELECT
    SUM(Total_Executions)                          AS Total_Executions,
    COUNT(DISTINCT ReportName)                     AS Total_Unique_Reports,
    COUNT(DISTINCT CASE WHEN IsSystemAccount = false
                   THEN UserName_Clean END)        AS Total_Active_Users,
    COUNT(DISTINCT Entity)                         AS Total_Labs
FROM workspace.gold.report_execution_summary;


-- ── KPI 2: Executions by lab ───────────────────────────────
SELECT
    Entity,
    SUM(Total_Executions)                          AS Total_Executions,
    COUNT(DISTINCT ReportName)                     AS Unique_Reports,
    COUNT(DISTINCT CASE WHEN IsSystemAccount = false
                   THEN UserName_Clean END)        AS Active_Users
FROM workspace.gold.report_execution_summary
GROUP BY Entity
ORDER BY Total_Executions DESC;


-- ── KPI 3: Top 10 most executed reports ───────────────────
SELECT
    ReportName,
    Entity,
    SUM(Total_Executions)                          AS Total_Executions,
    COUNT(DISTINCT UserName_Clean)                 AS Unique_Users,
    ROUND(AVG(Avg_Duration_Sec), 2)                AS Avg_Duration_Sec
FROM workspace.gold.report_execution_summary
GROUP BY ReportName, Entity
ORDER BY Total_Executions DESC
LIMIT 10;


-- ── KPI 4: Top 10 most active users (human only) ──────────
SELECT
    UserName_Clean,
    Domain,
    Entity,
    SUM(Total_User_Executions)                     AS Total_Executions,
    COUNT(DISTINCT ReportName)                     AS Reports_Run
FROM workspace.gold.report_execution_summary
WHERE IsSystemAccount = false
GROUP BY UserName_Clean, Domain, Entity
ORDER BY Total_Executions DESC
LIMIT 10;


-- ── KPI 5: Slowest reports by P95 duration ────────────────
SELECT
    ReportName,
    Entity,
    ROUND(AVG(Avg_Duration_Sec),      2)           AS Avg_Duration_Sec,
    ROUND(MAX(Max_Duration_Sec),      2)           AS Max_Duration_Sec,
    ROUND(AVG(P95_Duration_Sec),      2)           AS P95_Duration_Sec,
    ROUND(AVG(Avg_DataRetrieval_Sec), 2)           AS Avg_DataRetrieval_Sec,
    ROUND(AVG(Avg_Processing_Sec),    2)           AS Avg_Processing_Sec,
    ROUND(AVG(Avg_Rendering_Sec),     2)           AS Avg_Rendering_Sec,
    SUM(Total_Executions)                          AS Total_Executions
FROM workspace.gold.report_execution_summary
GROUP BY ReportName, Entity
ORDER BY P95_Duration_Sec DESC;


-- ── KPI 6: Heaviest reports by data volume ────────────────
SELECT
    ReportName,
    Entity,
    ROUND(AVG(Avg_Row_Count),  0)                  AS Avg_Row_Count,
    ROUND(AVG(Avg_Byte_Count), 0)                  AS Avg_Byte_Count,
    SUM(Total_Executions)                          AS Total_Executions
FROM workspace.gold.report_execution_summary
GROUP BY ReportName, Entity
ORDER BY Avg_Byte_Count DESC;


-- ── KPI 7: Human vs system split per lab ──────────────────
SELECT
    Entity,
    IsSystemAccount,
    SUM(Total_Executions)                          AS Total_Executions
FROM workspace.gold.report_execution_summary
GROUP BY Entity, IsSystemAccount
ORDER BY Entity, IsSystemAccount;
"""

print("Reference SQL queries for Databricks Dashboard UI:")
print(reference_sql)

# COMMAND ----------

# %sql
# WITH vol_totals AS (
#     SELECT
#         ReportName,
#         Entity,
#         UserName_Clean,
#         ROUND(AVG(Avg_Row_Count),  0)        AS Avg_Row_Count,
#         ROUND(AVG(Avg_Byte_Count), 0)        AS Avg_Byte_Count,
#         SUM(Total_User_Executions)           AS Total_Executions
#     FROM workspace.gold.report_execution_summary
#     WHERE ReportName IS NOT NULL
#     GROUP BY ReportName, Entity, UserName_Clean
# ),
# global_vol AS (
#     SELECT ReportName,
#         AVG(Avg_Byte_Count) AS Global_Bytes
#     FROM vol_totals
#     GROUP BY ReportName
# ),
# top_ranked AS (
#     SELECT ReportName,
#         RANK() OVER (ORDER BY Global_Bytes DESC) AS global_rank
#     FROM global_vol
# )
# SELECT
#     v.ReportName,
#     v.Entity,
#     v.UserName_Clean,
#     v.Avg_Row_Count,
#     v.Avg_Byte_Count,
#     v.Total_Executions,
#     tr.global_rank
# FROM vol_totals v
# INNER JOIN top_ranked tr ON v.ReportName = tr.ReportName
# WHERE tr.global_rank <= 10
# ORDER BY tr.global_rank, v.Avg_Byte_Count DESC;
