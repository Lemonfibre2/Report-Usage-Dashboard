# Databricks notebook source
# LAYER  : Silver
# PURPOSE: Clean, standardise and consolidate all 13 labs into one master table
# INPUT  : workspace.bronze.report_execution_raw
# OUTPUT : Delta table  workspace.silver.report_execution_clean
# ============================================================

# COMMAND ----------

# CELL 1 — Read from Bronze

BRONZE_TABLE = "workspace.bronze.report_execution_raw"
bronze_df = spark.table(BRONZE_TABLE)

print(f"Rows read from Bronze : {bronze_df.count()}")
print(f"Columns : {len(bronze_df.columns)}")
print()
bronze_df.printSchema()

# COMMAND ----------

# CELL 2 — Select only required columns

REQUIRED_COLUMNS = [
    "Entity",
    "Month",
    "ItemPath",
    "ReportName",
    "UserName",
    "UserExecCount",
    "NbExecutions",
    "Avg_TotalDuration_Sec",
    "Max_TotalDuration_Sec",
    "P95_TotalDuration_Sec",
    "Avg_DataRetrieval_Sec",
    "Avg_Processing_Sec",
    "Avg_Rendering_Sec",
    "Avg_RowCount",
    "Avg_ByteCount",
    "_source_lab"   # keep for traceability
]

# Check for any missing columns
missing = [c for c in REQUIRED_COLUMNS if c not in bronze_df.columns]
if missing:
    print(f"WARNING — missing columns: {missing}")
else:
    print("All required columns present.")

selected_df = bronze_df.select(REQUIRED_COLUMNS)
print(f"Columns after selection: {len(selected_df.columns)}")

# COMMAND ----------

# DBTITLE 1,CELL 3 — Standardise Month field to YYYY-MM format
# CELL 3 — Standardise Month field to YYYY-MM format
# Handles multiple input formats:
#   - "March 2026" or "Mar-26" -> "2026-03"
#   - "2026-04-01" (YYYY-MM-DD) -> "2026-04"
#   - "2026-03" -> "2026-03" (already correct)

from pyspark.sql import functions as F

MONTH_MAP = {
    "january": "01", "jan": "01",
    "february": "02", "feb": "02",
    "march": "03", "mar": "03",
    "april": "04", "apr": "04",
    "may": "05",
    "june": "06", "jun": "06",
    "july": "07", "jul": "07",
    "august": "08", "aug": "08",
    "september": "09", "sep": "09",
    "october": "10", "oct": "10",
    "november": "11", "nov": "11",
    "december": "12", "dec": "12"
}

def standardise_month(df):
    result = df
    
    # STEP 1: Strip time/day from YYYY-MM-DD format -> YYYY-MM
    result = result.withColumn(
        "Month",
        F.regexp_replace(F.col("Month"), "^(\\d{4}-\\d{2})-\\d{2}$", "$1")
    )
    
    # STEP 2: Handle "Mar-26" format -> "2026-03"
    for month_name, month_num in MONTH_MAP.items():
        result = result.withColumn(
            "Month",
            F.regexp_replace(
                F.col("Month"),
                f"(?i)^{month_name}-(\\d{{2}})$",
                f"20$1-{month_num}"
            )
        )
    
    # STEP 3: Handle "March 2026" format -> "2026-03"
    for month_name, month_num in MONTH_MAP.items():
        result = result.withColumn(
            "Month",
            F.regexp_replace(
                F.col("Month"),
                f"(?i){month_name}\\s+(\\d{{4}})",
                f"$1-{month_num}"
            )
        )
    
    return result

month_df = standardise_month(selected_df)

print("Month values after standardisation:")
month_df.select("Month").distinct().orderBy("Month").show(truncate=False)

# COMMAND ----------

# CELL 4 — Standardise UserName and extract Domain + clean username

def standardise_usernames(df):
    # Trim whitespace and lowercase
    df = df.withColumn(
        "UserName",
        F.lower(F.trim(F.col("UserName")))
    )

    # Extract domain  (everything before the backslash)
    df = df.withColumn(
        "Domain",
        F.when(
            F.col("UserName").contains("\\"),
            F.upper(F.split(F.col("UserName"), "\\\\")[0])
        ).otherwise(F.lit("UNKNOWN"))
    )

    # Extract clean username (everything after the backslash)
    df = df.withColumn(
        "UserName_Clean",
        F.when(
            F.col("UserName").contains("\\"),
            F.split(F.col("UserName"), "\\\\")[1]
        ).otherwise(F.col("UserName"))
    )

    # Keep original UserName as UserName_Raw for reference
    df = df.withColumnRenamed("UserName", "UserName_Raw")

    return df

username_df = standardise_usernames(month_df)

print("Domain values found:")
username_df.select("Domain").distinct().show(truncate=False)

print("Sample username breakdown:")
username_df.select("UserName_Raw", "Domain", "UserName_Clean").show(10, truncate=False)

# COMMAND ----------

# CELL 5 — Add IsSystemAccount flag

def add_system_flag(df):
    return df.withColumn(
        "IsSystemAccount",
        F.when(
            F.col("UserName_Raw").startswith("nt service"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

flagged_df = add_system_flag(username_df)

print("System account breakdown per lab:")
(flagged_df
    .groupBy("Entity", "IsSystemAccount")
    .count()
    .orderBy("Entity", "IsSystemAccount")
    .show(40, truncate=False))

# COMMAND ----------

# CELL 6 — Final column selection and ordering for Silver table

silver_df = flagged_df.select(
    "Entity",
    "Month",
    "ReportName",
    "ItemPath",
    "UserName_Raw",
    "UserName_Clean",
    "Domain",
    "IsSystemAccount",
    "UserExecCount",
    "NbExecutions",
    "Avg_TotalDuration_Sec",
    "Max_TotalDuration_Sec",
    "P95_TotalDuration_Sec",
    "Avg_DataRetrieval_Sec",
    "Avg_Processing_Sec",
    "Avg_Rendering_Sec",
    "Avg_RowCount",
    "Avg_ByteCount",
    "_source_lab"
)

print(f"Silver table — rows    : {silver_df.count()}")
print(f"Silver table — columns : {len(silver_df.columns)}")
print()
print("Final schema:")
silver_df.printSchema()

# COMMAND ----------

# CELL 7 — Save as Silver Delta table

SILVER_TABLE = "workspace.silver.report_execution_clean"

(silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE))

print(f"Saved : {SILVER_TABLE}")

# COMMAND ----------

# CELL 8 — Sanity check on saved Silver table

verify_df = spark.table(SILVER_TABLE)

print(f"Rows in saved Silver table: {verify_df.count()}")
print()

print("Row counts per Entity:")
(verify_df
    .groupBy("Entity")
    .count()
    .orderBy("Entity")
    .show(20, truncate=False))

print("Human vs System account split:")
(verify_df
    .groupBy("IsSystemAccount")
    .count()
    .show())

print()
print("=" * 55)
print("Silver complete — data cleaned and consolidated.")
print("Next : Gold_KPIs notebook")
print("=" * 55)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(*) from workspace.silver.report_execution_clean
# MAGIC limit 25
