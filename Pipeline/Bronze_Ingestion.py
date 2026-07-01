# Databricks notebook source
# LAYER  : Bronze
# PURPOSE: Land all 13 raw CSV files into Spark — no transformations
# OUTPUT : Delta table  workspace.bronze.report_execution_raw
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workbook into individual CSV

# COMMAND ----------

# pip install openpyxl

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# import pandas as pd
# import os

# excel_file = "/Volumes/workspace/bronze/raw/report_dashboard/May/Reportexecinfo-May.xlsx"

# output_folder = "/Volumes/workspace/bronze/raw/report_dashboard/May"

# xls = pd.ExcelFile(excel_file)

# for sheet in xls.sheet_names:
#     df = pd.read_excel(excel_file, sheet_name=sheet)

#     csv_path = f"{output_folder}/{sheet}.csv"

#     df.to_csv(csv_path, index=False)

#     print(f"Created: {csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/bronze/raw/report_dashboard"
 
# Add new month folder names here as data arrives
MONTH_FOLDERS = [
    "March",
    "April",
    "May",
]
 
BRONZE_TABLE = "workspace.bronze.report_execution_raw"
 
print(f"Base path     : {BASE_PATH}")
print(f"Month folders : {MONTH_FOLDERS}")
print(f"Bronze table  : {BRONZE_TABLE}")

# COMMAND ----------

# CELL 2 — Load all 13 CSVs as-is (raw, no transformations)

import os
 
def extract_lab_name(filename):
    """Extract clean lab name from filename regardless of naming convention."""
    # Remove .csv extension
    name = filename.replace(".csv", "").strip()
    # If long name with dash prefix, take everything after the last dash
    if "-" in name:
        name = name.split("-")[-1].strip()
    # Return uppercased lab name
    return name.upper()
 
# Quick test
test_cases = [
    "RX_RS_RP_ReportExecutionStats-CHLOE.csv",
    "RX_RS_RP_ReportExecutionStats -EMTC.csv",
    "RX_RS_RP_ReportExecutionStats-ELM_fixed.csv",
    "CHLOE.csv",
    "AICHI.csv",
    "LABEX.csv",
]
print("Lab name extraction test:")
for f in test_cases:
    print(f"  {f:<50} → {extract_lab_name(f)}")

# COMMAND ----------

# CELL 3 — Union all 13 raw DataFrames into one Bronze table

from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from functools import reduce
 
all_dfs     = []
load_errors = []
 
for month in MONTH_FOLDERS:
    month_path = f"{BASE_PATH}/{month}/*.csv"
    print(f"\nLoading month: {month} from {month_path}")
 
    # Get list of files in this month folder
    try:
        files = dbutils.fs.ls(f"{BASE_PATH}/{month}/")
        csv_files = [f for f in files if f.name.endswith(".csv")]
        print(f"  Found {len(csv_files)} CSV files")
    except Exception as e:
        print(f"  [FAIL] Could not list folder: {e}")
        load_errors.append((month, str(e)))
        continue
 
    # Load each file individually to tag with lab name
    for file_info in csv_files:
        lab = extract_lab_name(file_info.name)
        try:
            df = (spark.read
                    .option("header",      "true")
                    .option("inferSchema", "true")
                    .option("encoding",    "UTF-8")
                    .csv(file_info.path))
 
            # Strip BOM from column names — file artefact not a transform
            clean_cols = [c.replace("\ufeff", "").strip() for c in df.columns]
            df = df.toDF(*clean_cols)
 
            # Cast Month to string to ensure consistent schema across all files
            if 'Month' in df.columns:
                df = df.withColumn("Month", F.col("Month").cast("string"))
 
            # Tag with source lab and month for traceability
            df = df.withColumn("_source_lab",   lit(lab))
            df = df.withColumn("_source_month", lit(month))
 
            row_count = df.count()
            all_dfs.append(df)
            print(f"  [OK]   {lab:<12} {row_count:>5} rows  |  {len(df.columns)} cols")
 
        except Exception as e:
            load_errors.append((f"{month}/{lab}", str(e)))
            print(f"  [FAIL] {lab:<12} ERROR: {e}")
 
print()
print(f"Total DataFrames loaded : {len(all_dfs)}")
if load_errors:
    print(f"Errors                  : {load_errors}")

# COMMAND ----------

# CELL 4 — Save as Bronze Delta table
if not all_dfs:
    raise Exception("No DataFrames loaded — check file paths and errors above")
 
bronze_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    all_dfs
)
 
total_rows = bronze_df.count()
print(f"Bronze table — total rows    : {total_rows}")
print(f"Bronze table — columns       : {len(bronze_df.columns)}")
print()
print("Columns:")
for c in bronze_df.columns:
    print(f"  - {c}")

# COMMAND ----------

(bronze_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_source_month")
    .saveAsTable(BRONZE_TABLE))

print(f"Saved : {BRONZE_TABLE}")
print(f"Rows  : {total_rows}")
print(f"Partitioned by: _source_month")

# COMMAND ----------

# CELL 5 — Sanity check on saved Bronze table

verify_df = spark.table(BRONZE_TABLE)
 
print(f"Rows in saved table: {verify_df.count()}")
print()
 
print("Row counts per month and lab:")
(verify_df
    .groupBy("_source_month", "_source_lab")
    .count()
    .orderBy("_source_month", "_source_lab")
    .show(40, truncate=False))
 
print()
print("Row counts per month (total):")
(verify_df
    .groupBy("_source_month")
    .count()
    .orderBy("_source_month")
    .show(truncate=False))
 
print()
print("=" * 55)
print("Bronze complete — raw data landed, zero transformations.")
print("Next : Silver_Transformation notebook")
print("=" * 55)
