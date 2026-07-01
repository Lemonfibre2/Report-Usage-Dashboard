# Report Execution Tracking Dashboard

A end-to-end data engineering project built on **Databricks** using a **Medallion Architecture (Bronze → Silver → Gold)** to consolidate, clean, and analyse SSRS report execution statistics across 13 labs. The final output is an interactive **Databricks AI/BI Dashboard** providing KPI insights into report usage, performance, and data volume.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Source Data](#source-data)
- [Project Structure](#project-structure)
- [Pipeline Walkthrough](#pipeline-walkthrough)
  - [Phase 1 — File Preparation](#phase-1--file-preparation)
  - [Phase 2 — Bronze Layer](#phase-2--bronze-layer)
  - [Phase-3 — Silver Layer](#phase-3--silver-layer)
  - [Phase 4 — Gold Layer](#phase-4--gold-layer)
  - [Phase 5 — Dashboard](#phase-5--dashboard)
- [KPIs](#kpis)
- [Dashboard Datasets](#dashboard-datasets)
- [How to Run](#how-to-run)
- [Adding a New Month](#adding-a-new-month)
- [Tech Stack](#tech-stack)

---

## Project Overview

Each lab submits monthly **ReportExecutionStats** files from SQL Server Reporting Services (SSRS). These files capture report-level execution data — who ran what report, how often, and how long it took.

The goal of this project is to:
- Consolidate data across all 13 labs into a single unified dataset
- Clean and standardise inconsistent formats across files
- Calculate KPI metrics for dashboard reporting
- Enable management to track report usage, performance, and adoption trends

---

## Architecture

```
Raw Files (CSV / XLSX)
        │
        ▼
┌─────────────────────────────────────────────┐
│              BRONZE LAYER                   │
│   workspace.bronze.report_execution_raw     │
│   - Raw data landed as-is                   │
│   - No transformations                      │
│   - Partitioned by _source_month            │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│              SILVER LAYER                   │
│   workspace.silver.report_execution_clean   │
│   - Month standardised to YYYY-MM           │
│   - Username cleaned and domain extracted   │
│   - IsSystemAccount flag added              │
│   - All 13 labs consolidated                │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│               GOLD LAYER                    │
│  workspace.gold.report_execution_summary    │
│   - Aggregated at report + user level       │
│   - All KPI metrics pre-calculated          │
│   - Ready for dashboard consumption         │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│         DATABRICKS AI/BI DASHBOARD          │
│   - 3 pages: Overall, Time Tracking,        │
│     Summary                                 │
│   - 11 datasets / 3 global filters          │
└─────────────────────────────────────────────┘
```

---

## Source Data

| Item | Detail |
|---|---|
| Source system | SQL Server Reporting Services (SSRS) |
| Labs | 13 labs (AICHI, CHLOE, COLUMBUS, ECO2, ELIOT, ELTL, EMIL, EMTC, EOLP, EOLT, LABEX, SEDICO, TRISTAR) |
| Frequency | Monthly |
| Format | CSV and XLSX (mixed per month) |
| Schema | 15 columns per file |

**Schema:**

| Column | Type | Description |
|---|---|---|
| Entity | String | Lab name |
| Month | String/Date | Reporting month |
| ItemPath | String | Full SSRS path to report |
| ReportName | String | Short report name |
| UserName | String | Domain\username |
| UserExecCount | Integer | Executions by this user |
| NbExecutions | Integer | Total executions (repeated per user row) |
| Avg_TotalDuration_Sec | Float | Average total execution time |
| Max_TotalDuration_Sec | Float | Maximum execution time |
| P95_TotalDuration_Sec | Float | 95th percentile execution time |
| Avg_DataRetrieval_Sec | Float | Average data retrieval time |
| Avg_Processing_Sec | Float | Average processing time |
| Avg_Rendering_Sec | Float | Average rendering time |
| Avg_RowCount | Integer | Average rows returned |
| Avg_ByteCount | Integer | Average output size in bytes |

---

## Project Structure

```
report-execution-tracking/
│
├── notebooks/
│   ├── Bronze_Ingestion.py          # Raw data ingestion
│   ├── Silver_Transformation.py     # Cleaning and consolidation
│   └── Gold_Summary.py              # KPI aggregation
│
├── scripts/
│   ├── convert_xlsx_to_csv.py       # Convert XLSX files to CSV (Phase 1)
│   └── fix_labex_and_elm.py         # Fix LABEX and ELM file issues (Phase 1)
│
├── dashboard/
│   └── Dashboard_Dataset_SQLs.sql   # All 11 dataset SQL queries
│
├── docs/
│   └── Approach_Method.docx         # Full approach and method document
│
└── README.md
```

---

## Pipeline Walkthrough

### Phase 1 — File Preparation

Before loading into Databricks, source files require preparation:

**Run locally:**
```bash
python scripts/convert_xlsx_to_csv.py   # Converts 7 XLSX files to CSV
python scripts/fix_labex_and_elm.py     # Fixes LABEX tab delimiter + ELM extension issue
```

**What each script handles:**

| File | Issue | Fix |
|---|---|---|
| 7 XLSX files | datetime Month field | Converted to "Month YYYY" string |
| LABEX.csv | Tab-delimited, outer quotes | Stripped and re-saved as CSV |
| ELM.xlsx | Named .xlsx but is actually CSV | Read as CSV, saved with .csv extension |

Upload all prepared CSVs to:
```
/Volumes/workspace/bronze/raw/report_dashboard/March/
/Volumes/workspace/bronze/raw/report_dashboard/April/
```

---

### Phase 2 — Bronze Layer

**Notebook:** `Bronze_Ingestion.py`

- Reads all CSVs from each month folder using wildcard paths
- Extracts lab name from filename (handles both naming conventions)
- Tags each row with `_source_lab` and `_source_month`
- Unions all DataFrames into one Bronze table
- Saves as Delta table partitioned by `_source_month`

**Key design decisions:**
- Zero transformations — raw data preserved exactly as received
- Partition overwrite by month — safe to rerun without affecting other months
- Wildcard loading — new months require only a folder drop and config update

---

### Phase 3 — Silver Layer

**Notebook:** `Silver_Transformation.py`

- Reads from `workspace.bronze.report_execution_raw`
- Selects required columns only
- Standardises Month field to `YYYY-MM` format
- Cleans UserName — extracts domain prefix and clean username
- Flags NT SERVICE accounts as `IsSystemAccount = True`
- Saves as `workspace.silver.report_execution_clean`

**Key transformations:**

| Field | Before | After |
|---|---|---|
| Month | "March 2026", "Mar-26", datetime | "2026-03" |
| UserName | "ASIA\krishna" | UserName_Raw + Domain + UserName_Clean |
| IsSystemAccount | — | True if "nt service", else False |

---

### Phase 4 — Gold Layer

**Notebook:** `Gold_Summary.py`

- Reads from `workspace.silver.report_execution_clean`
- Aggregates at `Entity + Month + ReportName + UserName_Clean` grain
- Calculates all execution, duration, and volume metrics
- Saves as `workspace.gold.report_execution_summary`

**Why one Gold table:**
Rather than pre-building 7 separate KPI tables, a single summary table is used. All KPI views are built as SQL datasets in the Dashboard UI — giving full flexibility to slice by any dimension without modifying the pipeline.

---

### Phase 5 — Dashboard

**Tool:** Databricks AI/BI Dashboard

**3 pages:**
- **Overall Tracking** — headline KPIs, usage by lab, top reports, top users
- **Time Tracking** — performance heatmap, P95 slowest reports, duration breakdown
- **Summary** — full execution detail table, reports by lab table

**3 global filters:**
- Lab Name (`Entity`)
- Report Name (`ReportName`)
- User Name (`UserName_Clean`)

---

## KPIs

| # | KPI | Description |
|---|---|---|
| 1 | Total Executions | Sum of UserExecCount across all labs |
| 2 | Unique Reports | Count of distinct report names |
| 3 | Active Users | Count of distinct human usernames |
| 4 | Labs Reporting | Count of distinct labs |
| 5 | Executions by Lab | Total executions per lab |
| 6 | Top 10 Reports | Most executed reports globally |
| 7 | Top 10 Users | Most active human users |
| 8 | Slowest Reports (P95) | Reports with highest P95 execution time |
| 9 | Avg Duration Breakdown | Retrieval vs Processing vs Rendering per report |
| 10 | Heaviest Reports | Reports with highest average byte size per lab |
| 11 | Human vs System Split | Execution split between human and NT SERVICE accounts |

---

## Dashboard Datasets

| Dataset | Used for |
|---|---|
| `kpi_usage_summary` | Counter tiles — headline numbers |
| `kpi_executions_by_lab` | Bar chart — executions by lab |
| `kpi_top_reports` | Bar chart — top reports |
| `kpi_top_users` | Bar chart — top users globally |
| `kpi_top_users_by_lab` | Bar chart (faceted) — top users per lab |
| `kpi_performance` | Bar chart — slowest reports globally |
| `kpi_performance_detail` | Bar chart (faceted) — slowest reports per lab |
| `kpi_volume` | Bar chart — heaviest reports per lab |
| `kpi_account_split` | Pie chart — human vs system split |
| `kpi_avg_duration` | Heatmap — duration breakdown by component |
| `kpi_execution_detail` | Table — full execution detail |
| `kpi_reports_by_lab` | Table — all reports used per lab |

All SQL queries are in `dashboard/Dashboard_Dataset_SQLs.sql`

---

## How to Run

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Serverless compute or a running cluster
- Schemas created: `workspace.bronze`, `workspace.silver`, `workspace.gold`
- Files uploaded to `/Volumes/workspace/bronze/raw/report_dashboard/`

### Step 1 — Prepare files locally
```bash
pip install openpyxl
python scripts/convert_xlsx_to_csv.py
python scripts/fix_labex_and_elm.py
```

### Step 2 — Upload CSVs to Databricks
Upload all prepared CSV files to the correct month folder in the volume:
```
/Volumes/workspace/bronze/raw/report_dashboard/March/
/Volumes/workspace/bronze/raw/report_dashboard/April/
```

### Step 3 — Run notebooks in order
```
1. Bronze_Ingestion.py
2. Silver_Transformation.py
3. Gold_Summary.py
```

### Step 4 — Set up Databricks Job (optional)
Create a job with 3 sequential tasks — one per notebook. Trigger on demand or on a schedule when new monthly data arrives.

---

## Adding a New Month

1. Prepare the new month's CSV files (run local scripts if needed)
2. Upload to a new folder: `/Volumes/workspace/bronze/raw/report_dashboard/May/`
3. Add the folder name to `MONTH_FOLDERS` in `Bronze_Ingestion.py`:
```python
MONTH_FOLDERS = [
    "March",
    "April",
    "May",    # ← add this
]
```
4. Rerun the pipeline (Bronze → Silver → Gold)
5. Dashboard updates automatically ✅

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Databricks | Notebooks, Delta Lake, AI/BI Dashboard, Jobs |
| PySpark | Data ingestion and transformation |
| Delta Lake | Storage format for all 3 layers |
| SQL | Dashboard dataset queries |
| Python | Local file preparation scripts |
| SSRS | Upstream data source |
| Unity Catalog | Data governance and access control |

---

## Data Notes

- **NbExecutions vs UserExecCount** — `NbExecutions` is the total for a report repeated on every user row. All execution KPIs use `UserExecCount` (stored as `Total_User_Executions` in Gold) to avoid double counting.
- **ELIOT vs ELM** — The ELM file's internal Entity value is `ELIOT`. This is the value that appears in the dashboard, not `ELM`.
- **System accounts** — NT SERVICE accounts represent automated SSRS subscriptions. They are flagged via `IsSystemAccount` and excluded from user-level KPIs.
- **Domain prefixes** — Three domain groups exist across labs: `ASIA`, `ELCORP`, `LUXGROUP`.
