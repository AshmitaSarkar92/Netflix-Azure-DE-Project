# Netflix Azure Data Engineering Project

An end-to-end Azure data platform built around a simulated Netflix dataset — ingestion with Azure Data Factory, incremental Bronze/Silver/Gold layering on ADLS Gen2, Databricks Autoloader for streaming ingestion, Databricks Workflows for orchestration (ForEach + If/Else branching), and a governed Gold layer built with Delta Live Tables under Unity Catalog.

## Architecture Overview

![Resource Group Overview](netflix_setup_ss/01-resource-group-overview.png)

All resources live in a single resource group (`RG-NetflixProject`):

| Resource | Purpose |
|---|---|
| `adf-netflixproject-ash` | Azure Data Factory — HTTP ingestion of source files from GitHub into the data lake |
| `netflixprojectdatalake` | ADLS Gen2 storage account — `raw` / `bronze` / `silver` / `gold` containers |
| `netflix_adb` | Azure Databricks workspace — Autoloader, Workflows, Delta Live Tables, Unity Catalog |
| `access_netflix` | Access Connector for Azure Databricks — lets Unity Catalog read/write the data lake via managed identity |

**Data flow:**

```
GitHub (source CSVs)
      │  ADF HTTP linked service — ForEach + Copy Activity
      ▼
raw (ADLS Gen2)
      │  Databricks Autoloader (cloudFiles, schema evolution + checkpointing)
      ▼
bronze (ADLS Gen2, landed data)
      │  Databricks Workflows — parameterized Silver notebooks (ForEach + If/Else)
      ▼
silver (ADLS Gen2, cleaned Delta tables)
      │  Delta Live Tables (expectations, staging → view → final pattern)
      ▼
gold (Unity Catalog managed/external Delta tables)
      │  Partner Connect + SQL Warehouse
      ▼
Power BI
```

---

## 1. Ingestion — Azure Data Factory

A single pipeline pulls five source files from a public GitHub repo: `netflix_titles.csv` (the master dataset, handed off to Databricks Autoloader) plus four lookup tables — `netflix_directors`, `netflix_cast`, `netflix_countries`, `netflix_category` — copied via ADF's HTTP linked service and a parameter-driven `ForEach`.

| Step | Screenshot |
|---|---|
| Linked services — GitHub (HTTP) + ADLS Gen2 | ![Linked Services](netflix_adf_ss/01-linked-services-overview.png) |
| Dataset — `ds_github` (GitHub source connection) | ![ds_github](netflix_adf_ss/02-dataset-ds-github-connection.png) |
| Dataset — `ds_sink` (dynamic bronze/raw path) | ![ds_sink](netflix_adf_ss/03-dataset-ds-sink-connection.png) |
| Dataset — `ds_validation` (checks `netflix_titles.csv` landed) | ![ds_validation](netflix_adf_ss/04-dataset-ds-validation-connection.png) |
| Pipeline canvas — Web → Set Variable → Validation → ForEach | ![Pipeline Canvas](netflix_adf_ss/05-pipeline-canvas-overview.png) |
| Web activity — pulls GitHub repo metadata | ![Web Activity](netflix_adf_ss/06-web-activity-settings.png) |
| Set Variable — stores GitHub metadata for later use | ![Set Variable](netflix_adf_ss/07-set-variable-settings.png) |
| Validation activity — gates the run on `netflix_titles.csv` existing in `raw` | ![Validation Activity](netflix_adf_ss/08-validation-activity-settings.png) |
| ForEach — iterates the `p_array` parameter (one entry per lookup file) | ![ForEach Settings](netflix_adf_ss/09-foreach-settings.png) |
| ForEach — full settings view | ![ForEach Full](netflix_adf_ss/10-foreach-settings-full.png) |
| Copy Data — source (dynamic `file_name` per array item) | ![Copy Source](netflix_adf_ss/11-copydata-source-settings.png) |
| Copy Data — sink (dynamic `folder_name`/`file_name` into `bronze`) | ![Copy Sink](netflix_adf_ss/12-copydata-sink-settings.png) |
| Successful pipeline run — every activity green | ![Pipeline Run Success](netflix_adf_ss/13-pipeline-run-activity-list-success.png) |

**Key pattern:** a `Web` activity + `Set Variable` capture GitHub metadata, a `Validation` activity confirms the master CSV has landed before anything downstream runs, and a `ForEach` over an array parameter (`p_array`) drives a single parameterized `Copy Data` activity across all four lookup files — adding a fifth lookup table is a metadata change, not a new pipeline.

---

## 2. Storage — ADLS Gen2 (raw / Bronze / Silver / Gold)

| Step | Screenshot |
|---|---|
| All containers — `raw`, `bronze`, `silver`, `gold`, `metastore` | ![Containers Overview](netflix_setup_ss/03-datalake-containers-overview.png) |
| `raw/netflix_titles.csv` — landed by ADF/Autoloader source | ![Raw Netflix Titles](netflix_setup_ss/04-raw-container-netflix-titles.png) |
| `bronze` — one folder per table, plus Autoloader `checkpoints` | ![Bronze Listing](netflix_datalake_ss/01-bronze-container-listing.png) |
| `bronze/netflix_category` — landed lookup file | ![Bronze Category File](netflix_datalake_ss/02-bronze-netflix-category-file.png) |
| `silver/netflix_cast` — written as Delta (data + `_delta_log`) | ![Silver Cast Listing](netflix_datalake_ss/03-silver-netflix-cast-listing.png) |
| Silver `_delta_log` — Delta transaction log | ![Silver Delta Log](netflix_datalake_ss/04-silver-netflix-cast-delta-log.png) |
| `gold` — only Unity Catalog's internal `__unitystorage` folder | ![Gold Unitystorage](netflix_datalake_ss/05-gold-container-unitystorage.png) |

**Note on Gold storage:** Gold tables are Unity Catalog managed tables, so their physical files live under `gold/__unitystorage/...` organized by internal IDs rather than table names — normal Unity Catalog behavior.

---

## 3. Databricks — Bronze via Autoloader

Notebook: `1_Autoloader`. Streams `raw/netflix_titles.csv` into Bronze incrementally, tracking progress with a checkpoint so re-runs only pick up new data.

| Step | Screenshot |
|---|---|
| Notebook overview — catalog/schema setup, checkpoint path | ![Notebook Overview](netflix_db_ss/01-notebooks-overview-1_Autoloader-createschema-checkpoint.png) |
| `readStream` with Autoloader (`cloudFiles`, schema tracking) | ![Autoloader readStream](netflix_db_ss/02-1_Autoloader-readstream-cloudfiles.png) |
| `writeStream` — checkpoint + `trigger(availableNow=True)` into Bronze | ![Autoloader writeStream](netflix_db_ss/03-1_Autoloader-writestream-trigger-availablenow.png) |
| Bronze Delta read-back — confirms data landed | ![Bronze Output](netflix_db_ss/04-bronze-netflix-titles-display-output.png) |

```python
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", checkpoint_location) \
    .load("abfss://raw@netflixprojectdatalake.dfs.core.windows.net")

df.writeStream \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(availableNow=True) \
    .start("abfss://bronze@netflixprojectdatalake.dfs.core.windows.net/netflix_titles")
```

`trigger(availableNow=True)` processes everything currently sitting in `raw` and then stops — an incremental batch, not an always-on stream — which fits a daily/manual pipeline better than a continuously running job.

---

## 4. Databricks — Orchestration with Workflows

Two jobs coordinate the Silver layer, backed by five notebooks:

| Notebook | Role |
|---|---|
| `2_Silver` | Parameterized lookup-table cleaner — takes `sourcefolder`/`targetfolder` widgets, reads the matching Bronze CSV, writes it to Silver |
| `3_Silverlookup` | Builds the array of the four lookup tables (`sourcefolder`/`targetfolder` pairs) and publishes it with `dbutils.jobs.taskValues.set` so a ForEach task can consume it |
| `4_Silver_transformation` | Full cleaning of the master `netflix_titles` table (type casting, column splits, dedup/aggregation checks) |
| `5_Silverlookup` | Reads today's ISO weekday into a widget and publishes it via `taskValues.set` |
| `6_Silver_print` | Reads that weekday value back with `taskValues.get` (the "nothing to do today" branch) |

### 4a. Job: "Bronze to Silver Load" — lookup tables

`Lookup_folder_location` (`3_Silverlookup`) builds the file array, then `Silver_notebook_iteration` runs `2_Silver` once per lookup table via a **"Loop over this task"** ForEach — one job run processes all four lookup tables.

| Step | Screenshot |
|---|---|
| Job run — both tasks succeeded, ForEach looped 4× | ![Bronze to Silver Load Run](netflix_db_ss/18-workflow-bronze-to-silver-load-run-graph.png) |
| Job task graph detail | ![Bronze to Silver Load Tasks](netflix_db_ss/19-job-bronze-to-silver-load-tasks-details.png) |
| `3_Silverlookup` — builds the files array | ![Files Array](netflix_db_ss/13-3_Silverlookup-files-array.png) |
| `3_Silverlookup` — publishes the array via `taskValues.set` | ![taskValues.set Array](netflix_db_ss/15-3_Silverlookup-taskvalues-set-array.png) |
| `2_Silver` — widgets + dynamic Bronze read | ![2_Silver Widgets](netflix_db_ss/12-2_Silver-widgets-lookup-read-csv.png) |
| `2_Silver`/lookup write — append mode into Silver | ![Lookup Write Append](netflix_db_ss/14-3_Silverlookup-write-delta-append.png) |

### 4b. Job: "Silver Load titles" — weekday-gated master data

`WeekdayLookup` (`5_Silverlookup`) reads today's ISO weekday into a widget and publishes it; an `If/Else` condition checks whether it equals `7` (Sunday); the **True** branch runs `SilverMasterData` (the heavy `4_Silver_transformation` cleaning job), the **False** branch runs `FalseNotebook` (`6_Silver_print`, which just reads the value back and exits) — so the expensive master-data rebuild only runs once a week.

| Step | Screenshot |
|---|---|
| Job task graph — WeekdayLookup → If/Else → two branches | ![Silver Load Titles Tasks](netflix_db_ss/20-job-silver-load-titles-ifelse-tasks.png) |
| Job run — False branch taken, succeeded | ![Silver Load Titles Run](netflix_db_ss/21-job-silver-load-titles-run-false-branch-success.png) |
| `5_Silverlookup` — weekday widget + `taskValues.set` | ![Weekday Widget](netflix_db_ss/16-5_Silverlookup-weekday-widget-taskvalues-set.png) |
| `6_Silver_print` — reads it back with `taskValues.get` | ![taskValues.get](netflix_db_ss/17-6_Silver_print-taskvalues-get-weekoutput.png) |

```python
# 5_Silverlookup
dbutils.widgets.text("weekday", "7")
var = int(dbutils.widgets.get("weekday"))
dbutils.jobs.taskValues.set(key="weekoutput", value=var)

# 6_Silver_print
var = dbutils.jobs.taskValues.get(taskKey="WeekdayLookup", key="weekoutput")
print(var)
```

### 4c. Silver cleaning — `4_Silver_transformation`

| Step | Screenshot |
|---|---|
| Read Bronze `netflix_titles` as Delta | ![Read Bronze Delta](netflix_db_ss/11-4_Silver_transformation-read-bronze-delta.png) |
| `rating` column — split on `-` to drop qualifiers | ![Rating Split](netflix_db_ss/06-4_Silver_transformation-rating-split.png) |
| `duration_minutes`/`duration_seasons` cast to Integer + schema check | ![Duration Cast](netflix_db_ss/07-4_Silver_transformation-duration-cast-printschema.png) |
| `title` → `shorttitle` split (drops the subtitle after `:`) | ![Shorttitle Split](netflix_db_ss/08-4_Silver_transformation-shorttitle-split.png) |
| `groupBy("type").agg(count)` — sanity-check row counts by Movie/TV Show | ![GroupBy Count](netflix_db_ss/09-4_Silver_transformation-groupby-agg-count.png) |
| Temp view + SQL query against the cleaned frame | ![Temp View SQL](netflix_db_ss/10-4_Silver_transformation-tempview-sql-query.png) |
| Final write — Delta, overwrite, into `silver/netflix_titles` | ![Silver Write Overwrite](netflix_db_ss/05-4_Silver_transformation-write-delta-overwrite.png) |

---

## 5. Gold Layer — Delta Live Tables

Pipeline **"Gold Load"**, built in notebook `7_DLT_Gold`. Four lookup tables stream straight through with a not-null `show_id` expectation; `netflix_titles` goes through a three-layer staging → view → final pattern so an extra transformation (`new_flag`) can be applied before the stricter final-table rules are enforced.

| Step | Screenshot |
|---|---|
| Pipeline DAG — all 7 tables, completed run | ![Gold DAG](netflix_db_ss/22-dlt-gold-load-pipeline-dag-completed.png) |
| Pipeline details panel | ![Pipeline Details](netflix_db_ss/23-dlt-gold-load-pipeline-details-panel.png) |
| Tables list — output counts + expectation status | ![Tables List](netflix_db_ss/24-dlt-gold-load-tables-list-expectations.png) |
| Code — imports + shared `expect_all_or_drop` rule set | ![DLT Imports](netflix_db_ss/25-dlt-7_DLT_Gold-code-import-lookuptables-rules.png) |
| `gold_netflix_directors` / `gold_netflix_cast` table defs | ![Directors/Cast](netflix_db_ss/26-dlt-7_DLT_Gold-directors-cast-table-defs.png) |
| `gold_netflix_countries` / `gold_netflix_category` table defs | ![Countries/Category](netflix_db_ss/27-dlt-7_DLT_Gold-countries-category-table-defs.png) |
| `gold_netflix_titles_stg` → `gold_netflix_titles_trans` view | ![Titles Staging/View](netflix_db_ss/28-dlt-7_DLT_Gold-titles-stg-view-pattern.png) |
| `gold_netflix_titles` — final table with masterdata rules | ![Titles Final](netflix_db_ss/29-dlt-7_DLT_Gold-titles-masterdata-rules-final.png) |
| Query proof — `gold_netflix_directors` populated | ![Query Result](netflix_db_ss/30-dlt-gold-directors-query-result.png) |

```python
lookuptables_rules = {"rule1": "show_id is NOT NULL"}

@dlt.table(name="gold_netflix_directors")
@dlt.expect_all_or_drop(lookuptables_rules)
def gold_netflix_directors():
    return (spark.readStream.format("delta")
            .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_directors"))

# same pattern for gold_netflix_cast, gold_netflix_countries, gold_netflix_category

@dlt.table(name="gold_netflix_titles_stg")
def gold_netflix_titles_stg():
    return (spark.readStream.format("delta")
            .load("abfss://silver@netflixprojectdatalake.dfs.core.windows.net/netflix_titles"))

@dlt.view(name="gold_netflix_titles_trans")
def gold_netflix_titles_trans():
    return spark.readStream.table("gold_netflix_titles_stg").withColumn("new_flag", lit(1))

masterdata_rules = {"rule1": "new_flag is NOT NULL", "rule2": "show_id is NOT NULL"}

@dlt.table(name="gold_netflix_titles")
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflix_titles():
    return spark.readStream.table("gold_netflix_titles_trans")
```

---

## 6. Governance — Unity Catalog

| Step | Screenshot |
|---|---|
| Access Connector for Azure Databricks (`access_netflix`) | ![Access Connector](netflix_setup_ss/02-access-connector-overview.png) |
| Storage Credential (`ash_creds`) — wraps the connector's managed identity | ![Storage Credential](netflix_db_ss/31-unitycatalog-external-data-credentials.png) |
| External Locations — `raw` / `bronze` / `silver` / `gold`, all on `ash_creds` | ![External Locations](netflix_db_ss/32-unitycatalog-external-locations-all-4.png) |

Unity Catalog reaches the data lake through the Access Connector (a managed identity) rather than embedded storage keys. One **External Location** per container points at its `abfss://` path and is backed by the same **Storage Credential**, so every Bronze/Silver/Gold read or write in every notebook above goes through this single, centrally governed identity. Tables live in `netflix_catalog.netflix_elt_schema`.

---

## 7. Serving — Power BI

Gold tables are queried directly from Power BI through Databricks' built-in **Partner Connect**: from the Gold catalog/schema in Databricks, Partner Connect sets up (or reuses) a **SQL Warehouse** and generates a ready-to-use connection — downloaded as a `.pbids` file that opens straight into Power BI Desktop with the server hostname, HTTP path, and catalog/schema already filled in. Power BI authenticates against the SQL Warehouse (Azure AD or a personal access token) and queries the Gold Delta tables directly — no data copy or separate export step — so changes made by the DLT pipeline are visible in the report on the next refresh.

*(No screenshots captured for this step yet — this section documents the intended connection path. A short walkthrough can be added once the Power BI side is built out.)*

---

## Tech Stack

- **Orchestration:** Azure Data Factory (HTTP ingestion, parameterized ForEach) + Databricks Workflows (ForEach, If/Else branching)
- **Storage:** Azure Data Lake Storage Gen2 (raw / Bronze / Silver / Gold medallion architecture)
- **Compute:** Azure Databricks (Autoloader, Delta Live Tables, Unity Catalog)
- **Governance:** Unity Catalog with an Access Connector managed identity (no embedded storage keys)
- **Serving:** Partner Connect + Databricks SQL Warehouse → Power BI
- **Source data:** Public GitHub repo (simulated Netflix catalog CSVs)

## Repository Structure

```
├── adf_pipelines/       # ADF pipeline, dataset, and linked service JSON
├── databricks/          # notebooks: 1_Autoloader, 2_Silver, 3_Silverlookup,
│                         # 4_Silver_transformation, 5_Silverlookup, 6_Silver_print,
│                         # 7_DLT_Gold
├── source_scripts/       # source data generation scripts
└── screenshots/          # netflix_setup_ss, netflix_adf_ss, netflix_datalake_ss, netflix_db_ss
```
