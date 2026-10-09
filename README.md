# Netflix Azure Data Engineering Project

![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-00A972?style=for-the-badge)
![ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

A full Azure data pipeline built on a sample Netflix dataset. Azure Data Factory pulls in the data, it moves through Bronze/Silver/Gold stages on ADLS Gen2 (loaded incrementally with Databricks Autoloader), Databricks Workflows handle the orchestration (looping through tables, and branching to skip steps when not needed), and the final Gold layer is built and governed with Delta Live Tables under Unity Catalog.

## Project Overview

### The Problem

New data doesn't all show up the same way. Some files (the lookup tables) get added or updated on their own schedule. The full title dataset only needs a heavy cleanup once a week, not every time the pipeline runs. And if the pipeline doesn't check that a file actually arrived before processing it, it'll quietly process bad or missing data without anyone noticing.

### The Solution

Only process what's new, only do the expensive work when it's actually needed, and always check the data before trusting it. One pipeline loops through all the lookup tables instead of needing a separate pipeline for each one. Autoloader keeps track of what's already been loaded, so re-runs don't reprocess the same files. An If/Else check means the full title cleanup only runs once a week. And Delta Live Tables checks each row before it reaches the Gold layer, so bad data gets dropped instead of breaking a report downstream.

| Technique | First principle |
|---|---|
| `ForEach`-driven ADF pipeline (`p_array`) | One pipeline, driven by metadata — a new lookup table is config, not a new pipeline |
| Validation activity gates the run | Don't process downstream until the source file has actually landed |
| Autoloader (`cloudFiles`) incremental ingestion | Process each file once; adapt to schema changes automatically, no full reprocessing |
| If/Else weekday gate on the heavy Silver job | Don't pay the cost of a full master-data rebuild every run — only when it's actually needed |
| `taskValues` (array hand-off, weekday flag) | Pass state between tasks without hardcoding values or standing up external state storage |
| DLT expectations (`expect_all_or_drop`) | Bad rows get dropped before they reach Gold, not discovered after a report breaks |
| Staging → view → final table pattern (titles) | Separate "landed" from "business-ready" so each stage's quality rule is explicit |
| Keyless governance (Access Connector + Unity Catalog) | Identity grants access — no keys to leak |

Same rule everywhere: the pipeline does more work only when the data actually demands it — never on a fixed schedule regardless of need.

### Why This Stands Out

Every technique here earns its place by making the pipeline cheaper to run or safer to trust, not by checking a box. The weekday gate means the full master-data transformation — the most expensive Silver job — runs once a week instead of on every trigger, without a separate scheduler or manual toggle. The `ForEach` array pattern means adding a fifth lookup table is a one-line config change, not a new pipeline or a new notebook. Delta Live Tables expectations catch bad rows at the Gold boundary, where a silent failure would otherwise surface as a wrong number in a Power BI report days later. Unity Catalog's Access Connector removes storage keys from every notebook and pipeline entirely. Together, these are the choices that keep the pipeline efficient and trustworthy as more sources get added — not just working once, but working well as it grows.

## Architecture Overview

![Netflix Architecture Flow](screenshots/netflix_setup_ss/00-netflix_architecture_flow.png)

GitHub source files are pulled in by Azure Data Factory, landed in a `raw`/`bronze` data lake via Databricks Autoloader, cleaned into Silver by Databricks Workflows (ForEach for the lookup tables, If/Else to gate the weekly master-data rebuild), modeled into governed Gold tables with Delta Live Tables under Unity Catalog, then served to Power BI through a Databricks SQL Warehouse via Partner Connect.

---

## Resource Group

![Resource Group Overview](screenshots/netflix_setup_ss/01-resource-group-overview.png)

All resources live in a single resource group (`RG-NetflixProject`):

| Resource | Purpose |
|---|---|
| `adf-netflixproject-ash` | Azure Data Factory — HTTP ingestion of source files from GitHub into the data lake |
| `netflixprojectdatalake` | ADLS Gen2 storage account — `raw` / `bronze` / `silver` / `gold` containers |
| `netflix_adb` | Azure Databricks workspace — Autoloader, Workflows, Delta Live Tables, Unity Catalog |
| `access_netflix` | Access Connector for Azure Databricks — lets Unity Catalog read/write the data lake via managed identity |

---

## 1. Ingestion — Azure Data Factory

A single pipeline pulls five source files from a public GitHub repo: `netflix_titles.csv` (the master dataset, handed off to Databricks Autoloader) plus four lookup tables — `netflix_directors`, `netflix_cast`, `netflix_countries`, `netflix_category` — copied via ADF's HTTP linked service and a parameter-driven `ForEach`.

| Step | Screenshot |
|---|---|
| Linked services — GitHub (HTTP) + ADLS Gen2 | ![Linked Services](screenshots/netflix_adf_ss/01-linked-services-overview.png) |
| Dataset — `ds_github` (GitHub source connection) | ![ds_github](screenshots/netflix_adf_ss/02-dataset-ds-github-connection.png) |
| Dataset — `ds_sink` (dynamic bronze/raw path) | ![ds_sink](screenshots/netflix_adf_ss/03-dataset-ds-sink-connection.png) |
| Dataset — `ds_validation` (checks `netflix_titles.csv` landed) | ![ds_validation](screenshots/netflix_adf_ss/04-dataset-ds-validation-connection.png) |
| Pipeline canvas — Web → Set Variable → Validation → ForEach | ![Pipeline Canvas](screenshots/netflix_adf_ss/05-pipeline-canvas-overview.png) |
| Web activity — pulls GitHub repo metadata | ![Web Activity](screenshots/netflix_adf_ss/06-web-activity-settings.png) |
| Set Variable — stores GitHub metadata for later use | ![Set Variable](screenshots/netflix_adf_ss/07-set-variable-settings.png) |
| Validation activity — gates the run on `netflix_titles.csv` existing in `raw` | ![Validation Activity](screenshots/netflix_adf_ss/08-validation-activity-settings.png) |
| ForEach — iterates the `p_array` parameter (one entry per lookup file) | ![ForEach Settings](screenshots/netflix_adf_ss/09-foreach-settings.png) |
| ForEach — full settings view | ![ForEach Full](screenshots/netflix_adf_ss/10-foreach-settings-full.png) |
| Copy Data — source (dynamic `file_name` per array item) | ![Copy Source](screenshots/netflix_adf_ss/11-copydata-source-settings.png) |
| Copy Data — sink (dynamic `folder_name`/`file_name` into `bronze`) | ![Copy Sink](screenshots/netflix_adf_ss/12-copydata-sink-settings.png) |
| Successful pipeline run — every activity green | ![Pipeline Run Success](screenshots/netflix_adf_ss/13-pipeline-run-activity-list-success.png) |

**Key pattern:** a `Web` activity + `Set Variable` capture GitHub metadata, a `Validation` activity confirms the master CSV has landed before anything downstream runs, and a `ForEach` over an array parameter (`p_array`) drives a single parameterized `Copy Data` activity across all four lookup files — adding a fifth lookup table is a metadata change, not a new pipeline.

---

## 2. Storage — ADLS Gen2 (raw / Bronze / Silver / Gold)

| Step | Screenshot |
|---|---|
| All containers — `raw`, `bronze`, `silver`, `gold`, `metastore` | ![Containers Overview](screenshots/netflix_setup_ss/03-datalake-containers-overview.png) |
| `raw/netflix_titles.csv` — landed by ADF/Autoloader source | ![Raw Netflix Titles](screenshots/netflix_setup_ss/04-raw-container-netflix-titles.png) |
| `bronze` — one folder per table, plus Autoloader `checkpoints` | ![Bronze Listing](screenshots/netflix_datalake_ss/01-bronze-container-listing.png) |
| `bronze/netflix_category` — landed lookup file | ![Bronze Category File](screenshots/netflix_datalake_ss/02-bronze-netflix-category-file.png) |
| `silver/netflix_cast` — written as Delta (data + `_delta_log`) | ![Silver Cast Listing](screenshots/netflix_datalake_ss/03-silver-netflix-cast-listing.png) |
| Silver `_delta_log` — Delta transaction log | ![Silver Delta Log](screenshots/netflix_datalake_ss/04-silver-netflix-cast-delta-log.png) |
| `gold` — only Unity Catalog's internal `__unitystorage` folder | ![Gold Unitystorage](screenshots/netflix_datalake_ss/05-gold-container-unitystorage.png) |

**Note on Gold storage:** Gold tables are Unity Catalog managed tables, so their physical files live under `gold/__unitystorage/...` organized by internal IDs rather than table names — normal Unity Catalog behavior.

---

## 3. Databricks — Bronze via Autoloader

Notebook: `1_Autoloader`. Streams `raw/netflix_titles.csv` into Bronze incrementally, tracking progress with a checkpoint so re-runs only pick up new data.

| Step | Screenshot |
|---|---|
| Notebook overview — catalog/schema setup, checkpoint path | ![Notebook Overview](screenshots/netflix_db_ss/01-notebooks-overview-1_Autoloader-createschema-checkpoint.png) |
| `readStream` with Autoloader (`cloudFiles`, schema tracking) | ![Autoloader readStream](screenshots/netflix_db_ss/02-1_Autoloader-readstream-cloudfiles.png) |
| `writeStream` — checkpoint + `trigger(availableNow=True)` into Bronze | ![Autoloader writeStream](screenshots/netflix_db_ss/03-1_Autoloader-writestream-trigger-availablenow.png) |
| Bronze Delta read-back — confirms data landed | ![Bronze Output](screenshots/netflix_db_ss/04-bronze-netflix-titles-display-output.png) |

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
| Job run — both tasks succeeded, ForEach looped 4× | ![Bronze to Silver Load Run](screenshots/netflix_db_ss/18-workflow-bronze-to-silver-load-run-graph.png) |
| Job task graph detail | ![Bronze to Silver Load Tasks](screenshots/netflix_db_ss/19-job-bronze-to-silver-load-tasks-details.png) |
| `3_Silverlookup` — builds the files array | ![Files Array](screenshots/netflix_db_ss/13-3_Silverlookup-files-array.png) |
| `3_Silverlookup` — publishes the array via `taskValues.set` | ![taskValues.set Array](screenshots/netflix_db_ss/15-3_Silverlookup-taskvalues-set-array.png) |
| `2_Silver` — widgets + dynamic Bronze read | ![2_Silver Widgets](screenshots/netflix_db_ss/12-2_Silver-widgets-lookup-read-csv.png) |
| `2_Silver`/lookup write — append mode into Silver | ![Lookup Write Append](screenshots/netflix_db_ss/14-3_Silverlookup-write-delta-append.png) |

### 4b. Job: "Silver Load titles" — weekday-gated master data

`WeekdayLookup` (`5_Silverlookup`) reads today's ISO weekday into a widget and publishes it; an `If/Else` condition checks whether it equals `7` (Sunday); the **True** branch runs `SilverMasterData` (the heavy `4_Silver_transformation` cleaning job), the **False** branch runs `FalseNotebook` (`6_Silver_print`, which just reads the value back and exits) — so the expensive master-data rebuild only runs once a week.

| Step | Screenshot |
|---|---|
| Job task graph — WeekdayLookup → If/Else → two branches | ![Silver Load Titles Tasks](screenshots/netflix_db_ss/20-job-silver-load-titles-ifelse-tasks.png) |
| Job run — False branch taken, succeeded | ![Silver Load Titles Run](screenshots/netflix_db_ss/21-job-silver-load-titles-run-false-branch-success.png) |
| `5_Silverlookup` — weekday widget + `taskValues.set` | ![Weekday Widget](screenshots/netflix_db_ss/16-5_Silverlookup-weekday-widget-taskvalues-set.png) |
| `6_Silver_print` — reads it back with `taskValues.get` | ![taskValues.get](screenshots/netflix_db_ss/17-6_Silver_print-taskvalues-get-weekoutput.png) |

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
| Read Bronze `netflix_titles` as Delta | ![Read Bronze Delta](screenshots/netflix_db_ss/11-4_Silver_transformation-read-bronze-delta.png) |
| `rating` column — split on `-` to drop qualifiers | ![Rating Split](screenshots/netflix_db_ss/06-4_Silver_transformation-rating-split.png) |
| `duration_minutes`/`duration_seasons` cast to Integer + schema check | ![Duration Cast](screenshots/netflix_db_ss/07-4_Silver_transformation-duration-cast-printschema.png) |
| `title` → `shorttitle` split (drops the subtitle after `:`) | ![Shorttitle Split](screenshots/netflix_db_ss/08-4_Silver_transformation-shorttitle-split.png) |
| `groupBy("type").agg(count)` — sanity-check row counts by Movie/TV Show | ![GroupBy Count](screenshots/netflix_db_ss/09-4_Silver_transformation-groupby-agg-count.png) |
| Temp view + SQL query against the cleaned frame | ![Temp View SQL](screenshots/netflix_db_ss/10-4_Silver_transformation-tempview-sql-query.png) |
| Final write — Delta, overwrite, into `silver/netflix_titles` | ![Silver Write Overwrite](screenshots/netflix_db_ss/05-4_Silver_transformation-write-delta-overwrite.png) |

---

## 5. Gold Layer — Delta Live Tables

Pipeline **"Gold Load"**, built in notebook `7_DLT_Gold`. Four lookup tables stream straight through with a not-null `show_id` expectation; `netflix_titles` goes through a three-layer staging → view → final pattern so an extra transformation (`new_flag`) can be applied before the stricter final-table rules are enforced.

| Step | Screenshot |
|---|---|
| Pipeline DAG — all 7 tables, completed run | ![Gold DAG](screenshots/netflix_db_ss/22-dlt-gold-load-pipeline-dag-completed.png) |
| Pipeline details panel | ![Pipeline Details](screenshots/netflix_db_ss/23-dlt-gold-load-pipeline-details-panel.png) |
| Tables list — output counts + expectation status | ![Tables List](screenshots/netflix_db_ss/24-dlt-gold-load-tables-list-expectations.png) |
| Code — imports + shared `expect_all_or_drop` rule set | ![DLT Imports](screenshots/netflix_db_ss/25-dlt-7_DLT_Gold-code-import-lookuptables-rules.png) |
| `gold_netflix_directors` / `gold_netflix_cast` table defs | ![Directors/Cast](screenshots/netflix_db_ss/26-dlt-7_DLT_Gold-directors-cast-table-defs.png) |
| `gold_netflix_countries` / `gold_netflix_category` table defs | ![Countries/Category](screenshots/netflix_db_ss/27-dlt-7_DLT_Gold-countries-category-table-defs.png) |
| `gold_netflix_titles_stg` → `gold_netflix_titles_trans` view | ![Titles Staging/View](screenshots/netflix_db_ss/28-dlt-7_DLT_Gold-titles-stg-view-pattern.png) |
| `gold_netflix_titles` — final table with masterdata rules | ![Titles Final](screenshots/netflix_db_ss/29-dlt-7_DLT_Gold-titles-masterdata-rules-final.png) |
| Query proof — `gold_netflix_directors` populated | ![Query Result](screenshots/netflix_db_ss/30-dlt-gold-directors-query-result.png) |

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
| Access Connector for Azure Databricks (`access_netflix`) | ![Access Connector](screenshots/netflix_setup_ss/02-access-connector-overview.png) |
| Storage Credential (`ash_creds`) — wraps the connector's managed identity | ![Storage Credential](screenshots/netflix_db_ss/31-unitycatalog-external-data-credentials.png) |
| External Locations — `raw` / `bronze` / `silver` / `gold`, all on `ash_creds` | ![External Locations](screenshots/netflix_db_ss/32-unitycatalog-external-locations-all-4.png) |

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
