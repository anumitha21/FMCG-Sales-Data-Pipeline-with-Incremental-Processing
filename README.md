# FMCG-Sales-Data-Pipeline-with-Incremental-Processing
### End-to-End incremental data pipeline on Databricks for FMCG order analytics, enabling efficient monthly recalculation using Spark and Delta Lake.
---

## Project Architecture

![Project Architecture](resources/project_architecture.png)

---

## Business Context — The Acquisition Use Case

**Parent Company A** has acquired **Child Company B** in the FMCG (Fast-Moving Consumer Goods) sector. Both companies operate independently with completely different data schemas, source systems, and reporting granularities.

The engineering challenge:

> *How do you unify two companies with different schemas, different data cadences, and different source systems into a single, analytics-ready data pipeline — without disrupting ongoing operations?*

| | Parent Company | Child Company |
|---|---|---|
| **Data Source** | Historical CSV exports via Databricks Volumes | Daily operational CSV files in Amazon S3 landing zone |
| **Schema Style** | Pre-denormalized flat structure | Normalized relational (customers, products, pricing, orders) |
| **Load Strategy** | Historical full load + incremental via `COPY INTO` | Full load followed by daily rolling incremental |
| **Granularity** | Monthly | Daily |
| **Ingestion Method** | Direct Gold load using SQL `COPY INTO` | Medallion pipeline: Bronze → Silver → Gold |

The solution harmonizes both data sources into a **unified Gold-layer analytics table** in the Databricks Unity Catalog, enabling cross-company business intelligence.

---

## Solution Architecture

The pipeline is built on **Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold), with Delta Lake as the open storage format, PySpark for distributed transformations, and Databricks Lakeflow Jobs for orchestration.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Amazon S3                                                                  │
│  ┌──────────────┐   ┌──────────────┐                                        │
│  │  RAW Data    │   │   Archive    │  ◄── Processed files moved here         │
│  │  (landing/)  │   │ (processed/) │                                        │
│  └──────┬───────┘   └──────────────┘                                        │
└─────────│───────────────────────────────────────────────────────────────────┘
          │
          ▼  Lakeflow Jobs (Databricks)
┌─────────────────────────────────────────────────────────────────────────────┐
│  Unity Catalog  ─  fmcg                                                     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │   Child Company Pipeline                                            │    │
│  │                                                                     │    │
│  │   Bronze (fmcg.bronze)  ──►  Silver (fmcg.silver)  ──►  Gold (Child) │  │
│  │   Raw Delta + CDF          Cleaned + Deduped          Fact + Dims   │    │
│  └──────────────────────────────────────────────┬──────────────────────┘    │
│                                                 │ MERGE                     │
│  Parent Company CSV ─────────────────────────── ▼ ───────────────────────── │
│  (COPY INTO)                          Gold Analytics Table                  │
│                                       fmcg.gold.fact_orders                 │
│                                       (Parent ORG — unified view)           │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────┐
│   Serving Layer         │
│  ┌───────────────────┐  │
│  │    Dashboards     │  │  ◄── vw_fact_orders_enriched
│  └───────────────────┘  │
│  ┌───────────────────┐  │
│  │  Databricks Genie │  │  ◄── Natural Language Querying
│  └───────────────────┘  │
└─────────────────────────┘
```

---

## Technical Stack

| Technology | Role in This Project |
|---|---|
| **Databricks** | Unified analytics platform — notebooks, Lakeflow Jobs, Unity Catalog, Genie |
| **Apache Spark / PySpark** | Distributed data processing and transformation across all pipeline layers |
| **Delta Lake** | ACID-compliant open-format storage; all Bronze, Silver, Gold tables are Delta |
| **Change Data Feed (CDF)** | Row-level change tracking on all Delta tables for efficient incremental processing |
| **Databricks Unity Catalog** | Centralized data governance — catalog `fmcg` with `bronze`, `silver`, `gold` schemas |
| **Databricks Lakeflow Jobs** | Workflow orchestration for automated full and incremental pipeline runs |
| **Amazon S3** | Source data storage — landing zone for new files, archive for processed files |
| **SQL `COPY INTO`** | Idempotent ingestion of parent company CSVs directly into the Gold layer |
| **Databricks Genie** | AI-powered natural language interface on top of the Gold analytics view |

---

## Repository Structure

```
FMCG-DATAENGINEERING/
│
├── data/
│   ├── parent_company/
│   │   ├── full_load/              # dim_customers, dim_products, dim_gross_price, fact_orders
│   │   └── incremental_load/       # Incremental fact_orders CSV + COPY INTO SQL query
│   │
│   └── child_company/
│       ├── full_load/              # customers, products, gross_price, orders (landing CSVs)
│       └── incremental_load/       # Daily order files: orders_YYYY_MM_DD.csv
│
├── notebook_code/
│   ├── setup/
│   │   ├── setup_catalog.ipynb         # Creates fmcg catalog + bronze/silver/gold schemas
│   │   ├── utilities.ipynb             # Shared schema variables (bronze_schema, silver_schema, gold_schema)
│   │   └── dim_date_table_creation.ipynb  # Generates date dimension in Gold layer
│   │
│   ├── dimension_data_processing/
│   │   ├── customers_data_processing.ipynb   # Bronze → Silver → Gold for customer master
│   │   ├── products_data_processing.ipynb    # Bronze → Silver → Gold for product master
│   │   └── pricing_data_processing.ipynb     # Bronze → Silver → Gold for gross price data
│   │
│   └── fact_data_processing/
│       ├── full_load_fact.ipynb              # Full historical load: Bronze → Silver → Gold
│       └── incremental_load_fact.ipynb       # Daily incremental load + merge with parent Gold
│
├── dashboards/
│   ├── denormalise_table_query_fmcg.txt    # SQL view creation for BI denormalization
│   └── fmcg_dashboard.pdf                  # Sample dashboard export
│
├── resources/
│   ├── project_architecture.png            # Architecture diagram
│   └──        # Editable architecture source file
│
└── snapshots/                                 # Supporting table view in databricks
```

---

## Pipeline Deep Dive

### 1. Unity Catalog Setup

The pipeline uses Databricks **Unity Catalog** for centralized governance. Three schemas are created under the `fmcg` catalog:

| Schema | Purpose |
|---|---|
| `fmcg.bronze` | Raw ingested data — minimal transformation, all metadata preserved |
| `fmcg.silver` | Cleansed, validated, deduplicated data ready for analytics joins |
| `fmcg.gold` | Business-ready fact and dimension tables, plus the unified parent org view |

Shared utility variables (`bronze_schema`, `silver_schema`, `gold_schema`) are defined in `utilities.ipynb` and loaded into every notebook via `%run`.

---

### 2. Full Load — Child Company

**Notebook:** `notebooks/fact_processing/1_full_load_fact.ipynb`

The full load pipeline processes the child company's complete historical order data:

**Bronze Layer**
- Reads all CSV files from the S3 `landing/` path using `spark.read.csv()` with wildcard
- Adds metadata columns: `read_timestamp`, `file_name`, `file_size` via the `_metadata` struct
- Writes to `fmcg.bronze.orders` as a Delta table with **Change Data Feed enabled**
- Moves processed files from `landing/` to `processed/` (archive) using `dbutils.fs.mv()`

**Silver Layer**
- Reads from the Bronze Delta table
- Applies PySpark transformations:
  - Drops rows with null `order_qty`
  - Validates and cleanses `customer_id` (non-numeric values replaced with `999999`)
  - Normalises `order_placement_date` — strips weekday prefixes and parses multiple date formats (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `MMMM dd, yyyy`) via `F.coalesce(F.try_to_date(...))`
  - Deduplicates on natural key: `order_id + order_placement_date + customer_id + product_id + order_qty`
- Joins with `fmcg.silver.products` to enrich with `product_code`
- Writes to `fmcg.silver.orders` (Delta, CDF enabled, `mergeSchema = true`)

**Gold Layer**
- Selects and renames columns to the canonical analytical schema: `order_id`, `date`, `customer_code`, `product_code`, `product_id`, `sold_quantity`
- Writes to `fmcg.gold.sb_fact_orders` (Delta, CDF enabled)

---

### 3. Incremental Load — Child Company

**Notebook:** `notebooks/fact_processing/2_incremental_load_fact.ipynb`

New order files arrive in the S3 landing zone daily (e.g., `orders_2025_12_01.csv`). The incremental pipeline is optimised to process **only the new batch**, not the entire historical table:

#### Staging Table Pattern

Instead of re-reading the full Bronze table, the incremental load writes the new batch to a **dedicated staging Delta table** (`fmcg.bronze.staging_orders`). All downstream Silver and Gold transformations operate exclusively on this staging table, minimising compute and avoiding full table scans.

```
New files in S3 landing/
        │
        ▼
fmcg.bronze.orders          ◄─── APPEND (full cumulative history)
fmcg.bronze.staging_orders  ◄─── OVERWRITE (current batch only)
        │
        ▼ PySpark Transformations
fmcg.silver.staging_orders  ◄─── OVERWRITE (transformed current batch)
        │
        ▼ Delta MERGE (upsert)
fmcg.silver.orders           ◄─── Upserted with new/changed records
        │
        ▼ Delta MERGE (upsert)
fmcg.gold.sb_fact_orders     ◄─── Updated with incremental records
```

#### Delta MERGE (Upsert)

Both Silver and Gold use `DeltaTable.alias().merge()` for upserts:
- **Match condition** (Silver): `order_placement_date + order_id + product_code + customer_id`
- **Match condition** (Gold): `date + order_id + product_code + customer_code`
- `whenMatchedUpdateAll()` — updates existing records if data has changed
- `whenNotMatchedInsertAll()` — inserts genuinely new records

#### Merging Child Gold into Parent Organization Gold

The child company's orders are at **daily granularity** while the parent's analytics layer expects **monthly granularity**. After the child Gold is updated, the pipeline:

1. Identifies which months are present in the current incremental batch
2. Creates a temp view of affected months (`incremental_months`)
3. Aggregates child daily data to monthly level
4. Performs a targeted Delta MERGE into `fmcg.gold.fact_orders` (the unified parent org table) — touching only the affected months, not the full history

---

### 4. Parent Company — Historical & Incremental Load

The parent company ingests data differently — no Bronze/Silver processing is needed because the data arrives already in a clean, analytics-ready format.

**Historical Full Load**: Parent dimension and fact CSVs are loaded directly into Gold Delta tables using standard Spark writes.

**Incremental Load**: New parent company records are loaded into Gold using Databricks `COPY INTO`:

```sql
COPY INTO fmcg.gold.fact_orders
FROM (
  SELECT 
    CAST(date AS DATE)             AS date,
    product_code,
    customer_code,
    CAST(sold_quantity AS BIGINT)  AS sold_quantity
  FROM '/Volumes/fmcg/gold/parent_incremental_data/fact_orders.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');
```

`COPY INTO` is **idempotent** — re-running it will not duplicate data because Databricks tracks which files have already been loaded.

---

### 5. Change Data Feed (CDF)

**Change Data Feed** is enabled on every Delta table in the pipeline:

```python
.option("delta.enableChangeDataFeed", "true")
```

CDF records every row-level change (insert, update, delete) in a dedicated changelog. This enables:
- **Efficient incremental reads** — downstream jobs only consume changed rows since the last checkpoint
- **Auditing** — full history of every change is stored and queryable
- **Reduced compute** — avoids expensive full-table scans on large historical tables

---

### 6. Gold Analytics View

A fully denormalized view `fmcg.gold.vw_fact_orders_enriched` is created as the single source of truth for all BI tools:

```sql
fact_orders
  LEFT JOIN dim_date        ON date = month_start_date
  LEFT JOIN dim_customers   ON customer_code
  LEFT JOIN dim_products    ON product_code
  LEFT JOIN dim_gross_price ON product_code AND fiscal_year
```

**Derived metric:**
```sql
total_amount_inr = sold_quantity × price_inr
```

This view exposes all attributes needed for cross-company reporting: market, platform, channel, product division/category, fiscal year, quarter, and revenue.

---

## Dimension Tables

| Table | Key Columns | Description |
|---|---|---|
| `fmcg.gold.dim_customers` | `customer_code`, `customer`, `market`, `platform`, `channel` | Customer master — markets include India and international |
| `fmcg.gold.dim_products` | `product_code`, `product`, `division`, `category`, `variant` | Product master with FMCG category hierarchy |
| `fmcg.gold.dim_gross_price` | `product_code`, `fiscal_year`, `price_inr` | Year-based pricing per product |
| `fmcg.gold.dim_date` | `date_key`, `month_start_date`, `year`, `month_name`, `quarter` | Calendar dimension for time-series analysis |

---

## BI Dashboard & Serving Layer

The Gold view powers two serving layer tools:

**Databricks Dashboards** — Pre-built visualisations connected directly to `vw_fact_orders_enriched` for:
- Revenue by market, channel, and platform
- Top products and customers by sold quantity / revenue
- Year-over-year and quarter-over-quarter trend analysis
- Cross-company (parent + child) consolidated reporting

**Databricks Genie** — AI-powered natural language interface allowing business users to query the Gold view conversationally:
- *"Which market had the highest revenue in Q3 2025?"*
- *"Show me top 10 products by sold quantity this year."*
- *"Compare parent company vs child company revenue by division."*

---

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Amazon S3 bucket configured as an external location in Unity Catalog
- Databricks cluster with Delta Lake runtime

### Setup Steps

1. **Configure catalog and schemas**
   - Run `notebooks/setup/setup_catalog.ipynb` to create the `fmcg` catalog and `bronze`, `silver`, `gold` schemas

2. **Generate the date dimension**
   - Run `notebooks/setup/dim_date_table_creation.ipynb`

3. **Load dimension data**
   - Run `notebooks/dimension_processing/1_customers_data_processing.ipynb`
   - Run `notebooks/dimension_processing/2_products_data_processing.ipynb`
   - Run `notebooks/dimension_processing/3_pricing_data_processing.ipynb`

4. **Run the full historical load**
   - Upload child company CSVs to the S3 `landing/` path
   - Run `notebooks/fact_processing/1_full_load_fact.ipynb`

5. **Schedule incremental loads**
   - Configure a Databricks Lakeflow Job to run `notebooks/fact_processing/2_incremental_load_fact.ipynb` on a daily schedule as new order files land in S3

6. **Create the analytics view**
   - Execute the SQL in `dashboards/denormalise_table_query_fmcg.txt` to create `vw_fact_orders_enriched`

7. **Connect your BI tool**
   - Point Databricks Dashboards or any BI tool (Power BI, Tableau, etc.) to `fmcg.gold.vw_fact_orders_enriched`

---

## Data Flow Summary

```
Parent Company CSVs ─────────────────────────────────────────────────────────────────┐
(Volumes / S3)                                                                       │
   └── COPY INTO ──────────────────────────────────────────────────────────────────► │
                                                                                     ▼
Child Company CSVs (S3 landing/)                                          fmcg.gold.fact_orders
   └── Bronze (raw Delta + CDF)                                           (Unified Parent ORG)
       └── Silver (PySpark cleanse + dedupe + MERGE)                               │
           └── Gold (canonical schema + MERGE)  ──── monthly rollup MERGE ────────►│
                                                                                    │
Dimension Tables (customers, products, pricing, date) ──────────────────────────────┼────►
                                                                                    ▼
                                                              vw_fact_orders_enriched
                                                              (denormalized Gold view)
                                                                          │
                                                              ┌───────────┴───────────┐
                                                              ▼                       ▼
                                                         Dashboards             Genie (AI Q&A)
```

---

*Built with Databricks · Delta Lake · Apache Spark · PySpark · Unity Catalog · Amazon S3*