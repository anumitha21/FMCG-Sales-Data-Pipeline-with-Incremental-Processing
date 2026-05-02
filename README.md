# FMCG Sales Data Pipeline — Incremental Processing on Databricks

End-to-end data pipeline unifying two FMCG companies into a single analytics-ready Gold layer using Databricks, Delta Lake, and PySpark.

---

## Business Context

**Parent Company A** acquired **Child Company B**. Both companies have different schemas, data cadences, and source systems. The goal is to merge them into one unified analytics table without disrupting operations.

| | Parent Company | Child Company |
|---|---|---|
| Data Source | CSV exports via Databricks Volumes | Daily CSV files in Amazon S3 |
| Schema | Pre-denormalized (flat) | Normalized (customers, products, orders) |
| Load Strategy | Full load + incremental via `COPY INTO` | Full load + daily incremental |
| Granularity | Monthly | Daily |
| Pipeline | Direct → Gold | Bronze → Silver → Gold |

---

## Architecture

![Project Architecture](resources/project_architecture.png)

```
Amazon S3 (landing/)
      │
      ▼  Databricks Lakeflow Jobs
┌─────────────────────────────────────────────────┐
│  Unity Catalog — fmcg                           │
│                                                 │
│  Child Company:                                 │
│  Bronze → Silver → Gold (sb_fact_orders)        │
│                          │                      │
│                          ▼  monthly MERGE        │
│  Parent Company CSV ──► fmcg.gold.fact_orders   │
│  (COPY INTO)             (unified table)        │
└─────────────────────────────────────────────────┘
      │
      ▼
vw_fact_orders_enriched
      │
      ├── Databricks Dashboards
      └── Databricks Genie (AI Q&A)
```

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Databricks | Platform — notebooks, jobs, Unity Catalog, Genie |
| PySpark | Distributed transformations across all layers |
| Delta Lake | ACID storage for all Bronze/Silver/Gold tables |
| Change Data Feed (CDF) | Row-level change tracking for incremental reads |
| Unity Catalog | Governance — `fmcg.bronze`, `fmcg.silver`, `fmcg.gold` |
| Lakeflow Jobs | Orchestration for scheduled pipeline runs |
| Amazon S3 | Landing zone for new files; archive for processed files |
| `COPY INTO` | Idempotent ingestion of parent company CSVs |
| Databricks Genie | Natural language querying on the Gold view |

---

## Repository Structure

```
FMCG-DATAENGINEERING/
├── data/
│   ├── parent_company/
│   │   ├── full_load/          # dim_customers, dim_products, dim_gross_price, fact_orders
│   │   └── incremental_load/   # Incremental fact_orders CSV + COPY INTO SQL
│   └── child_company/
│       ├── full_load/          # customers, products, gross_price, orders CSVs
│       └── incremental_load/   # Daily files: orders_YYYY_MM_DD.csv
│
├── notebook_code/
│   ├── setup/
│   │   ├── setup_catalog.ipynb             # Creates fmcg catalog + schemas
│   │   ├── utilities.ipynb                 # Shared schema variables via %run
│   │   └── dim_date_table_creation.ipynb   # Generates dim_date in Gold
│   ├── dimension_data_processing/
│   │   ├── customers_data_processing.ipynb
│   │   ├── products_data_processing.ipynb
│   │   └── pricing_data_processing.ipynb
│   └── fact_data_processing/
│       ├── full_load_fact.ipynb            # Historical load: Bronze → Silver → Gold
│       └── incremental_load_fact.ipynb     # Daily incremental + merge into parent Gold
│
├── dashboards/
│   ├── denormalise_table_query_fmcg.txt    # SQL to create vw_fact_orders_enriched
│   └── fmcg_dashboard.pdf
│
├── resources/
│   └── project_architecture.png
└── snapshots/
```

---

## Pipeline Walkthrough

### 1. Unity Catalog Setup

Three schemas under the `fmcg` catalog:

| Schema | What's stored |
|---|---|
| `fmcg.bronze` | Raw ingested data with metadata (`file_name`, `read_timestamp`, `file_size`) |
| `fmcg.silver` | Cleansed, validated, deduplicated data |
| `fmcg.gold` | Business-ready fact + dimension tables and the unified analytics view |

Schema variables (`bronze_schema`, `silver_schema`, `gold_schema`) are centralised in `utilities.ipynb` and loaded via `%run`.

---

### 2. Child Company — Full Load

`full_load_fact.ipynb` processes the complete historical order data through all three layers.

**Bronze**
- Reads all CSVs from S3 `landing/` with wildcard
- Captures file metadata via `_metadata` struct
- Writes to `fmcg.bronze.orders` (Delta, CDF enabled)
- Archives processed files to `processed/` using `dbutils.fs.mv()`

**Silver**
- Drops rows with null `order_qty`
- Cleanses `customer_id` — non-numeric values replaced with `999999`
- Normalises `order_placement_date` — strips weekday prefixes, parses multiple formats (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `MMMM dd, yyyy`) using `F.coalesce(F.try_to_date(...))`
- Deduplicates on: `order_id + order_placement_date + customer_id + product_id + order_qty`
- Joins with `fmcg.silver.products` to enrich with `product_code`
- Writes to `fmcg.silver.orders` (Delta, CDF enabled, `mergeSchema=true`)

**Gold**
- Renames columns to canonical schema: `order_id`, `date`, `customer_code`, `product_code`, `sold_quantity`
- Writes to `fmcg.gold.sb_fact_orders` (Delta, CDF enabled)

---

### 3. Child Company — Incremental Load

`incremental_load_fact.ipynb` runs daily as new files land in S3 (e.g. `orders_2025_12_01.csv`).

**Staging Table Pattern** — new batch is written to a dedicated staging table so all transformations operate only on the new data, not the full history:

```
S3 landing/ (new file)
      │
      ├──► fmcg.bronze.orders          (APPEND — cumulative history)
      └──► fmcg.bronze.staging_orders  (OVERWRITE — current batch only)
                  │
                  ▼ PySpark transforms
           fmcg.silver.staging_orders  (OVERWRITE)
                  │
                  ▼ Delta MERGE
           fmcg.silver.orders          (upsert)
                  │
                  ▼ Delta MERGE
           fmcg.gold.sb_fact_orders    (upsert)
```

**Delta MERGE (Upsert)** — both Silver and Gold use `DeltaTable.merge()`:
- Silver match key: `order_placement_date + order_id + product_code + customer_id`
- Gold match key: `date + order_id + product_code + customer_code`
- `whenMatchedUpdateAll()` + `whenNotMatchedInsertAll()`

**Daily → Monthly Rollup** — child data is daily; parent Gold expects monthly. After updating child Gold, the pipeline:
1. Identifies affected months from the current batch
2. Aggregates daily records to monthly totals
3. Merges only the affected months into `fmcg.gold.fact_orders` — no full-table rewrite

---

### 4. Parent Company — Full & Incremental Load

Parent data arrives already clean, so no Bronze/Silver processing is needed.

- **Full load**: CSVs written directly to Gold Delta tables via Spark
- **Incremental load**: New records loaded using `COPY INTO` — idempotent, Databricks tracks loaded files to prevent duplicates

```sql
COPY INTO fmcg.gold.fact_orders
FROM (
  SELECT CAST(date AS DATE), product_code, customer_code, CAST(sold_quantity AS BIGINT)
  FROM '/Volumes/fmcg/gold/parent_incremental_data/fact_orders.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');
```

---

### 5. Change Data Feed (CDF)

Enabled on every Delta table:

```python
.option("delta.enableChangeDataFeed", "true")
```

CDF tracks every row-level insert/update/delete. Benefits:
- Downstream jobs read only changed rows since the last checkpoint — no full scans
- Full audit trail of every change, queryable at any time

---

### 6. Gold Analytics View

`fmcg.gold.vw_fact_orders_enriched` is the single source of truth for all BI tools:

```sql
fact_orders
  LEFT JOIN dim_date        ON date = month_start_date
  LEFT JOIN dim_customers   ON customer_code
  LEFT JOIN dim_products    ON product_code
  LEFT JOIN dim_gross_price ON product_code AND fiscal_year
```

Derived metric: `total_amount_inr = sold_quantity × price_inr`

---

## Dimension Tables

| Table | Key Columns |
|---|---|
| `fmcg.gold.dim_customers` | `customer_code`, `market`, `platform`, `channel` |
| `fmcg.gold.dim_products` | `product_code`, `division`, `category`, `variant` |
| `fmcg.gold.dim_gross_price` | `product_code`, `fiscal_year`, `price_inr` |
| `fmcg.gold.dim_date` | `month_start_date`, `year`, `month_name`, `quarter` |

---

## Serving Layer

**Databricks Dashboards** — connected to `vw_fact_orders_enriched`:
- Revenue by market, channel, platform
- Top products and customers by quantity / revenue
- YoY and QoQ trend analysis
- Cross-company consolidated reporting

**Databricks Genie** — natural language querying on the Gold view:
- *"Which market had the highest revenue in Q3 2025?"*
- *"Compare parent vs child company revenue by division."*

---

## Getting Started

**Prerequisites:** Databricks workspace with Unity Catalog, S3 bucket as external location, Delta Lake runtime.

1. Run `setup/setup_catalog.ipynb` — creates `fmcg` catalog and schemas
2. Run `setup/dim_date_table_creation.ipynb` — generates date dimension
3. Run dimension notebooks — customers → products → pricing
4. Upload child CSVs to S3 `landing/`, run `full_load_fact.ipynb`
5. Schedule `incremental_load_fact.ipynb` as a daily Lakeflow Job
6. Execute SQL in `dashboards/denormalise_table_query_fmcg.txt` to create the Gold view
7. Connect Dashboards or any BI tool to `fmcg.gold.vw_fact_orders_enriched`

---

*Databricks · Delta Lake · PySpark · Unity Catalog · Amazon S3 · Lakeflow Jobs*
