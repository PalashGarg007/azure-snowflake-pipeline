# Azure ADF + Snowflake — End-to-End BI Pipeline

> A fully orchestrated data pipeline that extracts the **Chinook** music store dataset from Azure SQL Database, stages it through Azure Blob Storage, and loads it into a Snowflake dimensional model — automated end-to-end with Azure Data Factory.

![Azure](https://img.shields.io/badge/Azure-SQL%20%7C%20ADF%20%7C%20Blob%20%7C%20Key%20Vault-0078D4?logo=microsoft-azure)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![SQL](https://img.shields.io/badge/SQL-MERGE%20%7C%20SCD%20Type%202%20%7C%20SHA--256-4479A1)

---

## Architecture

```
Azure SQL Database (chinookDB)
        │
        │  6 Copy Activities (parallel)
        ▼
Azure Data Factory ──── Azure Key Vault (secrets)
        │
        │  SAS-based staging
        ▼
Azure Blob Storage (adf-staging)
        │
        │  Snowflake COPY command
        ▼
Snowflake STAGE schema (6 raw tables)
        │
        │  SQL MERGE via Script Activity
        ▼
Snowflake DW schema
  ├── DATE_DIM      (36,525 rows — 100 years)
  ├── TIME_DIM      (1,440 rows — per minute)
  ├── CUSTOMER_DIM  (SCD Type 2 + SHA-256 hash)
  ├── ARTIST_DIM    (SCD Type 2)
  └── SALES_FACT    (DATE_DIM_KEY FK — no raw date)
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Source | Azure SQL Database | OLTP source — Chinook dataset |
| Orchestration | Azure Data Factory | Pipeline automation, scheduling, monitoring |
| Secrets | Azure Key Vault | Secure credential storage — no hardcoded passwords |
| Staging | Azure Blob Storage | Temporary landing zone for Snowflake bulk load |
| Warehouse | Snowflake | Target dimensional model |
| Auth | Managed Identity + RBAC | Passwordless ADF → Key Vault + Storage access |

---

## Key Technical Highlights

### SHA-256 hash-based change detection
Instead of comparing every column individually to detect changed customers, a SHA-256 hash is computed from all tracked columns. If the hash changes, the record has changed — one comparison instead of seven.

```sql
SHA2(
    COALESCE(FIRSTNAME,  '') || '|' ||
    COALESCE(LASTNAME,   '') || '|' ||
    COALESCE(COMPANY,    '') || '|' ||
    COALESCE(CITY,       '') || '|' ||
    COALESCE(STATE,      '') || '|' ||
    COALESCE(COUNTRY,    '') || '|' ||
    COALESCE(POSTALCODE, ''),
    256
) AS ROW_HASH
```

### SCD Type 2 on CUSTOMER_DIM and ARTIST_DIM
Historical versions of records are preserved — when a customer changes address, the old record is expired (`IS_ACTIVE = 'N'`, `EFF_END_DATE = today`) and a new active record is inserted. This lets you see what a customer's details were at the time of any historical sale.

### DATE_DIM_KEY in SALES_FACT
`SALES_FACT` stores `DATE_DIM_KEY` (an integer surrogate key in YYYYMMDD format) instead of a raw timestamp. This is the correct dimensional modelling pattern — fact tables reference dimension surrogate keys, enabling powerful date-based analytics using all `DATE_DIM` attributes (day of week, quarter, is weekend, etc.).

```sql
JOIN DW.DATE_DIM DD
    ON DD.DATE_KEY = TO_NUMBER(TO_CHAR(I.INVOICEDATE, 'YYYYMMDD'))
```

### Parameterized linked services
Both the SQL Server and Snowflake linked services in ADF are fully parameterized — server name, database, warehouse, username, role, and host are all runtime parameters. This means one linked service definition works for any environment (dev/staging/prod) without duplication.

### Single combined pipeline
All stages — raw copy, DATE/TIME generation, dimension loads, fact load — run in one pipeline with dependency arrows enforcing the correct execution order. This gives full end-to-end visibility in ADF Monitor with a single run history.

---

## Repository Structure

```
chinook-azure-snowflake-pipeline/
│
├── README.md
│
├── sql/
│   ├── 01_snowflake_setup.sql       — warehouse, database, schemas, role, user
│   ├── 02_stage_tables.sql          — 6 STAGE tables mirroring Azure SQL source
│   ├── 03_dw_tables.sql             — DATE_DIM, TIME_DIM, CUSTOMER_DIM, ARTIST_DIM, SALES_FACT
│   └── 04_load_stage_to_dw.sql      — full load script: DATE/TIME gen + MERGE + SALES_FACT
│
├── screenshots/
│   ├── 01_adf_pipeline_canvas.png   — full pipeline with all activities connected
│   ├── 02_adf_pipeline_run.png      — successful monitor run (all green)
│   ├── 03_linked_services.png       — all 5 linked services listed
│   ├── 04_copy_activity_config.png  — source/sink/staging settings
│   └── 05_snowflake_dw_tables.png   — Snowflake DW schema with row counts
│
└── docs/
    └── submission_report.pdf        — full project report with step-by-step explanation
```

---

## Pipeline Execution Order

```
Stage 1 (parallel):   Copy_Artist, Copy_Genre, Copy_Album,
                      Copy_Customer, Copy_Invoice, Copy_InvoiceLine
                                    │
Stage 2 (parallel):   Script_DATE_DIM,  Script_TIME_DIM
                                    │
Stage 3 (parallel):   Script_CUSTOMER_DIM,  Script_ARTIST_DIM
                                    │
Stage 4 (alone):      Script_SALES_FACT
```

Stage tables have no foreign key constraints, so all 6 copy activities run in parallel. DW loads are ordered because `SALES_FACT` references dimension surrogate keys that must exist first.

---

## Final Row Counts

| Table | Rows | Notes |
|---|---|---|
| `DATE_DIM` | 36,525 | 100 years — 2000-01-01 to 2099-12-31 |
| `TIME_DIM` | 1,440 | One row per minute of the day |
| `CUSTOMER_DIM` | 59 | One active record per customer |
| `ARTIST_DIM` | 275 | One active record per artist |
| `SALES_FACT` | 2,240 | One row per invoice line |

---

## How to Replicate

### Prerequisites
- Azure subscription (free tier works)
- Snowflake trial account (free at app.snowflake.com)
- Azure Data Studio (free from Microsoft)

### Step 1 — Azure SQL
1. Create an Azure SQL Server and database (`chinookDB`)
2. Create a non-admin user: `CREATE USER chinook_user WITH PASSWORD = '...'`
3. Run `01_Azure_Chinook_DataLoadScript.sql` as `chinook_user`

### Step 2 — Snowflake
1. Log in as `ACCOUNTADMIN`
2. Run `sql/01_snowflake_setup.sql`
3. Log out and log back in as `CHINOOK_USER` for all subsequent steps
4. Run `sql/02_stage_tables.sql`
5. Run `sql/03_dw_tables.sql`

### Step 3 — Azure Services
Create these in one Resource Group:
- Storage Account (HNS **disabled**)
- Key Vault — add secrets: `sql-server-password`, `snowflake-password`
- Azure Data Factory — enable System Assigned Managed Identity

Assign roles:
- Key Vault → ADF: `Key Vault Secrets User`
- Key Vault → your account: `Key Vault Secrets Officer`
- Storage → ADF: `Storage Blob Data Contributor`

### Step 4 — ADF Linked Services
Create 5 linked services (see `/docs/submission_report.pdf` for detailed config):
- `LS_KeyVault` — Managed Identity
- `LS_BlobStorage` — Managed Identity
- `LS_AzureSQLServer` — SQL auth, FQDN + username as parameters, password from Key Vault
- `LS_Snowflake` — Basic auth, 6 parameters, password from Key Vault
- `LS_BlobSAS` — SAS token (for Snowflake staging)

### Step 5 — Run Pipeline
1. Create pipeline `PL_Chinook_Load`
2. Add Copy Activities for all 6 stage tables (parallel)
3. Add Script Activities for DATE_DIM, TIME_DIM, CUSTOMER_DIM, ARTIST_DIM, SALES_FACT
4. Connect with dependency arrows (see pipeline execution order above)
5. Paste scripts from `sql/04_load_stage_to_dw.sql` into each Script Activity
6. Publish All → Debug → Monitor

---

## Screenshots to Add

After running your pipeline, capture these screenshots and add them to the `/screenshots` folder:

1. **ADF pipeline canvas** — full view of all connected activities
2. **ADF Monitor run** — click a successful run to show all activities green
3. **Linked services list** — Manage → Linked Services
4. **Snowflake DW tables** — run the verification query and screenshot results

---

## License

MIT — feel free to use, adapt, or build on this for your own projects.
