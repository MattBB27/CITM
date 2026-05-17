# CITM: Car Rental Inventory Management System

> Cloud-native data system designed for Azure. Flask operational application, Python ETL pipeline, and Kimball star-schema warehouse on Azure Synapse, consumed by both an in-app analytics page and an external Power BI dashboard.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-D71F00?logo=sqlalchemy&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.3-150458?logo=pandas&logoColor=white)
![Azure SQL](https://img.shields.io/badge/Azure_SQL-Operational_DB-0078D4?logo=microsoftazure&logoColor=white)
![Azure Synapse](https://img.shields.io/badge/Azure_Synapse-Data_Warehouse-0078D4?logo=microsoftazure&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?logo=powerbi&logoColor=black)
![pytest](https://img.shields.io/badge/pytest-17_passing-0A9EDC?logo=pytest&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue)

---

## Overview

CITM is a centralised car rental inventory and transaction management system delivered as a two-part academic project. It demonstrates the full lifecycle of a modern data system: operational data capture, cloud ETL into a dimensional warehouse, and analytical reporting through both an in-app page and an external BI tool.

The data layer runs live on Microsoft Azure: an Azure SQL Database for the operational store and an Azure Synapse Dedicated SQL Pool for the analytics warehouse, both provisioned through the Azure portal and consumed via real ODBC connection strings (see `config.py` for the structure). Roughly $130 of Azure free credits was spent operating the cloud resources over the course of the project. The Flask application and ETL pipeline run locally against the live cloud databases, in a production deployment they'd also be cloud-hosted (Azure Container Apps + Azure Data Factory).

- **Assignment 1**: [Data System Requirements & Design Specifications](./Assignment%201%20Data%20System%20Requirements%20and%20design%20specifications%20%284%29.pdf) (informational/functional/non-functional requirements, context diagram, star-schema ERD, data dictionary)
- **Assignment 2**: this repository, a working implementation of the design against live Azure data services

## What this project demonstrates

- **Data engineering end-to-end** — extracting from a transactional source, transforming with pandas, loading incrementally into a cloud MPP warehouse
- **Dimensional modelling** — Kimball star schema with one fact (`fact_loan_transaction`) and four conformed dimensions (`dim_customer`, `dim_vehicle`, `dim_date`, `dim_branch`)
- **Cloud database engineering on Azure** — Azure SQL for OLTP, Azure Synapse Dedicated SQL Pool for analytics, with real handling of Synapse's MPP-specific constraints
- **Application development** — Flask web app with SQLAlchemy ORM, server-side validation, computed availability logic, and a hybrid filter UI
- **Analytical reporting** — in-app KPI dashboard and external Power BI dashboard, both backed by the same star schema
- **Testing discipline** — 17 pytest unit tests across the transform layer (deduplication, type coercion, surrogate key resolution, incremental load guards)

## Tech stack

| Layer            | Technology                                                    |
| ---------------- | ------------------------------------------------------------- |
| Operational DB   | Azure SQL Database (`citm-db`)                                |
| Data warehouse   | Azure Synapse Dedicated SQL Pool (`citmwarehouse`)            |
| Application      | Python 3.11+, Flask 3.0, SQLAlchemy 2.0, Jinja2               |
| ETL              | Python, pandas 2.3, SQLAlchemy, pyodbc + ODBC Driver 18       |
| Analytics        | In-app `/analytics` page + external Power BI Desktop (`.pbix`)|
| Testing          | pytest 9.0                                                    |

---

## Architecture

```
┌──────────────────────────────────┐
│  Operational DB (Azure SQL)      │
│  citm-db                         │
│  customers / vehicles /          │
│  branches / loan_transactions    │
└────────────┬─────────────────────┘
             │
       ETL Pipeline (src/etl/)
       ┌─────▼──────┐
       │  EXTRACT   │  extract.py    — pandas.read_sql via SQLAlchemy
       │  TRANSFORM │  transform.py  — dedupe, type coerce, key generation
       │  LOAD      │  load.py       — INSERT...UNION ALL batches into Synapse
       └─────┬──────┘
             │
┌────────────▼─────────────────────┐
│  Data Warehouse (Synapse)        │
│  citmwarehouse                   │
│                                  │
│  fact_loan_transaction (centre)  │
│    ├── dim_customer              │
│    ├── dim_vehicle               │
│    ├── dim_date                  │
│    └── dim_branch                │
└──────┬─────────────────┬─────────┘
       │                 │
       ▼                 ▼
  In-app Analytics    Power BI Desktop
  /analytics          (.pbix file)
  (5 KPI cards,       (interactive
  branch revenue,     dashboard with
  monthly trend,      year slicer,
  top customers,      annual revenue,
  vehicle types)      drill-downs)
```

The full data dictionary and ERD are in the [Assignment 1 design report](./Assignment%201%20Data%20System%20Requirements%20and%20design%20specifications%20%284%29.pdf).

---

## Engineering highlights

### Synapse-specific adaptations
Azure Synapse Dedicated SQL Pool is not a drop-in for SQL Server. The implementation handles each quirk explicitly:

- **No enforced UNIQUE / FOREIGN KEY constraints** — DDL uses `NOT ENFORCED`; SQLAlchemy relationships use explicit `primaryjoin` expressions instead of FK metadata
- **No multi-row `VALUES (?,?),(?,?)` INSERTs** — replaced with the `INSERT...SELECT ... UNION ALL` pattern Synapse prefers for small batches
- **No transactions for DDL** — engine configured with `isolation_level="AUTOCOMMIT"`
- **Hash distribution incompatible with IDENTITY** — `fact_loan_transaction` distributes on `source_loan_id`
- **Read-after-write propagation lag in MPP** — handled with retry-with-backoff after upserts

### Incremental ETL
The fact load is incremental by design — repeat runs only insert new transactions, gated by the `source_loan_id` degenerate dimension. Dimensions use an upsert pattern with read-back to resolve surrogate keys.

### Computed business logic
- **Vehicle availability** is *computed*, not stored — a vehicle is "On Loan" if today's date falls between any of its loans' `loan_date` and `return_date`. No manual toggle, no stale flags.
- **Date dimension** is built from observed loan/return dates, with `day`, `month`, `year`, `quarter`, `day_of_week`, and `month_name` derived in pandas before load.

---

## Setup & run

### Prerequisites
- Python 3.11+
- Azure SQL Database (`citm-db`) provisioned on an Azure SQL Server
- Azure Synapse Dedicated SQL Pool (`citmwarehouse`) on a Synapse Workspace
- **ODBC Driver 18 for SQL Server** — [download here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- Power BI Desktop (optional — for the external dashboard)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Azure connection strings
Create a `.env` (see `.env.example`) with your credentials:
```python
OPERATIONAL_DB_URL = (
    "mssql+pyodbc://sqladmin:PASSWORD@yourserver.database.windows.net/citm-db"
    "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
)
WAREHOUSE_DB_URL = (
    "mssql+pyodbc://sqladmin:PASSWORD@yourworkspace.sql.azuresynapse.net/citmwarehouse"
    "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
)
```

### 3. Initialise database schemas (one-time)
```bash
python init_azure.py
```
Creates tables in both Azure SQL (operational) and Synapse (warehouse) using Synapse-compatible DDL (`UNIQUE NOT ENFORCED`, `ROUND_ROBIN` distribution).

### 4. Run the application
```bash
python run.py
```
- Auto-seeds the operational DB with 120 loan transactions on first run
- Web app at **http://localhost:5000**
- Pages: Dashboard, Customers, Vehicles, Loans, Branches, ETL, Analytics

### 5. Run the ETL pipeline
**From the web UI**: navigate to `/etl` → click **Execute ETL Pipeline**
**From CLI**:
```bash
python run_etl.py
```
Subsequent runs are incremental — only new records are loaded.

### 6. (Optional) Open the Power BI dashboard
Open `citm-analytics.pbix` in Power BI Desktop → **Home → Refresh** to pull the latest data from Synapse.

### 7. Run tests
```bash
python -m pytest tests/ -v
```

---

## Project structure

```
citm/
├── config.py                 # Azure connection strings (operational + warehouse)
├── run.py                    # Main entry point (seed + Flask web server)
├── run_etl.py                # Standalone CLI ETL runner
├── init_azure.py             # One-time DDL setup for Azure SQL + Synapse
├── requirements.txt
│
├── src/
│   ├── seed.py               # Seeds 25 customers, 20 vehicles, 5 branches, 120 loans
│   ├── app.py                # Flask routes (dashboard, CRUD, ETL panel, analytics)
│   ├── models/
│   │   ├── operational.py    # OLTP models: Customer, Vehicle, Branch, LoanTransaction
│   │   └── warehouse.py      # Star schema: FactLoanTransaction + 4 Dim tables
│   └── etl/
│       ├── extract.py        # Phase 1 — pandas.read_sql against Azure SQL
│       ├── transform.py      # Phase 2 — dedupe, type coerce, surrogate keys
│       ├── load.py           # Phase 3 — INSERT...UNION ALL into Synapse
│       └── pipeline.py       # Orchestrator — returns timing + row counts
│
├── templates/                # Jinja2 HTML (dark theme)
│   ├── base.html             # Sidebar nav + global styling
│   ├── dashboard.html        # Operational KPIs + warehouse status
│   ├── customers.html / customer_form.html
│   ├── vehicles.html         # Computed availability + active loan info
│   ├── vehicle_form.html
│   ├── loans.html            # Hybrid filter UI (search + branch + type + date range)
│   ├── loan_form.html        # Date validation, auto-calculated fee
│   ├── branches.html
│   ├── etl.html              # ETL control panel + run history
│   └── analytics.html        # 5 KPI tiles + annual revenue + 5 cross-tab reports
│
├── tests/
│   └── test_etl.py           # 17 pytest unit tests covering the transform layer
│
└── Assignment 1 ... .pdf     # Design report (requirements + ERD + data dictionary)
```

---

## ETL pipeline details

### Extract
Connects to Azure SQL via SQLAlchemy. Reads the four operational tables into pandas DataFrames using `pd.read_sql`. Logs row counts per table.

### Transform
| Output                  | Logic                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `dim_customer`          | Selects `customer_id`, `name`, `driver_license_number`; strips whitespace; deduplicates on `customer_id`                       |
| `dim_vehicle`           | Selects `vehicle_id`, `model`, `manufacturer`, `vehicle_type`; excludes operational fields (mileage, availability)             |
| `dim_branch`            | Selects `branch_id`, `branch_name`, `city`, `state`                                                                            |
| `dim_date`              | Built from all loan/return dates; computes `day`, `month`, `year`, `quarter`, `day_of_week`, `month_name`; key as `YYYYMMDD`   |
| `fact_loan_transaction` | Resolves surrogate keys from dim maps; computes `loan_duration_days` and `distance_driven`; incremental guard on `source_loan_id` |

### Load
- `INSERT INTO ... SELECT ... UNION ALL` pattern (Synapse's preferred small-batch DML)
- Engine configured with `isolation_level="AUTOCOMMIT"` (Synapse rejects explicit transactions)
- Dimensions use upsert pattern with read-back retry to handle Synapse MPP propagation delay
- Fact table appends new rows only

---

## Application features

| Page          | Highlights                                                                                                                   |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **Dashboard** | KPI tiles (customers, vehicles, available vehicles, loans, branches, revenue, warehouse fact rows) + recent transactions    |
| **Customers** | Registry with auto-incrementing IDs (max+1 pattern to avoid collisions)                                                      |
| **Vehicles**  | Fleet inventory with **computed** availability and active loan column                                                        |
| **Loans**     | Hybrid filter UI: free-text search + branch dropdown + vehicle type dropdown + date range, with pagination preserving state |
| **ETL**       | Pipeline architecture view, live warehouse state with row counts, run history with timing, single-click execution            |
| **Analytics** | 5 headline KPIs + annual revenue + 5 cross-tab reports + monthly trend; 5-min cache, auto-invalidates after ETL              |

Analytics report set:
- **Headline KPIs** — total revenue, total rentals, average fee, average duration, average distance
- **Annual Revenue** — year-by-year breakdown
- **Revenue by Branch** — total loans, revenue, average fee per branch
- **Vehicle Type Performance** — rentals, average duration, revenue per type
- **Top Customers by Spend** — TOP 10 with rentals and total spent
- **Monthly Revenue Trend** — month-by-month bar chart

---

## Sample data

Seeded into the operational DB on first run:
- **5** branches across major Australian cities (Sydney, Melbourne, Brisbane, Perth, Adelaide)
- **20** vehicles across 5 types (Sedan, Hatchback, SUV, Ute, Van) from 8 manufacturers
- **25** customers with realistic Australian names, licence numbers, and contact details
- **120** loan transactions throughout 2024 with computed fees, mileage, and durations

Demo data added during the live demonstration is dated 2026, allowing the Annual Revenue panel to show year-over-year comparison.

---

## Testing

17 pytest unit tests across four test classes covering each transform phase:

| Test class                 | Coverage                                                                                       |
| -------------------------- | ---------------------------------------------------------------------------------------------- |
| `TestTransformDimCustomer` | Deduplication, whitespace stripping, required columns, no operational fields leak              |
| `TestTransformDimVehicle`  | Row count, no mileage column, columns present                                                  |
| `TestTransformDimDate`     | Unique keys, YYYYMMDD format, expected dates, month names, quarter values                      |
| `TestTransformFactLoans`   | Row count, incremental skip, computed duration, computed distance, no null keys                |

```bash
python -m pytest tests/ -v
```

---

## Deployment scope

The data layer is fully cloud-hosted on Azure — the Azure SQL Database and Azure Synapse Dedicated SQL Pool are real, provisioned resources accessed through standard connection strings, not local emulators or stubs. The ETL pipeline performs genuine cross-cloud reads from Azure SQL into pandas DataFrames and writes back to Synapse over ODBC.

The Flask application and ETL pipeline themselves run on the developer's machine for the scope of this project. A production deployment of the same code would containerise the app to Azure Container Apps (or App Service) and orchestrate the ETL on a schedule via Azure Data Factory or Azure Functions — no code changes to the ETL or data models would be required.

## Known limitations & future extensions

- **No authentication / RBAC** — out of scope for the data-architecture focus of this assignment. A production extension would use Azure AD for SSO and Synapse Row-Level Security for branch-scoped data access.
- **No vehicle home-branch** — vehicles aren't tied to a specific branch; in production each vehicle would have a `branch_id` foreign key for fleet allocation analytics.
- **Single-region database deployment** — the Azure resources sit in one region; geo-replication and DR would be added for production resilience.
- **Manual ETL trigger** — production would orchestrate via Azure Data Factory on a schedule rather than from the web UI or CLI.

---

## License

Released under the [MIT License](./LICENSE).
