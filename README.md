# CITM — Car Rental Inventory Management System

**Assignment 2 Implementation** — Data System Implementation & Demonstration

---

## Project Overview

CITM is a full-stack data system implementing the requirements outlined in the CITM design document. It consists of three integrated layers running on Microsoft Azure:

1. **Operational Application** — Flask web app with SQLAlchemy ORM, backed by Azure SQL Database. Handles customer registration, vehicle management, branch operations, and loan transactions.
2. **ETL Pipeline** — Python pipeline using pandas and SQLAlchemy that extracts from Azure SQL, transforms into a dimensional model, and loads into Azure Synapse Analytics.
3. **Data Warehouse** — Star schema in Azure Synapse Dedicated SQL Pool, queried by both an in-app analytics page and an external Power BI dashboard.

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


## Project Structure

```
citm/
├── config.py                  # Azure connection strings (operational + warehouse)
├── run.py                     # Main entry point (seed + Flask web server)
├── run_etl.py                 # Standalone CLI ETL runner
├── init_azure.py              # One-time DDL setup for Azure SQL + Synapse
├── requirements.txt
├── README.md                  # This file
│
├── src/
│   ├── seed.py                # Seeds 25 customers, 20 vehicles, 5 branches, 120 loans
│   ├── app.py                 # Flask routes (dashboard, CRUD, ETL panel, analytics)
│   │
│   ├── models/
│   │   ├── operational.py     # OLTP models: Customer, Vehicle, Branch, LoanTransaction
│   │   └── warehouse.py       # Star schema: FactLoanTransaction + 4 Dim tables
│   │                          # (uses primaryjoin instead of FKs for Synapse compat)
│   │
│   └── etl/
│       ├── extract.py         # Phase 1 — pandas.read_sql against Azure SQL
│       ├── transform.py       # Phase 2 — dedupe, type coerce, surrogate keys
│       ├── load.py            # Phase 3 — INSERT...UNION ALL into Synapse
│       └── pipeline.py        # Orchestrator — returns timing + row counts
│
├── templates/                 # Jinja2 HTML
│   ├── base.html              # Sidebar nav + global styling (dark theme)
│   ├── dashboard.html         # Operational KPIs + warehouse status
│   ├── customers.html 
|   ├── customer_form.html
│   ├── vehicles.html          # Computed availability + active loan info
│   ├── vehicle_form.html
│   ├── loans.html             # Hybrid filter UI (search + branch + type + date range)
│   ├── loan_form.html         # Date validation, auto-calc fee
│   ├── branches.html
│   ├── etl.html               # ETL control panel + run history
│   └── analytics.html         # 5 KPI tiles + annual revenue + 5 cross-tab reports
│
├── tests/
│   └── test_etl.py            # 17 pytest unit tests (transform phase)
│
└── citm-analytics.pbix        # Power BI dashboard (separate file, not in repo by default)
```

---

## Setup & Run

### Prerequisites
- Python 3.11+
- Azure SQL Database (`citm-db`) provisioned on Azure SQL Server
- Azure Synapse Dedicated SQL Pool (`citmwarehouse`) on Synapse Workspace
- ODBC Driver 18 for SQL Server installed
(MUST DOWNLOAD)
available at https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17
- Power BI Desktop (optional — for external dashboard)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Azure connection strings
Copy DB connection strings to .env (see .env.example) with your credentials:
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
Creates tables in both Azure SQL (operational) and Synapse (warehouse) using
Synapse-compatible DDL (UNIQUE NOT ENFORCED, ROUND_ROBIN distribution).

### 4. Run the application
```bash
python run.py
```
- Auto-seeds the operational DB with 120 loan transactions on first run
- Web app at **http://localhost:5000**
- Pages: Dashboard, Customers, Vehicles, Loans, Branches, ETL, Analytics

### 5. Run ETL
**From the web UI**: navigate to `/etl` → click **Execute ETL Pipeline**.
**From CLI**:
```bash
python run_etl.py
```
Pipeline is **incremental** — only loads new records on subsequent runs.

### 6. (Optional) Open Power BI dashboard
1. Download Power BI Desktop (free)
2. Open `citm-analytics.pbix` (or build a new one — see below)
3. Click **Home → Refresh** to pull latest data from Synapse

### 7. Run tests
```bash
python -m pytest tests/ -v
```
17 unit tests covering all dimension transforms and the fact transform.

---

## ETL Pipeline Details

### Extract
Connects to Azure SQL via SQLAlchemy. Reads four operational tables into pandas DataFrames using `pd.read_sql`. Logs row counts per table.

### Transform
- **dim_customer** — selects `customer_id`, `name`, `driver_license_number`; strips whitespace; deduplicates on customer_id
- **dim_vehicle** — selects `vehicle_id`, `model`, `manufacturer`, `vehicle_type`; excludes operational fields (mileage, availability)
- **dim_branch** — selects `branch_id`, `branch_name`, `city`, `state`
- **dim_date** — built from all loan/return dates; computes `day`, `month`, `year`, `quarter`, `day_of_week`, `month_name`; key format `YYYYMMDD INT`
- **fact_loan_transaction** — resolves surrogate keys from dim maps; computes `loan_duration_days` and `distance_driven`; enforces incremental load via `source_loan_id` guard

### Load
- Uses `INSERT INTO ... SELECT ... UNION ALL` pattern (Synapse's preferred small-batch DML)
- Engine configured with `isolation_level="AUTOCOMMIT"` (Synapse rejects explicit transactions)
- Dimensions use upsert pattern; reads back with retry to handle Synapse MPP propagation delay
- Fact table appends new rows only (incremental guard via `source_loan_id`)

### Synapse-Specific Adaptations
The Synapse Dedicated SQL Pool has several quirks not present in standard SQL Server:
- No enforced UNIQUE / FOREIGN KEY constraints — handled with `NOT ENFORCED` syntax
- No multi-row `VALUES (?,?),(?,?)` INSERT syntax — handled via `INSERT...SELECT UNION ALL`
- No transactions for DDL — all init runs with autocommit
- Hash distribution can't use IDENTITY column — `fact_loan_transaction` distributes on `source_loan_id`
- Read-after-write may have propagation delay — handled with retry-with-backoff

---

## Application Features

### Dashboard
KPI tiles for total customers, vehicles, available vehicles, loan transactions, branches, total revenue, and warehouse fact rows. Recent loan transactions table. System status panel.

### Customers
Registry with full name, driver licence, email, phone. Auto-incrementing IDs (max+1 pattern to avoid collisions).

### Vehicles
Fleet inventory with **computed availability** — a vehicle is "On Loan" if today's date falls between any of its loans' loan_date and return_date. Active loan column shows loan ID, customer name, and return date when on loan. No manual toggle.

### Loans
Hybrid filter UI: free-text search (loan ID / customer name / vehicle ID) + branch dropdown + vehicle type dropdown + date range. Active filters visible as badges. Pagination preserves filter state. Server-side date validation prevents return < loan date.

### ETL Pipeline
Control panel showing pipeline architecture (Extract → Transform → Load), live warehouse state with row counts per table, and run history with timing. Single-click execution.

### Analytics
- **Headline KPIs**: total revenue, total rentals, average fee, average duration, average distance
- **Annual Revenue**: year-by-year breakdown with revenue bars
- **Revenue by Branch**: cross-tab with total loans, revenue, average fee
- **Vehicle Type Performance**: rentals, average duration, revenue
- **Top Customers by Spend**: TOP 10 with rentals and total spent
- **Monthly Revenue Trend**: month-by-month with bar chart visualisation
- 5-minute cache with manual refresh; auto-invalidates after ETL runs

### Power BI Dashboard
External `.pbix` file connected directly to Synapse via the Azure Synapse Analytics SQL connector. Star schema relationships configured in Model view. Includes year slicer, annual revenue comparison, and interactive drill-downs.

---

## Sample Data

Seed script populates the operational DB with:
- **5** branches across major Australian cities (Sydney, Melbourne, Brisbane, Perth, Adelaide)
- **20** vehicles across 5 types (Sedan, Hatchback, SUV, Ute, Van) from 8 manufacturers
- **25** customers with realistic Australian names, licence numbers, and contact details
- **120** loan transactions throughout 2024 with computed fees, mileage, and durations

Demo data added during the demonstration is dated 2026, allowing the Annual Revenue panel to show year-over-year comparison.

---

## Testing

17 pytest unit tests organised into 4 test classes covering each transform phase:

| Test Class | Coverage |
|-----------|----------|
| `TestTransformDimCustomer` | Deduplication, whitespace stripping, required columns, no operational fields leak |
| `TestTransformDimVehicle` | Row count, no mileage column, columns present |
| `TestTransformDimDate` | Unique keys, YYYYMMDD format, expected dates, month names, quarter values |
| `TestTransformFactLoans` | Row count, incremental skip, computed duration, computed distance, no null keys |

Run with `python -m pytest tests/ -v`.

---

## Known Limitations & Future Extensions

- **No authentication / RBAC** — out of scope for the data architecture focus of this assignment. Production extension would use Azure Active Directory for SSO and Synapse Row-Level Security for branch-scoped data access.
- **No vehicle home-branch** — vehicles aren't tied to a specific branch; in production each vehicle would have a `branch_id` foreign key for fleet allocation analytics.
- **Single-region deployment** — geo-replication and disaster recovery would be added for production resilience.
- **Manual ETL trigger** — production would orchestrate via Azure Data Factory on a schedule.