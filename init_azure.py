"""
init_azure.py
-------------
Creates tables in both Azure databases using Synapse-compatible DDL.

Synapse Dedicated SQL Pools do NOT support:
  - Enforced UNIQUE constraints  → use UNIQUE NOT ENFORCED
  - Enforced FOREIGN KEY constraints → use NOT ENFORCED
  - Standard IDENTITY syntax differs slightly

Run once:  python init_azure.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
import config

# ── Operational DB DDL (Azure SQL — standard T-SQL) ───────────────────────────
OPERATIONAL_DDL = [
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
    CREATE TABLE customers (
        customer_id            VARCHAR(50)  NOT NULL PRIMARY KEY,
        name                   VARCHAR(100) NOT NULL,
        driver_license_number  VARCHAR(50)  NOT NULL UNIQUE,
        email                  VARCHAR(100) NULL,
        phone                  VARCHAR(20)  NULL,
        created_at             DATETIME     DEFAULT GETDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='vehicles' AND xtype='U')
    CREATE TABLE vehicles (
        vehicle_id    VARCHAR(50)  NOT NULL PRIMARY KEY,
        model         VARCHAR(100) NOT NULL,
        manufacturer  VARCHAR(100) NOT NULL,
        vehicle_type  VARCHAR(50)  NOT NULL,
        mileage       INT          DEFAULT 0,
        is_available  BIT          DEFAULT 1,
        created_at    DATETIME     DEFAULT GETDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='branches' AND xtype='U')
    CREATE TABLE branches (
        branch_id    VARCHAR(50)  NOT NULL PRIMARY KEY,
        branch_name  VARCHAR(100) NOT NULL,
        city         VARCHAR(50)  NOT NULL,
        state        VARCHAR(50)  NOT NULL
    )
    """,
    """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='loan_transactions' AND xtype='U')
    CREATE TABLE loan_transactions (
        loan_id           VARCHAR(50)    NOT NULL PRIMARY KEY,
        customer_id       VARCHAR(50)    NOT NULL REFERENCES customers(customer_id),
        vehicle_id        VARCHAR(50)    NOT NULL REFERENCES vehicles(vehicle_id),
        branch_id         VARCHAR(50)    NOT NULL REFERENCES branches(branch_id),
        loan_date         DATE           NOT NULL,
        return_date       DATE           NOT NULL,
        loan_fee          DECIMAL(10,2)  NOT NULL,
        starting_mileage  INT            NOT NULL,
        ending_mileage    INT            NOT NULL,
        created_at        DATETIME       DEFAULT GETDATE()
    )
    """,
]

# ── Warehouse DDL (Azure Synapse Dedicated Pool) ──────────────────────────────
# Synapse rules:
#   - UNIQUE and FK constraints must be NOT ENFORCED
#   - Use HEAP or CLUSTERED COLUMNSTORE INDEX (default is CCI)
#   - No DEFAULT constraints in some distributions
WAREHOUSE_DDL = [
    """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dim_customer')
    CREATE TABLE dim_customer (
        customer_key          INT          NOT NULL IDENTITY(1,1),
        customer_id           VARCHAR(50)  NOT NULL,
        name                  VARCHAR(100) NULL,
        driver_license_number VARCHAR(50)  NULL
    )
    WITH (DISTRIBUTION = REPLICATE, HEAP)
    """,
    """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dim_vehicle')
    CREATE TABLE dim_vehicle (
        vehicle_key   INT          NOT NULL IDENTITY(1,1),
        vehicle_id    VARCHAR(50)  NOT NULL,
        model         VARCHAR(100) NULL,
        manufacturer  VARCHAR(100) NULL,
        vehicle_type  VARCHAR(50)  NULL
    )
    WITH (DISTRIBUTION = REPLICATE, HEAP)
    """,
    """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dim_branch')
    CREATE TABLE dim_branch (
        branch_key   INT          NOT NULL IDENTITY(1,1),
        branch_id    VARCHAR(50)  NOT NULL,
        branch_name  VARCHAR(100) NULL,
        city         VARCHAR(50)  NULL,
        state        VARCHAR(50)  NULL
    )
    WITH (DISTRIBUTION = REPLICATE, HEAP)
    """,
    """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='dim_date')
    CREATE TABLE dim_date (
        date_key     INT          NOT NULL,
        full_date    DATE         NOT NULL,
        day          INT          NULL,
        month        INT          NULL,
        year         INT          NULL,
        quarter      INT          NULL,
        day_of_week  VARCHAR(20)  NULL,
        month_name   VARCHAR(20)  NULL
    )
    WITH (DISTRIBUTION = REPLICATE, HEAP)
    """,
    """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='fact_loan_transaction')
    CREATE TABLE fact_loan_transaction (
        loan_fact_key      INT            NOT NULL IDENTITY(1,1),
        customer_key       INT            NULL,
        vehicle_key        INT            NULL,
        loan_date_key      INT            NULL,
        return_date_key    INT            NULL,
        branch_key         INT            NULL,
        source_loan_id     VARCHAR(50)    NULL,
        loan_fee           DECIMAL(10,2)  NULL,
        loan_duration_days INT            NULL,
        distance_driven    INT            NULL,
        starting_mileage   INT            NULL,
        ending_mileage     INT            NULL
    )
    WITH (DISTRIBUTION = HASH(source_loan_id), CLUSTERED COLUMNSTORE INDEX)
    """,
]


def run_ddl(engine, statements: list, label: str, autocommit: bool = False):
    if autocommit:
        # Synapse DDL (CREATE TABLE WITH DISTRIBUTION) cannot run inside a transaction
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            for stmt in statements:
                conn.execute(text(stmt.strip()))
    else:
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt.strip()))
    print(f"✓ {label} — tables ready")


if __name__ == "__main__":
    print("Connecting to Operational DB (Azure SQL)...")
    op_engine = create_engine(config.OPERATIONAL_DB_URL)
    run_ddl(op_engine, OPERATIONAL_DDL, "Operational DB")

    print("Connecting to Warehouse (Azure Synapse)...")
    wh_engine = create_engine(config.WAREHOUSE_DB_URL)
    run_ddl(wh_engine, WAREHOUSE_DDL, "Warehouse (Synapse)", autocommit=True)

    print("\nAll done. Run:  python run_etl.py")