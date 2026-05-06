"""
ETL — Extract Phase
--------------------
Pulls raw data from the operational (OLTP) database using SQLAlchemy + pandas.
In production, this would query Azure SQL Database via pyodbc or SQLAlchemy's
Azure SQL dialect.

FR 4.1 — extract operational data using Python and SQLAlchemy
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def extract_all(operational_url: str) -> dict[str, pd.DataFrame]:
    """
    Extract all source tables from the operational database.

    Returns
    -------
    dict with keys: 'customers', 'vehicles', 'branches', 'loans'
    """
    engine = create_engine(operational_url)
    logger.info("EXTRACT — connecting to operational database")

    with engine.connect() as conn:
        customers = pd.read_sql(text("SELECT * FROM customers"),          conn)
        vehicles  = pd.read_sql(text("SELECT * FROM vehicles"),           conn)
        branches  = pd.read_sql(text("SELECT * FROM branches"),           conn)
        loans     = pd.read_sql(text("SELECT * FROM loan_transactions"),  conn)

    summary = {
        "customers": len(customers),
        "vehicles":  len(vehicles),
        "branches":  len(branches),
        "loans":     len(loans),
    }
    logger.info(f"EXTRACT — completed: {summary}")

    return {
        "customers": customers,
        "vehicles":  vehicles,
        "branches":  branches,
        "loans":     loans,
        "_summary":  summary,
    }
