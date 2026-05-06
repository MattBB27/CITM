"""
ETL — Load Phase
-----------------
Loads transformed DataFrames into the data warehouse (star schema).
Uses an upsert pattern for dimensions and an incremental append for facts.

FR 4.3 — load transformed data into the data warehouse
FR 5.1 — generate fact tables and dimension tables using a star schema
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def _upsert_dimension(
    df:           pd.DataFrame,
    engine,
    table_name:   str,
    business_key: str,
    surrogate_key: str,
) -> dict:
    """
    Insert new dimension rows (skip existing by business key).
    Returns a dict mapping business_key → surrogate_key for ALL rows.
    """
    with engine.connect() as conn:
        existing = pd.read_sql(
            text(f"SELECT {business_key}, {surrogate_key} FROM {table_name}"), conn
        )

    existing_bkeys = set(existing[business_key].tolist())
    new_rows = df[~df[business_key].isin(existing_bkeys)]

    if not new_rows.empty:
        new_rows.to_sql(table_name, engine, if_exists="append", index=False)
        logger.info(f"LOAD — {table_name}: inserted {len(new_rows)} new rows")
    else:
        logger.info(f"LOAD — {table_name}: no new rows")

    # Re-read to include newly inserted rows with their surrogate keys
    with engine.connect() as conn:
        all_rows = pd.read_sql(
            text(f"SELECT {business_key}, {surrogate_key} FROM {table_name}"), conn
        )

    return dict(zip(all_rows[business_key], all_rows[surrogate_key]))


def load_dim_date(dim_date_df: pd.DataFrame, engine) -> dict:
    """Load dim_date (date_key is both PK and NK). Returns identity map."""
    with engine.connect() as conn:
        try:
            existing = pd.read_sql(text("SELECT date_key FROM dim_date"), conn)
            existing_keys = set(existing["date_key"].tolist())
        except Exception:
            existing_keys = set()

    new_rows = dim_date_df[~dim_date_df["date_key"].isin(existing_keys)]
    if not new_rows.empty:
        new_rows.to_sql("dim_date", engine, if_exists="append", index=False)
        logger.info(f"LOAD — dim_date: inserted {len(new_rows)} new rows")
    else:
        logger.info("LOAD — dim_date: no new rows")

    # Return identity map (date_key → date_key)
    return dict(zip(dim_date_df["date_key"], dim_date_df["date_key"]))


def get_loaded_loan_ids(engine) -> set:
    """Return loan IDs already in the fact table (incremental ETL guard)."""
    try:
        with engine.connect() as conn:
            result = pd.read_sql(
                text("SELECT source_loan_id FROM fact_loan_transaction"), conn
            )
        return set(result["source_loan_id"].tolist())
    except Exception:
        return set()


def load_fact(fact_df: pd.DataFrame, engine) -> int:
    """Append new fact rows. Returns count inserted."""
    if fact_df.empty:
        return 0
    fact_df.to_sql("fact_loan_transaction", engine, if_exists="append", index=False)
    logger.info(f"LOAD — fact_loan_transaction: inserted {len(fact_df)} rows")
    return len(fact_df)


def load_all(transformed: dict, warehouse_url: str) -> dict:
    """
    Load all dimension tables and return surrogate key maps + engine.
    Fact table is loaded separately once transforms are complete.
    """
    logger.info("LOAD — connecting to warehouse")
    engine = create_engine(warehouse_url)

    customer_map = _upsert_dimension(
        transformed["dim_customer"], engine, "dim_customer", "customer_id", "customer_key"
    )
    vehicle_map = _upsert_dimension(
        transformed["dim_vehicle"], engine, "dim_vehicle", "vehicle_id", "vehicle_key"
    )
    branch_map = _upsert_dimension(
        transformed["dim_branch"], engine, "dim_branch", "branch_id", "branch_key"
    )
    date_map = load_dim_date(transformed["dim_date"], engine)

    loaded_loan_ids = get_loaded_loan_ids(engine)

    return {
        "customer_map":     customer_map,
        "vehicle_map":      vehicle_map,
        "branch_map":       branch_map,
        "date_map":         date_map,
        "loaded_loan_ids":  loaded_loan_ids,
        "engine":           engine,
    }
