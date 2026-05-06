"""
ETL — Load Phase
-----------------
Uses single-statement INSERT...SELECT UNION ALL to load batches into Synapse.
This is Synapse's documented pattern for small-batch DML and ensures the
write is committed before the function returns.
"""
import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def _make_engine(url: str):
    return create_engine(url, execution_options={"isolation_level": "AUTOCOMMIT"})


def _read(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def _escape(v):
    """T-SQL literal escape."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    if hasattr(v, 'item'):  # numpy
        return str(v.item())
    s = str(v).replace("'", "''")
    return f"'{s}'"


def _bulk_insert(df: pd.DataFrame, table_name: str, engine, batch_size: int = 50):
    """
    Insert via INSERT INTO ... SELECT ... UNION ALL — Synapse's preferred
    pattern for small batches. Each batch is one statement = one commit.
    """
    cols = list(df.columns)
    cols_sql = ", ".join(cols)

    total = len(df)
    rows = df.to_dict(orient="records")

    with engine.connect() as conn:
        for i in range(0, total, batch_size):
            chunk = rows[i:i+batch_size]
            select_clauses = []
            for r in chunk:
                values = ", ".join(_escape(r[c]) for c in cols)
                select_clauses.append(f"SELECT {values}")
            sql = f"INSERT INTO {table_name} ({cols_sql}) " + " UNION ALL ".join(select_clauses)
            conn.execute(text(sql))

    logger.info(f"LOAD — {table_name}: inserted {total} rows")


def _read_with_retry(engine, sql: str, expected_min: int, retries: int = 6, delay: float = 2.0) -> pd.DataFrame:
    df = pd.DataFrame()
    for attempt in range(retries):
        df = _read(engine, sql)
        if len(df) >= expected_min:
            return df
        logger.info(f"LOAD — read got {len(df)}/{expected_min} rows, retry {attempt+1}/{retries}")
        time.sleep(delay)
    return df


def _upsert_dimension(df, engine, table_name, business_key, surrogate_key) -> dict:
    try:
        existing = _read(engine, f"SELECT {business_key}, {surrogate_key} FROM {table_name}")
        existing_bkeys = set(existing[business_key].tolist())
        logger.info(f"LOAD — {table_name}: {len(existing_bkeys)} existing rows")
    except Exception as e:
        logger.warning(f"LOAD — {table_name}: read failed ({e})")
        existing = pd.DataFrame(columns=[business_key, surrogate_key])
        existing_bkeys = set()

    new_rows = df[~df[business_key].isin(existing_bkeys)]
    total_expected = len(existing_bkeys) + len(new_rows)

    if not new_rows.empty:
        _bulk_insert(new_rows, table_name, engine)
        all_rows = _read_with_retry(
            engine,
            f"SELECT {business_key}, {surrogate_key} FROM {table_name}",
            expected_min=total_expected,
        )
    else:
        logger.info(f"LOAD — {table_name}: no new rows")
        all_rows = existing

    result = dict(zip(all_rows[business_key], all_rows[surrogate_key]))
    logger.info(f"LOAD — {table_name}: map has {len(result)} entries")
    return result


def load_dim_date(dim_date_df: pd.DataFrame, engine) -> dict:
    try:
        existing      = _read(engine, "SELECT date_key FROM dim_date")
        existing_keys = set(existing["date_key"].tolist())
        logger.info(f"LOAD — dim_date: {len(existing_keys)} existing rows")
    except Exception as e:
        logger.warning(f"LOAD — dim_date: read failed ({e})")
        existing_keys = set()

    new_rows = dim_date_df[~dim_date_df["date_key"].isin(existing_keys)]
    if not new_rows.empty:
        _bulk_insert(new_rows, "dim_date", engine)

    return dict(zip(dim_date_df["date_key"], dim_date_df["date_key"]))


def get_loaded_loan_ids(engine) -> set:
    try:
        result = _read(engine, "SELECT source_loan_id FROM fact_loan_transaction")
        return set(result["source_loan_id"].tolist())
    except Exception:
        return set()


def load_fact(fact_df: pd.DataFrame, engine) -> int:
    if fact_df.empty:
        return 0
    _bulk_insert(fact_df, "fact_loan_transaction", engine)
    return len(fact_df)


def load_all(transformed: dict, warehouse_url: str) -> dict:
    logger.info("LOAD — connecting to warehouse")
    engine = _make_engine(warehouse_url)

    customer_map = _upsert_dimension(transformed["dim_customer"], engine, "dim_customer", "customer_id", "customer_key")
    vehicle_map  = _upsert_dimension(transformed["dim_vehicle"],  engine, "dim_vehicle",  "vehicle_id",  "vehicle_key")
    branch_map   = _upsert_dimension(transformed["dim_branch"],   engine, "dim_branch",   "branch_id",   "branch_key")
    date_map     = load_dim_date(transformed["dim_date"], engine)
    loaded_loan_ids = get_loaded_loan_ids(engine)

    logger.info(f"LOAD — maps: customers={len(customer_map)} vehicles={len(vehicle_map)} "
                f"branches={len(branch_map)} dates={len(date_map)}")

    return {
        "customer_map":    customer_map,
        "vehicle_map":     vehicle_map,
        "branch_map":      branch_map,
        "date_map":        date_map,
        "loaded_loan_ids": loaded_loan_ids,
        "engine":          engine,
    }