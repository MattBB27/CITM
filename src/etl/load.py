"""
ETL Load Phase
-----------------
Optimised Synapse loading layer.

Improvements:
- Reduced retry overhead
- Larger batch sizes
- Connection pooling improvements
- Reduced unnecessary latency
- Preserved ALL core ETL logic and dimensional behaviour

Still uses Synapse-compatible INSERT ... SELECT UNION ALL batching.
"""

import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


# ── Engine ────────────────────────────────────────────────────────────────

def _make_engine(url: str):
    """
    Create SQLAlchemy engine optimised for Synapse.
    """
    return create_engine(
        url,
        execution_options={"isolation_level": "AUTOCOMMIT"},
        pool_pre_ping=True,
        pool_recycle=3600,
        fast_executemany=True,
    )


# ── Read Helpers ──────────────────────────────────────────────────────────

def _read(engine, sql: str) -> pd.DataFrame:
    """
    Execute SELECT query and return DataFrame.
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def _read_with_retry(
    engine,
    sql: str,
    expected_min: int,
    retries: int = 3,
    delay: float = 0.5,
) -> pd.DataFrame:
    """
    Retry reads briefly for Synapse propagation consistency.

    Synapse can occasionally delay visibility of inserted rows.
    Reduced retry overhead dramatically improves ETL runtime.
    """
    df = pd.DataFrame()

    for attempt in range(retries):
        df = _read(engine, sql)

        if len(df) >= expected_min:
            return df

        logger.info(
            f"LOAD — read got {len(df)}/{expected_min} rows, "
            f"retry {attempt + 1}/{retries}"
        )

        time.sleep(delay)

    return df


# ── SQL Value Escaping ────────────────────────────────────────────────────

def _escape(v):
    """
    Escape Python values into safe T-SQL literals.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"

    if isinstance(v, bool):
        return "1" if v else "0"

    if isinstance(v, (int, float)):
        return str(v)

    if hasattr(v, "item"):  # numpy scalar
        return str(v.item())

    s = str(v).replace("'", "''")
    return f"'{s}'"


# ── Bulk Insert ───────────────────────────────────────────────────────────

def _bulk_insert(
    df: pd.DataFrame,
    table_name: str,
    engine,
    batch_size: int = 250,
):
    """
    Synapse-compatible batch insert.

    Uses:
        INSERT INTO ... SELECT ... UNION ALL

    This is the recommended small-batch DML pattern for Synapse.

    Optimisations:
    - Larger batch sizes
    - Reduced Python overhead
    - Single transaction per batch
    """

    if df.empty:
        logger.info(f"LOAD — {table_name}: no rows to insert")
        return

    cols = list(df.columns)
    cols_sql = ", ".join(cols)

    rows = df.to_dict(orient="records")
    total = len(rows)

    start = time.perf_counter()

    with engine.connect() as conn:

        for i in range(0, total, batch_size):

            chunk = rows[i:i + batch_size]

            select_clauses = []

            for row in chunk:
                values = ", ".join(_escape(row[c]) for c in cols)
                select_clauses.append(f"SELECT {values}")

            sql = (
                f"INSERT INTO {table_name} ({cols_sql}) "
                + " UNION ALL ".join(select_clauses)
            )

            conn.execute(text(sql))

    duration = round(time.perf_counter() - start, 4)

    logger.info(
        f"LOAD — {table_name}: inserted {total} rows "
        f"in {duration}s"
    )


# ── Dimension Upsert ──────────────────────────────────────────────────────

def _upsert_dimension(
    df,
    engine,
    table_name,
    business_key,
    surrogate_key,
) -> dict:
    """
    Incrementally load dimension rows and return:
        business_key -> surrogate_key map
    """

    try:
        existing = _read(
            engine,
            f"""
            SELECT {business_key}, {surrogate_key}
            FROM {table_name}
            """
        )

        existing_bkeys = set(existing[business_key].tolist())

        logger.info(
            f"LOAD — {table_name}: "
            f"{len(existing_bkeys)} existing rows"
        )

    except Exception as e:

        logger.warning(
            f"LOAD — {table_name}: initial read failed ({e})"
        )

        existing = pd.DataFrame(
            columns=[business_key, surrogate_key]
        )

        existing_bkeys = set()

    # Incremental filter
    new_rows = df[
        ~df[business_key].isin(existing_bkeys)
    ]

    total_expected = len(existing_bkeys) + len(new_rows)

    # Insert only unseen rows
    if not new_rows.empty:

        logger.info(
            f"LOAD — {table_name}: "
            f"inserting {len(new_rows)} new rows"
        )

        _bulk_insert(
            new_rows,
            table_name,
            engine,
        )

        all_rows = _read_with_retry(
            engine,
            f"""
            SELECT {business_key}, {surrogate_key}
            FROM {table_name}
            """,
            expected_min=total_expected,
        )

    else:
        logger.info(f"LOAD — {table_name}: no new rows")
        all_rows = existing

    result = dict(
        zip(
            all_rows[business_key],
            all_rows[surrogate_key],
        )
    )

    logger.info(
        f"LOAD — {table_name}: "
        f"map has {len(result)} entries"
    )

    return result


# ── Date Dimension ────────────────────────────────────────────────────────

def load_dim_date(dim_date_df: pd.DataFrame, engine) -> dict:
    """
    Load date dimension incrementally.
    """

    try:
        existing = _read(
            engine,
            "SELECT date_key FROM dim_date"
        )

        existing_keys = set(
            existing["date_key"].tolist()
        )

        logger.info(
            f"LOAD — dim_date: "
            f"{len(existing_keys)} existing rows"
        )

    except Exception as e:

        logger.warning(
            f"LOAD — dim_date: read failed ({e})"
        )

        existing_keys = set()

    new_rows = dim_date_df[
        ~dim_date_df["date_key"].isin(existing_keys)
    ]

    if not new_rows.empty:

        logger.info(
            f"LOAD — dim_date: "
            f"inserting {len(new_rows)} rows"
        )

        _bulk_insert(
            new_rows,
            "dim_date",
            engine,
        )

    return dict(
        zip(
            dim_date_df["date_key"],
            dim_date_df["date_key"],
        )
    )


# ── Fact Helpers ──────────────────────────────────────────────────────────

def get_loaded_loan_ids(engine) -> set:
    """
    Get already-loaded fact IDs for incremental loading.
    """

    try:
        result = _read(
            engine,
            """
            SELECT source_loan_id
            FROM fact_loan_transaction
            """
        )

        return set(
            result["source_loan_id"].tolist()
        )

    except Exception:

        logger.warning(
            "LOAD — could not read existing fact rows"
        )

        return set()


def load_fact(fact_df: pd.DataFrame, engine) -> int:
    """
    Load fact table incrementally.
    """

    if fact_df.empty:

        logger.info(
            "LOAD — fact_loan_transaction: no new rows"
        )

        return 0

    _bulk_insert(
        fact_df,
        "fact_loan_transaction",
        engine,
    )

    return len(fact_df)


# ── Main Loader ───────────────────────────────────────────────────────────

def load_all(
    transformed: dict,
    warehouse_url: str,
) -> dict:
    """
    Load all dimensions and prepare mappings for fact loading.
    """

    logger.info("LOAD — connecting to warehouse")

    engine = _make_engine(warehouse_url)

    customer_map = _upsert_dimension(
        transformed["dim_customer"],
        engine,
        "dim_customer",
        "customer_id",
        "customer_key",
    )

    vehicle_map = _upsert_dimension(
        transformed["dim_vehicle"],
        engine,
        "dim_vehicle",
        "vehicle_id",
        "vehicle_key",
    )

    branch_map = _upsert_dimension(
        transformed["dim_branch"],
        engine,
        "dim_branch",
        "branch_id",
        "branch_key",
    )

    date_map = load_dim_date(
        transformed["dim_date"],
        engine,
    )

    loaded_loan_ids = get_loaded_loan_ids(engine)

    logger.info(
        f"LOAD — maps: "
        f"customers={len(customer_map)} "
        f"vehicles={len(vehicle_map)} "
        f"branches={len(branch_map)} "
        f"dates={len(date_map)}"
    )

    return {
        "customer_map": customer_map,
        "vehicle_map": vehicle_map,
        "branch_map": branch_map,
        "date_map": date_map,
        "loaded_loan_ids": loaded_loan_ids,
        "engine": engine,
    }