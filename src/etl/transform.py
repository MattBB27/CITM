"""
ETL Transform Phase
----------------------
Cleans, structures, and enriches raw operational data into the dimensional model.
Uses pandas for in-memory transformation.

"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ── Dimension Transforms ──────────────────────────────────────────────────────

def transform_dim_customer(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Select and deduplicate customer attributes for the customer dimension."""
    cols = ["customer_id", "name", "driver_license_number"]
    dim  = customers_df[cols].copy()
    dim  = dim.drop_duplicates(subset=["customer_id"])
    # Type enforcement
    dim["customer_id"]            = dim["customer_id"].astype(str).str.strip()
    dim["name"]                   = dim["name"].astype(str).str.strip()
    dim["driver_license_number"]  = dim["driver_license_number"].astype(str).str.strip()
    logger.info(f"TRANSFORM — dim_customer: {len(dim)} rows")
    return dim


def transform_dim_vehicle(vehicles_df: pd.DataFrame) -> pd.DataFrame:
    """Select and deduplicate vehicle attributes for the vehicle dimension."""
    cols = ["vehicle_id", "model", "manufacturer", "vehicle_type"]
    dim  = vehicles_df[cols].copy()
    dim  = dim.drop_duplicates(subset=["vehicle_id"])
    for c in cols:
        dim[c] = dim[c].astype(str).str.strip()
    logger.info(f"TRANSFORM — dim_vehicle: {len(dim)} rows")
    return dim


def transform_dim_branch(branches_df: pd.DataFrame) -> pd.DataFrame:
    """Select and deduplicate branch attributes for the branch dimension."""
    cols = ["branch_id", "branch_name", "city", "state"]
    dim  = branches_df[cols].copy()
    dim  = dim.drop_duplicates(subset=["branch_id"])
    for c in cols:
        dim[c] = dim[c].astype(str).str.strip()
    logger.info(f"TRANSFORM — dim_branch: {len(dim)} rows")
    return dim


def transform_dim_date(loans_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a date dimension from all loan/return dates.
    date_key is stored as YYYYMMDD integer (natural key == surrogate key for dates).
    """
    loan_dates   = pd.to_datetime(loans_df["loan_date"])
    return_dates = pd.to_datetime(loans_df["return_date"])
    all_dates    = pd.concat([loan_dates, return_dates]).dt.normalize().drop_duplicates()

    records = []
    for dt in all_dates:
        d = dt.date()
        records.append({
            "date_key":    int(d.strftime("%Y%m%d")),
            "full_date":   d,
            "day":         d.day,
            "month":       d.month,
            "year":        d.year,
            "quarter":     (d.month - 1) // 3 + 1,
            "day_of_week": dt.strftime("%A"),
            "month_name":  dt.strftime("%B"),
        })

    dim = pd.DataFrame(records).drop_duplicates(subset=["date_key"]).sort_values("date_key")
    logger.info(f"TRANSFORM — dim_date: {len(dim)} rows")
    return dim


def transform_fact_loans(
    loans_df:     pd.DataFrame,
    customer_map: dict,
    vehicle_map:  dict,
    date_map:     dict,
    branch_map:   dict,
    loaded_loan_ids: set,
) -> pd.DataFrame:
    """
    Transform loan transactions into the fact table.

    Parameters
    ----------
    *_map           : business_key -> surrogate_key dictionaries built after loading dims
    loaded_loan_ids : loan_ids already present in the warehouse (incremental load)
    """
    # Incremental filter - only process new records
    df = loans_df[~loans_df["loan_id"].isin(loaded_loan_ids)].copy()

    if df.empty:
        logger.info("TRANSFORM — fact_loan_transaction: 0 new rows (all already loaded)")
        return df

    # Map surrogate keys
    df["customer_key"]    = df["customer_id"].map(customer_map)
    df["vehicle_key"]     = df["vehicle_id"].map(vehicle_map)
    df["branch_key"]      = df["branch_id"].map(branch_map)
    df["loan_date_key"]   = pd.to_datetime(df["loan_date"]).dt.strftime("%Y%m%d").astype(int).map(date_map)
    df["return_date_key"] = pd.to_datetime(df["return_date"]).dt.strftime("%Y%m%d").astype(int).map(date_map)

    # Computed measures
    df["loan_duration_days"] = (
        pd.to_datetime(df["return_date"]) - pd.to_datetime(df["loan_date"])
    ).dt.days
    df["distance_driven"] = df["ending_mileage"] - df["starting_mileage"]

    # Select final columns
    fact = df[[
        "loan_id",          # -> source_loan_id (degenerate dim)
        "customer_key",
        "vehicle_key",
        "loan_date_key",
        "return_date_key",
        "branch_key",
        "loan_fee",
        "loan_duration_days",
        "distance_driven",
        "starting_mileage",
        "ending_mileage",
    ]].rename(columns={"loan_id": "source_loan_id"})

    fact["loan_fee"] = pd.to_numeric(fact["loan_fee"], errors="coerce")

    # Drop rows where any key lookup failed
    key_cols = ["customer_key", "vehicle_key", "loan_date_key", "return_date_key", "branch_key"]
    before = len(fact)
    fact = fact.dropna(subset=key_cols)
    if len(fact) < before:
        logger.warning(f"TRANSFORM — dropped {before - len(fact)} rows due to unresolved keys")

    logger.info(f"TRANSFORM — fact_loan_transaction: {len(fact)} new rows")
    return fact


# ── Orchestrator ──────────────────────────────────────────────────────────────

def transform_all(extracted: dict) -> dict:
    """Run all dimension transforms and return staged DataFrames."""
    logger.info("TRANSFORM — starting dimension transforms")
    return {
        "dim_customer": transform_dim_customer(extracted["customers"]),
        "dim_vehicle":  transform_dim_vehicle(extracted["vehicles"]),
        "dim_branch":   transform_dim_branch(extracted["branches"]),
        "dim_date":     transform_dim_date(extracted["loans"]),
        "loans_raw":    extracted["loans"],
    }
