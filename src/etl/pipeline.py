"""
ETL Pipeline Orchestrator
-----------------------------
Coordinates the Extract -> Transform -> Load sequence.
Produces a structured result dict suitable for logging and API responses.

"""
import time
import logging
from datetime import datetime

import config
from src.etl.extract   import extract_all
from src.etl.transform import transform_all, transform_fact_loans
from src.etl.load      import load_all, load_fact

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("citm.etl.pipeline")


def run_etl(
    operational_url: str = config.OPERATIONAL_DB_URL,
    warehouse_url:   str = config.WAREHOUSE_DB_URL,
) -> dict:
    """
    Run the full ETL pipeline.

    Returns
    -------
    dict with keys: status, started_at, completed_at,
                    total_duration_seconds, steps
    """
    pipeline_start = time.perf_counter()
    result = {
        "status":     "success",
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "steps":      {},
    }

    logger.info("=" * 60)
    logger.info("CITM ETL PIPELINE — STARTED")
    logger.info("=" * 60)

    try:
        # ── EXTRACT ──────────────────────────────────────────────────
        t0 = time.perf_counter()
        extracted = extract_all(operational_url)
        result["steps"]["extract"] = {
            "status":           "success",
            "records_extracted": extracted["_summary"],
            "duration_s":       round(time.perf_counter() - t0, 4),
        }

        # ── TRANSFORM (dimensions) ───────────────────────────────────
        t0 = time.perf_counter()
        transformed = transform_all(extracted)
        result["steps"]["transform_dimensions"] = {
            "status":   "success",
            "row_counts": {
                k: len(v)
                for k, v in transformed.items()
                if k != "loans_raw"
            },
            "duration_s": round(time.perf_counter() - t0, 4),
        }

        # ── LOAD (dimensions) ────────────────────────────────────────
        t0 = time.perf_counter()
        load_result = load_all(transformed, warehouse_url)
        result["steps"]["load_dimensions"] = {
            "status":        "success",
            "dim_customer":  len(load_result["customer_map"]),
            "dim_vehicle":   len(load_result["vehicle_map"]),
            "dim_branch":    len(load_result["branch_map"]),
            "dim_date":      len(load_result["date_map"]),
            "duration_s":    round(time.perf_counter() - t0, 4),
        }

        # ── TRANSFORM (fact) ─────────────────────────────────────────
        t0 = time.perf_counter()
        fact_df = transform_fact_loans(
            loans_df        = transformed["loans_raw"],
            customer_map    = load_result["customer_map"],
            vehicle_map     = load_result["vehicle_map"],
            date_map        = load_result["date_map"],
            branch_map      = load_result["branch_map"],
            loaded_loan_ids = load_result["loaded_loan_ids"],
        )
        result["steps"]["transform_facts"] = {
            "status":     "success",
            "new_records": len(fact_df),
            "duration_s": round(time.perf_counter() - t0, 4),
        }

        # ── LOAD (fact) ──────────────────────────────────────────────
        t0 = time.perf_counter()
        n_inserted = load_fact(fact_df, load_result["engine"])
        result["steps"]["load_facts"] = {
            "status":          "success",
            "records_inserted": n_inserted,
            "duration_s":      round(time.perf_counter() - t0, 4),
        }

    except Exception as exc:
        logger.exception("ETL pipeline FAILED")
        result["status"] = "failed"
        result["error"]  = str(exc)

    result["total_duration_s"] = round(time.perf_counter() - pipeline_start, 4)
    result["completed_at"]     = datetime.now().isoformat(timespec="seconds")

    logger.info("=" * 60)
    logger.info(f"CITM ETL PIPELINE — {result['status'].upper()} "
                f"({result['total_duration_s']}s)")
    logger.info("=" * 60)

    return result


if __name__ == "__main__":
    import json
    res = run_etl()
    print(json.dumps(res, indent=2))
