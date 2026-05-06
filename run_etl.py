"""
Standalone ETL Runner
----------------------
Runs the full ETL pipeline from the command line without starting the web server.
Useful for scheduled execution (e.g. cron / Azure Data Factory trigger).

Usage:
    python run_etl.py
    python run_etl.py --verbose
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import json
import logging

def main():
    parser = argparse.ArgumentParser(description="CITM ETL Pipeline Runner")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    from src.seed import setup_and_seed
    setup_and_seed(loan_count=120)

    from src.etl.pipeline import run_etl
    result = run_etl()

    print("\n" + "=" * 55)
    print("  ETL RESULT SUMMARY")
    print("=" * 55)
    print(json.dumps(result, indent=2))

    if result["status"] == "failed":
        sys.exit(1)

if __name__ == "__main__":
    main()
