"""
CITM — Entry Point
-------------------
Initialises databases, seeds sample data, and starts the Flask dev server.

Usage:
    python run.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

import config
from src.seed  import setup_and_seed
from src.models.warehouse import Base as WhBase
from sqlalchemy import create_engine

def main():
    print("=" * 55)
    print("  CITM — Car Rental Inventory Management System")
    print("=" * 55)

    # 1. Initialise databases and seed operational data
    print("\n[1/3] Setting up databases...")
    setup_and_seed(loan_count=120)

    # 2. Ensure warehouse schema exists
    print("[2/3] Initialising warehouse schema...")
    wh_engine = create_engine(config.WAREHOUSE_DB_URL)
    WhBase.metadata.create_all(wh_engine)
    print("      Warehouse schema ready.")

    # 3. Start Flask
    print("[3/3] Starting web server...")
    print(f"\n  → http://localhost:{config.PORT}")
    print("  → ETL Panel: http://localhost:{}/etl".format(config.PORT))
    print("  → Analytics: http://localhost:{}/analytics".format(config.PORT))
    print("\n  Press Ctrl+C to stop.\n")

    from src.app import create_app
    app = create_app()
    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG, use_reloader=False)

if __name__ == "__main__":
    main()