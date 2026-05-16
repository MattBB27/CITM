import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
os.makedirs(DB_DIR, exist_ok=True)

# ── Database Connection Strings ──────────────────────────────────────────────
# In production these would point to Azure SQL (operational) and
# Azure Synapse Analytics (warehouse). SQLite is used locally for demonstration.

OPERATIONAL_DB_URL = os.getenv("OPERATIONAL_DB_URL")
WAREHOUSE_DB_URL = os.getenv("WAREHOUSE_DB_URL")

# ── ETL ───────────────────────────────────────────────────────────────────────
ETL_BATCH_SIZE = 1000

# ── Flask ─────────────────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY") # not rlly needed
DEBUG      = True
HOST       = "0.0.0.0"
PORT       = 5000
