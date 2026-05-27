"""
CITM Tests
---------------
Tests for ETL extract, transform, and load phases.
Run:  python -m pytest tests/ -v
"""
import pytest
import pandas as pd
from datetime import date

# ── Transform unit tests ───────────────────────────────────────────────────────

from src.etl.transform import (
    transform_dim_customer,
    transform_dim_vehicle,
    transform_dim_branch,
    transform_dim_date,
    transform_fact_loans,
)


@pytest.fixture
def sample_customers():
    return pd.DataFrame([
        {"customer_id": "CUST-001", "name": "  James Nguyen  ",
         "driver_license_number": "NSW-DL-001", "email": "j@test.com", "phone": "0400"},
        {"customer_id": "CUST-002", "name": "Sarah Mitchell",
         "driver_license_number": "VIC-DL-002", "email": "s@test.com", "phone": "0401"},
        # Duplicate — should be dropped
        {"customer_id": "CUST-001", "name": "James Nguyen",
         "driver_license_number": "NSW-DL-001", "email": "j@test.com", "phone": "0400"},
    ])


@pytest.fixture
def sample_vehicles():
    return pd.DataFrame([
        {"vehicle_id": "VEH-001", "model": "Corolla",  "manufacturer": "Toyota",
         "vehicle_type": "Sedan",  "mileage": 18500, "is_available": True},
        {"vehicle_id": "VEH-002", "model": "RAV4",     "manufacturer": "Toyota",
         "vehicle_type": "SUV",    "mileage": 27400, "is_available": True},
    ])


@pytest.fixture
def sample_branches():
    return pd.DataFrame([
        {"branch_id": "BRN-001", "branch_name": "Sydney CBD", "city": "Sydney", "state": "NSW"},
        {"branch_id": "BRN-002", "branch_name": "Melbourne Central", "city": "Melbourne", "state": "VIC"},
    ])


@pytest.fixture
def sample_loans():
    return pd.DataFrame([
        {"loan_id": "LOAN-00001", "customer_id": "CUST-001", "vehicle_id": "VEH-001",
         "branch_id": "BRN-001", "loan_date": "2024-03-01", "return_date": "2024-03-08",
         "loan_fee": 595.00, "starting_mileage": 18500, "ending_mileage": 19340},
        {"loan_id": "LOAN-00002", "customer_id": "CUST-002", "vehicle_id": "VEH-002",
         "branch_id": "BRN-002", "loan_date": "2024-04-15", "return_date": "2024-04-18",
         "loan_fee": 330.00, "starting_mileage": 27400, "ending_mileage": 27760},
    ])


# ── Customer dimension ─────────────────────────────────────────────────────────

class TestTransformDimCustomer:

    def test_deduplication(self, sample_customers):
        result = transform_dim_customer(sample_customers)
        assert len(result) == 2, "Duplicate customer_id should be removed"

    def test_name_stripped(self, sample_customers):
        result = transform_dim_customer(sample_customers)
        james = result[result["customer_id"] == "CUST-001"].iloc[0]
        assert james["name"] == "James Nguyen", "Leading/trailing whitespace should be stripped"

    def test_required_columns(self, sample_customers):
        result = transform_dim_customer(sample_customers)
        assert set(["customer_id", "name", "driver_license_number"]).issubset(result.columns)

    def test_no_extra_columns(self, sample_customers):
        result = transform_dim_customer(sample_customers)
        assert "email" not in result.columns, "Operational-only fields must not leak into dimension"


# ── Vehicle dimension ──────────────────────────────────────────────────────────

class TestTransformDimVehicle:

    def test_row_count(self, sample_vehicles):
        result = transform_dim_vehicle(sample_vehicles)
        assert len(result) == 2

    def test_no_mileage_column(self, sample_vehicles):
        result = transform_dim_vehicle(sample_vehicles)
        assert "mileage" not in result.columns

    def test_columns_present(self, sample_vehicles):
        result = transform_dim_vehicle(sample_vehicles)
        for col in ["vehicle_id", "model", "manufacturer", "vehicle_type"]:
            assert col in result.columns


# ── Date dimension ─────────────────────────────────────────────────────────────

class TestTransformDimDate:

    def test_unique_dates(self, sample_loans):
        result = transform_dim_date(sample_loans)
        assert result["date_key"].is_unique

    def test_date_key_format(self, sample_loans):
        result = transform_dim_date(sample_loans)
        # All keys should be 8-digit integers (YYYYMMDD)
        assert all(10000000 <= k <= 99999999 for k in result["date_key"])

    def test_expected_dates(self, sample_loans):
        result = transform_dim_date(sample_loans)
        keys = set(result["date_key"])
        assert 20240301 in keys
        assert 20240308 in keys
        assert 20240415 in keys

    def test_month_name_populated(self, sample_loans):
        result = transform_dim_date(sample_loans)
        assert result["month_name"].notna().all()

    def test_quarter_values(self, sample_loans):
        result = transform_dim_date(sample_loans)
        assert set(result["quarter"]).issubset({1, 2, 3, 4})


# ── Fact transform ─────────────────────────────────────────────────────────────

class TestTransformFactLoans:

    def _build_maps(self, sample_customers, sample_vehicles, sample_branches, sample_loans):
        dim_c = transform_dim_customer(sample_customers)
        dim_v = transform_dim_vehicle(sample_vehicles)
        dim_b = transform_dim_branch(sample_branches)
        dim_d = transform_dim_date(sample_loans)

        # Simulate surrogate keys (1-based index)
        customer_map = {row["customer_id"]: i+1 for i, row in dim_c.iterrows()}
        vehicle_map  = {row["vehicle_id"]:  i+1 for i, row in dim_v.iterrows()}
        branch_map   = {row["branch_id"]:   i+1 for i, row in dim_b.iterrows()}
        date_map     = dict(zip(dim_d["date_key"], dim_d["date_key"]))
        return customer_map, vehicle_map, branch_map, date_map

    def test_row_count(self, sample_customers, sample_vehicles, sample_branches, sample_loans):
        c, v, b, d = self._build_maps(sample_customers, sample_vehicles, sample_branches, sample_loans)
        result = transform_fact_loans(sample_loans, c, v, d, b, set())
        assert len(result) == 2

    def test_incremental_skip(self, sample_customers, sample_vehicles, sample_branches, sample_loans):
        c, v, b, d = self._build_maps(sample_customers, sample_vehicles, sample_branches, sample_loans)
        result = transform_fact_loans(sample_loans, c, v, d, b, {"LOAN-00001"})
        assert len(result) == 1, "Already-loaded loan should be skipped"

    def test_computed_duration(self, sample_customers, sample_vehicles, sample_branches, sample_loans):
        c, v, b, d = self._build_maps(sample_customers, sample_vehicles, sample_branches, sample_loans)
        result = transform_fact_loans(sample_loans, c, v, d, b, set())
        row = result[result["source_loan_id"] == "LOAN-00001"].iloc[0]
        assert row["loan_duration_days"] == 7

    def test_computed_distance(self, sample_customers, sample_vehicles, sample_branches, sample_loans):
        c, v, b, d = self._build_maps(sample_customers, sample_vehicles, sample_branches, sample_loans)
        result = transform_fact_loans(sample_loans, c, v, d, b, set())
        row = result[result["source_loan_id"] == "LOAN-00001"].iloc[0]
        assert row["distance_driven"] == 840

    def test_no_null_keys(self, sample_customers, sample_vehicles, sample_branches, sample_loans):
        c, v, b, d = self._build_maps(sample_customers, sample_vehicles, sample_branches, sample_loans)
        result = transform_fact_loans(sample_loans, c, v, d, b, set())
        key_cols = ["customer_key", "vehicle_key", "loan_date_key", "return_date_key", "branch_key"]
        assert result[key_cols].notna().all().all()
