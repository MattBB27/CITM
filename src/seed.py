"""
Database Seeder
---------------
Initialises both databases (schema creation) and populates the operational
database with realistic sample data for demonstration purposes.

Run standalone:   python -m src.seed
"""
import random
import logging
from datetime import date, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from src.models.operational import Base as OpBase, Customer, Vehicle, Branch, LoanTransaction
from src.models.warehouse   import Base as WhBase

logger = logging.getLogger(__name__)

# ── Sample Data ────────────────────────────────────────────────────────────────

BRANCHES = [
    ("BRN-001", "Sydney Branch",       "Sydney",    "NSW"),
    ("BRN-002", "Melbourne Branch","Melbourne", "VIC"),
    ("BRN-003", "Brisbane Branch",    "Brisbane",  "QLD"),
    ("BRN-004", "Perth Branch",      "Perth",     "WA"),
    ("BRN-005", "Adelaide Branch",   "Adelaide",  "SA"),
]

VEHICLES = [
    ("VEH-001", "Corolla",    "Toyota",     "Sedan",     18500),
    ("VEH-002", "Camry",      "Toyota",     "Sedan",     32100),
    ("VEH-003", "RAV4",       "Toyota",     "SUV",       27400),
    ("VEH-004", "HiLux",      "Toyota",     "Ute",       45200),
    ("VEH-005", "Ranger",     "Ford",       "Ute",       38700),
    ("VEH-006", "Puma",       "Ford",       "SUV",       12300),
    ("VEH-007", "Mondeo",     "Ford",       "Sedan",     55800),
    ("VEH-008", "CX-5",       "Mazda",      "SUV",       21600),
    ("VEH-009", "3",          "Mazda",      "Hatchback", 9800),
    ("VEH-010", "BT-50",      "Mazda",      "Ute",       61200),
    ("VEH-011", "Tucson",     "Hyundai",    "SUV",       14700),
    ("VEH-012", "i30",        "Hyundai",    "Hatchback", 8400),
    ("VEH-013", "Cerato",     "Kia",        "Sedan",     11200),
    ("VEH-014", "Sportage",   "Kia",        "SUV",       23900),
    ("VEH-015", "Carnival",   "Kia",        "Van",       33600),
    ("VEH-016", "Outlander",  "Mitsubishi", "SUV",       48300),
    ("VEH-017", "ASX",        "Mitsubishi", "SUV",       17100),
    ("VEH-018", "Triton",     "Mitsubishi", "Ute",       72400),
    ("VEH-019", "Impreza",    "Subaru",     "Hatchback", 6900),
    ("VEH-020", "Forester",   "Subaru",     "SUV",       29700),
]

CUSTOMERS = [
    ("CUST-001", "James Nguyen",     "NSW-DL-482910", "j.nguyen@email.com",    "0412 345 678"),
    ("CUST-002", "Sarah Mitchell",   "VIC-DL-294810", "s.mitchell@email.com",  "0423 456 789"),
    ("CUST-003", "Liam Thompson",    "QLD-DL-571234", "l.thompson@email.com",  "0434 567 890"),
    ("CUST-004", "Emma Wilson",      "WA-DL-381047",  "e.wilson@email.com",    "0445 678 901"),
    ("CUST-005", "Noah Davis",       "SA-DL-293810",  "n.davis@email.com",     "0456 789 012"),
    ("CUST-006", "Olivia Brown",     "NSW-DL-104728", "o.brown@email.com",     "0467 890 123"),
    ("CUST-007", "William Taylor",   "VIC-DL-839201", "w.taylor@email.com",    "0478 901 234"),
    ("CUST-008", "Isabella Anderson","QLD-DL-650293", "i.anderson@email.com",  "0489 012 345"),
    ("CUST-009", "Jack Martinez",    "WA-DL-472018",  "j.martinez@email.com",  "0491 123 456"),
    ("CUST-010", "Mia Garcia",       "SA-DL-810347",  "m.garcia@email.com",    "0402 234 567"),
    ("CUST-011", "Henry Jackson",    "NSW-DL-293847", "h.jackson@email.com",   "0413 345 678"),
    ("CUST-012", "Charlotte Lee",    "VIC-DL-182039", "c.lee@email.com",       "0424 456 789"),
    ("CUST-013", "Lucas Harris",     "QLD-DL-473920", "l.harris@email.com",    "0435 567 890"),
    ("CUST-014", "Amelia Clark",     "WA-DL-920381",  "a.clark@email.com",     "0446 678 901"),
    ("CUST-015", "Ethan Lewis",      "SA-DL-381920",  "e.lewis@email.com",     "0457 789 012"),
    ("CUST-016", "Harper Robinson",  "NSW-DL-570293", "h.robinson@email.com",  "0468 890 123"),
    ("CUST-017", "Mason Walker",     "VIC-DL-204837", "m.walker@email.com",    "0479 901 234"),
    ("CUST-018", "Evelyn Hall",      "QLD-DL-930182", "e.hall@email.com",      "0481 012 345"),
    ("CUST-019", "Logan Allen",      "WA-DL-481029",  "l.allen@email.com",     "0492 123 456"),
    ("CUST-020", "Abigail Young",    "SA-DL-103847",  "a.young@email.com",     "0403 234 567"),
    ("CUST-021", "Elijah King",      "NSW-DL-839201", "e.king@email.com",      "0414 345 678"),
    ("CUST-022", "Sophie Wright",    "VIC-DL-293019", "s.wright@email.com",    "0425 456 789"),
    ("CUST-023", "Benjamin Scott",   "QLD-DL-184029", "b.scott@email.com",     "0436 567 890"),
    ("CUST-024", "Chloe Green",      "WA-DL-920184",  "c.green@email.com",     "0447 678 901"),
    ("CUST-025", "Alexander Baker",  "SA-DL-471830",  "a.baker@email.com",     "0458 789 012"),
]

DAILY_RATES = {
    "Sedan":     85.0,
    "Hatchback": 75.0,
    "SUV":       110.0,
    "Ute":       130.0,
    "Van":       120.0,
}

random.seed(42)


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _generate_loans(
    n: int,
    customers: list[str],
    vehicles: list[tuple],
    branches: list[str],
) -> list[dict]:
    """Generate n plausible loan transaction records."""
    loans = []
    start_window = date(2024, 1, 1)
    end_window   = date(2024, 12, 31)

    for i in range(1, n + 1):
        cust_id = random.choice(customers)
        veh     = random.choice(vehicles)   # (id, model, mfr, type, mileage)
        brn_id  = random.choice(branches)

        loan_date    = _random_date(start_window, end_window)
        duration     = random.randint(1, 14)
        return_date  = loan_date + timedelta(days=duration)

        daily_rate   = DAILY_RATES.get(veh[3], 90.0)
        # Weekend surcharge
        if loan_date.weekday() >= 5:
            daily_rate *= 1.15
        loan_fee = round(daily_rate * duration, 2)

        start_km  = veh[4] + random.randint(0, 5000)
        daily_km  = random.randint(40, 200)
        end_km    = start_km + daily_km * duration

        loans.append({
            "loan_id":          f"LOAN-{i:05d}",
            "customer_id":      cust_id,
            "vehicle_id":       veh[0],
            "branch_id":        brn_id,
            "loan_date":        loan_date,
            "return_date":      return_date,
            "loan_fee":         loan_fee,
            "starting_mileage": start_km,
            "ending_mileage":   end_km,
        })
    return loans


def setup_databases():
    """Create tables in both databases."""
    op_engine = create_engine(config.OPERATIONAL_DB_URL)
    wh_engine = create_engine(config.WAREHOUSE_DB_URL)

    OpBase.metadata.create_all(op_engine)
    WhBase.metadata.create_all(wh_engine)

    logger.info("Databases initialised (tables created)")
    return op_engine, wh_engine


def seed_operational(op_engine, loan_count: int = 120):
    """Populate the operational database with sample data."""
    Session = sessionmaker(bind=op_engine)
    session = Session()

    # Skip if already seeded
    if session.query(Customer).count() > 0:
        logger.info("Operational DB already seeded — skipping")
        session.close()
        return

    # Insert Branches
    for row in BRANCHES:
        session.add(Branch(
            branch_id=row[0], branch_name=row[1], city=row[2], state=row[3]
        ))

    # Insert Vehicles
    for row in VEHICLES:
        session.add(Vehicle(
            vehicle_id=row[0], model=row[1], manufacturer=row[2],
            vehicle_type=row[3], mileage=row[4], is_available=True
        ))

    # Insert Customers
    for row in CUSTOMERS:
        session.add(Customer(
            customer_id=row[0], name=row[1], driver_license_number=row[2],
            email=row[3], phone=row[4]
        ))

    session.commit()

    # Insert Loans
    customer_ids = [c[0] for c in CUSTOMERS]
    branch_ids   = [b[0] for b in BRANCHES]

    loans = _generate_loans(loan_count, customer_ids, VEHICLES, branch_ids)
    for l in loans:
        session.add(LoanTransaction(**l))

    session.commit()
    session.close()

    logger.info(f"Seeded: {len(BRANCHES)} branches, {len(VEHICLES)} vehicles, "
                f"{len(CUSTOMERS)} customers, {loan_count} loans")


def setup_and_seed(loan_count: int = 120):
    op_engine, _ = setup_databases()
    seed_operational(op_engine, loan_count)
    print("✓ Databases ready")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
    setup_and_seed()
