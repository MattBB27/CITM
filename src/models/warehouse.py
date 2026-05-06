"""
Data Warehouse Models — Star Schema (CITM Data Warehouse)
----------------------------------------------------------
Implements the dimensional model specified in the CITM design document.
In production this would reside in Azure Synapse Analytics.

Star Schema:
    fact_loan_transaction  ─── dim_customer
                           ─── dim_vehicle
                           ─── dim_date  (loan_date_key, return_date_key)
                           ─── dim_branch
"""
from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


# ── Dimension Tables ──────────────────────────────────────────────────────────

class DimCustomer(Base):
    __tablename__ = "dim_customer"

    customer_key          = Column(Integer, primary_key=True, autoincrement=True)
    customer_id           = Column(String(50),  nullable=False, unique=True)   # NK
    name                  = Column(String(100))
    driver_license_number = Column(String(50))

    loans = relationship("FactLoanTransaction",
                         foreign_keys="FactLoanTransaction.customer_key",
                         back_populates="customer")


class DimVehicle(Base):
    __tablename__ = "dim_vehicle"

    vehicle_key  = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id   = Column(String(50),  nullable=False, unique=True)            # NK
    model        = Column(String(100))
    manufacturer = Column(String(100))
    vehicle_type = Column(String(50))

    loans = relationship("FactLoanTransaction",
                         foreign_keys="FactLoanTransaction.vehicle_key",
                         back_populates="vehicle")


class DimDate(Base):
    __tablename__ = "dim_date"

    date_key     = Column(Integer, primary_key=True)   # YYYYMMDD — acts as PK + NK
    full_date    = Column(Date,    nullable=False)
    day          = Column(Integer)
    month        = Column(Integer)
    year         = Column(Integer)
    quarter      = Column(Integer)
    day_of_week  = Column(String(20))
    month_name   = Column(String(20))


class DimBranch(Base):
    __tablename__ = "dim_branch"

    branch_key  = Column(Integer, primary_key=True, autoincrement=True)
    branch_id   = Column(String(50),  nullable=False, unique=True)             # NK
    branch_name = Column(String(100))
    city        = Column(String(50))
    state       = Column(String(50))

    loans = relationship("FactLoanTransaction",
                         foreign_keys="FactLoanTransaction.branch_key",
                         back_populates="branch")


# ── Fact Table ────────────────────────────────────────────────────────────────

class FactLoanTransaction(Base):
    __tablename__ = "fact_loan_transaction"

    loan_fact_key     = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys → dimension tables
    customer_key      = Column(Integer, ForeignKey("dim_customer.customer_key"))
    vehicle_key       = Column(Integer, ForeignKey("dim_vehicle.vehicle_key"))
    loan_date_key     = Column(Integer, ForeignKey("dim_date.date_key"))
    return_date_key   = Column(Integer, ForeignKey("dim_date.date_key"))
    branch_key        = Column(Integer, ForeignKey("dim_branch.branch_key"))

    # Degenerate dimension (traceability back to source)
    source_loan_id    = Column(String(50), unique=True)

    # Measures / facts
    loan_fee          = Column(Numeric(10, 2))
    loan_duration_days = Column(Integer)
    distance_driven   = Column(Integer)
    starting_mileage  = Column(Integer)
    ending_mileage    = Column(Integer)

    # Relationships
    customer  = relationship("DimCustomer", foreign_keys=[customer_key],    back_populates="loans")
    vehicle   = relationship("DimVehicle",  foreign_keys=[vehicle_key],     back_populates="loans")
    branch    = relationship("DimBranch",   foreign_keys=[branch_key],      back_populates="loans")
    loan_date = relationship("DimDate",     foreign_keys=[loan_date_key])
    ret_date  = relationship("DimDate",     foreign_keys=[return_date_key])
