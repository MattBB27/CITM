"""
Data Warehouse Models - Star Schema (CITM Data Warehouse)
----------------------------------------------------------
Implements the dimensional model from the CITM design document.
Resides in Azure Synapse Analytics (Dedicated SQL Pool).

Note: Synapse does not support enforced UNIQUE or FOREIGN KEY constraints.
      Relationships use explicit primaryjoin expressions instead.
"""
from sqlalchemy import Column, Integer, String, Date, Numeric
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


# ── Dimension Tables ──────────────────────────────────────────────────────────

class DimCustomer(Base):
    __tablename__ = "dim_customer"

    customer_key          = Column(Integer, primary_key=True, autoincrement=True)
    customer_id           = Column(String(50), nullable=False)   # NK
    name                  = Column(String(100))
    driver_license_number = Column(String(50))

    loans = relationship(
        "FactLoanTransaction",
        primaryjoin="DimCustomer.customer_key == foreign(FactLoanTransaction.customer_key)",
        back_populates="customer",
    )


class DimVehicle(Base):
    __tablename__ = "dim_vehicle"

    vehicle_key  = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id   = Column(String(50), nullable=False)            # NK
    model        = Column(String(100))
    manufacturer = Column(String(100))
    vehicle_type = Column(String(50))

    loans = relationship(
        "FactLoanTransaction",
        primaryjoin="DimVehicle.vehicle_key == foreign(FactLoanTransaction.vehicle_key)",
        back_populates="vehicle",
    )


class DimDate(Base):
    __tablename__ = "dim_date"

    date_key     = Column(Integer, primary_key=True)   # YYYYMMDD
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
    branch_id   = Column(String(50), nullable=False)             # NK
    branch_name = Column(String(100))
    city        = Column(String(50))
    state       = Column(String(50))

    loans = relationship(
        "FactLoanTransaction",
        primaryjoin="DimBranch.branch_key == foreign(FactLoanTransaction.branch_key)",
        back_populates="branch",
    )


# ── Fact Table ────────────────────────────────────────────────────────────────

class FactLoanTransaction(Base):
    __tablename__ = "fact_loan_transaction"

    loan_fact_key      = Column(Integer, primary_key=True, autoincrement=True)

    # Keys (no FK constraints - Synapse Dedicated Pool limitation)
    customer_key       = Column(Integer)
    vehicle_key        = Column(Integer)
    loan_date_key      = Column(Integer)
    return_date_key    = Column(Integer)
    branch_key         = Column(Integer)

    # Degenerate dimension
    source_loan_id     = Column(String(50))

    # Measures
    loan_fee           = Column(Numeric(10, 2))
    loan_duration_days = Column(Integer)
    distance_driven    = Column(Integer)
    starting_mileage   = Column(Integer)
    ending_mileage     = Column(Integer)

    # Relationships with explicit join conditions
    customer = relationship(
        "DimCustomer",
        primaryjoin="foreign(FactLoanTransaction.customer_key) == DimCustomer.customer_key",
        back_populates="loans",
    )
    vehicle = relationship(
        "DimVehicle",
        primaryjoin="foreign(FactLoanTransaction.vehicle_key) == DimVehicle.vehicle_key",
        back_populates="loans",
    )
    branch = relationship(
        "DimBranch",
        primaryjoin="foreign(FactLoanTransaction.branch_key) == DimBranch.branch_key",
        back_populates="loans",
    )
    loan_date = relationship(
        "DimDate",
        primaryjoin="foreign(FactLoanTransaction.loan_date_key) == DimDate.date_key",
    )
    ret_date = relationship(
        "DimDate",
        primaryjoin="foreign(FactLoanTransaction.return_date_key) == DimDate.date_key",
    )