"""
Operational (Transactional) Database Models
--------------------------------------------
Represents the source OLTP system for CITM.
In production this would be an Azure SQL Database.
"""
from sqlalchemy import (Column, Integer, String, Date,
                        Numeric, ForeignKey, Boolean, DateTime)
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers"

    customer_id          = Column(String(50), primary_key=True)
    name                 = Column(String(100), nullable=False)
    driver_license_number = Column(String(50), nullable=False, unique=True)
    email                = Column(String(100))
    phone                = Column(String(20))
    created_at           = Column(DateTime, default=datetime.utcnow)

    loans = relationship("LoanTransaction", back_populates="customer")

    def to_dict(self):
        return {
            "customer_id": self.customer_id,
            "name": self.name,
            "driver_license_number": self.driver_license_number,
            "email": self.email,
            "phone": self.phone,
        }


class Vehicle(Base):
    __tablename__ = "vehicles"

    vehicle_id   = Column(String(50), primary_key=True)
    model        = Column(String(100), nullable=False)
    manufacturer = Column(String(100), nullable=False)
    vehicle_type = Column(String(50), nullable=False)
    mileage      = Column(Integer, default=0)
    is_available = Column(Boolean, default=True)

    loans = relationship("LoanTransaction", back_populates="vehicle")

    def to_dict(self):
        return {
            "vehicle_id":   self.vehicle_id,
            "model":        self.model,
            "manufacturer": self.manufacturer,
            "vehicle_type": self.vehicle_type,
            "mileage":      self.mileage,
            "is_available": self.is_available,
        }


class Branch(Base):
    __tablename__ = "branches"

    branch_id   = Column(String(50), primary_key=True)
    branch_name = Column(String(100), nullable=False)
    city        = Column(String(50), nullable=False)
    state       = Column(String(50), nullable=False)

    loans = relationship("LoanTransaction", back_populates="branch")

    def to_dict(self):
        return {
            "branch_id":   self.branch_id,
            "branch_name": self.branch_name,
            "city":        self.city,
            "state":       self.state,
        }


class LoanTransaction(Base):
    __tablename__ = "loan_transactions"

    loan_id          = Column(String(50), primary_key=True)
    customer_id      = Column(String(50), ForeignKey("customers.customer_id"), nullable=False)
    vehicle_id       = Column(String(50), ForeignKey("vehicles.vehicle_id"),   nullable=False)
    branch_id        = Column(String(50), ForeignKey("branches.branch_id"),    nullable=False)
    loan_date        = Column(Date, nullable=False)
    return_date      = Column(Date, nullable=False)
    loan_fee         = Column(Numeric(10, 2), nullable=False)
    starting_mileage = Column(Integer, nullable=False)
    ending_mileage   = Column(Integer, nullable=False)
    created_at       = Column(DateTime, default=datetime.utcnow)

    customer = relationship("Customer", back_populates="loans")
    vehicle  = relationship("Vehicle",  back_populates="loans")
    branch   = relationship("Branch",   back_populates="loans")

    def to_dict(self):
        return {
            "loan_id":          self.loan_id,
            "customer_id":      self.customer_id,
            "customer_name":    self.customer.name if self.customer else "",
            "vehicle_id":       self.vehicle_id,
            "vehicle_label":    f"{self.vehicle.manufacturer} {self.vehicle.model}" if self.vehicle else "",
            "branch_id":        self.branch_id,
            "branch_name":      self.branch.branch_name if self.branch else "",
            "loan_date":        str(self.loan_date),
            "return_date":      str(self.return_date),
            "loan_fee":         float(self.loan_fee),
            "starting_mileage": self.starting_mileage,
            "ending_mileage":   self.ending_mileage,
        }
