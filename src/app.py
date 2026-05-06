"""
CITM Flask Application
-----------------------
Provides the web interface for the Car Rental Inventory Management System.
Exposes:
  /               — Dashboard (KPIs from operational DB)
  /customers      — Customer management (FR 1.1, FR 1.2)
  /vehicles       — Vehicle management (FR 2.1, FR 2.2)
  /loans          — Loan transaction management (FR 3.1 – FR 3.4)
  /branches       — Branch listing
  /etl            — ETL control panel + run history
  /analytics      — Warehouse analytics queries (FR 5.2)
"""
import json
from datetime import date, datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, joinedload

import config
from src.models.operational import (
    Base as OpBase, Customer, Vehicle, Branch, LoanTransaction
)
from src.models.warehouse import (
    Base as WhBase, DimCustomer, DimVehicle, DimBranch, DimDate, FactLoanTransaction
)
from src.etl.pipeline import run_etl

# ── Engine / Session setup ────────────────────────────────────────────────────
# ── Engine / Session setup ────────────────────────────────────────────────────
# pool_pre_ping=True — drops stale Azure connections that time out after inactivity
# pool_recycle=1800  — recycle connections every 30 min (Azure closes idle at ~30min)
op_engine = create_engine(
    config.OPERATIONAL_DB_URL,
    connect_args={"check_same_thread": False} if "sqlite" in config.OPERATIONAL_DB_URL else {},
    pool_pre_ping=True,
    pool_recycle=1800,
)
wh_engine = create_engine(
    config.WAREHOUSE_DB_URL,
    connect_args={"check_same_thread": False} if "sqlite" in config.WAREHOUSE_DB_URL else {},
    pool_pre_ping=True,
    pool_recycle=1800,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)

OpSession = sessionmaker(bind=op_engine)
WhSession = sessionmaker(bind=wh_engine)

# Simple in-process cache for the warehouse fact count (avoids hitting Synapse on every page load)
_wh_cache = {"count": None, "expires": datetime.min}

# ── App factory ───────────────────────────────────────────────────────────────
def create_app() -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = config.SECRET_KEY

    # ── Dashboard ─────────────────────────────────────────────────────────────
    @app.route("/")
    def dashboard():
        db = OpSession()
        kpis = {
            "total_customers": db.query(Customer).count(),
            "total_vehicles":  db.query(Vehicle).count(),
            "available_vehicles": db.query(Vehicle).filter_by(is_available=True).count(),
            "total_loans":     db.query(LoanTransaction).count(),
            "total_branches":  db.query(Branch).count(),
        }

        # Revenue summary
        rev = db.execute(
            text("SELECT ROUND(SUM(loan_fee),2) FROM loan_transactions")
        ).scalar() or 0
        kpis["total_revenue"] = float(rev)

        # Recent loans
        recent = (
            db.query(LoanTransaction)
            .options(joinedload(LoanTransaction.customer),
                     joinedload(LoanTransaction.vehicle),
                     joinedload(LoanTransaction.branch))
            .order_by(LoanTransaction.loan_date.desc())
            .limit(8)
            .all()
        )
        db.close()

        # Warehouse fact count — cached for 60s to avoid slow Synapse round-trip on every load
        now = datetime.now()
        if _wh_cache["count"] is None or now > _wh_cache["expires"]:
            try:
                wdb = WhSession()
                _wh_cache["count"]   = wdb.query(FactLoanTransaction).count()
                _wh_cache["expires"] = now + timedelta(seconds=60)
                wdb.close()
            except Exception:
                _wh_cache["count"] = 0
        kpis["wh_fact_rows"] = _wh_cache["count"]

        return render_template("dashboard.html", kpis=kpis, recent=recent)

    # ── Customers ─────────────────────────────────────────────────────────────
    @app.route("/customers")
    def customers():
        db = OpSession()
        customers = db.query(Customer).order_by(Customer.name).all()
        db.close()
        return render_template("customers.html", customers=customers)

    @app.route("/customers/new", methods=["GET", "POST"])
    def customer_new():
        if request.method == "POST":
            db = OpSession()
            count = db.query(Customer).count()
            cust = Customer(
                customer_id           = f"CUST-{count+1:03d}",
                name                  = request.form["name"],
                driver_license_number = request.form["driver_license_number"],
                email                 = request.form.get("email"),
                phone                 = request.form.get("phone"),
            )
            db.add(cust)
            db.commit()
            flash(f"Customer '{cust.name}' registered successfully.", "success")
            db.close()
            return redirect(url_for("customers"))
        return render_template("customer_form.html", action="Register", customer=None)

    @app.route("/customers/<cid>/edit", methods=["GET", "POST"])
    def customer_edit(cid):
        db = OpSession()
        cust = db.query(Customer).get(cid)
        if request.method == "POST":
            cust.name                  = request.form["name"]
            cust.driver_license_number = request.form["driver_license_number"]
            cust.email                 = request.form.get("email")
            cust.phone                 = request.form.get("phone")
            db.commit()
            flash("Customer updated.", "success")
            db.close()
            return redirect(url_for("customers"))
        db.close()
        return render_template("customer_form.html", action="Edit", customer=cust)

    # ── Vehicles ──────────────────────────────────────────────────────────────
    @app.route("/vehicles")
    def vehicles():
        db = OpSession()
        vtype = request.args.get("type", "")
        avail = request.args.get("available", "")
        q = db.query(Vehicle)
        if vtype:
            q = q.filter_by(vehicle_type=vtype)
        if avail == "1":
            q = q.filter_by(is_available=True)
        elif avail == "0":
            q = q.filter_by(is_available=False)
        vehicles = q.order_by(Vehicle.manufacturer, Vehicle.model).all()
        types    = [r[0] for r in db.execute(text(
            "SELECT DISTINCT vehicle_type FROM vehicles ORDER BY vehicle_type")).fetchall()]
        db.close()
        return render_template("vehicles.html", vehicles=vehicles,
                               types=types, sel_type=vtype, sel_avail=avail)

    @app.route("/vehicles/<vid>/toggle", methods=["POST"])
    def vehicle_toggle(vid):
        db = OpSession()
        v = db.query(Vehicle).get(vid)
        v.is_available = not v.is_available
        db.commit()
        db.close()
        flash(f"{v.manufacturer} {v.model} availability updated.", "info")
        return redirect(url_for("vehicles"))

    @app.route("/vehicles/new", methods=["GET", "POST"])
    def vehicle_new():
        if request.method == "POST":
            db = OpSession()
            count = db.query(Vehicle).count()
            veh = Vehicle(
                vehicle_id   = f"VEH-{count+1:03d}",
                model        = request.form["model"],
                manufacturer = request.form["manufacturer"],
                vehicle_type = request.form["vehicle_type"],
                mileage      = int(request.form.get("mileage", 0)),
                is_available = True,
            )
            db.add(veh)
            db.commit()
            flash(f"Vehicle '{veh.manufacturer} {veh.model}' added.", "success")
            db.close()
            return redirect(url_for("vehicles"))
        return render_template("vehicle_form.html", vehicle=None)

    # ── Branches ──────────────────────────────────────────────────────────────
    @app.route("/branches")
    def branches():
        db = OpSession()
        branches = db.query(Branch).order_by(Branch.branch_name).all()
        db.close()
        return render_template("branches.html", branches=branches)

    # ── Loans ─────────────────────────────────────────────────────────────────
    @app.route("/loans")
    def loans():
        db  = OpSession()
        search  = request.args.get("search", "").strip()
        page    = int(request.args.get("page", 1))
        per_page = 20

        q = db.query(LoanTransaction).options(
            joinedload(LoanTransaction.customer),
            joinedload(LoanTransaction.vehicle),
            joinedload(LoanTransaction.branch)
        )
        if search:
            q = q.filter(LoanTransaction.loan_id.contains(search) |
                         LoanTransaction.customer_id.contains(search) |
                         LoanTransaction.vehicle_id.contains(search))

        total = q.count()
        txns  = (q.order_by(LoanTransaction.loan_date.desc())
                  .offset((page - 1) * per_page)
                  .limit(per_page)
                  .all())
        db.close()
        pages = (total + per_page - 1) // per_page
        return render_template("loans.html", loans=txns, total=total,
                               page=page, pages=pages, search=search)

    @app.route("/loans/new", methods=["GET", "POST"])
    def loan_new():
        db = OpSession()
        if request.method == "POST":
            count = db.query(LoanTransaction).count()
            veh   = db.query(Vehicle).get(request.form["vehicle_id"])
            start_km = veh.mileage if veh else 0
            loan_date   = date.fromisoformat(request.form["loan_date"])
            return_date = date.fromisoformat(request.form["return_date"])
            days        = (return_date - loan_date).days or 1
            fee         = round(float(request.form.get("loan_fee", days * 90)), 2)
            end_km      = start_km + days * 120  # estimated

            loan = LoanTransaction(
                loan_id          = f"LOAN-{count+1:05d}",
                customer_id      = request.form["customer_id"],
                vehicle_id       = request.form["vehicle_id"],
                branch_id        = request.form["branch_id"],
                loan_date        = loan_date,
                return_date      = return_date,
                loan_fee         = fee,
                starting_mileage = start_km,
                ending_mileage   = end_km,
            )
            if veh:
                veh.is_available = False
                veh.mileage      = end_km
            db.add(loan)
            db.commit()
            flash("Loan transaction recorded.", "success")
            db.close()
            return redirect(url_for("loans"))

        customers = db.query(Customer).order_by(Customer.name).all()
        vehicles  = db.query(Vehicle).filter_by(is_available=True).order_by(Vehicle.model).all()
        brs       = db.query(Branch).order_by(Branch.branch_name).all()
        db.close()
        return render_template("loan_form.html",
                               customers=customers, vehicles=vehicles, branches=brs)

    # ── ETL Control Panel ─────────────────────────────────────────────────────
    _etl_history: list[dict] = []

    @app.route("/etl")
    def etl():
        wdb = WhSession()
        wh_stats = {
            "dim_customer":  wdb.query(DimCustomer).count(),
            "dim_vehicle":   wdb.query(DimVehicle).count(),
            "dim_branch":    wdb.query(DimBranch).count(),
            "dim_date":      wdb.query(DimDate).count(),
            "fact_rows":     wdb.query(FactLoanTransaction).count(),
        }
        wdb.close()
        return render_template("etl.html", history=_etl_history[-10:], wh_stats=wh_stats)

    @app.route("/etl/run", methods=["POST"])
    def etl_run():
        result = run_etl()
        _etl_history.append(result)
        status  = "success" if result["status"] == "success" else "danger"
        message = (f"ETL completed in {result['total_duration_s']}s — "
                   f"{result.get('steps', {}).get('load_facts', {}).get('records_inserted', 0)}"
                   " new fact rows loaded.")
        flash(message, status)
        return redirect(url_for("etl"))

    @app.route("/etl/status")
    def etl_status():
        """JSON endpoint for ETL history (last run)."""
        return jsonify(_etl_history[-1] if _etl_history else {})

    # ── Analytics ─────────────────────────────────────────────────────────────
    @app.route("/analytics")
    def analytics():
        wdb = WhSession()

        # Revenue by branch — full GROUP BY required for Synapse
        rev_by_branch = wdb.execute(text("""
            SELECT b.branch_name, b.city,
                   COUNT(f.loan_fact_key) AS total_loans,
                   ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2) AS total_revenue,
                   ROUND(CAST(AVG(f.loan_fee) AS FLOAT),2) AS avg_fee
            FROM fact_loan_transaction f
            JOIN dim_branch b ON f.branch_key = b.branch_key
            GROUP BY b.branch_name, b.city
            ORDER BY total_revenue DESC
        """)).fetchall()

        # Revenue by month
        rev_by_month = wdb.execute(text("""
            SELECT d.year, d.month, d.month_name,
                   COUNT(f.loan_fact_key) AS loans,
                   ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2) AS revenue
            FROM fact_loan_transaction f
            JOIN dim_date d ON f.loan_date_key = d.date_key
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """)).fetchall()

        # Top vehicle types
        vtype_usage = wdb.execute(text("""
            SELECT v.vehicle_type,
                   COUNT(f.loan_fact_key) AS rentals,
                   ROUND(CAST(AVG(f.loan_duration_days) AS FLOAT),1) AS avg_days,
                   ROUND(CAST(AVG(f.distance_driven) AS FLOAT),0) AS avg_km,
                   ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2) AS revenue
            FROM fact_loan_transaction f
            JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
            GROUP BY v.vehicle_type
            ORDER BY rentals DESC
        """)).fetchall()

        # Top customers — TOP instead of LIMIT for T-SQL
        top_customers = wdb.execute(text("""
            SELECT TOP 10 c.name, c.customer_id,
                   COUNT(f.loan_fact_key) AS rentals,
                   ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2) AS total_spent,
                   ROUND(CAST(AVG(f.loan_duration_days) AS FLOAT),1) AS avg_days
            FROM fact_loan_transaction f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.name, c.customer_id
            ORDER BY total_spent DESC
        """)).fetchall()

        # Duration by vehicle type
        duration_by_type = wdb.execute(text("""
            SELECT v.vehicle_type,
                   ROUND(CAST(AVG(f.loan_duration_days) AS FLOAT),1) AS avg_duration,
                   ROUND(CAST(AVG(f.distance_driven) AS FLOAT),0) AS avg_distance
            FROM fact_loan_transaction f
            JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
            GROUP BY v.vehicle_type
            ORDER BY avg_duration DESC
        """)).fetchall()

        wdb.close()

        return render_template("analytics.html",
                               rev_by_branch=rev_by_branch,
                               rev_by_month=rev_by_month,
                               vtype_usage=vtype_usage,
                               top_customers=top_customers,
                               duration_by_type=duration_by_type)

    return app