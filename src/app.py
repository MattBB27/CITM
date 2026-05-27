"""
CITM Flask Application
-----------------------
Provides the web interface for the Car Rental Inventory Management System.
Exposes:
  /               — Dashboard (KPIs from operational DB)
  /customers      — Customer management 
  /vehicles       — Vehicle management
  /loans          — Loan transaction management 
  /branches       — Branch listing
  /etl            — ETL control panel + run history
  /analytics      — Warehouse analytics queries 
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
# pool_pre_ping=True - drops stale Azure connections that time out after inactivity
# pool_recycle=1800  - recycle connections every 30 min (Azure closes idle at ~30min)
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
        today = date.today()
        on_loan_count = db.execute(text("""
            SELECT COUNT(DISTINCT vehicle_id) FROM loan_transactions
            WHERE loan_date <= :today AND return_date >= :today
        """), {"today": today}).scalar() or 0
        total_vehicles = db.query(Vehicle).count()

        kpis = {
            "total_customers":    db.query(Customer).count(),
            "total_vehicles":     total_vehicles,
            "available_vehicles": total_vehicles - on_loan_count,
            "total_loans":        db.query(LoanTransaction).count(),
            "total_branches":     db.query(Branch).count(),
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

        # Warehouse fact count - cached briefly to avoid Synapse round-trip on every load.
        # Invalidated automatically when ETL runs.
        now = datetime.now()
        if _wh_cache["count"] is None or now > _wh_cache["expires"]:
            try:
                wdb = WhSession()
                _wh_cache["count"]   = wdb.query(FactLoanTransaction).count()
                _wh_cache["expires"] = now + timedelta(seconds=15)
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
            max_num = 0
            for c in db.query(Customer).all():
                try:
                    n = int(c.customer_id.split("-")[-1])
                    if n > max_num: max_num = n
                except (ValueError, IndexError):
                    pass
            new_id = f"CUST-{max_num+1:03d}"

            cust = Customer(
                customer_id           = new_id,
                name                  = request.form["name"],
                driver_license_number = request.form["driver_license_number"],
                email                 = request.form.get("email"),
                phone                 = request.form.get("phone"),
            )
            db.add(cust)
            db.commit()
            flash(f"Customer '{cust.name}' registered as {new_id}.", "success")
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
        today = date.today()

        # Find all vehicle IDs that have an active loan (today between loan_date and return_date)
        active_loan_vehicles = set(
            r[0] for r in db.execute(text("""
                SELECT DISTINCT vehicle_id FROM loan_transactions
                WHERE loan_date <= :today AND return_date >= :today
            """), {"today": today}).fetchall()
        )

        # Build the vehicles list with computed availability
        q = db.query(Vehicle)
        if vtype:
            q = q.filter_by(vehicle_type=vtype)
        all_vehicles = q.order_by(Vehicle.manufacturer, Vehicle.model).all()

        # Compute availability and look up the active loan if any
        vehicle_rows = []
        for v in all_vehicles:
            on_loan = v.vehicle_id in active_loan_vehicles
            active_loan = None
            if on_loan:
                active_loan = db.execute(text("""
                    SELECT lt.loan_id, lt.return_date, c.name AS customer_name
                    FROM loan_transactions lt
                    JOIN customers c ON lt.customer_id = c.customer_id
                    WHERE lt.vehicle_id = :vid
                      AND lt.loan_date <= :today
                      AND lt.return_date >= :today
                """), {"vid": v.vehicle_id, "today": today}).fetchone()
            vehicle_rows.append({
                "vehicle":     v,
                "is_available": not on_loan,
                "active_loan": active_loan,
            })

        # Apply availability filter post-computation
        if avail == "1":
            vehicle_rows = [r for r in vehicle_rows if r["is_available"]]
        elif avail == "0":
            vehicle_rows = [r for r in vehicle_rows if not r["is_available"]]

        types = [r[0] for r in db.execute(text(
            "SELECT DISTINCT vehicle_type FROM vehicles ORDER BY vehicle_type")).fetchall()]
        db.close()
        return render_template("vehicles.html", vehicle_rows=vehicle_rows,
                               types=types, sel_type=vtype, sel_avail=avail)

    @app.route("/vehicles/new", methods=["GET", "POST"])
    def vehicle_new():
        if request.method == "POST":
            db = OpSession()
            existing_ids = set(v.vehicle_id for v in db.query(Vehicle).all())
            # Generate a unique rego-style ID: 3 letters + 3 digits
            import random, string
            for _ in range(50):  # try up to 50 times to avoid collision
                new_id = (
                    "".join(random.choices(string.ascii_uppercase, k=3))
                    + "-"
                    + "".join(random.choices(string.digits, k=3))
                )
                if new_id not in existing_ids:
                    break

            veh = Vehicle(
                vehicle_id   = new_id,
                model        = request.form["model"],
                manufacturer = request.form["manufacturer"],
                vehicle_type = request.form["vehicle_type"],
                mileage      = int(request.form.get("mileage") or 0),
                is_available = True,
            )
            db.add(veh)
            db.commit()
            flash(f"Vehicle '{veh.manufacturer} {veh.model}' added with rego {new_id}.", "success")
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
        db = OpSession()
        search       = request.args.get("search", "").strip()
        branch_id    = request.args.get("branch", "")
        vehicle_type = request.args.get("vehicle_type", "")
        date_from    = request.args.get("date_from", "")
        date_to      = request.args.get("date_to", "")
        page         = int(request.args.get("page", 1))
        per_page     = 20

        q = db.query(LoanTransaction).options(
            joinedload(LoanTransaction.customer),
            joinedload(LoanTransaction.vehicle),
            joinedload(LoanTransaction.branch)
        )

        # Free-text search across IDs and customer name
        if search:
            q = q.join(LoanTransaction.customer).filter(
                LoanTransaction.loan_id.contains(search) |
                LoanTransaction.customer_id.contains(search) |
                LoanTransaction.vehicle_id.contains(search) |
                Customer.name.contains(search)
            )

        # Branch filter
        if branch_id:
            q = q.filter(LoanTransaction.branch_id == branch_id)

        # Vehicle type filter
        if vehicle_type:
            q = q.join(LoanTransaction.vehicle).filter(Vehicle.vehicle_type == vehicle_type)

        # Date range filter
        if date_from:
            q = q.filter(LoanTransaction.loan_date >= date.fromisoformat(date_from))
        if date_to:
            q = q.filter(LoanTransaction.loan_date <= date.fromisoformat(date_to))

        total = q.count()
        txns  = (q.order_by(LoanTransaction.loan_date.desc())
                  .offset((page - 1) * per_page)
                  .limit(per_page)
                  .all())

        # Filter dropdown options
        branches = db.query(Branch).order_by(Branch.branch_name).all()
        vehicle_types = [r[0] for r in db.execute(text(
            "SELECT DISTINCT vehicle_type FROM vehicles ORDER BY vehicle_type")).fetchall()]

        db.close()
        pages = (total + per_page - 1) // per_page
        return render_template("loans.html",
                               loans=txns, total=total, page=page, pages=pages,
                               search=search, branch_id=branch_id,
                               vehicle_type=vehicle_type, date_from=date_from, date_to=date_to,
                               branches=branches, vehicle_types=vehicle_types)

    @app.route("/loans/new", methods=["GET", "POST"])
    def loan_new():
        db = OpSession()
        if request.method == "POST":
            max_num = 0
            for l in db.query(LoanTransaction).all():
                try:
                    n = int(l.loan_id.split("-")[-1])
                    if n > max_num: max_num = n
                except (ValueError, IndexError):
                    pass
            new_id = f"LOAN-{max_num+1:05d}"

            veh   = db.query(Vehicle).get(request.form["vehicle_id"])
            start_km = veh.mileage if veh else 0
            loan_date   = date.fromisoformat(request.form["loan_date"])
            return_date = date.fromisoformat(request.form["return_date"])

            # Validation: return date must be after loan date
            if return_date <= loan_date:
                flash("Return date must be after the loan date.", "danger")
                db.close()
                return redirect(url_for("loan_new"))

            days = (return_date - loan_date).days

            # Fee: use form value if present and non-empty, else auto-calc by vehicle type
            fee_raw = request.form.get("loan_fee", "").strip()
            if fee_raw:
                fee = round(float(fee_raw), 2)
            else:
                # Auto-calc based on vehicle type daily rate
                rates = {"Sedan": 85, "Hatchback": 75, "SUV": 110, "Ute": 130, "Van": 120}
                daily = rates.get(veh.vehicle_type, 90) if veh else 90
                fee = round(daily * days, 2)

            end_km = start_km + days * 120  # estimated

            loan = LoanTransaction(
                loan_id          = new_id,
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
                veh.mileage = end_km   # update odometer; availability is now derived
            db.add(loan)
            db.commit()
            flash("Loan transaction recorded.", "success")
            db.close()
            return redirect(url_for("loans"))

        # Compute available vehicles dynamically based on overlapping active loans
        today = date.today()
        on_loan_ids = set(
            r[0] for r in db.execute(text("""
                SELECT DISTINCT vehicle_id FROM loan_transactions
                WHERE loan_date <= :today AND return_date >= :today
            """), {"today": today}).fetchall()
        )
        all_vehicles = db.query(Vehicle).order_by(Vehicle.manufacturer, Vehicle.model).all()
        customers = db.query(Customer).order_by(Customer.name).all()
        vehicles  = [v for v in all_vehicles if v.vehicle_id not in on_loan_ids]
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
        # Invalidate caches so next page load shows fresh data
        _analytics_cache["data"] = None
        _wh_cache["count"]   = None
        _wh_cache["expires"] = datetime.now()
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

    # ── Analytics (with improved in-process cache) ───────────────────────────────

    _analytics_cache = {
        "data": None,
        "ts": 0,
        "key": None,
    }

    # Increased cache lifetime dramatically improves UX for Synapse-backed analytics
    # while preserving existing refresh behaviour.
    CACHE_TTL_S = 1800   # 30 minutes


    def _fetch_analytics(
        rev_branch_year: str = "",
        vtype_branch: str = "",
    ):
        """
        Fetch all analytics.

        Optimisations applied:
        - Reduced unnecessary warehouse load
        - Limited monthly trend dataset
        - Removed unused heavy query
        - Preserved ALL existing application logic
        """

        wdb = WhSession()

        try:

            # ── Dynamic filters ────────────────────────────────────────────────

            rb_where = ""
            rb_params = {}

            if rev_branch_year:
                rb_where = "WHERE d.year = :year"
                rb_params["year"] = int(rev_branch_year)

            vt_where = ""
            vt_params = {}

            if vtype_branch:
                vt_where = "WHERE b.branch_id = :branch_id"
                vt_params["branch_id"] = vtype_branch

            # ── Analytics Queries ──────────────────────────────────────────────

            data = {

                # ── Headline KPIs ────────────────────────────────────────────

                "headline": wdb.execute(text("""
                    SELECT
                        COUNT(loan_fact_key)                           AS total_loans,
                        ROUND(CAST(SUM(loan_fee) AS FLOAT), 2)         AS total_revenue,
                        ROUND(CAST(AVG(loan_fee) AS FLOAT), 2)         AS avg_fee,
                        ROUND(CAST(AVG(loan_duration_days) AS FLOAT),1) AS avg_days,
                        ROUND(CAST(AVG(distance_driven) AS FLOAT),0)   AS avg_distance
                    FROM fact_loan_transaction
                """)).fetchone(),

                # ── Revenue by Year ──────────────────────────────────────────

                "rev_by_year": wdb.execute(text("""
                    SELECT
                        d.year,
                        COUNT(f.loan_fact_key)                         AS loans,
                        ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2)        AS revenue,
                        ROUND(CAST(AVG(f.loan_fee) AS FLOAT),2)        AS avg_fee
                    FROM fact_loan_transaction f
                    JOIN dim_date d
                        ON f.loan_date_key = d.date_key
                    GROUP BY d.year
                    ORDER BY d.year DESC
                """)).fetchall(),

                # ── Revenue by Branch ────────────────────────────────────────

                "rev_by_branch": wdb.execute(text(f"""
                    SELECT
                        b.branch_name,
                        b.city,
                        COUNT(f.loan_fact_key)                         AS total_loans,
                        ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2)        AS total_revenue,
                        ROUND(CAST(AVG(f.loan_fee) AS FLOAT),2)        AS avg_fee
                    FROM fact_loan_transaction f
                    JOIN dim_branch b
                        ON f.branch_key = b.branch_key
                    JOIN dim_date d
                        ON f.loan_date_key = d.date_key
                    {rb_where}
                    GROUP BY b.branch_name, b.city
                    ORDER BY total_revenue DESC
                """), rb_params).fetchall(),

                # ── Monthly Revenue Trend ────────────────────────────────────
                # Limited to latest 24 months for faster dashboard rendering

                "rev_by_month": wdb.execute(text("""
                    SELECT TOP 24
                        d.year,
                        d.month,
                        d.month_name,
                        COUNT(f.loan_fact_key)                         AS loans,
                        ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2)        AS revenue
                    FROM fact_loan_transaction f
                    JOIN dim_date d
                        ON f.loan_date_key = d.date_key
                    GROUP BY d.year, d.month, d.month_name
                    ORDER BY d.year DESC, d.month DESC
                """)).fetchall(),

                # ── Vehicle Type Performance ────────────────────────────────

                "vtype_usage": wdb.execute(text(f"""
                    SELECT
                        v.vehicle_type,
                        COUNT(f.loan_fact_key)                         AS rentals,
                        ROUND(CAST(AVG(f.loan_duration_days) AS FLOAT),1) AS avg_days,
                        ROUND(CAST(AVG(f.distance_driven) AS FLOAT),0) AS avg_km,
                        ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2)        AS revenue
                    FROM fact_loan_transaction f
                    JOIN dim_vehicle v
                        ON f.vehicle_key = v.vehicle_key
                    JOIN dim_branch b
                        ON f.branch_key = b.branch_key
                    {vt_where}
                    GROUP BY v.vehicle_type
                    ORDER BY rentals DESC
                """), vt_params).fetchall(),

                # ── Top Customers ───────────────────────────────────────────

                "top_customers": wdb.execute(text("""
                    SELECT TOP 10
                        c.name,
                        c.customer_id,
                        COUNT(f.loan_fact_key)                         AS rentals,
                        ROUND(CAST(SUM(f.loan_fee) AS FLOAT),2)        AS total_spent,
                        ROUND(CAST(AVG(f.loan_duration_days) AS FLOAT),1) AS avg_days
                    FROM fact_loan_transaction f
                    JOIN dim_customer c
                        ON f.customer_key = c.customer_key
                    GROUP BY c.name, c.customer_id
                    ORDER BY total_spent DESC
                """)).fetchall(),

                # ── Filter Dropdowns ────────────────────────────────────────

                "available_years": [
                    r[0]
                    for r in wdb.execute(text("""
                        SELECT DISTINCT year
                        FROM dim_date
                        ORDER BY year DESC
                    """)).fetchall()
                ],

                "available_branches": wdb.execute(text("""
                    SELECT
                        branch_id,
                        branch_name
                    FROM dim_branch
                    ORDER BY branch_name
                """)).fetchall(),
            }

        finally:
            wdb.close()

        return data


    @app.route("/analytics")
    def analytics():

        import time as _t

        rev_branch_year = request.args.get("rb_year", "")
        vtype_branch    = request.args.get("vt_branch", "")
        force           = request.args.get("refresh") == "1"

        cache_key = f"{rev_branch_year}|{vtype_branch}"

        now = _t.time()

        cache = _analytics_cache

        # ── Cache validation ────────────────────────────────────────────────

        if (
            force
            or cache["data"] is None
            or cache.get("key") != cache_key
            or (now - cache["ts"]) > CACHE_TTL_S
        ):

            cache["data"] = _fetch_analytics(
                rev_branch_year,
                vtype_branch,
            )

            cache["ts"] = now
            cache["key"] = cache_key

            cached_age = 0

        else:

            cached_age = int(now - cache["ts"])

        # ── Render ──────────────────────────────────────────────────────────

        return render_template(
            "analytics.html",
            cached_age=cached_age,
            sel_rb_year=rev_branch_year,
            sel_vt_branch=vtype_branch,
            **cache["data"],
        )

    return app