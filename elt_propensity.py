"""
ELT pipeline for customer propensity modeling (Workload 2 from the decision
memo).

This is genuinely ELT: load the raw, unfiltered event stream into the
warehouse UNTOUCHED, and only derive each model's specific features
afterward, as a versioned transform. Nothing is cleaned away at load time --
offer_shown/offer_accepted/disbursed/cancelled/rejected all land in the same
raw table, with every rate/fee/term/timestamp intact.

The payoff this module demonstrates: when a *future* model needs a feature
nobody anticipated when v1 was written (e.g. "did the customer get shown a
rate concession before accepting"), that feature can be derived straight
from the untouched raw table with zero new data collection -- because
nothing useful was thrown away at load time. See `derive_features_v2` and
the v1-vs-v2-on-MIS-data comparison in cli.py.
"""
import csv
import sqlite3


RAW_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT,
    lan TEXT,
    event_type TEXT,
    city TEXT,
    market TEXT,
    asset_type TEXT,
    amount REAL,
    rate REAL,
    fee REAL,
    tenure_months INTEGER,
    modified_emi_calculator INTEGER,
    timestamp TEXT
)
"""


def init_warehouse(conn):
    conn.execute(RAW_EVENTS_SCHEMA)
    conn.commit()


def load_raw(csv_path, conn):
    """Load every row from the source, untouched -- no filtering, no
    cleaning, no aggregation. This is the 'L' happening before any 'T'."""
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        conn.execute(
            """INSERT OR REPLACE INTO raw_events
               (event_id, customer_id, lan, event_type, city, market, asset_type,
                amount, rate, fee, tenure_months, modified_emi_calculator, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row["event_id"], row["customer_id"], row["lan"] or None, row["event_type"],
                row["city"], row["market"], row["asset_type"],
                float(row["amount"]), float(row["rate"]), float(row["fee"]),
                int(row["tenure_months"]), int(row["modified_emi_calculator"]),
                row["timestamp"],
            ),
        )
    conn.commit()
    return len(rows)


def derive_features_v1(conn):
    """The features a propensity model written today actually uses:
    did the customer convert, and on what terms. Nobody anticipated needing
    anything about the OTHER offers a customer saw before converting."""
    cur = conn.execute(
        """SELECT customer_id, lan, amount, rate, fee
           FROM raw_events WHERE event_type = 'disbursed'"""
    )
    return [
        {"customer_id": r[0], "lan": r[1], "converted": True, "amount": r[2], "rate": r[3], "fee": r[4]}
        for r in cur.fetchall()
    ]


def derive_features_v2(conn):
    """A feature nobody wrote v1 to anticipate: for each customer who
    eventually converted, how many *other* offers were they shown first,
    and did they ever touch the EMI calculator before converting -- a signal
    for price sensitivity / comparison-shopping behavior.

    This is derivable with ZERO new data collection, purely from the raw
    table already sitting in the warehouse -- because the ELT load never
    threw the other offers away."""
    customers = [r[0] for r in conn.execute(
        "SELECT DISTINCT customer_id FROM raw_events WHERE event_type = 'disbursed'"
    ).fetchall()]

    out = []
    for cust in customers:
        shown = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(modified_emi_calculator), 0) "
            "FROM raw_events WHERE customer_id = ? AND event_type = 'offer_shown'",
            (cust,),
        ).fetchone()
        offers_seen, touched_calculator = shown
        out.append({
            "customer_id": cust,
            "offers_seen_before_converting": offers_seen,
            "touched_emi_calculator": bool(touched_calculator),
        })
    return out


def run_elt_load(csv_path, conn):
    init_warehouse(conn)
    return load_raw(csv_path, conn)
