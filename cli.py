"""
SignalVault demo: runs the same raw event stream through both pipelines
from the decision memo, then proves the memo's actual claim in code --
that a feature a *future* model needs can be derived from the ELT raw
store with zero new data collection, but is already unrecoverable once
data has gone through the ETL-cleaned MIS path.
"""
import sqlite3
from pathlib import Path

from etl_mis import run_mis_pipeline
from elt_propensity import run_elt_load, derive_features_v1, derive_features_v2

DATA = Path(__file__).parent / "data" / "raw_events.csv"


def attempt_v2_from_mis_table(mis_table):
    """Try to compute the same v2 feature (offers seen before converting,
    EMI calculator touched) from the MIS rollup instead of the raw table.
    This must fail -- the MIS table was cleaned down to
    city/market/asset_type/disbursal_count and never had a customer_id or
    an offer_shown event in it to begin with."""
    available_columns = set(mis_table[0].keys()) if mis_table else set()
    required_columns = {"customer_id", "offers_seen_before_converting", "touched_emi_calculator"}
    missing = required_columns - available_columns
    if missing:
        raise ValueError(
            f"Cannot derive this feature from the MIS table -- required signal was "
            f"already discarded at load time (missing: {sorted(missing)}). "
            f"MIS table only has: {sorted(available_columns)}."
        )
    return "unreachable"


def main():
    print("=" * 70)
    print("ETL pipeline -- Workload 1: Daily MIS reporting")
    print("=" * 70)
    mis_table = run_mis_pipeline(DATA)
    for row in mis_table:
        print(f"  {row['city']:10s} {row['market']:15s} {row['asset_type']:16s} disbursals: {row['disbursal_count']}")
    total_disbursals = sum(r["disbursal_count"] for r in mis_table)
    print(f"  total disbursals (cancelled/rejected excluded, 1 per LAN): {total_disbursals}")

    print()
    print("=" * 70)
    print("ELT pipeline -- Workload 2: Customer propensity modeling")
    print("=" * 70)
    conn = sqlite3.connect(":memory:")
    n_loaded = run_elt_load(DATA, conn)
    n_raw = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    print(f"  loaded {n_loaded} raw events untouched (no filtering, no cleaning): {n_raw} rows in warehouse")
    by_type = conn.execute("SELECT event_type, COUNT(*) FROM raw_events GROUP BY event_type ORDER BY event_type").fetchall()
    for event_type, count in by_type:
        print(f"    {event_type:15s} {count}")

    print()
    print("  v1 features (what today's propensity model actually uses):")
    v1 = derive_features_v1(conn)
    for row in v1[:3]:
        print(f"    {row}")
    print(f"    ... ({len(v1)} converted customers total)")

    print()
    print("  Scenario: a future model needs a feature nobody wrote v1 for --")
    print("  'how many offers did this customer see before converting, and")
    print("  did they touch the EMI calculator' (price-sensitivity signal).")
    print()
    print("  Deriving v2 feature from the ELT raw store (zero new data collection):")
    v2 = derive_features_v2(conn)
    for row in v2:
        print(f"    {row}")

    print()
    print("  Attempting the SAME v2 feature from the ETL-cleaned MIS table instead:")
    try:
        attempt_v2_from_mis_table(mis_table)
        print("    ERROR: this should not have succeeded")
    except ValueError as e:
        print(f"    FAILED as expected: {e}")

    print()
    print("=" * 70)
    print("This demonstrates the decision memo's actual claim, not just its")
    print("prose: the ELT raw store can answer a question nobody anticipated")
    print("when it was first loaded; the ETL-cleaned MIS table cannot --")
    print("the offer-level signal it needed was already gone at load time.")
    print("=" * 70)


if __name__ == "__main__":
    main()
