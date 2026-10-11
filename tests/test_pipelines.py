import sqlite3
from pathlib import Path

import pytest

from etl_mis import run_mis_pipeline
from elt_propensity import run_elt_load, derive_features_v1, derive_features_v2
from cli import attempt_v2_from_mis_table

DATA = Path(__file__).parent.parent / "data" / "raw_events.csv"


@pytest.fixture
def warehouse():
    conn = sqlite3.connect(":memory:")
    run_elt_load(DATA, conn)
    return conn


def test_mis_excludes_cancelled_and_rejected():
    mis_table = run_mis_pipeline(DATA)
    total = sum(r["disbursal_count"] for r in mis_table)
    # 28 raw events include 1 cancelled + 2 rejected; neither should ever
    # surface in the MIS rollup.
    assert total == 6  # only the 6 actually-disbursed LANs


def test_mis_counts_one_disbursal_per_lan_not_per_customer():
    mis_table = run_mis_pipeline(DATA)
    # C101 and C106 each converted twice (two distinct LANs) -- the rollup
    # must count both, not collapse them to one row per customer.
    mumbai_south = [r for r in mis_table if r["market"] == "South Mumbai"]
    assert sum(r["disbursal_count"] for r in mumbai_south) == 2


def test_mis_table_has_no_customer_or_offer_level_detail():
    mis_table = run_mis_pipeline(DATA)
    columns = set(mis_table[0].keys())
    assert "customer_id" not in columns
    assert "rate" not in columns


def test_elt_load_retains_every_event_type_untouched(warehouse):
    counts = dict(warehouse.execute(
        "SELECT event_type, COUNT(*) FROM raw_events GROUP BY event_type"
    ).fetchall())
    assert counts["cancelled"] == 1
    assert counts["rejected"] == 2
    assert counts["offer_shown"] == 12
    assert counts["disbursed"] == 6


def test_elt_v1_features_cover_only_converted_customers(warehouse):
    v1 = derive_features_v1(warehouse)
    assert len(v1) == 6
    assert all(row["converted"] for row in v1)


def test_elt_v2_feature_derivable_from_raw_store_with_no_new_data(warehouse):
    v2 = derive_features_v2(warehouse)
    by_customer = {row["customer_id"]: row for row in v2}
    # C100 saw 3 offer_shown events (2 before LAN-5001's, 1 for it) before
    # converting, and one of them had the EMI calculator touched.
    assert by_customer["C100"]["offers_seen_before_converting"] == 3
    assert by_customer["C100"]["touched_emi_calculator"] is True
    # C101 converted twice with no calculator touches logged.
    assert by_customer["C101"]["touched_emi_calculator"] is False


def test_v2_feature_is_unrecoverable_from_the_etl_cleaned_mis_table():
    mis_table = run_mis_pipeline(DATA)
    with pytest.raises(ValueError, match="already discarded at load time"):
        attempt_v2_from_mis_table(mis_table)
