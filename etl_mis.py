"""
ETL pipeline for daily MIS reporting (Workload 1 from the decision memo).

This is genuinely ETL: clean and aggregate BEFORE storage. The two business
rules are settled and unlikely to be revisited, so it's safe to throw away
everything except the shape the report needs:

  - cancelled and rejected applications are excluded from the disbursal count
  - one disbursal is counted per distinct Loan Account Number (LAN), not per
    customer

Crucially: once this pipeline runs, the per-event detail (every offer shown,
every rate/fee/term, whether the EMI calculator was touched, why something
was rejected vs cancelled) is GONE. Only the cleaned rollup survives. That's
fine for this workload -- but it's exactly the data an ELT-style workload
(Workload 2) cannot afford to lose, which is the whole point of the memo.
"""
import csv
from collections import defaultdict


def extract(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def transform_and_load(events):
    """Clean + aggregate before storage. Returns the MIS rollup only --
    no raw events survive this pipeline."""
    seen_lans = set()
    rollup = defaultdict(int)

    for row in events:
        if row["event_type"] != "disbursed":
            continue  # cancelled/rejected excluded -- settled business rule
        lan = row["lan"]
        if not lan or lan in seen_lans:
            continue  # one disbursal per distinct LAN, not per customer
        seen_lans.add(lan)
        key = (row["city"], row["market"], row["asset_type"])
        rollup[key] += 1

    # The MIS table: only the cleaned, aggregated shape. This is literally
    # all that gets stored -- there is no "raw_mis_events" table sitting
    # underneath it.
    mis_table = [
        {"city": city, "market": market, "asset_type": asset_type, "disbursal_count": count}
        for (city, market, asset_type), count in sorted(rollup.items())
    ]
    return mis_table


def run_mis_pipeline(csv_path):
    events = extract(csv_path)
    return transform_and_load(events)
