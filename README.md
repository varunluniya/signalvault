# SignalVault

A runnable demonstration of an ETL-vs-ELT architecture decision for a
lending company, built around one real dataset: a stream of loan
application events (offer shown, offer accepted, disbursed, cancelled,
rejected) at the customer/offer level.

## The decision

Two workloads, two different right answers:

- **Daily MIS reporting** (city/market/asset disbursal rollup) — **ETL**.
  Two settled business rules (exclude cancelled/rejected, count one
  disbursal per Loan Account Number) make it safe to clean the data before
  storing it. The result is a fast, simple, trustworthy daily number.
- **Customer propensity modeling** (who will buy, what, when, at what
  price) — **ELT**. The raw offer-level history (every rate/fee/term a
  customer saw, and what they did next) is the exact signal a future model
  might need but today's model doesn't yet use. Clean it away at load time
  and it's gone for good.

## What this repo actually proves, not just argues

`etl_mis.py` runs the MIS pipeline: filter, aggregate, store — the raw
events never survive past this point. `elt_propensity.py` runs the
propensity pipeline: load every event untouched into a warehouse table,
derive features afterward as versioned transforms (`derive_features_v1`).

`cli.py` then does the thing a decision memo can't do on paper: it invents
a feature nobody anticipated when `derive_features_v1` was written — how
many offers a customer saw, and whether they touched the EMI calculator,
before converting — and derives it two ways:

1. From the ELT raw store (`derive_features_v2`): works immediately, zero
   new data collection, because the ELT load never threw the other offers
   away.
2. From the ETL-cleaned MIS table (`attempt_v2_from_mis_table`): fails on
   purpose, with the exact missing columns named in the error, because the
   MIS pipeline discarded customer-level and offer-level detail at load
   time, before anyone knew a future model would want it.

That asymmetry — recoverable from ELT, permanently gone from ETL — is the
decision memo's actual claim, demonstrated in code rather than described
in prose.

## Run it

```
python3 cli.py
```

Runs both pipelines against the same 28-event source CSV, then runs the
"future feature" comparison above with real output, not a description of
intended behavior.

```
python3 -m pytest -q
```

7 tests: MIS correctly excludes cancelled/rejected and counts one
disbursal per LAN (not per customer); the MIS table has no customer- or
offer-level columns; the ELT load retains every event type untouched;
`derive_features_v1` covers only converted customers; the unanticipated
`derive_features_v2` feature is correctly derivable from the raw store with
the exact expected values; and the same feature is provably unrecoverable
from the MIS table.

## Honest disclosure

The 28-event dataset is synthetic, built to match the specific example in
the underlying decision memo (a customer shown a ₹1,00,000 offer, then a
better ₹75,000 one days later, consistent with the real settled business
rules named there — cancelled/rejected exclusion, one disbursal per LAN).
There's no real warehouse, no real propensity model, and no real customer
data here; the point is the pipeline shape and the recoverability proof,
not a production system.
