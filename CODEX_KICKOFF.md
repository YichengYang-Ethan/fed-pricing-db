# Kickoff prompt for Codex

Paste everything below the line.

---

You are taking over a quantitative research project. **Your first job is to review it, not to
extend it.** Do not write new strategy code, do not buy data, and do not propose improvements
until you have finished the review and reported back.

## Context

The project prices FOMC decisions on two kinds of instrument and looks for a gap between them.

A CBOT 30-Day Fed Funds future (ZQ) settles at `100 - (arithmetic mean of daily EFFR over its
delivery month)`, so it is a **linear** claim on FOMC decisions with a coefficient the calendar
fixes in advance. Kalshi's `KXFEDDECISION` and Polymarket's Fed markets are **digital** claims
on the same decisions. Same underlying event, two instrument shapes, two venues with different
participants. If they price one decision differently by more than the cost of crossing both, a
duration-matched package of the two is a locked payoff in every state.

Read `HANDOFF.md` in this repository first. It has the full derivation, every API endpoint with
its exact parameters, and a table of measured facts about the data that will silently produce
wrong numbers if you do not know them.

## What is on this machine

```
~/Developer/fed-pricing-db/                          the data layer (public repo)
  fed.duckdb                    100 MB   the built panel; NOT in git
  scripts/                      10 pullers + the DuckDB builder
  raw/                          all source data; NOT in git
    cme/databento/              10 MB    ZQ settlements, the authoritative source
    cme/ibkr/                  140 KB    ZQ, most recent sessions only
    cme/*.parquet               17 files, an older yfinance pull, superseded
    cme/sofr/                            a probe only, 153 rows, essentially empty
    poly/prices.parquet          23 MB   9.25M minute + 300k hourly points
    poly/markets.parquet                 metadata, token_id -> outcome mapping
    kalshi/candles.parquet      4.1 MB   bid/ask OHLC
    kalshi/trades.parquet       6.0 MB   155,004 tick prints with taker_side
    fred/                      460 KB    EFFR, SOFR, IORB, DFEDTARU/L, DFF
  HANDOFF.md                             read this first
  build_report.json                      QA report from the last build; NOT in git

~/Developer/github.com/YichengYang-Ethan/fed-basis-backtest/    the analysis (public repo)
  harness/                      panel construction, cost model, the pipelines
  prereg/                       the design document; read its status header
  results/                      conclusions and per-meeting tables
  data/                         four derived tables, publishable, documented in data/README.md

~/.config/databento/key                  Databento API key, chmod 600
```

Both repositories are public on GitHub under `YichengYang-Ethan`. The panel itself is not
published, for licensing reasons stated in §6 of `HANDOFF.md`.

## Off limits

**Do not read, open, summarise, copy or transmit `~/Developer/fed-pricing-db/raw/s3scout/`.**
It is reconnaissance of a private employer S3 bucket and contains internal infrastructure
details. It is unrelated to this project's data and is excluded from every task below. If a
glob or a recursive tool would pull it in, narrow the glob.

Also do not commit or publish anything from `raw/` or `fed.duckdb`. Republishing CME settlement
prices is redistribution under Databento's licence and would create a real obligation; see §6
of `HANDOFF.md`.

## Phase 1: review the data layer

Reproduce, do not trust. For each item, state CONFIRMED or REFUTED with the command you ran and
the number it printed.

1. **Cross-source agreement.** Databento and IBKR overlap on ZQ daily settlements. The claim is
   8,607 overlapping `(date, delivery_month)` observations with **zero** disagreement. Verify.
2. **No duplicates.** `(trade_date, delivery_month)` should be unique in
   `raw/cme/databento/zq_outrights.parquet`. CME posts a preliminary and a final settlement, and
   a naive per-chunk dedup misses the ones that cross a month boundary.
3. **Contract-year decoding.** ZQ lists 60 consecutive delivery months and the symbol carries a
   single-digit year. Confirm every row resolves to exactly one plausible decade.
4. **Polymarket completeness.** The CLOB `prices-history` endpoint silently ignores `endTs`, so
   a naive pull truncates. Check for gaps: for each token, are there long stretches with no
   points relative to the market's own lifetime?
5. **Timestamp alignment.** Confirm from the data, not from the docs, that a decision-day ZQ
   settle already contains the announcement. The stated evidence is that 2024-09-18 implies
   −50.83bp and so do the next three sessions, while 2024-09-17 implies −42.08bp.
6. **Kalshi close time.** Confirm `close_time` across all `KXFEDDECISION` markets. The claim is
   13:59:00 ET with no exceptions, and that the final minute is one of the most liquid.
7. **Run `build_db.py`** and read `build_report.json`. It emits per-table anomalies. Report any
   anomaly the previous work did not already account for.

## Phase 2: review the analysis

1. **Reproduce the headline.** `fed-basis-backtest/data/hedge_error.csv` claims the hedge error,
   computed from published EFFR rather than estimated, is MAE **0.134 cents** per 25bp-equivalent
   contract over 25 meetings from 2022-01 to 2025-08, and **3.554 cents** over 8 meetings from
   2025-09. Rebuild it from FRED alone and confirm or refute both figures.
2. **Check the instrument selection** in `data/fomc_instruments.csv` is a pure function of the
   FOMC calendar with no price input, and that the FRONT/BACK eligibility rule is correctly
   applied. The claim is that 45 of 49 meetings get a viable instrument, with FRONT eligible
   on 27 and BACK on 23.
3. **Attack the lookahead gates.** `prereg/PREREGISTRATION.md` §G lists them. For each, find a
   way the current code could still leak future information, or state that it cannot.
4. **Find a bug.** Read `harness/pinned_regime.py` and `harness/build_public_data.py` line by
   line and look for an error that changes a number. Previous rounds found several, so assume
   more exist.

## Phase 3: judge what is missing

Only after phases 1 and 2. Given what you have verified, decide what data the project actually
needs next and justify each item by naming the specific question it would answer. Known
candidates, which you should evaluate rather than accept:

- **Intraday CME.** There is currently one settlement per day. Kalshi stops trading at 13:59 ET
  and ZQ settles at 15:00 ET, so the venues cannot be aligned to a common instant. Databento
  sells `tbbo`, `mbp-1` and `trades` on the same `GLBX.MDP3` dataset. Price it before
  recommending it, and say which days are actually needed rather than assuming the full range.
- **Order-book depth.** Kalshi's `liquidity_dollars` is zero on all 65 markets and Polymarket
  publishes no historical book, so position size is currently proxied by traded volume only.
  Is there a source, and what would it change?
- **SOFR futures (SR1/SR3).** Never pulled. A second curve on the same policy path.

Rank by what each unlocks per dollar and per hour. If you conclude the project needs no new
data, say so and explain what the existing data can still answer.

## Deliverable

One markdown report with:

- A table of every claim you checked, with CONFIRMED / REFUTED / UNVERIFIABLE, the command, and
  the number you got.
- Every bug found, with a minimal reproduction.
- The Phase 3 ranking with costs.
- A short list of anything you believe is true but could not verify with the data on this
  machine, and what would settle it.

Be adversarial. Findings that overturn something are more useful than findings that agree. If a
number reproduces exactly, say so plainly; if it reproduces only under a particular choice of
anchor, sample or convention, say which one, because that is usually where the error is.
