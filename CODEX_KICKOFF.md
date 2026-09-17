# Kickoff prompt for Codex

Paste everything below the line. Works from the Codex app with "Full access" (which reaches the
local filesystem), from the CLI, or from Codex cloud with only the public repos. Phase 0 makes
the agent report what it can actually reach, and Phase 4 activates only when the local panel is
readable, so the same text works everywhere without editing.

---

You are taking over a quantitative research project. **Your first job is to review it, not to
extend it.** Do not write new strategy code, do not buy data, and do not propose improvements
until you have finished the review and reported back. Be adversarial: a finding that overturns
something is worth more than a finding that agrees.

## Where everything is

On this machine, if you can reach the filesystem:

```
~/Developer/fed-pricing-db/                      data layer
  fed.duckdb                  ~100 MB  the built panel; NOT in git
  raw/cme/databento/           10 MB   ZQ settlements, the authoritative source
  raw/cme/ibkr/               140 KB   ZQ, most recent sessions only
  raw/poly/prices.parquet      23 MB   9.25M minute + 300k hourly points
  raw/kalshi/                  10 MB   candles.parquet + trades.parquet
  raw/fred/                   460 KB   EFFR, SOFR, IORB, DFEDTARU/L, DFF
  scripts/                             10 pullers + build_db.py
  HANDOFF.md                           read this first
~/Developer/github.com/YichengYang-Ethan/fed-basis-backtest/    analysis
~/.config/databento/key                Databento API key, chmod 600
```

Both are also public on GitHub, which is the fallback if you have no filesystem access:

```
https://github.com/YichengYang-Ethan/fed-basis-backtest     the analysis and the derived tables
https://github.com/YichengYang-Ethan/fed-pricing-db         the data pullers and the DuckDB builder
```

The panel and all raw data are deliberately **not** in git, because republishing CME settlement
prices is redistribution under Databento's licence. Do not commit or upload anything from `raw/`
or `fed.duckdb`.

Read `fed-pricing-db/HANDOFF.md` first: it has the derivation, every API endpoint
with exact parameters, and a table of measured facts about the data that silently produce wrong
numbers if you do not know them.

## The idea

A CBOT 30-Day Fed Funds future (ZQ) settles at

```
100 - (arithmetic mean of daily EFFR over its delivery month)
```

so a ZQ position is a **linear** claim on FOMC decisions with a coefficient the calendar fixes
in advance. Kalshi's `KXFEDDECISION` and Polymarket's Fed markets are **digital** claims on the
same decisions. Same underlying event, two instrument shapes, two venues with different
participants. If the two price one decision differently by more than the cost of crossing both,
a duration-matched package of them is a locked payoff in every state.

The instrument is a ZQ **calendar spread**, not an outright. With `w = days_post / days_in_month`
(the fraction of the meeting's own month spent at the new rate) and a decision of `D` percentage
points:

```
FRONT   F_M     - F_{M+1}  =  (1 - w) * D        span = 1 - w
BACK    F_{M-1} - F_M      =       w  * D        span = w
implied D = (F_near - F_far) / span
```

The unknown post-decision EFFR level cancels in both, which is the entire reason to prefer a
spread: an outright's implied probability needs the prevailing EFFR, which publishes a business
day late, and a 1bp error in that level moves the implied probability by `4/w` percentage points.

ZQ is **$41.67 per basis point**. A 25bp digital pays $1, so one ZQ spread hedges
`1041.75 * span` digital contracts. Define `basis_cents = 4 * (D_cme_bp - D_venue_bp)`. Enter
when it clears a gate, hold both legs to settlement, and

```
net_cents = |basis| - sign(basis) * hedge_error - venue_cost - zq_cost
```

One structural trap to design around: `KXFEDDECISION` is a **five-outcome mutually exclusive
ladder** (cut 50 / cut 25 / hold / hike 25 / hike 50), not a binary. A single digital leg against
one ZQ contract is not a hedge. It pays the same in two of five states and takes a large loss in
a third. Replicating a linear ZQ payoff needs a portfolio weighted by each outcome's move.

## Phase 0: establish what you can actually reach

Report this in one short block before doing anything else:

- Network access? Can you fetch `https://fred.stlouisfed.org/graph/fredgraph.csv?id=EFFR`?
- Can you read `~/Developer/fed-pricing-db/fed.duckdb` and `raw/`? Say yes or no plainly, since
  it decides whether Phase 4 runs.
- What files are in `fed-basis-backtest/data/`?

Phase 1 works from the derived tables plus FRED alone and is the highest-value check either way.
Phase 2 is a code review. Phase 4 needs the panel; run it if you can reach it, skip it and say
so if you cannot.

## Phase 1: independently rebuild the headline result

Do **not** run the author's `harness/build_public_data.py`. Write your own implementation from
the specification below and compare. An independent reimplementation is a real check; rerunning
someone else's code is not.

The claim: because a ZQ contract settles on the mean of published EFFR over its delivery month,
the hedge's error is **computed, not estimated**. For each meeting with a known outcome, take the
two delivery months of its selected instrument, compute each month's mean daily EFFR over
**calendar** days (weekends and holidays carry the previous business day's rate), and

```
spread_terminal_bp = ((100 - mean_near) - (100 - mean_far)) * 100
resid_bp           = spread_terminal_bp - span * realized_change_bp
err_cents          = 4 * resid_bp / span
```

Pull EFFR yourself from `https://fred.stlouisfed.org/graph/fredgraph.csv?id=EFFR`. Take the FOMC
calendar, `span`, `leg_near`, `leg_far` and `realized_change_bp` from
`fed-basis-backtest/data/fomc_instruments.csv`.

Confirm or refute each of these, reporting the number you got:

| claim | value |
| --- | --- |
| pinned regime (2022-01 to 2025-08), n | 25 meetings |
| pinned MAE | **0.134 cents** |
| pinned median / p90 / max | 0.000 / 0.390 / 2.000 |
| pinned exactly zero | 20 of 25 |
| drifting regime (2025-09 onward), n | 8 meetings |
| drifting MAE | **3.554 cents** |
| drifting median / p90 / max | 2.496 / 8.489 / 9.467 |

"Exactly zero" uses a tolerance of 1e-3 cents. Check the tolerance is doing honest work rather
than hiding a real residual: what are the actual non-zero values sitting just below it?

Then check the instrument selection in `data/fomc_instruments.csv` is a **pure function of the
FOMC calendar with no price input**. A delivery month carrying any *other* meeting's rate change
must disqualify the construction that uses it. The claim is FRONT eligible on 27 of the 49
meetings, BACK on 23, and 45 meetings with at least one viable instrument. Note that `w = 0`
(a meeting on the last day of a month) is not degenerate: the whole decision lands in M+1 and
FRONT has span 1.0.

## Phase 2: review the code and the reasoning

1. **Find a bug.** Read `harness/pinned_regime.py` and `harness/build_public_data.py` line by
   line. Earlier rounds found several real errors, so assume more exist. A bug that changes a
   published number is the most valuable thing you can return.
2. **Attack the lookahead gates.** `prereg/PREREGISTRATION.md` lists them. For each, find a way
   the current code could still leak future information, or argue that it cannot. Read that
   file's status header first: it is a design document that was never frozen, and it says so.
3. **Check the claimed traps are real.** These are asserted in `HANDOFF.md` §3 as measured facts.
   Say which you can verify from where you are, which need the panel, and whether any is stated
   more strongly than its evidence supports:
   - ZQ settles 14:00 CT, an hour **after** the 14:00 ET FOMC announcement, so a decision-day
     settle already contains the outcome.
   - Kalshi `KXFEDDECISION` closes 13:59:00 ET, and that final minute is one of the most liquid.
   - Kalshi's taker fee `ceil_to_cent(0.07 * C * p * (1-p))` peaks at p = 0.50, i.e. it is most
     expensive exactly when the basis is most likely to be wide.
   - A frozen CLOB midpoint prints every minute exactly like a live one, so liveness has to be
     measured as *variety* of distinct midpoints, on in-play legs only.
   - A continuous futures series is the wrong contract: at 19 days before a meeting the front
     contract has zero exposure to that meeting in 73% of cases.
4. **Read `results/PINNED_REGIME.md` adversarially.** Its limitations section says nine trades
   and t = 1.95 is not significance, that the basis is measured midpoint to midpoint with an
   assumed half-spread, and that the regime ended. Is it honest about what it found, or does any
   claim outrun its evidence?

## Phase 3: judge what data is missing

Only after Phases 1 and 2, and grounded in what you actually verified. For each candidate, name
the specific question it would answer and estimate the cost before recommending it. Evaluate
these rather than accepting them, and add anything you think is missing:

- **Intraday CME.** There is one settlement per day. Kalshi stops trading at 13:59 ET and ZQ
  settles at 15:00 ET, so the venues cannot be aligned to a common instant. Databento sells
  `tbbo`, `mbp-1` and `trades` on the same `GLBX.MDP3` dataset. Price it, and say which days are
  actually needed rather than assuming the full range.
- **Order-book depth, both venues.** Kalshi's `liquidity_dollars` is zero on all 65 markets and
  Polymarket publishes no historical book, so position size is currently proxied by traded
  volume only. Is there a source, and what would it change?
- **SOFR futures (SR1/SR3).** Never pulled. A second curve on the same policy path, usable as an
  independent cross-check on the ZQ-implied decision.

Rank by what each unlocks per dollar and per hour. If you conclude no new data is needed, say so
and explain what the existing panel can still answer.

## Phase 4: the panel, if you can read it

- Databento and IBKR overlap on ZQ daily settlements. The claim is **8,607** overlapping
  `(date, delivery_month)` observations with **zero** disagreement, max difference 0.000000bp.
- `(trade_date, delivery_month)` must be unique in `raw/cme/databento/zq_outrights.parquet`.
  CME posts a preliminary and a final settlement, and a per-chunk dedup misses the ones that
  cross a month boundary.
- Polymarket's `prices-history` endpoint silently ignores `endTs`, so a naive pull truncates.
  Check for gaps per token relative to each market's own lifetime.
- Run `scripts/build_db.py` and read the anomalies it emits into `build_report.json`.

Read only the paths named in this prompt. Other directories on the machine hold unrelated work
and are out of scope; if a recursive tool would pull them in, narrow the glob. You have broad
filesystem access, so this fence is yours to respect rather than something enforced for you.

## Deliverable

One markdown report:

- A table of every claim checked: CONFIRMED / REFUTED / UNVERIFIABLE, the command you ran, and
  the number you got.
- Every bug found, with a minimal reproduction.
- The Phase 3 ranking with costs.
- A list of things you believe are true but could not verify from where you are running, and
  exactly what would settle each one.

If a number reproduces exactly, say so plainly. If it reproduces only under a particular choice
of anchor, sample or convention, name that choice, because that is usually where the error is.
