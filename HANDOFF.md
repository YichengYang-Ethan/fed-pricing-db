# Handoff: Fed decision basis, prediction markets vs CME fed funds futures

Everything needed to rebuild the data and work the strategy. Two public repos:

- **https://github.com/YichengYang-Ethan/fed-pricing-db** — the pullers and the DuckDB builder (this repo)
- **https://github.com/YichengYang-Ethan/fed-basis-backtest** — the analysis, plus derived tables in `data/`

---

## 1. The idea

A CBOT 30-Day Fed Funds future (ZQ) settles at

```
100 - (arithmetic mean of daily EFFR over its delivery month)
```

so a ZQ position is a **linear** claim on FOMC decisions with a coefficient the calendar fixes
in advance. Kalshi's `KXFEDDECISION` and Polymarket's Fed markets are **digital** claims on the
same decisions. Same underlying event, two instrument shapes, two venues with different
participants. When the two price one decision differently by more than the cost of crossing
both, a duration-matched package of them is a locked payoff in every state of the world.

### The instrument: a ZQ calendar spread, not an outright

Let `w = days_post / days_in_month`, the fraction of the meeting's own month spent at the new
rate (`days_post` counts from the effective date, normally the day after the decision). Then
for a decision of size `D` percentage points:

```
FRONT spread   F_M     - F_{M+1}  =  (1 - w) * D          span = 1 - w
BACK  spread   F_{M-1} - F_M      =       w  * D          span = w

implied decision   D = (F_near - F_far) / span
```

**The unknown post-decision EFFR level cancels in both.** That is the whole reason to use a
spread rather than a single contract: an outright's implied probability requires you to know
the prevailing EFFR, which is published a business day late, and a 1bp error in that level
moves the implied probability by `4/w` percentage points, which blows up for a late-month
meeting. The spread is a difference of two observed prices and contains no forecast rate.

Selection rule, a pure function of the FOMC calendar with no price input:

- A delivery month carrying **any other** meeting's rate change disqualifies the construction
  that uses it. Set `front_eligible` / `back_eligible` accordingly.
- Take the eligible construction with the larger span.
- `w = 0` (meeting on the last day of the month) is not degenerate: the whole decision lands
  in M+1 and FRONT has span 1.0.
- FRONT and BACK are complementary, because `w` is large exactly when a meeting falls early in
  its month, which is exactly when the previous month is clean. Together they cover 37 of 49
  meetings; FRONT alone covers 23.

### Sizing and the trade

ZQ is **$41.67 per basis point**, $4,167 per price point. A 25bp digital pays $1. So

```
one ZQ spread hedges   1041.75 * span   digital contracts
```

Define the basis in cents per 25bp-equivalent contract:

```
basis_cents = 4 * (D_cme_bp - D_venue_bp)
```

Enter when `|basis|` clears the gate, hold **both legs to settlement**, and the payoff is

```
net_cents = |basis| - sign(basis) * hedge_error - venue_cost - zq_cost
```

The hedge error is not a free parameter. Once a delivery month is past, the contract's terminal
price is a known function of published EFFR, so it is computed exactly rather than estimated.
`fed-basis-backtest/data/hedge_error.csv` has it per meeting, built from FRED alone.

### The one structural trap to design around

`KXFEDDECISION` is a **five-outcome mutually exclusive ladder** (cut 50 / cut 25 / hold /
hike 25 / hike 50), not a binary. One digital leg against one ZQ contract is **not** a hedge:
it pays the same amount in two of five states and takes a large loss in a third. Replicating a
linear ZQ payoff needs a portfolio weighted by each outcome's move, `m_k` units of leg `k`.
Check the tails are actually quotable before assuming the portfolio is executable.

---

## 2. Data acquisition

### 2.1 CME ZQ settlements — Databento (paid, cheap)

The authoritative source. `scripts/databento_zq_pull.py`, then `scripts/databento_zq_outrights.py`.

```python
import databento as db
c = db.Historical(open(os.path.expanduser("~/.config/databento/key")).read().strip())
SETTLE = int(db.StatType.SETTLEMENT_PRICE)          # == 3
d = c.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols=["ZQ.FUT"],          # parent symbology: the whole ZQ complex
        stype_in="parent",
        schema="statistics",
        start=month_start, end=next_month_start)     # walk month by month
df = d.to_df()
s = df[df.stat_type == SETTLE]
```

- **Cost: $0.65** for 2022-01 through 2026-09, statistics only. The $125 signup credit covers it
  many times over.
- **Do not buy the `definition` schema** (a further $2.78). The statistics response already
  carries `symbol`, so the mapping is free.
- `ts_ref` is the trade date; `ts_recv` is when the record arrived.
- **CME publishes a preliminary and a final settlement.** Dedup on `(trade_date, symbol)`
  keeping the last by `ts_recv`, and **do it globally, not per request chunk** — a final
  settlement posted after midnight UTC lands in the next chunk and a per-chunk dedup misses it.
  That left 1,560 duplicate pairs on the first run.
- ZQ lists **60 consecutive delivery months**. The symbol's year is a single digit, which is
  unambiguous only because 60 < 120; resolve the decade by requiring the horizon to land in
  `[-1, 60]` months.
- Response also contains exchange-listed **calendar spreads** (`ZQV6-ZQX6` style), 823,140 rows
  across 2,714 spread symbols. Useful and free in the same pull.

Result: 70,579 outright rows, 2021-12-31 to 2026-08-31, 116 delivery months (2022-01 to 2031-08).

### 2.2 CME ZQ — IBKR (free, covers the most recent weeks)

`scripts/ibkr_zq_pull.py`, read-only, via `ib_async`. Needed only because a Databento pull is a
point-in-time purchase; IBKR gives the last few sessions for free.

```python
con = Future(symbol="ZQ", lastTradeDateOrContractMonth=ym, exchange="CBOT",
             currency="USD", includeExpired=True)
# CRITICAL: for an EXPIRED contract endDateTime="" means "now" and returns ~33 stale bars.
# Anchoring at the contract's own last trade date returns the full history.
end = f"{c.lastTradeDateOrContractMonth} 23:59:59 US/Central"
bars = ib.reqHistoricalData(c, endDateTime=end, durationStr="2 Y",
                            barSizeSetting="1 day", whatToShow="TRADES", useRTH=False)
```

Gateway/TWS ports tried in order: 4002, 7497, 4001, 7496. Enable "Read-Only API".
Cross-validated against Databento on 8,795 overlapping daily settlements: **100% exact, zero
disagreement to the last decimal.**

### 2.3 Polymarket — public, no auth

`scripts/poly_pull.py` → `scripts/poly_post.py`, helpers in `scripts/poly_common.py`.

```
metadata   GET https://gamma-api.polymarket.com/events?slug={slug}
prices     GET https://clob.polymarket.com/prices-history?market={token_id}&startTs={ts}&fidelity={60|1}
```

- `fidelity=60` hourly, `fidelity=1` minute. Both are available for the full life of a market;
  a probe pulled 180,811 and 191,492 minute points in single calls.
- **`endTs` is silently ignored.** You must walk forward from `startTs` in steps and stop when
  the response stops advancing. This is the single easiest way to get a truncated panel.
- Send a **browser User-Agent**; the default urllib UA gets blocked.
- `token_id` is the Gamma `clobTokenId`. `event_id` is the slug.
- **`p` is the CLOB midpoint, not a trade price.** There is no historical bid/ask, so execution
  cost has to be assumed rather than measured.
- An empty book returns exactly `0.5` as a placeholder. Flag it, do not average it in.

Result: 9.25M minute points + 300k hourly, 2022-12-15 to 2026-09-17, 33 FOMC meetings, 2 to 5
outcome legs each. This is the only prediction-market source covering 2023-2025.

### 2.4 Kalshi — public, no auth for market data

`scripts/kalshi_pull.py`. Base `https://api.elections.kalshi.com/trade-api/v2`.

```
GET /series/{series}                                          series metadata, fee_type
GET /events/{event_ticker}?with_nested_markets=true           the 5 legs of a meeting
GET /markets/{ticker}                                         status, result, close_time, tick_size
GET /series/{series}/markets/{ticker}/candlesticks            bid/ask/price OHLC
    params: start_ts, end_ts, period_interval = 1 | 60 | 1440 (minutes); chunk the range
GET /markets/trades?ticker={ticker}&limit=1000&cursor=...     tick-level prints, taker_side
```

Series is `KXFEDDECISION`. Candles carry `bid_close` / `ask_close`, which Polymarket does not.

- `/markets/trades` only serves a **trailing window**; candles cover the full life. Record which
  is which, they are not interchangeable.
- Coverage today: 13 meetings (2026-07 to 2028-01), quotes from 2025-09-29, 155,004 trades from
  2026-07-11. **Nothing before 2026-07**, which is why Polymarket carries the historical work.

### 2.5 FRED — public, no key needed

`scripts/fetch_fred.py`.

```
GET https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES_ID}
SERIES = EFFR, SOFR, IORB, DFEDTARU, DFEDTARL, DFF
```

EFFR from 2000, DFF from 1954. **EFFR is published one business day late** (NY Fed, ~09:00 ET),
so on the afternoon of day *d* you know EFFR only through *d−1*. This is precisely the exposure
the calendar spread removes and an outright does not.

---

## 3. Facts that will otherwise produce wrong numbers

These are measured, not assumed. Each one silently manufactures or destroys an edge.

| | |
| --- | --- |
| **ZQ settles 14:00 CT = 15:00 ET** | One hour **after** the 14:00 ET FOMC announcement. A decision-day settle already contains the outcome. Verified: 2024-09-18 implies −50.83bp and so do the next three sessions, while 2024-09-17 implies −42.08bp. Never compare a decision-day ZQ settle to an earlier prediction-market snapshot, and drop decision day from any basis panel. |
| **Kalshi closes 13:59:00 ET** | Expiration 14:05, settlement about 9 minutes later. All 65 `KXFEDDECISION` markets, no exceptions. There is **no post-announcement Kalshi price**; the last tape print is 13:58:59. The exit is the payout, not a sale. |
| **13:59 is the most liquid minute** | Not a thin close. 82,465 contracts traded in that one bar on an in-play leg, 1-cent spread, and 61 of the last 61 one-minute bars two-sided. |
| **Kalshi taker fee** | `ceil_to_cent(0.07 * C * p * (1-p))`, `fee_type = quadratic_with_maker_fees`, `fee_multiplier = 1`. It **peaks at p = 0.50 at 1.75c per contract**, i.e. it is most expensive exactly when the market is most uncertain and the basis is most likely to be wide. Maker side is charged too. Tick is 1 cent, `linear_cent`. |
| **A frozen CLOB midpoint prints every minute like a live one** | Presence is not liveness; **variety** is. Gate on distinct midpoints over a trailing window, measured **only on legs in [0.05, 0.95]**, since a deep out-of-the-money leg is legitimately still. A leg-sum sanity band does not catch this. |
| **Polymarket buckets are not pure binaries** | `CUT50P` means "50+ bps". Some events fold two named markets (e.g. 50 and 75) into one bucket. Mapping that bucket to a single move is a lower bound and leaves an open tail. |
| **ZQ tick** | 0.0025 ($10.4175) for the nearest delivery month, 0.0050 ($20.835) for all others. Converted to probability, one tick is `tick / (0.25 * span)` — which is why a late-month meeting's own contract is unusable: span 2.4bp means one tick is about 10 probability points. |
| **A continuous futures series is the wrong contract** | At 19 days before a meeting the front contract has zero exposure to that meeting in 73% of cases, and 100% at 30 days. Always hold a named delivery month. |
| **DuckDB reserved words** | `date`, `asof`, `days` all break aliasing with a confusing "syntax error at or near AS". Quote them. |
| **EFFR pass-through** | Steps by exactly the target change on the effective date, 17 of 17 changes since 2022, MAE 0.000bp. There is no level-reset risk; the residual risk is intra-month drift. |

---

## 4. Build order

```bash
python3 scripts/fetch_fred.py            # public, instant
python3 scripts/build_meetings.py        # FOMC calendar + w, days_post, effective dates
python3 scripts/poly_pull.py             # hours; resumable, parts in raw/_parts
python3 scripts/poly_post.py             # re-anchors minute windows, writes raw/poly/
python3 scripts/kalshi_pull.py           # public
python3 scripts/databento_zq_pull.py     # ~11 min, $0.65
python3 scripts/databento_zq_outrights.py
python3 scripts/ibkr_zq_pull.py          # optional, most recent sessions only
python3 scripts/build_db.py              # idempotent, writes fed.duckdb + build_report.json
```

Then in `fed-basis-backtest`:

```bash
python3 harness/pinned_regime.py --csv results/
python3 harness/build_public_data.py
```

`build_db.py` is idempotent and emits a QA report with per-table anomalies; read it rather than
trusting the load.

---

## 5. Where the data does not exist yet

- **Intraday CME.** The largest gap. There is one settlement per day. Kalshi stops at 13:59 ET
  and ZQ settles at 15:00 ET, so the two venues cannot be aligned to a common instant without
  intraday ZQ. Databento sells it on the same `GLBX.MDP3` dataset: `tbbo`, `mbp-1` or `trades`.
  Pull only the days around each meeting to keep the cost down.
- **Order-book depth, both venues.** Kalshi's `liquidity_dollars` is zero on all 65 markets, and
  Polymarket publishes no historical book. Size feasibility can currently only be proxied by
  traded volume.
- **SOFR futures (SR1/SR3).** Never pulled. A second curve on the same policy path, useful as an
  independent cross-check on the ZQ-implied decision.

---

## 6. Licensing, which constrains what can be published

Republishing CME settlement prices is **redistribution** under Databento's licence. It converts
a non-professional subscriber into a redistributor, which requires an information licence
agreement with CME and redistribution fees. Kalshi's terms prohibit redistribution outright.
Derived data is permitted provided it **cannot be reverse-engineered back to the feed**.

Practical rule used here: raw panels stay local (`raw/`, `*.duckdb`, `*.parquet` are gitignored),
and anything published is **one row per FOMC meeting**, never a daily series. Two of the four
published tables are built from FRED alone and carry no licence encumbrance at all, which is why
the main result is independently reproducible from public data.
