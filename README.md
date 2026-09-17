# fed-pricing-db

> Builds the panel analysed in [fed-basis-backtest](https://github.com/YichengYang-Ethan/fed-basis-backtest). Code only:
> `raw/`, `*.duckdb` and `*.parquet` are gitignored, because republishing CME settlements is
> redistribution under Databento's licence and Kalshi's terms prohibit it outright.

One DuckDB file (`fed.duckdb`) that lines up three venues pricing the same FOMC decisions:

* **Kalshi** KXFEDDECISION ladders (C26/C25/H0/H25/H26 legs -> CUT50P/CUT25/HOLD/HIKE25/HIKE50P), candles + trades + market metadata,
* **Polymarket** Fed-decision events (CLOB price history per YES/NO token, hourly and minute),
* **CME** 30-Day Fed Funds futures (ZQ, per-contract files + the ZQ=F continuous series) with FRED policy rates (EFFR, SOFR, IORB, target range, DFF),

plus a `meetings` spine (49 FOMC decision dates 2022-01-26..2028-01-26) and views that turn each venue into a daily probability /
fair-price series and compute the cross-venue basis.

Built 2026-09-17T06:23:28Z by `scripts/build_db.py` (idempotent: deletes and recreates `fed.duckdb`; exits 1 if an integrity test fails).
Raw inputs live under `raw/<source>/` (parquet + JSON manifests written by the collectors; see their manifests for pull details).

## Tables and views

| name | kind | rows | contents |
|---|---|---|---|
| `meetings` | table | 49 | one row per FOMC decision date 2022-2028: Kalshi event/status/result, Polymarket slugs (decision vs other), ZQ contract + source, effective date, delta = days_post/days_in_month, realized change (FRED DFEDTARU) |
| `meeting_windows` | table | 49 | per meeting: previous meeting date and previous effective date (start of the inter-meeting window used by v_prob_cme_daily / v_basis_daily) |
| `kalshi_markets` | table | 147 | Kalshi market metadata snapshot (147 markets: 65 KXFEDDECISION legs + KXFED rate-threshold, KXRATECUT, KXFEDHIKE) incl. status, result, settlement, volume, open interest; parsed `*_ts_utc` timestamps |
| `kalshi_candles` | table | 624,766 | Kalshi candlesticks P=1440/60/1 (ts = bar end); YES bid/ask OHLC, trade OHLC, vol, oi, is_partial; derived bar_end_utc, trade_date_et (daily), mid, spread |
| `kalshi_trades` | table | 155,004 | public trades (trailing window only, earliest 2026-07-11): taker side, yes/no price, count, block flag |
| `poly_markets` | table | 249 | Polymarket market metadata (249 markets / 55 events): question, canonical outcome + outcome_detail, token ids, dates, volume, negRisk, resolution |
| `poly_prices` | table | 23,943,238 | CLOB prices-history points: hourly (fidelity 60) for every token's life and minute (fidelity 1) for the last 45 days; p = midpoint |
| `poly_event_meeting` | table | 53 | event -> meeting link with role (decision/other), is_primary (highest-volume single-decision event per meeting), canonical-leg count, any-hike flag |
| `cme_zq` | table | 23,955 | ZQ daily bars: ZQ=F continuous (is_continuous) + 16 contract files ZQU26..ZQZ27; settle = close, is_partial for the fetch-day bar |
| `cme_sofr_probe` | table | 153 | SR1/SR3 yfinance probes (1 live bar or junk 2020 placeholders) kept for the record; not usable as history |
| `fred_rates` | table | 26,377 | wide daily table: date, effr, sofr, iorb, tgt_upper, tgt_lower, dff (null on holidays / before series start) |
| `fred_long` | table | 50,263 | the six FRED series in long form (series_id, date, value) |
| `kalshi_daily_quotes` | table | 22,475 | dense daily grid per KXFEDDECISION leg with forward-filled daily quotes (stale_days), mid, spread, two_sided, quoted |
| `poly_daily_yes` | table | 12,327 | dense daily grid per Polymarket canonical leg: last hourly YES price of the ET day, forward-filled up to the FOMC date (stale_days, p_is_half) |
| `coverage_matrix` | table | 49 | per-meeting row counts by venue and granularity (also in README) |
| `v_effr_calendar` | view | 9,571 | calendar-day EFFR forward-filled over weekends/holidays (the settlement convention of ZQ) |
| `v_prob_kalshi_daily` | view | 4,495 | per meeting x ET day: 5 leg mids, raw sum, sum of bids/asks, normalized p_*, expected move, liquidity counters, in_window |
| `v_prob_poly_daily` | view | 2,943 | per meeting x event x ET day: canonical-leg YES prices (legs sharing an outcome summed), raw sum, normalized p_*, expected move, is_primary, in_window |
| `v_zq_meeting` | view | 12,814 | per meeting x date: meeting-month ZQ bar from the contract file when present else ZQ=F inside the month; source, pre_decision, days_to_decision |
| `v_prob_cme_daily` | view | 811 | per meeting x CME date: EFFR_pre, fair prices per move, implied move, binary and clamped CME probabilities, Kalshi/Poly ladder-weighted fair ZQ |
| `v_basis_daily` | view | 811 | per meeting x CME date: Kalshi/Poly implied ZQ minus settle (bp), same-day variants, and probability gaps CME-Kalshi, CME-Poly, Kalshi-Poly for HIKE25/HOLD/CUT25 |

### Column conventions

* `ts` is int64 UTC epoch seconds in every raw table. Derived timestamps (`*_utc`) are naive UTC `TIMESTAMP`s; ET conversions use
  `timezone('America/New_York', to_timestamp(ts))`, so nothing depends on the session `TimeZone`.
* Canonical `outcome` enum everywhere: `CUT50P, CUT25, HOLD, HIKE25, HIKE50P, OTHER`. Kalshi: C26->CUT50P, C25->CUT25, H0->HOLD, H25->HIKE25,
  H26->HIKE50P; KXFED / KXRATECUT / KXFEDHIKE series are `OTHER`. Polymarket mapping details are in `poly_markets.outcome_detail`
  (e.g. `HIKE25P` = "25+ bps hike" = ANY hike in the 4-leg events 2024-03..2026-04, stored under `HIKE50P`; `CUT50`/`CUT75P` both under `CUT50P`).
  CME and FRED rows carry `OTHER` as a placeholder.
* Kalshi prices are dollars per contract (0-1); `bid_close`/`ask_close` are YES quotes; `price_*` are trade based and null when the bar had no trades.
  Daily (P=1440) bars end at 00:00 ET (`ts % 86400` = 14400 in EDT, 18000 in EST); `trade_date_et` is the ET day the bar covers.
  `mid = (bid_close+ask_close)/2`, set to NULL for the empty-book state `bid=0, ask>=0.99` (the post-settlement bar of every settled leg).
* Polymarket `p` is the CLOB midpoint (can sit slightly outside [0,1]; an empty book returns exactly 0.5). `market_id` = Gamma conditionId,
  `event_id` = slug, `token_id` = clobTokenId, `outcome_label` = Yes/No (or the listed outcome name for the one non-Yes/No market).
* `cme_zq.settle == close` (yfinance has no settlement feed); `open_interest` is null (no OI history on yfinance; live snapshot in
  `raw/cme/manifest.json`). `is_partial` marks the in-progress Globex session bar on the fetch day. `is_continuous` marks ZQ=F rows.

### How the daily views are aligned

* `v_prob_kalshi_daily.date` = `trade_date_et` (the ET day the daily bar covers). The view is a dense calendar grid per event, quotes are
  forward-filled from the last available bar (`max_stale_days`, `n_legs_bar_today`); `p_*` = leg mid / `sum_raw` over quoted legs;
  `e_move_bp` = expected move with CUT50P=-50, CUT25=-25, HOLD=0, HIKE25=+25, HIKE50P=+50. `sum_bid`/`sum_ask` bound the no-arbitrage
  range; `n_legs_two_sided` and `max_spread` are the liquidity screen used by test T2a.
* `v_prob_poly_daily.date` = ET day; value = last hourly YES observation of the day per leg, legs sharing a canonical outcome summed, dense
  grid up to the FOMC date with forward fill (`max_stale_days`, `n_legs_obs_today`, `n_legs_half` = legs at the 0.5 placeholder).
  `is_primary` marks the highest-volume single-decision event per meeting (Sep-2026: `fed-decision-in-september-762`, not the `-568` mirror).
  For events whose hike bucket is "25+ bps" (any hike), `hike50p_is_any_hike` is true and `e_move_bp` uses +25 for that bucket.
* `v_zq_meeting`: for meetings whose contract file exists (`source = 'contract'`, 2026-09..2027-12) every bar of that contract; otherwise
  (`continuous_in_month`) the ZQ=F bars dated inside the meeting month. The CME collector verified that ZQ=F is the calendar-month contract
  through its last trading day (no roll after the FOMC day), so post-decision days are included and flagged `pre_decision = false`.
* `v_prob_cme_daily` / `v_basis_daily` only contain dates in the **inter-meeting window**: `date > prev_effective_date` (the previous
  meeting's effective date, table `meeting_windows`), because the hold-vs-move model needs the last observed EFFR to be the rate that
  prevails until this decision. Contract files carry years of history before that window (kept in `v_zq_meeting`, not priced there);
  meetings whose window has not started (2027-03 onward) therefore have no basis rows yet.
  `effr_pre` = last EFFR observed strictly before `date`; `fair_hold = 100 - effr_pre`; fair prices for each move use
  `delta = days_post / days_in_month` from `meetings`; `implied_move_bp = (fair_hold - settle) * 100 / delta`; `p_hike25_binary` =
  `implied_move_bp / 25` (hold-vs-hike25 model, unclamped; negative values mean cut pricing); `p_hike25_cme / p_cut25_cme / p_hold_cme` is the
  clamped hold-vs-nearest-move split used for the probability gaps. `kalshi_fair_zq` / `poly_fair_zq` = `fair_hold - delta * E[move]/100`
  using the prediction-market ladder **as of 00:00 ET on `date`** (the last complete daily observation before the CME session, i.e. the
  prediction-market row for ET day `date - 1`); `*_sameday` variants use the end of ET day `date`. `delta = 0` (2024-01-31 and 2024-07-31,
  effective date in the next month) makes the implied move undefined (NULL) — use the next month's contract for those meetings.
* `v_basis_daily`: `basis_kalshi_bp = (kalshi_fair_zq - settle) * 100` etc.; probability gaps are `cme - kalshi`, `cme - poly`, `kalshi - poly`
  for HIKE25 / HOLD / CUT25. The as-of alignment reproduces the independent reference for 2026-09-11 (+2.97bp) exactly (test T5).

## Provenance and limits

* **Kalshi REST (api.elections.kalshi.com)**, pulled 2026-09-17T05:25Z. Only 13 KXFEDDECISION events (2026-07 .. 2028-01) still exist; the
  26 events for 2023-05 .. 2026-06 are purged from the API (empty legs) and are absent here, so Kalshi history starts with the 2026-07-29
  meeting. Candles (P=1440/60/1) cover each market's whole life (minute bars only the last 45 days, sparse: no bar when no activity).
  The public trades endpoint only serves a trailing window: earliest trade 2026-07-11, complete for the Sep-2026 window but missing 17-43%
  of the Jul-2026 legs' volume (per-market coverage in `raw/kalshi/manifest.json -> trade_coverage`); use candle `vol` for full-life volume.
  Open markets have in-progress bars flagged `is_partial`. Settlements: 26JUL H0=yes (2026-07-29T18:07Z), 26SEP H25=yes (2026-09-16T18:08Z).
* **A private L2 order-book archive** was scouted, not loaded: it covers 2026-04-07 onward (571 GB) but
  contains **zero KXFEDDECISION rows** (the recorder's universe cap excludes the series); see `raw/s3scout/findings.json`. Nothing in this DB
  comes from S3.
* **Polymarket** (Gamma + CLOB prices-history), pulled 2026-09-17. 55 events / 249 markets from 2022-03 to 2027-01; the 7 events of 2022 are
  AMM-era (no CLOB history, metadata only), so prices start 2022-12-15. Hourly = fidelity 60 for the whole life; minute = fidelity 1 for the
  45 days before the effective end. Known quirks kept raw: 0.5 placeholders on empty books, post-resolution tails for 2023 events (trimmed in
  `v_prob_poly_daily` at the FOMC date, kept in `poly_prices`), 418 rows slightly outside [0,1], two single-decision events for Sep-2026,
  market end dates 1-2 days before the FOMC date in 2023-24. Path/dissent/threshold markets are `OTHER` and excluded from the probability view.
* **CME via yfinance** (ZQ=F 2000-09..2026-09-16 plus 16 contract files ZQU26..ZQZ27 from 2021-10/2022). `settle` is the yfinance close;
  month-end ZQ=F close matches 100 - calendar-day mean EFFR within 0.25bp in every month since 2022 (test T1), which is what justifies using
  ZQ=F inside the meeting month. SOFR probes (SR1/SR3) returned no usable history and sit in `cme_sofr_probe` for the record only.
* **FRED** (fredgraph.csv) EFFR/SOFR/IORB/DFEDTARU/DFEDTARL/DFF through 2026-09-15/16; holidays are null rows. DFEDTARU had not yet
  published the post-2026-09-16 level at pull time, so `meetings.realized_change_bp` is null for that meeting (IORB 3.65->3.90 on 09-17
  corroborates the HIKE25 settlement).
* `meetings.delta` uses the next US federal business day after the meeting as the effective date (pandas USFederalHolidayCalendar).

## Coverage matrix (meeting x venue x granularity x rows)

Rows are counts of stored rows. Kalshi bars/trades are for the linked event (all 5 legs); Polymarket rows are for the linked single-decision
events (and, separately, the path/dissent/meta "other" events); ZQ rows are those exposed by `v_zq_meeting` (pre-decision / total, the
whole contract history for contract-sourced months); "basis days" counts pre-decision CME session dates inside the inter-meeting window
with a Kalshi (K) or Polymarket (P) basis in `v_basis_daily`.

| meeting | realized | Kalshi event (status) | K 1440 / 60 / 1 bars | K trades | Poly decision events: hourly / minute | Poly other events: hourly / minute | ZQ source (rows pre-decision / total) | basis days K / P |
|---|---|---|---|---|---|---|---|---|
| 2022-01-26 | HOLD | — | 0 / 0 / 0 | 0 | 0: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQF22 (16 / 20) | 0 / 0 |
| 2022-03-16 | HIKE25 | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQH22 (11 / 23) | 0 / 0 |
| 2022-05-04 | HIKE50P | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQK22 (2 / 21) | 0 / 0 |
| 2022-06-15 | HIKE50P | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQM22 (10 / 21) | 0 / 0 |
| 2022-07-27 | HIKE50P | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQN22 (17 / 20) | 0 / 0 |
| 2022-09-21 | HIKE50P | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQU22 (13 / 21) | 0 / 0 |
| 2022-11-02 | HIKE50P | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQX22 (1 / 21) | 0 / 0 |
| 2022-12-14 | HIKE50P | — | 0 / 0 / 0 | 0 | 1: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQZ22 (9 / 21) | 0 / 0 |
| 2023-02-01 | HIKE25 | — | 0 / 0 / 0 | 0 | 1: 23,368 / 1,366,355 | 0: 0 / 0 | continuous_in_month ZQG23 (0 / 19) | 0 / 0 |
| 2023-03-22 | HIKE25 | — | 0 / 0 / 0 | 0 | 1: 16,210 / 971,119 | 0: 0 / 0 | continuous_in_month ZQH23 (15 / 23) | 0 / 14 |
| 2023-05-03 | HIKE25 | FEDDECISION-23MAY (purged) | 0 / 0 / 0 | 0 | 1: 9,804 / 593,797 | 0: 0 / 0 | continuous_in_month ZQK23 (2 / 22) | 0 / 2 |
| 2023-06-14 | HOLD | FEDDECISION-23JUN (purged) | 0 / 0 / 0 | 0 | 1: 7,920 / 471,888 | 0: 0 / 0 | continuous_in_month ZQM23 (9 / 21) | 0 / 9 |
| 2023-07-26 | HIKE25 | FEDDECISION-23JUL (purged) | 0 / 0 / 0 | 0 | 1: 6,336 / 380,112 | 0: 0 / 0 | continuous_in_month ZQN23 (16 / 20) | 0 / 16 |
| 2023-09-20 | HOLD | FEDDECISION-23SEP (purged) | 0 / 0 / 0 | 0 | 1: 6,052 / 259,024 | 0: 0 / 0 | continuous_in_month ZQU23 (12 / 20) | 0 / 12 |
| 2023-11-01 | HOLD | FEDDECISION-23NOV (purged) | 0 / 0 / 0 | 0 | 1: 4,148 / 248,700 | 0: 0 / 0 | continuous_in_month ZQX23 (0 / 22) | 0 / 0 |
| 2023-12-13 | HOLD | FEDDECISION-23DEC (purged) | 0 / 0 / 0 | 0 | 1: 4,908 / 261,618 | 0: 0 / 0 | continuous_in_month ZQZ23 (8 / 20) | 0 / 6 |
| 2024-01-31 | HOLD | FEDDECISION-24JAN31 (purged) | 0 / 0 / 0 | 0 | 1: 7,764 / 388,380 | 0: 0 / 0 | continuous_in_month ZQF24 (20 / 21) | 0 / 9 |
| 2024-03-20 | HOLD | FEDDECISION-24MAR20 (purged) | 0 / 0 / 0 | 0 | 1: 9,432 / 541,324 | 0: 0 / 0 | continuous_in_month ZQH24 (13 / 20) | 0 / 13 |
| 2024-05-01 | HOLD | FEDDECISION-24MAY (purged) | 0 / 0 / 0 | 0 | 1: 5,392 / 323,770 | 0: 0 / 0 | continuous_in_month ZQK24 (0 / 22) | 0 / 0 |
| 2024-06-12 | HOLD | FEDDECISION-24JUN (purged) | 0 / 0 / 0 | 0 | 1: 8,104 / 486,218 | 0: 0 / 0 | continuous_in_month ZQM24 (7 / 19) | 0 / 7 |
| 2024-07-31 | HOLD | FEDDECISION-24JUL (purged) | 0 / 0 / 0 | 0 | 1: 9,416 / 517,626 | 0: 0 / 0 | continuous_in_month ZQN24 (21 / 22) | 0 / 21 |
| 2024-09-18 | CUT50P | FEDDECISION-24SEP (purged) | 0 / 0 / 0 | 0 | 1: 10,584 / 518,086 | 0: 0 / 0 | continuous_in_month ZQU24 (11 / 20) | 0 / 11 |
| 2024-11-07 | CUT25 | FEDDECISION-24NOV (purged) | 0 / 0 / 0 | 0 | 1: 24,502 / 711,969 | 0: 0 / 0 | continuous_in_month ZQX24 (4 / 20) | 0 / 4 |
| 2024-12-18 | CUT25 | KXFEDDECISION-24DEC (purged) | 0 / 0 / 0 | 0 | 1: 34,408 / 711,516 | 0: 0 / 0 | continuous_in_month ZQZ24 (12 / 21) | 0 / 12 |
| 2025-01-29 | HOLD | KXFEDDECISION-25JAN (purged) | 0 / 0 / 0 | 0 | 1: 19,926 / 647,414 | 0: 0 / 0 | continuous_in_month ZQF25 (18 / 21) | 0 / 18 |
| 2025-03-19 | HOLD | KXFEDDECISION-25MAR (purged) | 0 / 0 / 0 | 0 | 1: 17,480 / 518,202 | 0: 0 / 0 | continuous_in_month ZQH25 (12 / 21) | 0 / 12 |
| 2025-05-07 | HOLD | KXFEDDECISION-25MAY (purged) | 0 / 0 / 0 | 0 | 1: 19,990 / 517,806 | 0: 0 / 0 | continuous_in_month ZQK25 (4 / 21) | 0 / 4 |
| 2025-06-18 | HOLD | KXFEDDECISION-25JUN (purged) | 0 / 0 / 0 | 0 | 1: 25,312 / 517,952 | 0: 0 / 0 | continuous_in_month ZQM25 (12 / 20) | 0 / 12 |
| 2025-07-30 | HOLD | KXFEDDECISION-25JUL (purged) | 0 / 0 / 0 | 0 | 1: 25,544 / 517,812 | 0: 0 / 0 | continuous_in_month ZQN25 (21 / 23) | 0 / 21 |
| 2025-09-17 | CUT25 | KXFEDDECISION-25SEP (purged) | 0 / 0 / 0 | 0 | 1: 25,520 / 517,941 | 0: 0 / 0 | continuous_in_month ZQU25 (11 / 21) | 0 / 11 |
| 2025-10-29 | CUT25 | KXFEDDECISION-25OCT (purged) | 0 / 0 / 0 | 0 | 1: 25,416 / 519,747 | 0: 0 / 0 | continuous_in_month ZQV25 (20 / 23) | 0 / 20 |
| 2025-12-10 | CUT25 | KXFEDDECISION-25DEC (purged) | 0 / 0 / 0 | 0 | 1: 25,327 / 515,948 | 2: 8,246 / 316,134 | continuous_in_month ZQZ25 (7 / 22) | 0 / 7 |
| 2026-01-28 | HOLD | KXFEDDECISION-26JAN (purged) | 0 / 0 / 0 | 0 | 1: 25,495 / 517,622 | 1: 1,679 / 100,909 | continuous_in_month ZQF26 (17 / 20) | 0 / 17 |
| 2026-03-18 | HOLD | KXFEDDECISION-26MAR (purged) | 0 / 0 / 0 | 0 | 1: 26,747 / 517,476 | 3: 19,452 / 661,372 | continuous_in_month ZQH26 (12 / 22) | 0 / 12 |
| 2026-04-29 | HOLD | KXFEDDECISION-26APR (purged) | 0 / 0 / 0 | 0 | 1: 29,157 / 516,112 | 1: 17,350 / 571,212 | continuous_in_month ZQJ26 (19 / 21) | 0 / 19 |
| 2026-06-17 | HOLD | KXFEDDECISION-26JUN (purged) | 0 / 0 / 0 | 0 | 1: 41,065 / 645,625 | 1: 16,942 / 581,604 | continuous_in_month ZQM26 (12 / 21) | 0 / 12 |
| 2026-07-29 | HOLD | KXFEDDECISION-26JUL (finalized) | 1,427 / 25,698 / 62,879 | 38,969 | 1: 28,154 / 644,779 | 1: 15,102 / 528,476 | continuous_in_month ZQN26 (19 / 22) | 19 / 19 |
| 2026-09-16 | — | KXFEDDECISION-26SEP (finalized) | 1,607 / 27,985 / 69,721 | 102,502 | 2: 32,860 / 813,492 | 1: 18,983 / 581,178 | contract ZQU26 (1,245 / 1,246) | 32 / 32 |
| 2026-10-28 | — | KXFEDDECISION-26OCT (active) | 1,526 / 24,234 / 22,405 | 7,636 | 1: 21,780 / 647,051 | 1: 14,754 / 563,531 | contract ZQV26 (1,226 / 1,226) | 0 / 0 |
| 2026-12-09 | — | KXFEDDECISION-26DEC (active) | 1,552 / 23,869 / 25,915 | 2,297 | 1: 11,782 / 647,060 | 1: 3,073 / 183,640 | contract ZQZ26 (1,183 / 1,183) | 0 / 0 |
| 2027-01-27 | — | KXFEDDECISION-27JAN (active) | 1,509 / 24,411 / 20,288 | 1,304 | 1: 11,779 / 647,047 | 0: 0 / 0 | contract ZQF27 (1,163 / 1,163) | 0 / 0 |
| 2027-03-17 | — | KXFEDDECISION-27MAR (active) | 1,495 / 23,857 / 7,705 | 175 | 0: 0 / 0 | 0: 0 / 0 | contract ZQH27 (1,121 / 1,121) | 0 / 0 |
| 2027-04-28 | — | KXFEDDECISION-27APR (active) | 1,424 / 20,191 / 11,304 | 335 | 0: 0 / 0 | 0: 0 / 0 | contract ZQJ27 (1,101 / 1,101) | 0 / 0 |
| 2027-06-09 | — | KXFEDDECISION-27JUN (active) | 1,357 / 19,411 / 12,628 | 318 | 0: 0 / 0 | 0: 0 / 0 | contract ZQM27 (1,058 / 1,058) | 0 / 0 |
| 2027-07-28 | — | KXFEDDECISION-27JUL (active) | 1,375 / 19,403 / 11,783 | 319 | 0: 0 / 0 | 0: 0 / 0 | contract ZQN27 (1,039 / 1,039) | 0 / 0 |
| 2027-09-15 | — | KXFEDDECISION-27SEP (active) | 1,350 / 19,312 / 14,429 | 153 | 0: 0 / 0 | 0: 0 / 0 | contract ZQU27 (995 / 995) | 0 / 0 |
| 2027-10-27 | — | KXFEDDECISION-27OCT (active) | 1,380 / 19,694 / 15,263 | 340 | 0: 0 / 0 | 0: 0 / 0 | contract ZQV27 (973 / 973) | 0 / 0 |
| 2027-12-08 | — | KXFEDDECISION-27DEC (active) | 1,355 / 20,155 / 9,189 | 552 | 0: 0 / 0 | 0: 0 / 0 | contract ZQZ27 (931 / 931) | 0 / 0 |
| 2028-01-26 | — | KXFEDDECISION-28JAN (active) | 1,426 / 20,201 / 8,565 | 104 | 0: 0 / 0 | 0: 0 / 0 | continuous_in_month ZQF28 (0 / 0) | 0 / 0 |

## Integrity tests

Full results in [`TESTS.md`](TESTS.md). T2a/T2b are run exactly as specified (every meeting x date where all legs are quoted) and fail on
far-dated / thin ladders; `TESTS.md` shows the same sums restricted to the inter-meeting window, where the Kalshi ladders (and the
negRisk-era Polymarket events) are all inside [0.95, 1.10].

| result | test |
|---|---|
| PASS | T1 ZQ=F month-end settle == 100 - mean(calendar-day ffill EFFR) within 0.30bp, 2024-01..2026-08 |
| **FAIL** | T2a Kalshi: 0.95 <= raw sum of 5 leg mids <= 1.10 on every meeting x date where all 5 legs are quoted |
| **FAIL** | T2b Polymarket: 0.95 <= raw sum of canonical-leg YES prices <= 1.10 on every event x date with all canonical legs quoted |
| PASS | T3 ts strictly increasing per market x interval, no duplicate (market, ts) |
| PASS | T4a Kalshi KXFEDDECISION-26SEP-H25: 1440 mid on 2026-09-15 in [0.86,0.88] and result = yes |
| PASS | T4b Polymarket 'Fed Decision in September?' HIKE25 YES: last hourly price on 2026-09-16 >= 0.90 |
| PASS | T5 v_basis_daily 2026-09-11 (Sep-2026 meeting): Kalshi basis in [+2.5,+3.5]bp (independent reference +2.97bp) |
| PASS | T6 row counts equal the raw parquet inputs |
| PASS | T7 Kalshi settled outcome == FRED DFEDTARU realized outcome wherever both exist |

## Scripts

* `scripts/build_db.py` — this loader (`python3 scripts/build_db.py [--db PATH] [--skip-tests]`).
* `scripts/kalshi_pull.py`, `scripts/fetch_cme_yf.py`, `scripts/fetch_fred.py`, `scripts/build_meetings.py` — collectors that wrote `raw/`.
  The Polymarket collector scripts are referenced from `raw/poly/manifest.json -> scripts`.

## First backtest result (2026-09-17): NO-GO on the CME lead-lag trade

Sample: 24 meetings (2023-02 .. 2026-09), 350 clean pre-decision days of Polymarket-vs-CME basis
(`v_basis_daily WHERE poly_ok AND pre_decision`), 23 meetings / 282 days after dropping
uninformative days (delta <= 0.05, i.e. meetings at the very end of their month).

**The basis is normally ~0.** mean +0.13bp, median +0.06bp, sd 0.73bp. Only 14.5% of days exceed
1bp (= $42 per ZQ contract) and 5.0% exceed 2bp. Round-trip frictions (Kalshi/Poly fees + spread on
the ~486 contracts that hedge one ZQ, plus ZQ spread) are roughly 1.0-1.2bp, so fewer than ~7% of
days clear the hurdle at all.

**The directional signal does not replicate.** Testing whether today's basis predicts tomorrow's
move in the prediction-market probability:

| sample | n pairs | corr(basis, next-day dP) | basis > +1bp: next-day dP | win rate |
| --- | --- | --- | --- | --- |
| all meetings | 113 | **+0.139** | +2.88pp (n=26) | 65.4% |
| **excluding 2026-09** | 82 | **-0.118** | **-1.07pp** (n=9) | **44.4%** |

The entire effect is the September 2026 meeting, which is the most dislocated meeting in the sample
(mean basis +1.23bp vs a per-meeting mean of about +/-0.2bp elsewhere, max 3.13bp). Remove it and the
correlation flips sign and the win rate falls below a coin flip. This is single-event luck, not an edge.

**What survives.** (1) The structural fact that prediction markets and CME fed funds futures are
normally tightly aligned on Fed policy (median basis 0.06bp = $2.50 per ZQ contract) - the venues are
NOT independently priced. (2) September 2026 as a documented outlier worth explaining on its own terms.
(3) This database, which is what made the negative result cheap to obtain.

## Correction to the NO-GO (2026-09-17, after GPT Pro's stage-1 validation)

GPT Pro independently validated the policy -> EFFR -> ZQ settlement mapping for 32 meetings
(2022-2025). Every number they reported reproduces EXACTLY from FRED daily EFFR in this database
(their manually transcribed EFFR series was compared day by day against `fred_rates`: 1,001 business
days, ZERO discrepancies, all 40 change points captured). Verified results:

- 17/17 policy changes pass through to EFFR one-for-one at 1bp publication precision; 31/32 including holds
- post-meeting mean EFFR residual is exactly zero in 27/32 meetings; MAE 0.085654bp, max 0.75bp
- normalized to a $1 / 25bp digital: hedge error MAE **0.343 cents**, max **3.00 cents** per contract

**The hedge leg works.** That was never the binding constraint.

**Their section 5 invalidates the hedge instrument used in the NO-GO above.** The ZQ friction per
Kalshi contract scales as 1/w (w = fraction of the settlement month the decision affects), because
h = N / (41.67 x 25 x w). The NO-GO used the MEETING-MONTH contract, where w is often small
(median w in the sample is around 0.4, and 90 of 282 sample days have w < 0.2, where the ZQ leg alone
costs a median 32.7 cents per Kalshi contract). Using a contract with larger w is strictly better.

Worked example, Sep-2026 meeting (the only meeting where both contract files exist):

| hedge instrument | w | ZQ friction / Kalshi contract | net edge median | days net > 0 |
| --- | --- | --- | --- | --- |
| ZQU26 (meeting month) | 0.467 | 4.53c | +0.57c | 6/10 |
| ZQV26 (next month)    | 1.000 | 2.11c | **+4.52c** | **8/10** |

The gross edge is unchanged (the two contracts agree to within 1.9c on average, which is the
cross-contract consistency check); only the friction halves. A first attempt at this used
w = 28/31 for the September decision inside the October contract, which is wrong -- the decision is
effective 2026-09-17, before October starts, so it covers the full month (w = 1.0). The error was
caught by a reverse-solve sanity check that implied a +92bp October meeting expectation.

**Status: the NO-GO is SUSPENDED, not confirmed.** It was computed with the wrong hedge instrument.
The correct test needs, for each historical meeting, the settlement price of the FOLLOWING month's
ZQ contract. Expired contracts are not available from yfinance (only the 16 currently-live contracts
exist in `cme_zq`), so this cannot be backtested with free data. Required next input: historical
per-contract ZQ settlements (CME DataMine, Barchart, or the IBKR historical futures feed).

## Corrected backtest with the right hedge instrument (2026-09-17, IBKR data)

The missing input was historical settlements for EXPIRED ZQ contracts. Pulled from IBKR
(`scripts/ibkr_zq_pull.py`, read-only API): 18 contracts / 8,777 daily bars, delivery months
2025-09 .. 2027-02. Two gotchas worth recording:

- IBKR lists ZQ on exchange **CBOT** (not ECBOT/CME) and retains expired contracts only back to
  last-trade-date 2025-09-30, so meetings before 2025-09 cannot be tested this way.
- For an expired contract `endDateTime=""` means "now" and returns ~33 stale bars. Anchoring the
  request at the contract's own last trade date returns the full 2 years (512 bars).

Method: for each meeting, hedge with the contract of the FOLLOWING delivery month (decision is
effective the next business day, so it covers that whole month: w = 1.0), subtract the next
meeting's contribution where one falls inside that month, and restrict to days after the PREVIOUS
meeting's effective date (otherwise the EFFR base does not yet reflect the last decision -- this
alone produced a spurious 26.8bp basis on one row).

Result, 9 meetings / 259 pre-decision days, net of Kalshi/Poly fees+spread and a $22 ZQ round trip:

| meeting | contract | basis median | net median | days net>0 | actual decision |
| --- | --- | --- | --- | --- | --- |
| 2025-09-17 | ZQV25 | -0.74bp | -0.49c | 39% | -25bp |
| 2025-10-29 | ZQX25 | -0.20bp | -3.50c | 19% | -25bp |
| 2025-12-10 | ZQF26 | +0.43bp | -0.73c | 46% | -25bp |
| 2026-01-28 | ZQG26 | -0.39bp | -2.76c | 21% | hold |
| 2026-03-18 | ZQJ26 | -0.22bp | -3.64c | 10% | hold |
| 2026-04-29 | ZQK26 | +0.62bp | -2.66c | 15% | hold |
| 2026-06-17 | ZQN26 | +0.47bp | -2.16c | 19% | hold |
| **2026-07-29** | ZQQ26 | **+3.12bp** | **+7.69c** | **88%** | hold |
| **2026-09-16** | ZQV26 | **+2.43bp** | **+4.55c** | **84%** | +25bp |

**Only 2 of 9 meetings are profitable; the median day is -1.97c. The strategy does not clear
frictions over the available sample.** GPT Pro's contract-choice fix is correct and material (it is
the difference between +0.57c and +4.52c of net median edge on the Sep-2026 meeting) but it does not
rescue the strategy.

The two profitable meetings are the two most recent, and the split is sharp: basis median +2.68bp
and 86% of days positive from 2026-07 onward, versus +0.05bp and 24% before. Both boring
explanations were tested. Polymarket liquidity is lower in the recent pair ($144M/$208M vs a $260M
earlier median) but comparable to 2026-06 ($165M), and staleness is zero for every meeting. The leg
structure did change (4 canonical legs with an "any hike" top bucket before 2026-06, 5 legs after),
but 2026-06-17 has the NEW structure and is still unprofitable, so structure alone does not create
the edge -- and in the earlier cutting regime the missing HIKE25 leg carried ~zero probability.

**Verdict: no tradable edge on the sample; a suggestive regime shift with n=2.** The reading that
fits is that prediction markets lag CME on hawkish repricing, which would only show up once the
cutting cycle ended. The next FOMC (2026-10-28) is a genuine out-of-sample test of exactly that.
