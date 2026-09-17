# Integrity tests — fed.duckdb

Built 2026-09-17T06:23:28Z by `scripts/build_db.py`.

| # | Test | Result |
|---|------|--------|
| 1 | T1 ZQ=F month-end settle == 100 - mean(calendar-day ffill EFFR) within 0.30bp, 2024-01..2026-08 | PASS |
| 2 | T2a Kalshi: 0.95 <= raw sum of 5 leg mids <= 1.10 on every meeting x date where all 5 legs are quoted | **FAIL** |
| 3 | T2b Polymarket: 0.95 <= raw sum of canonical-leg YES prices <= 1.10 on every event x date with all canonical legs quoted | **FAIL** |
| 4 | T3 ts strictly increasing per market x interval, no duplicate (market, ts) | PASS |
| 5 | T4a Kalshi KXFEDDECISION-26SEP-H25: 1440 mid on 2026-09-15 in [0.86,0.88] and result = yes | PASS |
| 6 | T4b Polymarket 'Fed Decision in September?' HIKE25 YES: last hourly price on 2026-09-16 >= 0.90 | PASS |
| 7 | T5 v_basis_daily 2026-09-11 (Sep-2026 meeting): Kalshi basis in [+2.5,+3.5]bp (independent reference +2.97bp) | PASS |
| 8 | T6 row counts equal the raw parquet inputs | PASS |
| 9 | T7 Kalshi settled outcome == FRED DFEDTARU realized outcome wherever both exist | PASS |

## Details

### T1 ZQ=F month-end settle == 100 - mean(calendar-day ffill EFFR) within 0.30bp, 2024-01..2026-08

**PASS** — 32 months checked (expected 32); max |diff| = 0.250bp in 2024-03 (settle 94.6675 vs implied 94.6700); 0 months exceed 0.30bp

| month | days | mean EFFR | implied settle | ZQ=F last settle | last trade day | diff bp |
|---|---|---|---|---|---|---|
| 2024-01 | 31 | 5.3300 | 94.6700 | 94.6700 | 2024-01-31 | -0.00 |
| 2024-02 | 29 | 5.3300 | 94.6700 | 94.6700 | 2024-02-29 | -0.00 |
| 2024-03 | 31 | 5.3300 | 94.6700 | 94.6675 | 2024-03-28 | -0.25 |
| 2024-04 | 30 | 5.3300 | 94.6700 | 94.6700 | 2024-04-30 | -0.00 |
| 2024-05 | 31 | 5.3300 | 94.6700 | 94.6700 | 2024-05-31 | -0.00 |
| 2024-06 | 30 | 5.3300 | 94.6700 | 94.6675 | 2024-06-28 | -0.25 |
| 2024-07 | 31 | 5.3300 | 94.6700 | 94.6700 | 2024-07-31 | -0.00 |
| 2024-08 | 31 | 5.3300 | 94.6700 | 94.6700 | 2024-08-30 | -0.00 |
| 2024-09 | 30 | 5.1300 | 94.8700 | 94.8700 | 2024-09-30 | +0.00 |
| 2024-10 | 31 | 4.8300 | 95.1700 | 95.1700 | 2024-10-31 | -0.00 |
| 2024-11 | 30 | 4.6383 | 95.3617 | 95.3600 | 2024-11-29 | -0.17 |
| 2024-12 | 31 | 4.4752 | 95.5248 | 95.5250 | 2024-12-31 | +0.02 |
| 2025-01 | 31 | 4.3300 | 95.6700 | 95.6725 | 2025-01-31 | +0.25 |
| 2025-02 | 28 | 4.3300 | 95.6700 | 95.6700 | 2025-02-28 | -0.00 |
| 2025-03 | 31 | 4.3300 | 95.6700 | 95.6675 | 2025-03-31 | -0.25 |
| 2025-04 | 30 | 4.3300 | 95.6700 | 95.6700 | 2025-04-30 | -0.00 |
| 2025-05 | 31 | 4.3300 | 95.6700 | 95.6700 | 2025-05-30 | -0.00 |
| 2025-06 | 30 | 4.3300 | 95.6700 | 95.6700 | 2025-06-30 | -0.00 |
| 2025-07 | 31 | 4.3300 | 95.6700 | 95.6700 | 2025-07-31 | -0.00 |
| 2025-08 | 31 | 4.3300 | 95.6700 | 95.6700 | 2025-08-29 | -0.00 |
| 2025-09 | 30 | 4.2247 | 95.7753 | 95.7750 | 2025-09-30 | -0.03 |
| 2025-10 | 31 | 4.0881 | 95.9119 | 95.9125 | 2025-10-31 | +0.06 |
| 2025-11 | 30 | 3.8763 | 96.1237 | 96.1225 | 2025-11-28 | -0.12 |
| 2025-12 | 31 | 3.7206 | 96.2794 | 96.2775 | 2025-12-31 | -0.19 |
| 2026-01 | 31 | 3.6400 | 96.3600 | 96.3600 | 2026-01-30 | +0.00 |
| 2026-02 | 28 | 3.6400 | 96.3600 | 96.3600 | 2026-02-27 | +0.00 |
| 2026-03 | 31 | 3.6400 | 96.3600 | 96.3575 | 2026-03-31 | -0.25 |
| 2026-04 | 30 | 3.6400 | 96.3600 | 96.3600 | 2026-04-30 | +0.00 |
| 2026-05 | 31 | 3.6277 | 96.3723 | 96.3700 | 2026-05-29 | -0.23 |
| 2026-06 | 30 | 3.6253 | 96.3747 | 96.3750 | 2026-06-30 | +0.03 |
| 2026-07 | 31 | 3.6281 | 96.3719 | 96.3725 | 2026-07-31 | +0.06 |
| 2026-08 | 31 | 3.6300 | 96.3700 | 96.3700 | 2026-08-31 | +0.00 |

### T2a Kalshi: 0.95 <= raw sum of 5 leg mids <= 1.10 on every meeting x date where all 5 legs are quoted

**FAIL** — quoted = daily bar present and non-empty book (not bid=0/ask>=0.99) on all 5 legs. All dates: 3070 eligible meeting-days, 526 outside [0.95,1.10] (min 0.7, median 1.005, max 1.8). Inter-meeting window (where the basis is defined): 87 eligible, 0 outside (range 0.995..1.035). within 60 days of the decision: 137 eligible / 0 outside; within 120 days of the decision: 296 eligible / 4 outside; all 5 spreads <= 0.05: 406 eligible / 36 outside; all 5 legs two-sided (bid > 0 and ask < 1): 1396 eligible / 260 outside. Daily close bid/ask are not a synchronous snapshot: 58 eligible days have sum(bid) > 1 and 64 have sum(ask) < 1. Violations: nearest is 92 days before its decision (median 368 days), 522 of 526 are more than 120 days out, 266 of 526 have at least one leg with no bid (wide one-sided book).

| scope | eligible days | outside [0.95,1.10] | min | median | max |
|---|---|---|---|---|---|
| all dates (the test as specified) | 3070 | 526 | 0.7 | 1.005 | 1.8 |
| inter-meeting window (date > previous meeting's effective date) | 87 | 0 | 0.995 | 1.015 | 1.035 |
| within 60 days of the decision | 137 | 0 | 0.985 | 1.015 | 1.05 |
| within 120 days of the decision | 296 | 4 | 0.9 | 1.015 | 1.095 |
| all 5 spreads <= 0.05 | 406 | 36 | 0.79 | 1.015 | 1.09 |
| all 5 legs two-sided (bid > 0 and ask < 1) | 1396 | 260 | 0.785 | 1.0 | 1.67 |

| Kalshi event | eligible days | outside | min | max | eligible in window | outside in window | min (window) | max (window) |
|---|---|---|---|---|---|---|---|---|
| KXFEDDECISION-26DEC | 245 | 17 | 0.88 | 1.245 | 0 | 0 | None | None |
| KXFEDDECISION-26JUL | 260 | 22 | 0.91 | 1.395 | 40 | 0 | 1.0 | 1.035 |
| KXFEDDECISION-26OCT | 243 | 24 | 0.805 | 1.62 | 0 | 0 | None | None |
| KXFEDDECISION-26SEP | 270 | 22 | 0.955 | 1.57 | 47 | 0 | 0.995 | 1.035 |
| KXFEDDECISION-27APR | 219 | 50 | 0.79 | 1.58 | 0 | 0 | None | None |
| KXFEDDECISION-27DEC | 210 | 56 | 0.7 | 1.685 | 0 | 0 | None | None |
| KXFEDDECISION-27JAN | 250 | 54 | 0.865 | 1.775 | 0 | 0 | None | None |
| KXFEDDECISION-27JUL | 238 | 37 | 0.805 | 1.57 | 0 | 0 | None | None |
| KXFEDDECISION-27JUN | 230 | 57 | 0.91 | 1.76 | 0 | 0 | None | None |
| KXFEDDECISION-27MAR | 238 | 45 | 0.855 | 1.64 | 0 | 0 | None | None |
| KXFEDDECISION-27OCT | 226 | 48 | 0.715 | 1.725 | 0 | 0 | None | None |
| KXFEDDECISION-27SEP | 222 | 55 | 0.83 | 1.625 | 0 | 0 | None | None |
| KXFEDDECISION-28JAN | 219 | 39 | 0.885 | 1.8 | 0 | 0 | None | None |

### T2b Polymarket: 0.95 <= raw sum of canonical-leg YES prices <= 1.10 on every event x date with all canonical legs quoted

**FAIL** — quoted = every canonical leg has an hourly observation that ET day and none is the 0.5 empty-book placeholder (date <= FOMC date). All dates: 2900 eligible event-days, 112 outside [0.95,1.10] (min 0.502, median 1.004, max 2.12). Primary events in the inter-meeting window: 1307 eligible, 65 outside (range 0.502..1.62). primary events only: 2887 eligible / 102 outside; primary events, inter-meeting window, meetings from 2024-06 (negRisk-era books): 848 eligible / 0 outside; primary events, within 30 days of the decision: 918 eligible / 30 outside. Violations are thin books: 2023/early-2024 events (wide-quote midpoints), launch days of new events months before the meeting, and the low-volume Sep-2026 mirror event fed-decision-in-september-568.

| scope | eligible days | outside [0.95,1.10] | min | median | max |
|---|---|---|---|---|---|
| all decision events, all dates <= FOMC date (the test as specified) | 2900 | 112 | 0.502 | 1.004 | 2.12 |
| primary events only | 2887 | 102 | 0.502 | 1.004 | 2.12 |
| primary events, inter-meeting window | 1307 | 65 | 0.502 | 1.003 | 1.62 |
| primary events, inter-meeting window, meetings from 2024-06 (negRisk-era books) | 848 | 0 | 0.955 | 1.002 | 1.028 |
| primary events, within 30 days of the decision | 918 | 30 | 0.502 | 1.002 | 1.62 |

| Poly event | primary | canonical legs | eligible days | outside | min | median | max | eligible in window | outside in window |
|---|---|---|---|---|---|---|---|---|---|
| fed-interest-rates-february-2023 | True | 4 | 43 | 13 | 1.01 | 1.06 | 1.62 | 43 | 13 |
| fed-interest-rates-march-2023 | True | 4 | 36 | 1 | 1.0 | 1.02 | 1.11 | 36 | 1 |
| fed-interest-rates-may-2023 | True | 4 | 40 | 4 | 0.99 | 1.04 | 1.23 | 39 | 4 |
| fed-interest-rates-june-2023 | True | 4 | 42 | 3 | 0.9 | 1.018 | 1.405 | 41 | 3 |
| fed-interest-rates-july-2023 | True | 3 | 44 | 1 | 0.975 | 1.01 | 1.27 | 41 | 0 |
| fed-interest-rates-september-2023 | True | 2 | 58 | 2 | 0.54 | 1.0 | 1.02 | 55 | 1 |
| fed-interest-rates-november-2023 | True | 2 | 44 | 0 | 0.97 | 1.0 | 1.075 | 41 | 0 |
| fed-interest-rates-december-2023 | True | 2 | 46 | 3 | 0.55 | 1.0 | 1.446 | 41 | 3 |
| fed-interest-rates-january-2024 | True | 3 | 52 | 37 | 0.772 | 0.813 | 1.255 | 45 | 34 |
| fed-interest-rates-march-2024 | True | 4 | 50 | 7 | 0.999 | 1.027 | 2.02 | 48 | 5 |
| fed-interest-rates-may-2024 | True | 4 | 29 | 1 | 0.502 | 1.009 | 1.019 | 29 | 1 |
| fed-interest-rates-june-2024 | True | 4 | 43 | 0 | 0.998 | 1.005 | 1.025 | 41 | 0 |
| fed-interest-rates-july-2024 | True | 4 | 50 | 0 | 0.993 | 1.007 | 1.016 | 48 | 0 |
| fed-interest-rates-september-2024 | True | 4 | 56 | 0 | 0.983 | 1.006 | 1.023 | 48 | 0 |
| fed-interest-rates-november-2024 | True | 5 | 98 | 2 | 0.848 | 1.003 | 1.039 | 49 | 0 |
| fed-interest-rates-december-2024 | True | 5 | 135 | 5 | 0.936 | 1.001 | 1.188 | 40 | 0 |
| fed-interest-rates-january-2025 | True | 5 | 84 | 0 | 0.966 | 1.003 | 1.048 | 41 | 0 |
| fed-decision-in-march | True | 4 | 92 | 0 | 0.962 | 1.002 | 1.02 | 48 | 0 |
| fed-decision-in-may-2025 | True | 4 | 105 | 0 | 0.965 | 1.003 | 1.02 | 48 | 0 |
| fed-decision-in-june | True | 4 | 129 | 2 | 0.949 | 1.002 | 1.48 | 41 | 0 |
| fed-decision-in-july | True | 4 | 133 | 1 | 0.933 | 1.003 | 1.014 | 40 | 0 |
| fed-decision-in-september | True | 4 | 129 | 1 | 0.955 | 1.003 | 1.127 | 48 | 0 |
| fed-decision-in-october | True | 4 | 130 | 0 | 0.953 | 1.001 | 1.019 | 41 | 0 |
| fed-decision-in-december | True | 4 | 133 | 2 | 0.923 | 1.005 | 1.125 | 41 | 0 |
| fed-decision-in-january | True | 4 | 133 | 3 | 0.935 | 1.001 | 1.168 | 48 | 0 |
| fed-decision-in-march-885 | True | 4 | 141 | 1 | 0.98 | 1.003 | 1.15 | 48 | 0 |
| fed-decision-in-april | True | 4 | 169 | 2 | 0.944 | 1.001 | 1.23 | 41 | 0 |
| fed-decision-in-june-825 | True | 5 | 188 | 1 | 0.963 | 1.007 | 1.355 | 48 | 0 |
| fed-decision-in-july-181 | True | 5 | 133 | 1 | 0.982 | 1.007 | 1.465 | 41 | 0 |
| fed-decision-in-september-568 | False | 5 | 13 | 10 | 1.039 | 1.14 | 1.236 | 0 | 0 |
| fed-decision-in-september-762 | True | 5 | 127 | 1 | 0.953 | 1.011 | 1.194 | 48 | 0 |
| fed-decision-in-october-20260617190323537 | True | 5 | 93 | 6 | 0.929 | 1.013 | 2.12 | 0 | 0 |
| fed-decision-in-january-20260729233815502 | True | 5 | 51 | 1 | 0.993 | 1.034 | 1.555 | 0 | 0 |
| fed-decision-in-december-20260729232808632 | True | 5 | 51 | 1 | 0.969 | 1.026 | 1.375 | 0 | 0 |

### T3 ts strictly increasing per market x interval, no duplicate (market, ts)

**PASS** — kalshi_candles (market_id, interval, ts): 0 violation(s); kalshi_candles monotonic: 0 violation(s); poly_prices (token_id, fidelity, ts): 0 violation(s); poly_prices monotonic: 0 violation(s); kalshi_trades trade_id unique: 0 violation(s); cme_zq (contract, date): 0 violation(s); fred_long (series_id, date): 0 violation(s); fred_rates date unique: 0 violation(s); kalshi_daily_quotes (market_id, trade_date_et): 0 violation(s); poly_daily_yes (market_id, date_et): 0 violation(s)

- kalshi_candles (market_id, interval, ts): 0
- kalshi_candles monotonic: 0
- poly_prices (token_id, fidelity, ts): 0
- poly_prices monotonic: 0
- kalshi_trades trade_id unique: 0
- cme_zq (contract, date): 0
- fred_long (series_id, date): 0
- fred_rates date unique: 0
- kalshi_daily_quotes (market_id, trade_date_et): 0
- poly_daily_yes (market_id, date_et): 0

### T4a Kalshi KXFEDDECISION-26SEP-H25: 1440 mid on 2026-09-15 in [0.86,0.88] and result = yes

**PASS** — mid=0.875 (bid 0.87 / ask 0.88), result='yes'

### T4b Polymarket 'Fed Decision in September?' HIKE25 YES: last hourly price on 2026-09-16 >= 0.90

**PASS** — event=fed-decision-in-september-762 (primary=True), last hourly p=0.9995 at 2026-09-16 21:00:29Z

### T5 v_basis_daily 2026-09-11 (Sep-2026 meeting): Kalshi basis in [+2.5,+3.5]bp (independent reference +2.97bp)

**PASS** — basis_kalshi_bp=+2.965 (ZQU26 settle 96.2675, Kalshi-ladder fair 96.2972, EFFR_pre 3.63, delta 0.4667, Kalshi E[move]=15.61bp, P(hike25)=0.610 as of 00:00 ET 09-11); same-day variant +1.144bp; Poly basis +2.953bp

### T6 row counts equal the raw parquet inputs

**PASS** — meetings=49; kalshi_markets=147; kalshi_candles=624766; kalshi_trades=155004; poly_markets=249; poly_prices=23943238; cme_zq=23955; fred_long=50263

### T7 Kalshi settled outcome == FRED DFEDTARU realized outcome wherever both exist

**PASS** — 0 mismatch(es)

## Verdict

7 passed, 2 failed.

Failing tests are data-quality statements about the venues (see the scope tables above); they do not indicate a loading error: the same sums are inside [0.95, 1.10] on every Kalshi meeting-day in the inter-meeting window where the basis is defined, and the Polymarket violations are thin 2023/early-2024 books, launch days months before a meeting and the Sep-2026 mirror event. The views expose `in_window`, `days_to_decision`, `n_legs_two_sided`, `max_spread`, `sum_bid`, `sum_ask` (Kalshi) and `is_primary`, `n_legs_half`, `n_legs_obs_today` (Polymarket) so consumers can apply their own liquidity screen.
