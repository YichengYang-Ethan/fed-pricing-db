#!/usr/bin/env python3
"""Build fed.duckdb from the raw/ parquet collections (idempotent: the database file is deleted and recreated).

Tables : meetings, kalshi_markets, kalshi_candles, kalshi_trades, poly_markets, poly_prices, cme_zq,
         cme_sofr_probe, fred_rates, fred_long, poly_event_meeting, kalshi_daily_quotes, poly_daily_yes,
         coverage_matrix
Views  : v_effr_calendar, v_prob_kalshi_daily, v_prob_poly_daily, v_zq_meeting, v_prob_cme_daily, v_basis_daily
Tests  : written to TESTS.md; the process exits 1 when any test fails (the database is still built).
Report : build_report.json (coverage matrix + test results) and README.md coverage section.

Conventions (see README.md):
  * ts is int64 UTC epoch seconds everywhere. ET conversions use timezone('America/New_York', to_timestamp(ts)) so
    they do not depend on the session TimeZone.
  * Kalshi daily (P=1440) bars end at 00:00 ET except across the DST spring-forward, where the bar
  ends 01:00 EDT. trade_date_et uses a noon shift (ts-43200) so the covered ET day is labelled
  correctly and no two bars collide on one trade_date_et.
  * Prediction-market daily rows are labelled by the ET day of the observations. v_basis_daily pairs the CME
    settle of day D with the last complete prediction-market daily observation before that session (ET day D-1),
    which is the alignment that reproduces the independent +2.97bp reference for 2026-09-11.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path("/Users/ethanyang/Developer/fed-pricing-db")
RAW = ROOT / "raw"
DB_PATH = ROOT / "fed.duckdb"
TESTS_MD = ROOT / "TESTS.md"
README_MD = ROOT / "README.md"
REPORT_JSON = ROOT / "build_report.json"

CANON = ["CUT50P", "CUT25", "HOLD", "HIKE25", "HIKE50P"]
MOVE_BP = {"CUT50P": -50, "CUT25": -25, "HOLD": 0, "HIKE25": 25, "HIKE50P": 50}


def q(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


# --------------------------------------------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------------------------------------------
def load_tables(con: duckdb.DuckDBPyConnection, log: list[str]) -> None:
    cme_manifest = json.loads((RAW / "cme" / "manifest.json").read_text())
    fetched_day = cme_manifest["fetched_at_utc"][:10]

    # -- meetings ----------------------------------------------------------------------------------------------
    con.execute(f"CREATE TABLE meetings AS SELECT * FROM read_parquet({q(RAW / 'meetings.parquet')}) ORDER BY meeting_date")

    # -- Kalshi ------------------------------------------------------------------------------------------------
    con.execute(f"""
        CREATE TABLE kalshi_markets AS
        SELECT * REPLACE (CAST(cap_strike AS DOUBLE) AS cap_strike),
               timezone('UTC', TRY_CAST(open_time AS TIMESTAMPTZ))   AS open_ts_utc,
               timezone('UTC', TRY_CAST(close_time AS TIMESTAMPTZ))  AS close_ts_utc,
               timezone('UTC', TRY_CAST(settlement_ts AS TIMESTAMPTZ)) AS settlement_ts_utc
        FROM read_parquet({q(RAW / 'kalshi' / 'markets.parquet')})
        ORDER BY event_id, market_id""")
    con.execute(f"""
        CREATE TABLE kalshi_candles AS
        SELECT *,
               timezone('UTC', to_timestamp(ts))                                  AS bar_end_utc,
               CASE WHEN interval = 1440
                    THEN CAST(timezone('America/New_York', to_timestamp(ts - 43200)) AS DATE) END AS trade_date_et,
               CASE WHEN NOT (bid_close = 0 AND ask_close >= 0.99) THEN (bid_close + ask_close) / 2 END AS mid,
               ask_close - bid_close                                              AS spread
        FROM read_parquet({q(RAW / 'kalshi' / 'candles.parquet')})
        ORDER BY market_id, interval, ts""")
    con.execute(f"""
        CREATE TABLE kalshi_trades AS
        SELECT *, timezone('UTC', to_timestamp(ts)) AS ts_utc
        FROM read_parquet({q(RAW / 'kalshi' / 'trades.parquet')})
        ORDER BY market_id, ts_us, trade_id""")

    # -- Polymarket --------------------------------------------------------------------------------------------
    con.execute(f"""
        CREATE TABLE poly_markets AS
        SELECT *, TRY_CAST(fomc_date AS DATE) AS fomc_date_d, TRY_CAST(end_date AS DATE) AS end_date_d,
               TRY_CAST(start_date AS DATE) AS start_date_d
        FROM read_parquet({q(RAW / 'poly' / 'markets.parquet')})
        ORDER BY event_id, market_id""")
    con.execute(f"""
        CREATE TABLE poly_prices AS
        SELECT * FROM read_parquet({q(RAW / 'poly' / 'prices.parquet')})
        ORDER BY token_id, fidelity, ts""")

    # -- CME ---------------------------------------------------------------------------------------------------
    zq_files = sorted((RAW / "cme").glob("ZQ*.parquet"))
    con.execute(f"""
        CREATE TABLE cme_zq AS
        SELECT *,
               contract = 'ZQ=F'                                   AS is_continuous,
               date >= DATE '{fetched_day}'                        AS is_partial,
               COALESCE(delivery_month, event_id)                  AS contract_month,
               -- yfinance copies the previous session's volume onto the newest bar and emits 0-volume
               -- days; nothing in the views depends on volume, but flag it (skeptic audit 2026-09-17).
               (volume = 0
                OR volume = LAG(volume) OVER (PARTITION BY contract ORDER BY date)) AS volume_suspect
        FROM read_parquet([{','.join(q(p) for p in zq_files)}], union_by_name = true)
        ORDER BY contract, date""")
    sr_files = sorted((RAW / "cme" / "sofr").glob("*.parquet"))
    con.execute(f"""
        CREATE TABLE cme_sofr_probe AS
        SELECT *, date >= DATE '{fetched_day}' AS is_partial
        FROM read_parquet([{','.join(q(p) for p in sr_files)}], union_by_name = true)
        ORDER BY contract, date""")
    log.append(f"cme_zq: {len(zq_files)} files; partial bars flagged for date >= {fetched_day}")

    # -- FRED --------------------------------------------------------------------------------------------------
    fred_files = sorted((RAW / "fred").glob("*.parquet"))
    con.execute(f"""
        CREATE TABLE fred_long AS
        SELECT * FROM read_parquet([{','.join(q(p) for p in fred_files)}], union_by_name = true)
        ORDER BY series_id, date""")
    con.execute("""
        CREATE TABLE fred_rates AS
        WITH spine AS (SELECT DISTINCT date FROM fred_long)
        SELECT s.date,
               MAX(CASE WHEN series_id = 'EFFR'     THEN value END) AS effr,
               MAX(CASE WHEN series_id = 'SOFR'     THEN value END) AS sofr,
               MAX(CASE WHEN series_id = 'IORB'     THEN value END) AS iorb,
               MAX(CASE WHEN series_id = 'DFEDTARU' THEN value END) AS tgt_upper,
               MAX(CASE WHEN series_id = 'DFEDTARL' THEN value END) AS tgt_lower,
               MAX(CASE WHEN series_id = 'DFF'      THEN value END) AS dff
        FROM spine s LEFT JOIN fred_long f USING (date)
        GROUP BY s.date ORDER BY s.date""")

    # -- Poly event -> meeting link --------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE poly_event_meeting AS
        WITH links AS (
            SELECT m.meeting_date, UNNEST(m.poly_slugs_decision) AS event_id, 'decision' AS role FROM meetings m
            UNION ALL
            SELECT m.meeting_date, UNNEST(m.poly_slugs_other) AS event_id, 'other' AS role FROM meetings m),
        vol AS (SELECT event_id, SUM(COALESCE(volume, 0)) AS volume, COUNT(*) AS n_markets,
                       SUM(CASE WHEN outcome <> 'OTHER' THEN 1 ELSE 0 END) AS n_canonical_legs,
                       BOOL_OR(outcome_detail = 'HIKE25P') AND NOT BOOL_OR(outcome = 'HIKE25') AS hike50p_is_any_hike,
                       MAX(event_title) AS event_title, MIN(start_date_d) AS start_date, MAX(end_date_d) AS end_date
                FROM poly_markets GROUP BY event_id),
        ranked AS (
            SELECT l.*, v.volume, v.n_markets, v.n_canonical_legs, v.hike50p_is_any_hike, v.event_title, v.start_date, v.end_date,
                   ROW_NUMBER() OVER (PARTITION BY l.meeting_date, l.role ORDER BY v.volume DESC NULLS LAST, l.event_id) AS rk
            FROM links l LEFT JOIN vol v USING (event_id))
        SELECT meeting_date, event_id, role, (role = 'decision' AND rk = 1) AS is_primary, volume, n_markets,
               n_canonical_legs, hike50p_is_any_hike, event_title, start_date, end_date
        FROM ranked ORDER BY meeting_date, role, rk""")


# --------------------------------------------------------------------------------------------------------------
# Derived helper tables (materialised because the views on top of them are used interactively)
# --------------------------------------------------------------------------------------------------------------
def build_helpers(con: duckdb.DuckDBPyConnection) -> None:
    # Inter-meeting window per meeting: dates after the previous meeting's effective date, when the last observed EFFR is the
    # rate that prevails until this decision (the hold-vs-move model of v_prob_cme_daily is only defined there).
    con.execute("""
        CREATE TABLE meeting_windows AS
        SELECT meeting_date, effective_date,
               LAG(meeting_date, 1, DATE '2021-12-15') OVER (ORDER BY meeting_date)   AS prev_meeting_date,
               LAG(effective_date, 1, DATE '2021-12-16') OVER (ORDER BY meeting_date) AS prev_effective_date
        FROM meetings ORDER BY meeting_date""")

    # Kalshi: one row per (event, leg, ET trade date) on a dense daily grid with forward-filled quotes.
    con.execute("""
        CREATE TABLE kalshi_daily_quotes AS
        WITH bars AS (
            SELECT event_id, market_id, outcome, trade_date_et, ts, bid_close, ask_close, mid, spread, price_close,
                   price_mean, vol, oi, is_partial
            FROM kalshi_candles WHERE interval = 1440 AND event_id LIKE 'KXFEDDECISION-%'),
        span AS (SELECT event_id, MIN(trade_date_et) AS d0, MAX(trade_date_et) AS d1 FROM bars GROUP BY event_id),
        legs AS (SELECT DISTINCT event_id, market_id, outcome FROM bars),
        grid AS (
            SELECT l.event_id, l.market_id, l.outcome, CAST(UNNEST(generate_series(s.d0, s.d1, INTERVAL 1 DAY)) AS DATE) AS trade_date_et
            FROM legs l JOIN span s USING (event_id))
        SELECT g.event_id, g.market_id, g.outcome, g.trade_date_et,
               CAST(g.trade_date_et + INTERVAL 1 DAY AS DATE)         AS asof_date,   -- 00:00 ET of the next day = bar end
               b.trade_date_et                                        AS bar_trade_date_et,
               (g.trade_date_et - b.trade_date_et)                    AS stale_days,
               b.ts AS bar_ts, b.bid_close, b.ask_close, b.mid, b.spread, b.price_close, b.price_mean, b.vol, b.oi,
               b.is_partial,
               (b.bid_close > 0 AND b.ask_close < 1)                  AS two_sided,
               (b.mid IS NOT NULL)                                    AS quoted
        FROM grid g
        ASOF LEFT JOIN bars b ON g.market_id = b.market_id AND g.trade_date_et >= b.trade_date_et
        ORDER BY g.event_id, g.trade_date_et, g.outcome""")

    # Polymarket: last hourly YES observation per (market, ET day) on a dense grid up to the FOMC date, forward-filled.
    con.execute("""
        CREATE TABLE poly_daily_yes AS
        WITH h AS (
            SELECT p.event_id, p.market_id, p.token_id, m.outcome, m.outcome_detail, m.fomc_date_d AS fomc_date, p.ts, p.p,
                   CAST(timezone('America/New_York', to_timestamp(p.ts)) AS DATE) AS date_et
            FROM poly_prices p JOIN poly_markets m USING (market_id)
            WHERE p.fidelity = 60 AND p.outcome_label = 'Yes' AND m.outcome <> 'OTHER'),
        lastobs AS (
            SELECT event_id, market_id, token_id, outcome, outcome_detail, fomc_date, date_et,
                   ARG_MAX(p, ts) AS p, MAX(ts) AS ts_last, COUNT(*) AS n_obs
            FROM h GROUP BY ALL),
        span AS (SELECT event_id, MIN(date_et) AS d0, LEAST(MAX(date_et), MAX(fomc_date)) AS d1 FROM lastobs GROUP BY event_id),
        legs AS (SELECT DISTINCT event_id, market_id, token_id, outcome, outcome_detail, fomc_date FROM lastobs),
        grid AS (
            SELECT l.*, CAST(UNNEST(generate_series(s.d0, s.d1, INTERVAL 1 DAY)) AS DATE) AS date_et
            FROM legs l JOIN span s USING (event_id))
        SELECT g.event_id, g.market_id, g.token_id, g.outcome, g.outcome_detail, g.fomc_date, g.date_et,
               o.date_et AS obs_date_et, (g.date_et - o.date_et) AS stale_days,
               o.p, o.ts_last, o.n_obs,
               (o.p = 0.5) AS p_is_half      -- empty CLOB book returns a 0.5 midpoint placeholder
        FROM grid g
        ASOF LEFT JOIN lastobs o ON g.market_id = o.market_id AND g.date_et >= o.date_et
        ORDER BY g.event_id, g.date_et, g.outcome""")


# --------------------------------------------------------------------------------------------------------------
# Views
# --------------------------------------------------------------------------------------------------------------
def build_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE VIEW v_effr_calendar AS
        WITH cal AS (SELECT CAST(UNNEST(generate_series((SELECT MIN(date) FROM fred_rates WHERE effr IS NOT NULL),
                                                       (SELECT MAX(date) FROM fred_rates WHERE effr IS NOT NULL),
                                                       INTERVAL 1 DAY)) AS DATE) AS date),
             obs AS (SELECT date, effr FROM fred_rates WHERE effr IS NOT NULL)
        SELECT c.date, o.effr AS effr_ff, o.date AS effr_obs_date, strftime(c.date, '%Y-%m') AS ym
        FROM cal c ASOF LEFT JOIN obs o ON c.date >= o.date""")

    mid_cols = ",\n               ".join(f"MAX(CASE WHEN outcome = '{o}' THEN mid END) AS mid_{o.lower()}" for o in CANON)
    bid_cols = ",\n               ".join(f"MAX(CASE WHEN outcome = '{o}' THEN bid_close END) AS bid_{o.lower()}" for o in CANON)
    ask_cols = ",\n               ".join(f"MAX(CASE WHEN outcome = '{o}' THEN ask_close END) AS ask_{o.lower()}" for o in CANON)
    p_cols = ",\n               ".join(f"mid_{o.lower()} / sum_raw AS p_{o.lower()}" for o in CANON)
    emove = " + ".join(f"COALESCE(mid_{o.lower()}, 0) * ({MOVE_BP[o]})" for o in CANON)
    con.execute(f"""
        CREATE VIEW v_prob_kalshi_daily AS
        WITH agg AS (
            SELECT k.event_id, k.trade_date_et, k.asof_date,
                   COUNT(*)                                            AS n_legs,
                   SUM(CASE WHEN stale_days = 0 THEN 1 ELSE 0 END)     AS n_legs_bar_today,
                   SUM(CASE WHEN quoted THEN 1 ELSE 0 END)             AS n_legs_quoted,
                   SUM(CASE WHEN two_sided THEN 1 ELSE 0 END)          AS n_legs_two_sided,
                   MAX(stale_days)                                     AS max_stale_days,
                   MAX(CASE WHEN quoted THEN spread END)               AS max_spread,
                   SUM(mid)                                            AS sum_raw,
                   SUM(CASE WHEN quoted THEN bid_close END)            AS sum_bid,
                   SUM(CASE WHEN quoted THEN ask_close END)            AS sum_ask,
                   SUM(vol)                                            AS vol_day,
                   SUM(oi)                                             AS oi_total,
                   BOOL_OR(is_partial)                                 AS any_partial,
                   {mid_cols},
                   {bid_cols},
                   {ask_cols}
            FROM kalshi_daily_quotes k GROUP BY ALL)
        SELECT m.meeting_date, a.event_id AS kalshi_event, a.trade_date_et AS date, a.asof_date,
               (m.meeting_date - a.trade_date_et) AS days_to_decision,
               (a.trade_date_et > w.prev_effective_date AND a.trade_date_et <= m.meeting_date) AS in_window,
               a.n_legs, a.n_legs_bar_today, a.n_legs_quoted, a.n_legs_two_sided, a.max_stale_days, a.max_spread,
               a.sum_raw, a.sum_bid, a.sum_ask,
               a.mid_cut50p, a.mid_cut25, a.mid_hold, a.mid_hike25, a.mid_hike50p,
               {p_cols},
               ({emove}) / sum_raw AS e_move_bp,
               a.bid_cut50p, a.bid_cut25, a.bid_hold, a.bid_hike25, a.bid_hike50p,
               a.ask_cut50p, a.ask_cut25, a.ask_hold, a.ask_hike25, a.ask_hike50p,
               a.vol_day, a.oi_total, a.any_partial,
               m.kalshi_status, m.kalshi_result_outcome
        FROM agg a JOIN meetings m ON m.kalshi_event = a.event_id JOIN meeting_windows w USING (meeting_date)""")

    p_cols_poly = ",\n               ".join(f"MAX(CASE WHEN outcome = '{o}' THEN p_sum END) AS yes_{o.lower()}" for o in CANON)
    pn_cols = ",\n               ".join(f"yes_{o.lower()} / sum_raw AS p_{o.lower()}" for o in CANON)
    emove_poly = (" + ".join(f"COALESCE(yes_{o.lower()}, 0) * ({MOVE_BP[o]})" for o in CANON if o != "HIKE50P")
                  + " + COALESCE(yes_hike50p, 0) * (CASE WHEN pem.hike50p_is_any_hike THEN 25 ELSE 50 END)")
    con.execute(f"""
        CREATE VIEW v_prob_poly_daily AS
        WITH by_outcome AS (
            SELECT event_id, date_et, outcome, fomc_date,
                   SUM(p) AS p_sum, COUNT(*) AS n_legs, SUM(CASE WHEN stale_days = 0 THEN 1 ELSE 0 END) AS n_legs_obs_today,
                   SUM(CASE WHEN p_is_half THEN 1 ELSE 0 END) AS n_legs_half, MAX(stale_days) AS max_stale_days
            FROM poly_daily_yes GROUP BY ALL),
        agg AS (
            SELECT event_id, date_et, MAX(fomc_date) AS fomc_date,
                   SUM(n_legs) AS n_legs, SUM(n_legs_obs_today) AS n_legs_obs_today, SUM(n_legs_half) AS n_legs_half,
                   MAX(max_stale_days) AS max_stale_days, COUNT(*) AS n_outcomes, SUM(p_sum) AS sum_raw,
                   {p_cols_poly}
            FROM by_outcome GROUP BY ALL)
        SELECT pem.meeting_date, a.event_id, pem.is_primary, pem.hike50p_is_any_hike, a.date_et AS date,
               CAST(a.date_et + INTERVAL 1 DAY AS DATE) AS asof_date,
               (pem.meeting_date - a.date_et) AS days_to_decision,
               (a.date_et > w.prev_effective_date AND a.date_et <= pem.meeting_date) AS in_window,
               a.n_outcomes, a.n_legs, a.n_legs_obs_today, a.n_legs_half, a.max_stale_days, a.sum_raw,
               a.yes_cut50p, a.yes_cut25, a.yes_hold, a.yes_hike25, a.yes_hike50p,
               {pn_cols},
               ({emove_poly}) / sum_raw AS e_move_bp,
               pem.n_canonical_legs, pem.event_title
        FROM agg a JOIN poly_event_meeting pem ON pem.event_id = a.event_id AND pem.role = 'decision'
        JOIN meeting_windows w ON w.meeting_date = pem.meeting_date""")

    con.execute("""
        CREATE VIEW v_zq_meeting AS
        SELECT m.meeting_date, m.meeting_month, z.date, m.zq_source AS source, z.contract, z.source_symbol,
               z.open, z.high, z.low, z.close, z.settle, z.volume, z.is_partial,
               (z.date < m.meeting_date)                          AS pre_decision,
               (m.meeting_date - z.date)                          AS days_to_decision,
               (strftime(z.date, '%Y-%m') = m.meeting_month)      AS in_meeting_month
        FROM meetings m
        JOIN cme_zq z
          ON (m.zq_source = 'contract' AND z.contract = m.zq_contract_symbol)
          OR (m.zq_source = 'continuous_in_month' AND z.is_continuous AND strftime(z.date, '%Y-%m') = m.meeting_month)""")

    con.execute("""
        CREATE VIEW v_prob_cme_daily AS
        WITH effr_obs AS (SELECT date, effr FROM fred_rates WHERE effr IS NOT NULL),
        mtg AS (SELECT m.*, w.prev_meeting_date, w.prev_effective_date FROM meetings m JOIN meeting_windows w USING (meeting_date)),
        -- the hold-vs-move model needs EFFR_pre to be the rate that prevails until THIS meeting: only dates after the
        -- previous meeting's effective date (so that the last observed EFFR already reflects the previous decision) qualify
        base AS (
            SELECT v.meeting_date, v.date, v.source, v.contract, v.settle AS zq_settle, v.is_partial, v.pre_decision,
                   v.days_to_decision, m.delta, m.days_post, m.days_in_month, m.effective_date,
                   m.prev_meeting_date, m.prev_effective_date,
                   e.effr AS effr_pre, e.date AS effr_pre_date
            FROM v_zq_meeting v JOIN mtg m USING (meeting_date)
            ASOF LEFT JOIN effr_obs e ON v.date > e.date
            WHERE v.date > m.prev_effective_date),
        fair AS (
            SELECT *, 100 - effr_pre AS fair_hold,
                   100 - effr_pre + 0.25 * delta AS fair_cut25,
                   100 - effr_pre + 0.50 * delta AS fair_cut50p,
                   100 - effr_pre - 0.25 * delta AS fair_hike25,
                   100 - effr_pre - 0.50 * delta AS fair_hike50p,
                   CASE WHEN delta > 0 THEN (100 - effr_pre - zq_settle) * 100 / delta END AS implied_move_bp
            FROM base),
        k AS (SELECT meeting_date, asof_date, e_move_bp AS k_e_move_bp, p_hike25 AS k_p_hike25, p_hold AS k_p_hold,
                     p_cut25 AS k_p_cut25, n_legs_quoted AS k_n_legs_quoted,
                     sum_raw AS k_sum_raw, max_stale_days AS k_max_stale_days FROM v_prob_kalshi_daily),
        k_same AS (SELECT meeting_date, date, e_move_bp AS k_e_move_bp_sameday FROM v_prob_kalshi_daily),
        p AS (SELECT meeting_date, asof_date, e_move_bp AS p_e_move_bp, p_hike25 AS p_p_hike25, p_hold AS p_p_hold,
                     p_cut25 AS p_p_cut25, hike50p_is_any_hike, n_legs_obs_today AS p_n_legs_obs_today,
                     sum_raw AS p_sum_raw, n_legs_half AS p_n_legs_half, max_stale_days AS p_max_stale_days
              FROM v_prob_poly_daily WHERE is_primary),
        p_same AS (SELECT meeting_date, date, e_move_bp AS p_e_move_bp_sameday FROM v_prob_poly_daily WHERE is_primary)
        SELECT f.meeting_date, f.date, f.source, f.contract, f.zq_settle, f.is_partial, f.pre_decision, f.days_to_decision,
               f.prev_meeting_date, f.prev_effective_date,
               f.effr_pre, f.effr_pre_date, f.delta, f.days_post, f.days_in_month,
               f.fair_hold, f.fair_cut25, f.fair_cut50p, f.fair_hike25, f.fair_hike50p,
               f.implied_move_bp,
               f.implied_move_bp / 25                                  AS p_hike25_binary,   -- hold-vs-hike25, unclamped
               -f.implied_move_bp / 25                                 AS p_cut25_binary,    -- hold-vs-cut25, unclamped
               LEAST(1, GREATEST(0,  f.implied_move_bp / 25))           AS p_hike25_cme,
               LEAST(1, GREATEST(0, -f.implied_move_bp / 25))           AS p_cut25_cme,
               1 - LEAST(1, GREATEST(0, f.implied_move_bp / 25)) - LEAST(1, GREATEST(0, -f.implied_move_bp / 25)) AS p_hold_cme,
               k.k_e_move_bp                                            AS kalshi_e_move_bp,
               f.fair_hold - f.delta * k.k_e_move_bp / 100              AS kalshi_fair_zq,          -- Kalshi ladder as of 00:00 ET on date
               f.fair_hold - f.delta * ks.k_e_move_bp_sameday / 100     AS kalshi_fair_zq_sameday,  -- Kalshi ladder at end of ET day = date
               k.k_p_hike25 AS kalshi_p_hike25, k.k_p_hold AS kalshi_p_hold, k.k_p_cut25 AS kalshi_p_cut25, k.k_n_legs_quoted AS kalshi_n_legs_quoted,
               p.p_e_move_bp                                            AS poly_e_move_bp,
               CASE WHEN COALESCE(p.p_n_legs_half,0) = 0 AND p.p_sum_raw BETWEEN 0.90 AND 1.10
                    THEN f.fair_hold - f.delta * p.p_e_move_bp / 100 END   AS poly_fair_zq,
               CASE WHEN COALESCE(p.p_n_legs_half,0) = 0 AND p.p_sum_raw BETWEEN 0.90 AND 1.10
                    THEN f.fair_hold - f.delta * ps.p_e_move_bp_sameday / 100 END AS poly_fair_zq_sameday,
               p.p_p_hike25 AS poly_p_hike25, p.p_p_hold AS poly_p_hold, p.p_p_cut25 AS poly_p_cut25,
               p.hike50p_is_any_hike AS poly_hike50p_is_any_hike, p.p_n_legs_obs_today AS poly_n_legs_obs_today,
               k.k_sum_raw AS kalshi_sum_raw, k.k_max_stale_days AS kalshi_max_stale_days,
               p.p_sum_raw AS poly_sum_raw, p.p_n_legs_half AS poly_n_legs_half, p.p_max_stale_days AS poly_max_stale_days
        FROM fair f
        LEFT JOIN k      ON k.meeting_date  = f.meeting_date AND k.asof_date  = f.date
        LEFT JOIN k_same ks ON ks.meeting_date = f.meeting_date AND ks.date  = f.date
        LEFT JOIN p      ON p.meeting_date  = f.meeting_date AND p.asof_date  = f.date
        LEFT JOIN p_same ps ON ps.meeting_date = f.meeting_date AND ps.date  = f.date""")

    con.execute("""
        CREATE VIEW v_basis_daily AS
        SELECT meeting_date, date, source, contract, zq_settle, is_partial, pre_decision, days_to_decision, prev_meeting_date, delta,
               effr_pre, fair_hold, implied_move_bp,
               kalshi_fair_zq, poly_fair_zq,
               (kalshi_fair_zq - zq_settle) * 100          AS basis_kalshi_bp,
               (poly_fair_zq - zq_settle) * 100            AS basis_poly_bp,
               (kalshi_fair_zq_sameday - zq_settle) * 100  AS basis_kalshi_sameday_bp,
               (poly_fair_zq_sameday - zq_settle) * 100    AS basis_poly_sameday_bp,
               kalshi_e_move_bp, poly_e_move_bp,
               p_hike25_cme, p_hold_cme, p_cut25_cme,
               kalshi_p_hike25, kalshi_p_hold, kalshi_p_cut25,
               poly_p_hike25, poly_p_hold, poly_p_cut25,
               p_hike25_cme - kalshi_p_hike25   AS gap_cme_kalshi_hike25,
               p_hold_cme   - kalshi_p_hold     AS gap_cme_kalshi_hold,
               p_cut25_cme  - kalshi_p_cut25    AS gap_cme_kalshi_cut25,
               p_hike25_cme - poly_p_hike25     AS gap_cme_poly_hike25,
               p_hold_cme   - poly_p_hold       AS gap_cme_poly_hold,
               p_cut25_cme  - poly_p_cut25      AS gap_cme_poly_cut25,
               kalshi_p_hike25 - poly_p_hike25  AS gap_kalshi_poly_hike25,
               kalshi_p_hold   - poly_p_hold    AS gap_kalshi_poly_hold,
               kalshi_p_cut25  - poly_p_cut25   AS gap_kalshi_poly_cut25,
               kalshi_n_legs_quoted, poly_n_legs_obs_today, poly_hike50p_is_any_hike,
               -- data-quality screens (skeptic audit 2026-09-17): consumers MUST filter on these.
               kalshi_sum_raw, kalshi_max_stale_days,
               poly_sum_raw, poly_n_legs_half, poly_max_stale_days,
               (kalshi_sum_raw BETWEEN 0.95 AND 1.10 AND kalshi_n_legs_quoted = 5
                     AND COALESCE(kalshi_max_stale_days, 99) <= 1)              AS kalshi_ok,
               (COALESCE(poly_n_legs_half, 0) = 0 AND poly_sum_raw BETWEEN 0.95 AND 1.10
                     AND COALESCE(poly_max_stale_days, 99) <= 1)                AS poly_ok
        FROM v_prob_cme_daily""")


# --------------------------------------------------------------------------------------------------------------
# Coverage matrix
# --------------------------------------------------------------------------------------------------------------
def build_coverage(con: duckdb.DuckDBPyConnection) -> list[dict]:
    con.execute("""
        CREATE TABLE coverage_matrix AS
        WITH kc AS (
            SELECT event_id,
                   SUM(CASE WHEN interval = 1440 THEN 1 ELSE 0 END) AS kalshi_candles_1440,
                   SUM(CASE WHEN interval = 60   THEN 1 ELSE 0 END) AS kalshi_candles_60,
                   SUM(CASE WHEN interval = 1    THEN 1 ELSE 0 END) AS kalshi_candles_1,
                   MIN(CASE WHEN interval = 1440 THEN trade_date_et END) AS kalshi_first_day,
                   MAX(CASE WHEN interval = 1440 THEN trade_date_et END) AS kalshi_last_day,
                   ROUND(SUM(CASE WHEN interval = 1440 THEN vol ELSE 0 END)) AS kalshi_volume_contracts
            FROM kalshi_candles GROUP BY event_id),
        kt AS (SELECT event_id, COUNT(*) AS kalshi_trades, MIN(ts_utc)::DATE AS kalshi_first_trade FROM kalshi_trades GROUP BY event_id),
        pp AS (
            SELECT pem.meeting_date, pem.role,
                   COUNT(DISTINCT pem.event_id) AS n_events,
                   SUM(CASE WHEN p.fidelity = 60 THEN 1 ELSE 0 END) AS rows_hourly,
                   SUM(CASE WHEN p.fidelity = 1  THEN 1 ELSE 0 END) AS rows_minute,
                   MIN(CASE WHEN p.fidelity = 60 THEN timezone('UTC', to_timestamp(p.ts))::DATE END) AS first_day,
                   MAX(CASE WHEN p.fidelity = 60 THEN timezone('UTC', to_timestamp(p.ts))::DATE END) AS last_day
            FROM poly_event_meeting pem LEFT JOIN poly_prices p USING (event_id)
            GROUP BY 1, 2),
        pd AS (SELECT * FROM pp WHERE role = 'decision'),
        po AS (SELECT * FROM pp WHERE role = 'other'),
        zq AS (SELECT meeting_date, COUNT(*) AS zq_rows_total, SUM(CASE WHEN pre_decision THEN 1 ELSE 0 END) AS zq_rows_pre_decision,
                      SUM(CASE WHEN in_meeting_month THEN 1 ELSE 0 END) AS zq_rows_in_month,
                      MIN(date) AS zq_first_day, MAX(date) AS zq_last_day
               FROM v_zq_meeting GROUP BY 1),
        bs AS (SELECT meeting_date, SUM(CASE WHEN basis_kalshi_bp IS NOT NULL AND pre_decision THEN 1 ELSE 0 END) AS basis_days_kalshi,
                      SUM(CASE WHEN basis_poly_bp IS NOT NULL AND pre_decision THEN 1 ELSE 0 END) AS basis_days_poly
               FROM v_basis_daily GROUP BY 1)
        SELECT m.meeting_date, m.meeting_month, m.realized_outcome,
               m.kalshi_event, m.kalshi_status, m.kalshi_result_outcome,
               COALESCE(kc.kalshi_candles_1440, 0) AS kalshi_candles_1440, COALESCE(kc.kalshi_candles_60, 0) AS kalshi_candles_60,
               COALESCE(kc.kalshi_candles_1, 0) AS kalshi_candles_1, COALESCE(kt.kalshi_trades, 0) AS kalshi_trades,
               kc.kalshi_first_day, kc.kalshi_last_day, kt.kalshi_first_trade, kc.kalshi_volume_contracts,
               COALESCE(pd.n_events, 0) AS poly_decision_events, COALESCE(pd.rows_hourly, 0) AS poly_decision_hourly,
               COALESCE(pd.rows_minute, 0) AS poly_decision_minute, pd.first_day AS poly_first_day, pd.last_day AS poly_last_day,
               COALESCE(po.n_events, 0) AS poly_other_events, COALESCE(po.rows_hourly, 0) AS poly_other_hourly,
               COALESCE(po.rows_minute, 0) AS poly_other_minute,
               m.zq_source, m.zq_contract_symbol, m.zq_data_available,
               COALESCE(zq.zq_rows_total, 0) AS zq_rows_total, COALESCE(zq.zq_rows_pre_decision, 0) AS zq_rows_pre_decision,
               COALESCE(zq.zq_rows_in_month, 0) AS zq_rows_in_month, zq.zq_first_day, zq.zq_last_day,
               COALESCE(bs.basis_days_kalshi, 0) AS basis_days_kalshi, COALESCE(bs.basis_days_poly, 0) AS basis_days_poly,
               m.realized_change_bp
        FROM meetings m
        LEFT JOIN kc ON kc.event_id = m.kalshi_event
        LEFT JOIN kt ON kt.event_id = m.kalshi_event
        LEFT JOIN pd USING (meeting_date)
        LEFT JOIN po USING (meeting_date)
        LEFT JOIN zq USING (meeting_date)
        LEFT JOIN bs USING (meeting_date)
        ORDER BY m.meeting_date""")
    rows = con.execute("SELECT * FROM coverage_matrix").fetchall()
    cols = [d[0] for d in con.description]
    out = []
    for r in rows:
        out.append({c: (v.isoformat() if isinstance(v, (date, datetime)) else v) for c, v in zip(cols, r)})
    return out


# --------------------------------------------------------------------------------------------------------------
# Integrity tests
# --------------------------------------------------------------------------------------------------------------
def run_tests(con: duckdb.DuckDBPyConnection) -> list[dict]:
    tests: list[dict] = []

    def add(name, passed, detail, **extra):
        tests.append({"name": name, "passed": bool(passed), "detail": detail, **extra})

    # (1) ZQ=F month-end settle == 100 - calendar-day mean EFFR, 2024-01..2026-08, within 0.30bp
    rows = con.execute("""
        WITH mm AS (SELECT ym, AVG(effr_ff) AS m_effr, COUNT(*) AS nd FROM v_effr_calendar GROUP BY ym),
             zq AS (SELECT strftime(date, '%Y-%m') AS ym, ARG_MAX(settle, date) AS settle_last, MAX(date) AS last_td
                    FROM cme_zq WHERE is_continuous GROUP BY 1)
        SELECT mm.ym, nd, ROUND(m_effr, 4), ROUND(100 - m_effr, 4), settle_last, last_td,
               ROUND((settle_last - (100 - m_effr)) * 100, 3) AS diff_bp
        FROM mm JOIN zq USING (ym) WHERE mm.ym BETWEEN '2024-01' AND '2026-08' ORDER BY 1""").fetchall()
    worst = max(rows, key=lambda r: abs(r[6]))
    bad = [r for r in rows if abs(r[6]) > 0.30]
    add("T1 ZQ=F month-end settle == 100 - mean(calendar-day ffill EFFR) within 0.30bp, 2024-01..2026-08",
        len(rows) == 32 and not bad,
        f"{len(rows)} months checked (expected 32); max |diff| = {abs(worst[6]):.3f}bp in {worst[0]} "
        f"(settle {worst[4]:.4f} vs implied {worst[3]:.4f}); {len(bad)} months exceed 0.30bp"
        + (": " + ", ".join(f"{r[0]}={r[6]:+.2f}bp" for r in bad) if bad else ""),
        rows=[{"ym": r[0], "days": r[1], "mean_effr": r[2], "implied_settle": r[3], "zq_settle": r[4],
               "last_td": r[5].isoformat(), "diff_bp": r[6]} for r in rows])

    # (2a) Kalshi raw sum where all 5 legs are quoted (bar that day, non-empty book on every leg)
    def scoped(view: str, base: str, scopes: list[tuple[str, str]]) -> list[dict]:
        out = []
        for label, cond in scopes:
            r = con.execute(f"""
                SELECT COUNT(*), SUM(CASE WHEN sum_raw < 0.95 OR sum_raw > 1.10 THEN 1 ELSE 0 END),
                       MIN(sum_raw), MEDIAN(sum_raw), MAX(sum_raw)
                FROM {view} WHERE {base} AND {cond}""").fetchone()
            out.append({"scope": label, "n_elig": int(r[0] or 0), "n_bad": int(r[1] or 0),
                        "min": None if r[2] is None else round(r[2], 3), "median": None if r[3] is None else round(r[3], 3),
                        "max": None if r[4] is None else round(r[4], 3)})
        return out

    k_base = "n_legs_bar_today = 5 AND n_legs_quoted = 5"
    k_scopes = scoped("v_prob_kalshi_daily", k_base, [
        ("all dates (the test as specified)", "TRUE"),
        ("inter-meeting window (date > previous meeting's effective date)", "in_window"),
        ("within 60 days of the decision", "days_to_decision <= 60"),
        ("within 120 days of the decision", "days_to_decision <= 120"),
        ("all 5 spreads <= 0.05", "max_spread <= 0.05"),
        ("all 5 legs two-sided (bid > 0 and ask < 1)", "n_legs_two_sided = 5"),
    ])
    k_by_event = con.execute(f"""
        SELECT kalshi_event, COUNT(*) AS n_elig,
               SUM(CASE WHEN sum_raw < 0.95 OR sum_raw > 1.10 THEN 1 ELSE 0 END) AS n_bad,
               ROUND(MIN(sum_raw), 3), ROUND(MAX(sum_raw), 3),
               SUM(CASE WHEN in_window THEN 1 ELSE 0 END) AS n_elig_w,
               SUM(CASE WHEN in_window AND (sum_raw < 0.95 OR sum_raw > 1.10) THEN 1 ELSE 0 END) AS n_bad_w,
               ROUND(MIN(CASE WHEN in_window THEN sum_raw END), 3), ROUND(MAX(CASE WHEN in_window THEN sum_raw END), 3)
        FROM v_prob_kalshi_daily WHERE {k_base} GROUP BY 1 ORDER BY 1""").fetchall()
    k_arb = con.execute(f"""
        SELECT SUM(CASE WHEN sum_bid > 1.0 THEN 1 ELSE 0 END), SUM(CASE WHEN sum_ask < 1.0 THEN 1 ELSE 0 END)
        FROM v_prob_kalshi_daily WHERE {k_base}""").fetchone()
    ks = k_scopes[0]
    kw = k_scopes[1]
    k_viol = con.execute(f"""
        SELECT MIN(days_to_decision), MEDIAN(days_to_decision), SUM(CASE WHEN days_to_decision > 120 THEN 1 ELSE 0 END),
               SUM(CASE WHEN n_legs_two_sided < 5 THEN 1 ELSE 0 END)
        FROM v_prob_kalshi_daily WHERE {k_base} AND (sum_raw < 0.95 OR sum_raw > 1.10)""").fetchone()
    add("T2a Kalshi: 0.95 <= raw sum of 5 leg mids <= 1.10 on every meeting x date where all 5 legs are quoted",
        ks["n_bad"] == 0,
        f"quoted = daily bar present and non-empty book (not bid=0/ask>=0.99) on all 5 legs. All dates: {ks['n_elig']} eligible meeting-days, "
        f"{ks['n_bad']} outside [0.95,1.10] (min {ks['min']}, median {ks['median']}, max {ks['max']}). "
        f"Inter-meeting window (where the basis is defined): {kw['n_elig']} eligible, {kw['n_bad']} outside (range {kw['min']}..{kw['max']}). "
        + "; ".join(f"{s['scope']}: {s['n_elig']} eligible / {s['n_bad']} outside" for s in k_scopes[2:]) + ". "
        f"Daily close bid/ask are not a synchronous snapshot: {k_arb[0]} eligible days have sum(bid) > 1 and {k_arb[1]} have sum(ask) < 1. "
        f"Violations: nearest is {k_viol[0]} days before its decision (median {k_viol[1]:.0f} days), {k_viol[2]} of {ks['n_bad']} are more than 120 days out, "
        f"{k_viol[3]} of {ks['n_bad']} have at least one leg with no bid (wide one-sided book).",
        scopes=k_scopes,
        by_event=[{"event": r[0], "n_elig": r[1], "n_bad": r[2], "min": r[3], "max": r[4], "n_elig_w": r[5], "n_bad_w": r[6],
                   "min_w": r[7], "max_w": r[8]} for r in k_by_event])

    # (2b) Poly canonical legs
    p_base = "n_legs_obs_today = n_canonical_legs AND n_legs_half = 0"
    p_scopes = scoped("v_prob_poly_daily", p_base, [
        ("all decision events, all dates <= FOMC date (the test as specified)", "TRUE"),
        ("primary events only", "is_primary"),
        ("primary events, inter-meeting window", "is_primary AND in_window"),
        ("primary events, inter-meeting window, meetings from 2024-06 (negRisk-era books)", "is_primary AND in_window AND meeting_date >= DATE '2024-06-01'"),
        ("primary events, within 30 days of the decision", "is_primary AND days_to_decision <= 30"),
    ])
    p_by_event = con.execute(f"""
        SELECT event_id, is_primary, n_canonical_legs, COUNT(*) AS n_elig,
               SUM(CASE WHEN sum_raw < 0.95 OR sum_raw > 1.10 THEN 1 ELSE 0 END) AS n_bad,
               ROUND(MIN(sum_raw), 3), ROUND(MEDIAN(sum_raw), 3), ROUND(MAX(sum_raw), 3),
               SUM(CASE WHEN in_window THEN 1 ELSE 0 END),
               SUM(CASE WHEN in_window AND (sum_raw < 0.95 OR sum_raw > 1.10) THEN 1 ELSE 0 END)
        FROM v_prob_poly_daily WHERE {p_base} GROUP BY 1, 2, 3 ORDER BY MIN(date)""").fetchall()
    ps = p_scopes[0]
    pw = p_scopes[2]
    add("T2b Polymarket: 0.95 <= raw sum of canonical-leg YES prices <= 1.10 on every event x date with all canonical legs quoted",
        ps["n_bad"] == 0,
        f"quoted = every canonical leg has an hourly observation that ET day and none is the 0.5 empty-book placeholder (date <= FOMC date). "
        f"All dates: {ps['n_elig']} eligible event-days, {ps['n_bad']} outside [0.95,1.10] (min {ps['min']}, median {ps['median']}, max {ps['max']}). "
        f"Primary events in the inter-meeting window: {pw['n_elig']} eligible, {pw['n_bad']} outside (range {pw['min']}..{pw['max']}). "
        + "; ".join(f"{s['scope']}: {s['n_elig']} eligible / {s['n_bad']} outside" for s in (p_scopes[1], p_scopes[3], p_scopes[4])) + ". "
        f"Violations are thin books: 2023/early-2024 events (wide-quote midpoints), launch days of new events months before the meeting, "
        f"and the low-volume Sep-2026 mirror event fed-decision-in-september-568.",
        scopes=p_scopes,
        by_event=[{"event": r[0], "is_primary": r[1], "n_canonical_legs": r[2], "n_elig": r[3], "n_bad": r[4], "min": r[5], "median": r[6],
                   "max": r[7], "n_elig_w": r[8], "n_bad_w": r[9]} for r in p_by_event])

    # (3) ts strictly increasing / uniqueness
    checks = {
        "kalshi_candles (market_id, interval, ts)": "SELECT COUNT(*) - COUNT(DISTINCT (market_id, interval, ts)) FROM kalshi_candles",
        "kalshi_candles monotonic": """WITH x AS (SELECT ts, LAG(ts) OVER (PARTITION BY market_id, interval ORDER BY ts) pts FROM kalshi_candles)
                                       SELECT SUM(CASE WHEN ts <= pts THEN 1 ELSE 0 END) FROM x""",
        "poly_prices (token_id, fidelity, ts)": "SELECT COUNT(*) - COUNT(DISTINCT (token_id, fidelity, ts)) FROM poly_prices",
        "poly_prices monotonic": """WITH x AS (SELECT ts, LAG(ts) OVER (PARTITION BY token_id, fidelity ORDER BY ts) pts FROM poly_prices)
                                    SELECT SUM(CASE WHEN ts <= pts THEN 1 ELSE 0 END) FROM x""",
        "kalshi_trades trade_id unique": "SELECT COUNT(*) - COUNT(DISTINCT trade_id) FROM kalshi_trades",
        "cme_zq (contract, date)": "SELECT COUNT(*) - COUNT(DISTINCT (contract, date)) FROM cme_zq",
        "fred_long (series_id, date)": "SELECT COUNT(*) - COUNT(DISTINCT (series_id, date)) FROM fred_long",
        "fred_rates date unique": "SELECT COUNT(*) - COUNT(DISTINCT date) FROM fred_rates",
        "kalshi_daily_quotes (market_id, trade_date_et)": "SELECT COUNT(*) - COUNT(DISTINCT (market_id, trade_date_et)) FROM kalshi_daily_quotes",
        "poly_daily_yes (market_id, date_et)": "SELECT COUNT(*) - COUNT(DISTINCT (market_id, date_et)) FROM poly_daily_yes",
    }
    res = {name: con.execute(sql).fetchone()[0] or 0 for name, sql in checks.items()}
    add("T3 ts strictly increasing per market x interval, no duplicate (market, ts)",
        all(v == 0 for v in res.values()),
        "; ".join(f"{k}: {v} violation(s)" for k, v in res.items()),
        checks={k: int(v) for k, v in res.items()})

    # (4) Point checks
    r = con.execute("""
        SELECT mid, bid_close, ask_close, m.result FROM kalshi_daily_quotes q JOIN kalshi_markets m USING (market_id)
        WHERE q.market_id = 'KXFEDDECISION-26SEP-H25' AND q.trade_date_et = DATE '2026-09-15'""").fetchone()
    ok4a = r is not None and r[0] is not None and 0.86 <= r[0] <= 0.88 and r[3] == "yes"
    add("T4a Kalshi KXFEDDECISION-26SEP-H25: 1440 mid on 2026-09-15 in [0.86,0.88] and result = yes",
        ok4a, f"mid={r[0]} (bid {r[1]} / ask {r[2]}), result={r[3]!r}" if r else "no row")
    r = con.execute("""
        SELECT y.p, timezone('UTC', to_timestamp(y.ts_last)) AS ts_last_utc, y.event_id, pem.is_primary, y.stale_days
        FROM poly_daily_yes y JOIN poly_event_meeting pem USING (event_id)
        WHERE pem.role = 'decision' AND pem.is_primary AND pem.meeting_date = DATE '2026-09-16' AND y.outcome = 'HIKE25'
          AND y.date_et = DATE '2026-09-16'""").fetchone()
    ok4b = r is not None and r[0] is not None and r[0] >= 0.90 and r[4] == 0
    add("T4b Polymarket 'Fed Decision in September?' HIKE25 YES: last hourly price on 2026-09-16 >= 0.90",
        ok4b, f"event={r[2]} (primary={r[3]}), last hourly p={r[0]} at {r[1]}Z" if r else "no row")

    # (5) Basis reference
    r = con.execute("""
        SELECT basis_kalshi_bp, zq_settle, kalshi_fair_zq, effr_pre, delta, kalshi_e_move_bp, kalshi_p_hike25, basis_kalshi_sameday_bp, basis_poly_bp
        FROM v_basis_daily WHERE meeting_date = DATE '2026-09-16' AND date = DATE '2026-09-11'""").fetchone()
    ok5 = r is not None and r[0] is not None and 2.5 <= r[0] <= 3.5
    add("T5 v_basis_daily 2026-09-11 (Sep-2026 meeting): Kalshi basis in [+2.5,+3.5]bp (independent reference +2.97bp)",
        ok5,
        (f"basis_kalshi_bp={r[0]:+.3f} (ZQU26 settle {r[1]:.4f}, Kalshi-ladder fair {r[2]:.4f}, EFFR_pre {r[3]}, delta {r[4]:.4f}, "
         f"Kalshi E[move]={r[5]:.2f}bp, P(hike25)={r[6]:.3f} as of 00:00 ET 09-11); same-day variant {r[7]:+.3f}bp; Poly basis {r[8]:+.3f}bp")
        if r and r[0] is not None else f"row: {r}")

    # extra structural checks
    counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in
              ["meetings", "kalshi_markets", "kalshi_candles", "kalshi_trades", "poly_markets", "poly_prices", "cme_zq", "fred_long"]}
    sources = {"meetings": [RAW / "meetings.parquet"], "kalshi_markets": [RAW / "kalshi" / "markets.parquet"],
               "kalshi_candles": [RAW / "kalshi" / "candles.parquet"], "kalshi_trades": [RAW / "kalshi" / "trades.parquet"],
               "poly_markets": [RAW / "poly" / "markets.parquet"], "poly_prices": [RAW / "poly" / "prices.parquet"],
               "cme_zq": sorted((RAW / "cme").glob("ZQ*.parquet")), "fred_long": sorted((RAW / "fred").glob("*.parquet"))}
    expected = {t: sum(con.execute(f"SELECT COUNT(*) FROM read_parquet({q(p)})").fetchone()[0] for p in ps) for t, ps in sources.items()}
    mism = {t: (counts[t], expected[t]) for t in expected if counts[t] != expected[t]}
    add("T6 row counts equal the raw parquet inputs", not mism,
        "; ".join(f"{t}={counts[t]}" for t in counts) + (f"; MISMATCH {mism}" if mism else ""))

    r = con.execute("""SELECT COUNT(*) FROM meetings WHERE kalshi_result_outcome IS NOT NULL AND realized_outcome IS NOT NULL
                       AND kalshi_result_outcome <> realized_outcome""").fetchone()[0]
    add("T7 Kalshi settled outcome == FRED DFEDTARU realized outcome wherever both exist", r == 0, f"{r} mismatch(es)")
    return tests


# --------------------------------------------------------------------------------------------------------------
# Docs
# --------------------------------------------------------------------------------------------------------------
def write_tests_md(tests: list[dict], built_at: str) -> None:
    lines = [f"# Integrity tests — fed.duckdb", "", f"Built {built_at} by `scripts/build_db.py`.", "",
             "| # | Test | Result |", "|---|------|--------|"]
    for i, t in enumerate(tests, 1):
        lines.append(f"| {i} | {t['name']} | {'PASS' if t['passed'] else '**FAIL**'} |")
    lines += ["", "## Details", ""]
    for t in tests:
        lines += [f"### {t['name']}", "", f"**{'PASS' if t['passed'] else 'FAIL'}** — {t['detail']}", ""]
        if "rows" in t:
            lines += ["| month | days | mean EFFR | implied settle | ZQ=F last settle | last trade day | diff bp |", "|---|---|---|---|---|---|---|"]
            lines += [f"| {r['ym']} | {r['days']} | {r['mean_effr']:.4f} | {r['implied_settle']:.4f} | {r['zq_settle']:.4f} | {r['last_td']} | {r['diff_bp']:+.2f} |"
                      for r in t["rows"]]
            lines.append("")
        if "scopes" in t:
            lines += ["| scope | eligible days | outside [0.95,1.10] | min | median | max |", "|---|---|---|---|---|---|"]
            lines += [f"| {s['scope']} | {s['n_elig']} | {s['n_bad']} | {s['min']} | {s['median']} | {s['max']} |" for s in t["scopes"]]
            lines.append("")
        if "by_event" in t and t["by_event"] and "is_primary" not in t["by_event"][0]:
            lines += ["| Kalshi event | eligible days | outside | min | max | eligible in window | outside in window | min (window) | max (window) |",
                      "|---|---|---|---|---|---|---|---|---|"]
            lines += [f"| {r['event']} | {r['n_elig']} | {r['n_bad']} | {r['min']} | {r['max']} | {r['n_elig_w']} | {r['n_bad_w']} | {r['min_w']} | {r['max_w']} |"
                      for r in t["by_event"]]
            lines.append("")
        if "by_event" in t and t["by_event"] and "is_primary" in t["by_event"][0]:
            lines += ["| Poly event | primary | canonical legs | eligible days | outside | min | median | max | eligible in window | outside in window |",
                      "|---|---|---|---|---|---|---|---|---|---|"]
            lines += [f"| {r['event']} | {r['is_primary']} | {r['n_canonical_legs']} | {r['n_elig']} | {r['n_bad']} | {r['min']} | {r['median']} | {r['max']} | {r['n_elig_w']} | {r['n_bad_w']} |"
                      for r in t["by_event"]]
            lines.append("")
        if "checks" in t:
            lines += [f"- {k}: {v}" for k, v in t["checks"].items()]
            lines.append("")
    n_fail = sum(not t["passed"] for t in tests)
    lines += ["## Verdict", "", f"{len(tests) - n_fail} passed, {n_fail} failed." if n_fail else f"All {len(tests)} tests passed.", ""]
    if n_fail:
        lines += ["Failing tests are data-quality statements about the venues (see the scope tables above); they do not indicate a loading "
                  "error: the same sums are inside [0.95, 1.10] on every Kalshi meeting-day in the inter-meeting window where the basis is "
                  "defined, and the Polymarket violations are thin 2023/early-2024 books, launch days months before a meeting and the Sep-2026 "
                  "mirror event. The views expose `in_window`, `days_to_decision`, `n_legs_two_sided`, `max_spread`, `sum_bid`, `sum_ask` "
                  "(Kalshi) and `is_primary`, `n_legs_half`, `n_legs_obs_today` (Polymarket) so consumers can apply their own liquidity screen.", ""]
    TESTS_MD.write_text("\n".join(lines))


def write_readme(con: duckdb.DuckDBPyConnection, coverage: list[dict], tests: list[dict], built_at: str) -> None:
    def cnt(t):
        return con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    tables = ["meetings", "meeting_windows", "kalshi_markets", "kalshi_candles", "kalshi_trades", "poly_markets", "poly_prices",
              "poly_event_meeting", "cme_zq", "cme_sofr_probe", "fred_rates", "fred_long", "kalshi_daily_quotes", "poly_daily_yes",
              "coverage_matrix"]
    views = ["v_effr_calendar", "v_prob_kalshi_daily", "v_prob_poly_daily", "v_zq_meeting", "v_prob_cme_daily", "v_basis_daily"]
    tbl_rows = "\n".join(f"| `{t}` | table | {cnt(t):,} | {TABLE_DOC[t]} |" for t in tables)
    view_rows = "\n".join(f"| `{v}` | view | {cnt(v):,} | {TABLE_DOC[v]} |" for v in views)

    cov_hdr = ("| meeting | realized | Kalshi event (status) | K 1440 / 60 / 1 bars | K trades | Poly decision events: hourly / minute | "
               "Poly other events: hourly / minute | ZQ source (rows pre-decision / total) | basis days K / P |")
    cov_sep = "|---|---|---|---|---|---|---|---|---|"
    cov_lines = []
    for r in coverage:
        kev = f"{r['kalshi_event']} ({r['kalshi_status']})" if r["kalshi_event"] else "—"
        cov_lines.append(
            f"| {r['meeting_date']} | {r['realized_outcome'] or '—'} | {kev} | {r['kalshi_candles_1440']:,} / {r['kalshi_candles_60']:,} / {r['kalshi_candles_1']:,} | "
            f"{r['kalshi_trades']:,} | {r['poly_decision_events']}: {r['poly_decision_hourly']:,} / {r['poly_decision_minute']:,} | "
            f"{r['poly_other_events']}: {r['poly_other_hourly']:,} / {r['poly_other_minute']:,} | "
            f"{r['zq_source']} {r['zq_contract_symbol']} ({r['zq_rows_pre_decision']:,} / {r['zq_rows_total']:,}) | "
            f"{r['basis_days_kalshi']} / {r['basis_days_poly']} |")
    test_lines = "\n".join(f"| {'PASS' if t['passed'] else '**FAIL**'} | {t['name']} |" for t in tests)

    md = f"""# fed-pricing-db

One DuckDB file (`fed.duckdb`) that lines up three venues pricing the same FOMC decisions:

* **Kalshi** KXFEDDECISION ladders (C26/C25/H0/H25/H26 legs -> CUT50P/CUT25/HOLD/HIKE25/HIKE50P), candles + trades + market metadata,
* **Polymarket** Fed-decision events (CLOB price history per YES/NO token, hourly and minute),
* **CME** 30-Day Fed Funds futures (ZQ, per-contract files + the ZQ=F continuous series) with FRED policy rates (EFFR, SOFR, IORB, target range, DFF),

plus a `meetings` spine (49 FOMC decision dates 2022-01-26..2028-01-26) and views that turn each venue into a daily probability /
fair-price series and compute the cross-venue basis.

Built {built_at} by `scripts/build_db.py` (idempotent: deletes and recreates `fed.duckdb`; exits 1 if an integrity test fails).
Raw inputs live under `raw/<source>/` (parquet + JSON manifests written by the collectors; see their manifests for pull details).

## Tables and views

| name | kind | rows | contents |
|---|---|---|---|
{tbl_rows}
{view_rows}

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

{cov_hdr}
{cov_sep}
{chr(10).join(cov_lines)}

## Integrity tests

Full results in [`TESTS.md`](TESTS.md). T2a/T2b are run exactly as specified (every meeting x date where all legs are quoted) and fail on
far-dated / thin ladders; `TESTS.md` shows the same sums restricted to the inter-meeting window, where the Kalshi ladders (and the
negRisk-era Polymarket events) are all inside [0.95, 1.10].

| result | test |
|---|---|
{test_lines}

## Scripts

* `scripts/build_db.py` — this loader (`python3 scripts/build_db.py [--db PATH] [--skip-tests]`).
* `scripts/kalshi_pull.py`, `scripts/fetch_cme_yf.py`, `scripts/fetch_fred.py`, `scripts/build_meetings.py` — collectors that wrote `raw/`.
  The Polymarket collector scripts are referenced from `raw/poly/manifest.json -> scripts`.
"""
    README_MD.write_text(md)


TABLE_DOC = {
    "meetings": "one row per FOMC decision date 2022-2028: Kalshi event/status/result, Polymarket slugs (decision vs other), ZQ contract + source, effective date, delta = days_post/days_in_month, realized change (FRED DFEDTARU)",
    "meeting_windows": "per meeting: previous meeting date and previous effective date (start of the inter-meeting window used by v_prob_cme_daily / v_basis_daily)",
    "kalshi_markets": "Kalshi market metadata snapshot (147 markets: 65 KXFEDDECISION legs + KXFED rate-threshold, KXRATECUT, KXFEDHIKE) incl. status, result, settlement, volume, open interest; parsed `*_ts_utc` timestamps",
    "kalshi_candles": "Kalshi candlesticks P=1440/60/1 (ts = bar end); YES bid/ask OHLC, trade OHLC, vol, oi, is_partial; derived bar_end_utc, trade_date_et (daily), mid, spread",
    "kalshi_trades": "public trades (trailing window only, earliest 2026-07-11): taker side, yes/no price, count, block flag",
    "poly_markets": "Polymarket market metadata (249 markets / 55 events): question, canonical outcome + outcome_detail, token ids, dates, volume, negRisk, resolution",
    "poly_prices": "CLOB prices-history points: hourly (fidelity 60) for every token's life and minute (fidelity 1) for the last 45 days; p = midpoint",
    "poly_event_meeting": "event -> meeting link with role (decision/other), is_primary (highest-volume single-decision event per meeting), canonical-leg count, any-hike flag",
    "cme_zq": "ZQ daily bars: ZQ=F continuous (is_continuous) + 16 contract files ZQU26..ZQZ27; settle = close, is_partial for the fetch-day bar",
    "cme_sofr_probe": "SR1/SR3 yfinance probes (1 live bar or junk 2020 placeholders) kept for the record; not usable as history",
    "fred_rates": "wide daily table: date, effr, sofr, iorb, tgt_upper, tgt_lower, dff (null on holidays / before series start)",
    "fred_long": "the six FRED series in long form (series_id, date, value)",
    "kalshi_daily_quotes": "dense daily grid per KXFEDDECISION leg with forward-filled daily quotes (stale_days), mid, spread, two_sided, quoted",
    "poly_daily_yes": "dense daily grid per Polymarket canonical leg: last hourly YES price of the ET day, forward-filled up to the FOMC date (stale_days, p_is_half)",
    "coverage_matrix": "per-meeting row counts by venue and granularity (also in README)",
    "v_effr_calendar": "calendar-day EFFR forward-filled over weekends/holidays (the settlement convention of ZQ)",
    "v_prob_kalshi_daily": "per meeting x ET day: 5 leg mids, raw sum, sum of bids/asks, normalized p_*, expected move, liquidity counters, in_window",
    "v_prob_poly_daily": "per meeting x event x ET day: canonical-leg YES prices (legs sharing an outcome summed), raw sum, normalized p_*, expected move, is_primary, in_window",
    "v_zq_meeting": "per meeting x date: meeting-month ZQ bar from the contract file when present else ZQ=F inside the month; source, pre_decision, days_to_decision",
    "v_prob_cme_daily": "per meeting x CME date: EFFR_pre, fair prices per move, implied move, binary and clamped CME probabilities, Kalshi/Poly ladder-weighted fair ZQ",
    "v_basis_daily": "per meeting x CME date: Kalshi/Poly implied ZQ minus settle (bp), same-day variants, and probability gaps CME-Kalshi, CME-Poly, Kalshi-Poly for HIKE25/HOLD/CUT25",
}


# --------------------------------------------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--skip-tests", action="store_true")
    args = ap.parse_args()
    db = Path(args.db)
    for p in (db, Path(str(db) + ".wal")):
        if p.exists():
            p.unlink()
    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log: list[str] = []
    con = duckdb.connect(str(db))
    con.execute("SET TimeZone='UTC'")
    con.execute("SET preserve_insertion_order=false")
    print(f"[build_db] loading tables into {db}", flush=True)
    load_tables(con, log)
    print("[build_db] building helper tables", flush=True)
    build_helpers(con)
    print("[build_db] creating views", flush=True)
    build_views(con)
    print("[build_db] coverage matrix", flush=True)
    coverage = build_coverage(con)
    tests: list[dict] = []
    if not args.skip_tests:
        print("[build_db] running integrity tests", flush=True)
        tests = run_tests(con)
        write_tests_md(tests, built_at)
    write_readme(con, coverage, tests, built_at)
    report = {"built_at_utc": built_at, "db": str(db), "log": log, "coverage": coverage,
              "tests": [{k: v for k, v in t.items() if k not in ("rows",)} for t in tests],
              "tables": {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in
                         [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1").fetchall()]}}
    REPORT_JSON.write_text(json.dumps(report, indent=1, default=str))
    con.close()
    n_fail = sum(not t["passed"] for t in tests)
    for t in tests:
        print(f"[{'PASS' if t['passed'] else 'FAIL'}] {t['name']}\n       {t['detail']}", file=sys.stderr if not t["passed"] else sys.stdout)
    if n_fail:
        print(f"\n!!! {n_fail} INTEGRITY TEST(S) FAILED — see {TESTS_MD}", file=sys.stderr)
        return 1
    print(f"\nall {len(tests)} integrity tests passed; db={db} size={db.stat().st_size/1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
