#!/usr/bin/env python3
"""Pull Kalshi Fed-decision market data (candles, trades, metadata) into raw/kalshi/.

Outputs (all under raw/kalshi/):
  candles.parquet, trades.parquet, markets.parquet, manifest.json
Staging (resumable, removed on success): raw/kalshi/_stage/
"""
import datetime as dt
import json
import os
import shutil
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = "/Users/ethanyang/Developer/fed-pricing-db"
INV = f"{ROOT}/raw/kalshi_fed_inventory.json"
OUT = f"{ROOT}/raw/kalshi"
STAGE = f"{OUT}/_stage"
LOG = os.environ.get("KALSHI_PULL_LOG", f"{STAGE}/pull.log")
BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = "KXFEDDECISION"
NOW = int(time.time())
MAX_CANDLES = 5000
MINUTE_WINDOW_DAYS = 45
MINUTE_CHUNK_S = 72 * 3600
HOUR_CHUNK_S = 200 * 24 * 3600  # 4800 candles < 5000
MIN_INTERVAL_S = 0.5  # ~2 req/s

LEG2OUTCOME = {"C26": "CUT50P", "C25": "CUT25", "H0": "HOLD", "H25": "HIKE25", "H26": "HIKE50P"}

os.makedirs(STAGE, exist_ok=True)
anomalies: list[str] = []
_last_req = 0.0
sess = requests.Session()
sess.headers["User-Agent"] = "fed-pricing-db/0.1 (research; polite)"


def utc_iso(ts: int) -> str:
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    line = f"{utc_iso(time.time())} {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def anomaly(msg: str) -> None:
    anomalies.append(msg)
    log("ANOMALY " + msg)


def get(path: str, **params):
    """GET with rate limit and backoff. Returns (status, json_or_None)."""
    global _last_req
    for attempt in range(4):
        wait = MIN_INTERVAL_S - (time.time() - _last_req)
        if wait > 0:
            time.sleep(wait)
        try:
            r = sess.get(BASE + path, params=params, timeout=60)
        except requests.RequestException as e:
            _last_req = time.time()
            log(f"  net-error {path} {params} attempt={attempt} {e}")
            time.sleep(5)
            continue
        _last_req = time.time()
        if r.status_code == 200:
            return 200, r.json()
        if r.status_code == 429 or r.status_code >= 500:
            log(f"  http {r.status_code} {path} attempt={attempt}; sleeping 5s")
            time.sleep(5)
            continue
        # 4xx other than 429: do not retry
        return r.status_code, (r.text or "")[:300]
    return -1, "exhausted retries"


def iso_to_epoch(s):
    if not s:
        return None
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def fnum(x):
    try:
        return None if x in (None, "") else float(x)
    except (TypeError, ValueError):
        return None


def stage_path(kind: str, key: str) -> str:
    return f"{STAGE}/{kind}__{key}.parquet"


def stage_done(kind: str, key: str) -> bool:
    return os.path.exists(stage_path(kind, key))


def stage_write(kind: str, key: str, df: pd.DataFrame) -> None:
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), stage_path(kind, key))


# ----------------------------------------------------------------------------------------------
# Candles
# ----------------------------------------------------------------------------------------------
CANDLE_COLS = [
    "venue", "event_id", "market_id", "outcome", "ts", "interval",
    "bid_close", "ask_close", "price_close", "vol", "oi",
    "bid_open", "bid_high", "bid_low", "ask_open", "ask_high", "ask_low",
    "price_open", "price_high", "price_low", "price_mean", "price_previous", "is_partial",
]


def parse_candles(cands, event_id, market_id, outcome, interval):
    rows = []
    for c in cands:
        yb = c.get("yes_bid") or {}
        ya = c.get("yes_ask") or {}
        p = c.get("price") or {}
        rows.append({
            "venue": "kalshi", "event_id": event_id, "market_id": market_id, "outcome": outcome,
            "ts": int(c["end_period_ts"]), "interval": int(interval),
            "bid_close": fnum(yb.get("close_dollars")), "ask_close": fnum(ya.get("close_dollars")),
            "price_close": fnum(p.get("close_dollars")),
            "vol": fnum(c.get("volume_fp")), "oi": fnum(c.get("open_interest_fp")),
            "bid_open": fnum(yb.get("open_dollars")), "bid_high": fnum(yb.get("high_dollars")), "bid_low": fnum(yb.get("low_dollars")),
            "ask_open": fnum(ya.get("open_dollars")), "ask_high": fnum(ya.get("high_dollars")), "ask_low": fnum(ya.get("low_dollars")),
            "price_open": fnum(p.get("open_dollars")), "price_high": fnum(p.get("high_dollars")), "price_low": fnum(p.get("low_dollars")),
            "price_mean": fnum(p.get("mean_dollars")), "price_previous": fnum(p.get("previous_dollars")),
        })
    return rows


def fetch_candles(series, ticker, event_id, outcome, interval, start_ts, end_ts, chunk_s=None):
    """Fetch candles over [start_ts, end_ts], chunking so each request stays <= 5000 candles."""
    rows = []
    if end_ts <= start_ts:
        anomaly(f"{ticker} P={interval}: empty window start={start_ts} end={end_ts}; skipped")
        return rows
    if chunk_s is None:
        chunk_s = (MAX_CANDLES - 200) * interval * 60
    s = start_ts
    n_req = 0
    while s < end_ts:
        e = min(s + chunk_s, end_ts)
        status, body = get(f"/series/{series}/markets/{ticker}/candlesticks",
                           start_ts=s, end_ts=e, period_interval=interval)
        n_req += 1
        if status != 200:
            anomaly(f"{ticker} P={interval} [{s},{e}] http {status}: {body}; chunk skipped")
        else:
            rows.extend(parse_candles(body.get("candlesticks", []), event_id, ticker, outcome, interval))
        s = e
    return rows, n_req


def pull_candles_for_market(series, mk, event_id, outcome, intervals=(1440, 60, 1)):
    ticker = mk["ticker"]
    open_ts = iso_to_epoch(mk["open_time"])
    close_ts = iso_to_epoch(mk["close_time"])
    settled = mk.get("status") in ("finalized", "settled", "closed", "determined") or bool(mk.get("result"))
    life_end = min(close_ts, NOW) if close_ts else NOW
    # The API returns bars with end_period_ts <= end_ts, so end_ts must be padded by one period or
    # the final partial bar (decision day / decision hour) is dropped. Settled markets return nothing
    # past their last bar; open markets return the in-progress bar, flagged is_partial (ts > NOW).
    counts = {}
    for interval in intervals:
        key = f"{ticker}__P{interval}"
        if stage_done("candles", key):
            df = pd.read_parquet(stage_path("candles", key))
            counts[f"candles_{interval}"] = len(df)
            continue
        pad = interval * 60
        if interval == 1:
            win_start = max(open_ts, life_end - MINUTE_WINDOW_DAYS * 86400)
            res = fetch_candles(series, ticker, event_id, outcome, 1, win_start, life_end + pad, chunk_s=MINUTE_CHUNK_S)
        elif interval == 60:
            res = fetch_candles(series, ticker, event_id, outcome, 60, open_ts, life_end + pad, chunk_s=HOUR_CHUNK_S)
        else:
            res = fetch_candles(series, ticker, event_id, outcome, 1440, open_ts, life_end + pad)
        rows, n_req = res if isinstance(res, tuple) else (res, 0)
        df = pd.DataFrame(rows, columns=CANDLE_COLS)
        df["is_partial"] = df["ts"] > NOW
        if len(df):
            df = df.drop_duplicates(subset=["market_id", "interval", "ts"]).sort_values("ts")
        stage_write("candles", key, df)
        counts[f"candles_{interval}"] = len(df)
        log(f"  {ticker} P={interval}: {len(df)} candles ({n_req} req) settled={settled}")
    return counts


# ----------------------------------------------------------------------------------------------
# Trades
# ----------------------------------------------------------------------------------------------
TRADE_COLS = [
    "venue", "event_id", "market_id", "outcome", "ts", "ts_us", "created_time", "trade_id",
    "count", "taker_side", "taker_book_side", "yes_price", "no_price", "is_block_trade",
]


def pull_trades_for_market(ticker, event_id, outcome):
    key = ticker
    if stage_done("trades", key):
        return len(pd.read_parquet(stage_path("trades", key)))
    rows = []
    cursor = None
    pages = 0
    while True:
        params = {"ticker": ticker, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        status, body = get("/markets/trades", **params)
        if status != 200:
            anomaly(f"{ticker} trades page {pages} cursor={cursor!r} http {status}: {body}; pagination aborted (partial)")
            break
        pages += 1
        for t in body.get("trades", []):
            ct = t.get("created_time")
            d = dt.datetime.fromisoformat(ct.replace("Z", "+00:00"))
            rows.append({
                "venue": "kalshi", "event_id": event_id, "market_id": ticker, "outcome": outcome,
                "ts": int(d.timestamp()), "ts_us": int(d.timestamp() * 1_000_000), "created_time": ct,
                "trade_id": t.get("trade_id"), "count": fnum(t.get("count_fp")),
                "taker_side": t.get("taker_side"), "taker_book_side": t.get("taker_book_side"),
                "yes_price": fnum(t.get("yes_price_dollars")), "no_price": fnum(t.get("no_price_dollars")),
                "is_block_trade": bool(t.get("is_block_trade", False)),
            })
        cursor = body.get("cursor") or None
        if not cursor or not body.get("trades"):
            break
        if pages % 25 == 0:
            log(f"  {ticker} trades: {pages} pages, {len(rows)} rows so far")
    df = pd.DataFrame(rows, columns=TRADE_COLS)
    if len(df):
        before = len(df)
        df = df.drop_duplicates(subset=["trade_id"]).sort_values("ts_us")
        if len(df) != before:
            anomaly(f"{ticker} trades: {before - len(df)} duplicate trade_ids dropped")
    stage_write("trades", key, df)
    log(f"  {ticker} trades: {len(df)} rows over {pages} pages")
    return len(df)


# ----------------------------------------------------------------------------------------------
# Markets metadata
# ----------------------------------------------------------------------------------------------
def market_row(mk, series_ticker, series_info, outcome, leg):
    ev = mk.get("event_ticker")
    return {
        "venue": "kalshi", "event_id": ev, "market_id": mk["ticker"], "outcome": outcome,
        "ts": iso_to_epoch(mk.get("open_time")),
        "ticker": mk["ticker"], "event_ticker": ev, "series_ticker": series_ticker, "leg": leg,
        "title": mk.get("title"), "subtitle": mk.get("subtitle") or mk.get("yes_sub_title"),
        "expiration_value": mk.get("expiration_value"),
        "market_type": mk.get("market_type"), "strike_type": mk.get("strike_type"),
        "custom_strike": json.dumps(mk.get("custom_strike")) if mk.get("custom_strike") is not None else None,
        "floor_strike": fnum(mk.get("floor_strike")), "cap_strike": fnum(mk.get("cap_strike")),
        "status": mk.get("status"), "result": mk.get("result"),
        "created_time": mk.get("created_time"), "open_time": mk.get("open_time"), "close_time": mk.get("close_time"),
        "expected_expiration_time": mk.get("expected_expiration_time"), "expiration_time": mk.get("expiration_time"),
        "latest_expiration_time": mk.get("latest_expiration_time"),
        "settlement_ts": mk.get("settlement_ts"), "occurrence_datetime": mk.get("occurrence_datetime"),
        "settlement_value": fnum(mk.get("settlement_value_dollars")),
        "settlement_timer_seconds": mk.get("settlement_timer_seconds"),
        "can_close_early": mk.get("can_close_early"), "early_close_condition": mk.get("early_close_condition"),
        "rules_primary": mk.get("rules_primary"), "rules_secondary": mk.get("rules_secondary"),
        "price_level_structure": mk.get("price_level_structure"),
        "tick_size": fnum(((mk.get("price_ranges") or [{}])[0]).get("step")),
        "volume_fp": fnum(mk.get("volume_fp")), "volume_24h_fp": fnum(mk.get("volume_24h_fp")),
        "open_interest_fp": fnum(mk.get("open_interest_fp")), "liquidity_dollars": fnum(mk.get("liquidity_dollars")),
        "last_price": fnum(mk.get("last_price_dollars")),
        "yes_bid": fnum(mk.get("yes_bid_dollars")), "yes_ask": fnum(mk.get("yes_ask_dollars")),
        "fee_type": series_info.get("fee_type"), "fee_multiplier": series_info.get("fee_multiplier"),
        "series_frequency": series_info.get("frequency"), "series_title": series_info.get("title"),
        "contract_terms_url": series_info.get("contract_terms_url"),
        "settlement_sources": json.dumps(series_info.get("settlement_sources")),
        "snapshot_ts": NOW,
    }


def get_series(ticker):
    status, body = get(f"/series/{ticker}")
    if status != 200:
        anomaly(f"series {ticker} http {status}: {body}")
        return {}
    return body.get("series", {})


def get_market(ticker):
    status, body = get(f"/markets/{ticker}")
    if status != 200:
        anomaly(f"market {ticker} http {status}: {body}; skipped")
        return None
    return body.get("market")


# ----------------------------------------------------------------------------------------------
def main():
    t0 = time.time()
    log(f"=== kalshi_pull start NOW={NOW} ({utc_iso(NOW)})")
    inv = json.load(open(INV))
    counts = {}
    market_rows = []
    series_meta = {}

    # ---- KXFEDDECISION ----
    s_fed = get_series(SERIES)
    series_meta[SERIES] = s_fed
    events_with_legs = [(k, v) for k, v in inv.items() if v.get("legs")]
    events_empty = [k for k, v in inv.items() if not v.get("legs")]
    for k in events_empty:
        anomaly(f"event {k} has no legs in inventory (purged from Kalshi API); skipped")
    log(f"{len(events_with_legs)} events with legs, {len(events_empty)} empty events skipped")

    for ev_ticker, ev in sorted(events_with_legs, key=lambda kv: min(l["close"] for l in kv[1]["legs"].values())):
        counts[ev_ticker] = {}
        for leg, info in sorted(ev["legs"].items()):
            ticker = info["ticker"]
            outcome = LEG2OUTCOME.get(leg)
            if outcome is None:
                anomaly(f"{ticker}: unknown leg code {leg}, mapped to OTHER")
                outcome = "OTHER"
            log(f"[{ev_ticker}/{leg}] {ticker}")
            mk = get_market(ticker)
            if mk is None:
                counts[ev_ticker][leg] = {"error": "market metadata unavailable"}
                continue
            market_rows.append(market_row(mk, SERIES, s_fed, outcome, leg))
            c = pull_candles_for_market(SERIES, mk, ev_ticker, outcome)
            c["trades"] = pull_trades_for_market(ticker, ev_ticker, outcome)
            counts[ev_ticker][leg] = c

    # ---- Related series (metadata + P=1440 only) ----
    related = {}
    for series_ticker, ev_ticker in [("KXRATECUT", "KXRATECUT-26DEC31"), ("KXFEDHIKE", "KXFEDHIKE-2")]:
        s_info = get_series(series_ticker)
        series_meta[series_ticker] = s_info
        status, body = get(f"/events/{ev_ticker}", with_nested_markets="true")
        if status != 200:
            anomaly(f"event {ev_ticker} http {status}: {body}; skipped")
            continue
        mks = body.get("event", {}).get("markets", []) or []
        related[ev_ticker] = {}
        for mk in mks:
            log(f"[{ev_ticker}] {mk['ticker']} ({mk.get('subtitle') or mk.get('yes_sub_title')})")
            market_rows.append(market_row(mk, series_ticker, s_info, "OTHER", None))
            c = pull_candles_for_market(series_ticker, mk, ev_ticker, "OTHER", intervals=(1440,))
            related[ev_ticker][mk["ticker"]] = c
        if not mks:
            anomaly(f"event {ev_ticker}: no nested markets returned")

    s_info = get_series("KXFED")
    series_meta["KXFED"] = s_info
    cursor = None
    kxfed_events = []
    while True:
        params = {"series_ticker": "KXFED", "status": "open", "with_nested_markets": "true", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        status, body = get("/events", **params)
        if status != 200:
            anomaly(f"KXFED open events http {status}: {body}")
            break
        kxfed_events.extend(body.get("events", []))
        cursor = body.get("cursor") or None
        if not cursor:
            break
    log(f"KXFED open events: {len(kxfed_events)}")
    for ev in kxfed_events:
        ev_ticker = ev["event_ticker"]
        related[ev_ticker] = {}
        for mk in ev.get("markets", []) or []:
            log(f"[{ev_ticker}] {mk['ticker']} ({mk.get('subtitle') or mk.get('yes_sub_title')})")
            market_rows.append(market_row(mk, "KXFED", s_info, "OTHER", None))
            c = pull_candles_for_market("KXFED", mk, ev_ticker, "OTHER", intervals=(1440,))
            related[ev_ticker][mk["ticker"]] = c

    # ---- Assemble ----
    log("assembling parquet files")
    cand_files = sorted(f for f in os.listdir(STAGE) if f.startswith("candles__") and f.endswith(".parquet"))
    cand = pd.concat([pd.read_parquet(f"{STAGE}/{f}") for f in cand_files], ignore_index=True) if cand_files else pd.DataFrame(columns=CANDLE_COLS)
    cand = cand.astype({"ts": "int64", "interval": "int32"})
    cand = cand.sort_values(["market_id", "interval", "ts"]).reset_index(drop=True)
    pq.write_table(pa.Table.from_pandas(cand, preserve_index=False), f"{OUT}/candles.parquet", compression="zstd")

    trade_files = sorted(f for f in os.listdir(STAGE) if f.startswith("trades__") and f.endswith(".parquet"))
    trades = pd.concat([pd.read_parquet(f"{STAGE}/{f}") for f in trade_files], ignore_index=True) if trade_files else pd.DataFrame(columns=TRADE_COLS)
    if len(trades):
        trades = trades.astype({"ts": "int64", "ts_us": "int64"}).sort_values(["market_id", "ts_us"]).reset_index(drop=True)
    pq.write_table(pa.Table.from_pandas(trades, preserve_index=False), f"{OUT}/trades.parquet", compression="zstd")

    markets = pd.DataFrame(market_rows)
    markets["ts"] = markets["ts"].astype("int64")
    pq.write_table(pa.Table.from_pandas(markets, preserve_index=False), f"{OUT}/markets.parquet", compression="zstd")

    # candle-count sanity vs counts dict
    by = cand.groupby(["market_id", "interval"]).size().to_dict()
    tb = trades.groupby("market_id").size().to_dict() if len(trades) else {}

    # Trade-history coverage: Kalshi's public /markets/trades only serves a trailing window
    # (probed: max_ts before the first served trade returns 0 rows). Quantify per market
    # using daily-candle volume before the first served trade.
    trade_coverage = {}
    daily = cand[cand["interval"] == 1440]
    for mid, grp in trades.groupby("market_id"):
        first_ts = int(grp["ts"].min())
        d = daily[daily["market_id"] == mid]
        vol_total = float(d["vol"].sum())
        vol_before = float(d[d["ts"] < first_ts]["vol"].sum())
        trade_coverage[mid] = {
            "first_trade_utc": utc_iso(first_ts),
            "last_trade_utc": utc_iso(int(grp["ts"].max())),
            "n_trades": int(len(grp)),
            "trades_contracts": float(grp["count"].sum()),
            "candle_vol_total": vol_total,
            "candle_vol_before_first_trade": vol_before,
            "share_of_volume_missing_from_trades": (vol_before / vol_total) if vol_total else None,
        }
        if vol_before > 0:
            anomaly(f"{mid} trades: API serves trades only from {trade_coverage[mid]['first_trade_utc']}; "
                    f"{vol_before:,.0f} of {vol_total:,.0f} candle contracts ({vol_before / vol_total:.1%}) predate first served trade")
    for mid in set(cand["market_id"]) - set(tb):
        if mid in {r["market_id"] for r in market_rows if r["series_ticker"] == SERIES}:
            anomaly(f"{mid}: zero trades returned")
    if trade_coverage:
        floor = min(v["first_trade_utc"] for v in trade_coverage.values())
        anomaly(f"SUMMARY trades: public /markets/trades serves only a trailing window; earliest trade across all "
                f"{len(trade_coverage)} markets is {floor} (probe: max_ts before it returns 0 rows, min_ts/max_ts do not "
                f"extend it). Full-life volume is only available via candles (P=1440/P=60 vol).")

    # Volume reconciliation: sum of daily-candle volume vs market metadata volume_fp (open markets
    # differ slightly because metadata and candles were fetched seconds apart / in-progress bar).
    volume_check = {}
    daily_vol = cand[cand["interval"] == 1440].groupby("market_id")["vol"].sum().to_dict()
    for r in market_rows:
        mid = r["market_id"]
        if mid not in daily_vol:
            continue
        vf = r.get("volume_fp") or 0.0
        dv = float(daily_vol[mid])
        rel = (dv - vf) / vf if vf else None
        volume_check[mid] = {"volume_fp": vf, "sum_daily_candle_vol": dv, "rel_diff": rel}
        if vf and abs(rel) > 0.02 and r["series_ticker"] == SERIES:
            anomaly(f"{mid}: daily-candle volume {dv:,.0f} vs metadata volume_fp {vf:,.0f} ({rel:+.1%})")

    manifest = {
        "source": "kalshi",
        "api_base": BASE,
        "pulled_at_utc": utc_iso(NOW),
        "now_ts": NOW,
        "duration_s": round(time.time() - t0, 1),
        "files": {
            "candles.parquet": {"rows": int(len(cand)), "columns": list(cand.columns)},
            "trades.parquet": {"rows": int(len(trades)), "columns": list(trades.columns)},
            "markets.parquet": {"rows": int(len(markets)), "columns": list(markets.columns)},
        },
        "schema_notes": {
            "ts": "UTC epoch seconds int64; candles: end_period_ts of the bar; trades: created_time floored to seconds (ts_us has microseconds); markets: open_time",
            "interval": "candle period in minutes (1440/60/1)",
            "prices": "dollars per contract (0-1); bid/ask are YES-side quotes; price_* is last-trade based and null when no trades in bar",
            "vol": "volume_fp contracts traded in bar; oi: open_interest_fp at bar end",
            "trades.count": "count_fp contracts; taker_side yes/no; yes_price/no_price dollars",
            "outcome": "canonical enum CUT50P/CUT25/HOLD/HIKE25/HIKE50P/OTHER; related series (KXRATECUT/KXFEDHIKE/KXFED) are OTHER",
            "is_partial": "True when the bar's end_period_ts is after pulled_at (in-progress bar of an open market); settled markets have none",
            "bar_alignment": "daily bars end at 04:00Z (midnight ET); a settled market's final daily/hourly bar ends after close_time and holds decision-day/hour volume",
            "trades_history": "public trades endpoint only serves a trailing window (see trade_coverage / anomalies); candles cover the full life",
        },
        "windows": {
            "P1440_P60": "open_time -> min(close_time, now) per market metadata",
            "P1": f"last {MINUTE_WINDOW_DAYS} days before min(close_time, now), chunked {MINUTE_CHUNK_S // 3600}h",
            "P60_chunk_days": HOUR_CHUNK_S // 86400,
            "api_max_candles_per_request": MAX_CANDLES,
        },
        "outcome_map": LEG2OUTCOME,
        "series": series_meta,
        "events_skipped_empty": events_empty,
        "counts": {"KXFEDDECISION": counts, "related_P1440": related},
        "trade_coverage": trade_coverage,
        "volume_check": volume_check,
        "counts_from_parquet": {
            "candles_by_market_interval": {f"{k[0]}|{k[1]}": int(v) for k, v in by.items()},
            "trades_by_market": {k: int(v) for k, v in tb.items()},
        },
        "anomalies": anomalies,
    }
    with open(f"{OUT}/manifest.json", "w") as f:
        json.dump(manifest, f, indent=1, default=str)
    log(f"=== done in {manifest['duration_s']}s: candles={len(cand)} trades={len(trades)} markets={len(markets)} anomalies={len(anomalies)}")
    if os.environ.get("KALSHI_KEEP_STAGE") != "1":
        shutil.rmtree(STAGE, ignore_errors=True)
    return manifest


if __name__ == "__main__":
    main()
