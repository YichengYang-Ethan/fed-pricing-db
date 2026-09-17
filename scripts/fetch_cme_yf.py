#!/usr/bin/env python3
"""Fetch CME rate futures daily bars from yfinance into raw/cme/*.parquet.

Continuous ZQ=F (front month == current calendar month; rolls to M+1 after the FOMC
decision day when M has a meeting), the 16 live ZQ contracts, and the SR3/SR1 probes.
Every parquet file shares one schema so `read_parquet('raw/cme/*.parquet')` works.
"""
import json, sys, time, warnings
from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

warnings.filterwarnings("ignore")
ROOT = Path("/Users/ethanyang/Developer/fed-pricing-db")
OUT = ROOT / "raw" / "cme"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "sofr").mkdir(exist_ok=True)

FOMC = ["2022-01-26","2022-03-16","2022-05-04","2022-06-15","2022-07-27","2022-09-21","2022-11-02","2022-12-14",
        "2023-02-01","2023-03-22","2023-05-03","2023-06-14","2023-07-26","2023-09-20","2023-11-01","2023-12-13",
        "2024-01-31","2024-03-20","2024-05-01","2024-06-12","2024-07-31","2024-09-18","2024-11-07","2024-12-18",
        "2025-01-29","2025-03-19","2025-05-07","2025-06-18","2025-07-30","2025-09-17","2025-10-29","2025-12-10",
        "2026-01-28","2026-03-18","2026-04-29","2026-06-17","2026-07-29","2026-09-16","2026-10-28","2026-12-09",
        "2027-01-27","2027-03-17","2027-04-28","2027-06-09","2027-07-28","2027-09-15","2027-10-27","2027-12-08",
        "2028-01-26"]
FOMC_BY_MONTH = {d[:7]: date.fromisoformat(d) for d in FOMC}
CODES = "FGHJKMNQUVXZ"  # Jan..Dec
CODE2M = {c: i + 1 for i, c in enumerate(CODES)}

ZQ_CONTRACTS = [f"ZQ{m}26.CBT" for m in "UVXZ"] + [f"ZQ{m}27.CBT" for m in CODES]
SR_SYMBOLS = ["SR3=F", "SR1=F", "SR3U26.CME", "SR3Z26.CME", "SR1V26.CME"]

SCHEMA = pa.schema([
    ("venue", pa.string()), ("event_id", pa.string()), ("market_id", pa.string()),
    ("outcome", pa.string()), ("ts", pa.int64()), ("date", pa.date32()),
    ("contract", pa.string()), ("source_symbol", pa.string()), ("product", pa.string()),
    ("delivery_month", pa.string()), ("front_contract_est", pa.string()),
    ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("settle", pa.float64()), ("volume", pa.int64()), ("open_interest", pa.int64()),
])


def parse_symbol(sym: str):
    """'ZQU26.CBT' -> (product 'ZQ', contract 'ZQU26', delivery '2026-09'); 'ZQ=F' -> continuous."""
    base = sym.split(".")[0]
    if base.endswith("=F"):
        return base[:-2], base, None
    prod, code, yy = base[:-3], base[-3], base[-2:]
    return prod, base, f"20{yy}-{CODE2M[code]:02d}"


def front_contract_est(d: date, product: str):
    """Front contract for the continuous series == the calendar-month contract, through the last trading day.

    Empirically there is NO intra-month roll after the FOMC decision day: on every month-end since 2022 the
    ZQ=F close equals 100 - calendar-day mean EFFR of that same month within 0.25bp (see manifest
    `settlement_check`), including meeting months with rate changes (e.g. 2024-09-30 94.87 = Sep settle,
    then 95.18 on 2024-10-01 = Oct contract). A post-meeting roll would have broken this by ~25-50bp.
    """
    return f"{product}{CODES[d.month-1]}{d.year % 100:02d}"


def settlement_check(zq_path: Path, effr_path: Path):
    """ZQ=F last-trading-day close vs 100 - calendar-day forward-filled mean EFFR, per month since 2022."""
    if not effr_path.exists():
        return {"skipped": "raw/fred/EFFR.parquet missing"}
    zq = pq.read_table(zq_path).to_pandas()
    zq["date"] = pd.to_datetime(zq["date"])
    zq = zq[zq["date"] >= "2022-01-01"]
    e = pq.read_table(effr_path).to_pandas()
    e["date"] = pd.to_datetime(e["date"])
    e = e.dropna(subset=["value"]).set_index("date")["value"]
    cal = pd.date_range("2022-01-01", zq["date"].max(), freq="D")
    ff = e.reindex(cal).ffill()
    mmean = ff.groupby(ff.index.to_period("M")).mean()
    last = zq.sort_values("date").groupby(zq["date"].dt.to_period("M")).last()
    complete = [p for p in last.index if last.loc[p, "date"] >= p.end_time.normalize() - pd.Timedelta(days=4)]
    rows = []
    for p in complete:
        if p not in mmean.index:
            continue
        diff_bp = (last.loc[p, "close"] - (100 - mmean[p])) * 100
        rows.append({"month": str(p), "last_td": str(last.loc[p, "date"].date()), "close": round(float(last.loc[p, "close"]), 4),
                     "implied_settle": round(float(100 - mmean[p]), 4), "diff_bp": round(float(diff_bp), 2)})
    diffs = [abs(r["diff_bp"]) for r in rows]
    return {"rule": "last-trading-day ZQ=F close == 100 - calendar-day mean EFFR of the SAME month (weekends/holidays carry prior EFFR)",
            "months": len(rows), "max_abs_diff_bp": max(diffs) if diffs else None,
            "n_exceed_0p25bp": sum(d > 0.2501 for d in diffs), "worst": sorted(rows, key=lambda r: -abs(r["diff_bp"]))[:5],
            "implication": "no intra-month roll: ZQ=F is the calendar-month contract through month end, including after FOMC decision days"}


def fetch(sym: str, period="max", tries=3):
    last = None
    for i in range(tries):
        try:
            h = yf.Ticker(sym).history(period=period, auto_adjust=False, actions=False)
            return h, None
        except Exception as e:  # yfinance raises on invalid period / 404
            last = repr(e)[:300]
            if "invalid" in last.lower():
                return pd.DataFrame(), last
            time.sleep(2 * (i + 1))
    return pd.DataFrame(), last


def to_table(h: pd.DataFrame, sym: str) -> pa.Table:
    product, contract, delivery = parse_symbol(sym)
    idx = pd.DatetimeIndex(h.index)
    dates = [d.date() for d in (idx.tz_localize(None) if idx.tz is not None else idx)]
    ts = [int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) for d in dates]
    if delivery is None:  # continuous
        event_id = [f"{d.year:04d}-{d.month:02d}" for d in dates]
        fce = [front_contract_est(d, product) for d in dates]
    else:
        event_id = [delivery] * len(dates)
        fce = [None] * len(dates)
    vol = pd.to_numeric(h["Volume"], errors="coerce").astype("Int64")
    cols = {
        "venue": ["cme"] * len(dates), "event_id": event_id, "market_id": [contract] * len(dates),
        "outcome": ["OTHER"] * len(dates), "ts": ts, "date": dates,
        "contract": [contract] * len(dates), "source_symbol": [sym] * len(dates),
        "product": [product] * len(dates), "delivery_month": [delivery] * len(dates),
        "front_contract_est": fce,
        "open": h["Open"].astype(float).tolist(), "high": h["High"].astype(float).tolist(),
        "low": h["Low"].astype(float).tolist(), "close": h["Close"].astype(float).tolist(),
        "settle": h["Close"].astype(float).tolist(),
        "volume": [None if pd.isna(v) else int(v) for v in vol],
        "open_interest": [None] * len(dates),
    }
    return pa.Table.from_pydict(cols, schema=SCHEMA)


def oi_snapshot(sym: str):
    try:
        info = yf.Ticker(sym).info
        return {"open_interest": info.get("openInterest"), "expire_iso": info.get("expireIsoDate"),
                "short_name": info.get("shortName")}
    except Exception as e:
        return {"error": repr(e)[:200]}


def main():
    manifest = {"source": "yfinance", "fetched_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "yfinance_version": yf.__version__, "schema": [f.name for f in SCHEMA],
                "ts_semantics": "int64 UTC epoch seconds at 00:00:00 UTC of the trading date (daily bars); join on `date`",
                "settle_semantics": "settle == yfinance Close (no separate settlement feed)",
                "open_interest": "yfinance has no OI history; column is all-null. Live snapshot per contract in `contracts[*].oi_snapshot`",
                "outcome": "OTHER for every futures row (canonical enum placeholder; futures are not binary outcomes)",
                "continuous_front_rule": "ZQ=F front == current calendar-month contract through its LAST trading day; rolls to M+1 only at the month boundary (no roll after the FOMC decision day). front_contract_est = calendar-month contract for every bar",
                "partial_bar_warning": "bars dated the fetch day (trade date in progress on Globex) are intraday snapshots, not settlements",
                "files": {}, "symbols_ok": [], "symbols_failed": {}, "anomalies": []}
    today = datetime.now(timezone.utc).date()
    jobs = [("ZQ=F", OUT / "ZQ_F.parquet")] + [(s, OUT / f"{s}.parquet") for s in ZQ_CONTRACTS] \
        + [(s, OUT / "sofr" / f"{s.replace('=', '_')}.parquet") for s in SR_SYMBOLS]
    for sym, path in jobs:
        h, err = fetch(sym)
        if h.empty and sym in ("SR1=F", "SR1V26.CME"):
            h, err2 = fetch(sym, period="5d")  # period=max rejected for these; keep whatever exists
            err = f"{err} | fallback period=5d -> {len(h)} rows"
        if h.empty:
            manifest["symbols_failed"][sym] = err or "empty"
            manifest["anomalies"].append(f"{sym}: no data ({err or 'empty'}); nothing written")
            print(f"[FAIL] {sym}: {err}")
            continue
        h = h[~h.index.duplicated(keep="last")].sort_index()
        tbl = to_table(h, sym)
        pq.write_table(tbl, path, compression="zstd")
        df = tbl.to_pandas()
        rec = {"symbol": sym, "rows": len(df), "first_date": str(df["date"].min()), "last_date": str(df["date"].max()),
               "last_close": float(df["close"].iloc[-1]), "zero_volume_rows": int((df["volume"].fillna(0) == 0).sum()),
               "period_used": "max" if "fallback" not in (err or "") else "5d", "note": err}
        if sym != "ZQ=F" and not sym.startswith("SR"):
            rec["oi_snapshot"] = oi_snapshot(sym)
        gap = df["date"].sort_values()
        big_gaps = [(str(a), str(b)) for a, b in zip(gap[:-1], gap[1:]) if (b - a).days > 10]
        if big_gaps:
            rec["date_gaps_gt_10d"] = big_gaps[:5]
            manifest["anomalies"].append(f"{sym}: {len(big_gaps)} gap(s) >10 calendar days, e.g. {big_gaps[0]}")
        if len(df) < 30:
            manifest["anomalies"].append(f"{sym}: only {len(df)} bar(s) returned ({rec['first_date']}..{rec['last_date']}); not usable as history")
        if df["date"].max() >= today:
            rec["partial_last_bar"] = str(df["date"].max())
            manifest["anomalies"].append(f"{sym}: last bar {df['date'].max()} is the in-progress session (partial, not a settlement)")
        manifest["files"][str(path.relative_to(ROOT))] = rec
        manifest["symbols_ok"].append(sym)
        print(f"[OK] {sym}: {len(df)} rows {rec['first_date']}..{rec['last_date']} -> {path.name}")
        time.sleep(0.4)
    # Sanity: ZQ=F should equal the meeting-month contract through the decision day (Sep 2026 has ZQU26)
    try:
        zq = pq.read_table(OUT / "ZQ_F.parquet").to_pandas().set_index("date")["close"]
        u = pq.read_table(OUT / "ZQU26.CBT.parquet").to_pandas().set_index("date")["close"]
        j = pd.concat([zq, u], axis=1, keys=["zq", "u26"]).dropna()
        j = j[(j.index >= date(2026, 9, 1)) & (j.index <= date(2026, 9, 16))]
        maxdiff = float((j["zq"] - j["u26"]).abs().max()) if len(j) else None
        manifest["front_month_check_sep2026"] = {"days": int(len(j)), "max_abs_diff": maxdiff}
        if maxdiff is None or maxdiff > 0.0026:
            manifest["anomalies"].append(f"ZQ=F vs ZQU26 Sep 1-16 2026 max diff {maxdiff} (expected 0)")
    except Exception as e:
        manifest["anomalies"].append(f"front-month check failed: {e!r}")
    try:
        manifest["settlement_check"] = settlement_check(OUT / "ZQ_F.parquet", ROOT / "raw" / "fred" / "EFFR.parquet")
        sc = manifest["settlement_check"]
        if sc.get("n_exceed_0p25bp"):
            manifest["anomalies"].append(f"settlement check: {sc['n_exceed_0p25bp']} month(s) exceed 0.25bp, worst {sc['worst'][0]}")
    except Exception as e:
        manifest["anomalies"].append(f"settlement check failed: {e!r}")
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=1, default=str))
    print(json.dumps({"ok": manifest["symbols_ok"], "failed": manifest["symbols_failed"], "anomalies": manifest["anomalies"]}, indent=1))


if __name__ == "__main__":
    main()
