#!/usr/bin/env python3
"""Pull historical daily settlements for expired ZQ (30-Day Fed Funds) contracts from IBKR.

READ ONLY. This script requests historical bars and contract details. It never places, modifies or
cancels an order, and it never reads or writes positions. Run it against a paper login if you have
one; if the gateway is logged into a live account, enable "Read-Only API" in the API settings first.

Why: the Kalshi-vs-CME basis test needs, for each FOMC meeting, the settlement price of the ZQ
contract whose delivery month the decision fully covers (usually the month AFTER the meeting month).
Those contracts have expired and are not available from yfinance, which only serves the 16 currently
listed contracts. IBKR keeps expired futures, so this closes the last data gap.

Usage:
    python3 scripts/ibkr_zq_pull.py --probe                 # connect, verify one contract, exit
    python3 scripts/ibkr_zq_pull.py --from 2022-01 --to 2026-12
"""
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path

import pandas as pd
from ib_async import IB, Future, util

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "raw" / "cme" / "ibkr"
PORTS = [(4002, "Gateway paper"), (7497, "TWS paper"), (4001, "Gateway live"), (7496, "TWS live")]
MONTH_CODE = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M", 7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}


def months(a: str, b: str) -> list[str]:
    y0, m0 = map(int, a.split("-"))
    y1, m1 = map(int, b.split("-"))
    out, y, m = [], y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y}{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def connect(ib: IB) -> str:
    for port, label in PORTS:
        try:
            ib.connect("127.0.0.1", port, clientId=17, timeout=8, readonly=True)
            return f"{label} (port {port})"
        except Exception:
            continue
    raise SystemExit(
        "No IBKR API on 4002/7497/4001/7496.\n"
        "Launch IB Gateway or TWS, log in, then enable:\n"
        "  Configure > API > Settings > 'Enable ActiveX and Socket Clients'\n"
        "  and tick 'Read-Only API' (this script only reads).\n"
        "Paper gateway listens on 4002, paper TWS on 7497."
    )


def pull(ib: IB, ym: str) -> tuple[pd.DataFrame | None, str]:
    """Daily TRADES bars for the ZQ contract of delivery month ym (YYYYMM)."""
    con = Future(symbol="ZQ", lastTradeDateOrContractMonth=ym, exchange="CBOT", currency="USD",
                 includeExpired=True)
    details = ib.reqContractDetails(con)
    if not details:  # IBKR indexes ZQ by exact last trade date, not by YYYYMM, for some months
        probe = Future(symbol="ZQ", exchange="CBOT", currency="USD", includeExpired=True)
        allc = ib.reqContractDetails(probe)
        hit = [d for d in allc if d.contract.lastTradeDateOrContractMonth.startswith(ym)]
        details = hit[:1]
    if not details:
        return None, "no contract details (not listed / no permission)"
    c = details[0].contract
    # CRITICAL: for an expired contract endDateTime="" means "now", which returns almost nothing.
    # Anchoring the request at the contract's own last trade date returns the full history.
    end = f"{c.lastTradeDateOrContractMonth} 23:59:59 US/Central"
    bars = ib.reqHistoricalData(c, endDateTime=end, durationStr="2 Y", barSizeSetting="1 day",
                                whatToShow="TRADES", useRTH=False, formatDate=1, timeout=90)
    if not bars:
        return None, "contract found but no historical bars returned"
    df = util.df(bars)
    code = f"ZQ{MONTH_CODE[int(ym[4:6])]}{ym[2:4]}"
    df = df.assign(contract=code, delivery_month=f"{ym[:4]}-{ym[4:6]}",
                   ib_local_symbol=c.localSymbol, ib_con_id=c.conId, source="ibkr")
    df = df.rename(columns={"date": "date", "close": "settle"})
    return df, f"{len(df)} bars {df.date.min()} -> {df.date.max()}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="a", default="2022-01")
    ap.add_argument("--to", dest="b", default="2026-12")
    ap.add_argument("--probe", action="store_true", help="connect + fetch one contract, then exit")
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    ib = IB()
    where = connect(ib)
    print(f"[ibkr] connected: {where}  server {ib.client.serverVersion()}")
    accounts = ib.managedAccounts()
    print(f"[ibkr] accounts visible: {accounts} (read-only; no orders are sent)")

    targets = ["202307"] if args.probe else months(args.a, args.b)
    report, frames = {}, []
    for ym in targets:
        try:
            df, note = pull(ib, ym)
        except Exception as e:  # keep going; one missing month must not kill the run
            df, note = None, f"error: {type(e).__name__}: {e}"
        report[ym] = note
        print(f"  {ym}: {note}")
        if df is not None:
            frames.append(df)
        ib.sleep(0.6)  # IBKR pacing: <60 historical requests per 10 minutes
        if args.probe:
            break

    if frames:
        all_df = pd.concat(frames, ignore_index=True)
        path = OUT / "zq_contracts_ibkr.parquet"
        all_df.to_parquet(path, index=False)
        print(f"[ibkr] wrote {len(all_df):,} rows / {all_df.contract.nunique()} contracts -> {path}")
    (OUT / "pull_report.json").write_text(json.dumps(report, indent=1))
    ib.disconnect()


if __name__ == "__main__":
    main()
