#!/usr/bin/env python3
"""Fetch FRED series (full history) via fredgraph.csv into raw/fred/<ID>.parquet + manifest.json."""
import io, json, sys, time, urllib.request
from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path("/Users/ethanyang/Developer/fed-pricing-db")
OUT = ROOT / "raw" / "fred"
OUT.mkdir(parents=True, exist_ok=True)
SERIES = ["EFFR", "SOFR", "IORB", "DFEDTARU", "DFEDTARL", "DFF"]
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={id}"
SCHEMA = pa.schema([
    ("venue", pa.string()), ("event_id", pa.string()), ("market_id", pa.string()), ("outcome", pa.string()),
    ("ts", pa.int64()), ("date", pa.date32()), ("series_id", pa.string()), ("value", pa.float64()),
])


def fetch_csv(sid: str, tries=3) -> str:
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(URL.format(id=sid), headers={"User-Agent": "fed-pricing-db/0.1"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read().decode("utf-8")
        except Exception as e:
            last = e
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"{sid}: {last!r}")


def main():
    manifest = {"source": "FRED fredgraph.csv", "fetched_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "schema": [f.name for f in SCHEMA],
                "ts_semantics": "int64 UTC epoch seconds at 00:00:00 UTC of observation_date; join on `date`",
                "value_semantics": "percent per annum as published; FRED '.' (missing, e.g. holidays) -> null, row kept",
                "outcome": "OTHER for every row (canonical enum placeholder)", "files": {}, "anomalies": []}
    for sid in SERIES:
        try:
            txt = fetch_csv(sid)
        except Exception as e:
            manifest["anomalies"].append(f"{sid}: download failed {e!r}")
            print(f"[FAIL] {sid}: {e!r}")
            continue
        df = pd.read_csv(io.StringIO(txt))
        if list(df.columns[:2]) != ["observation_date", sid]:
            manifest["anomalies"].append(f"{sid}: unexpected columns {list(df.columns)}")
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.drop_duplicates("date").sort_values("date")
        ts = [int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) for d in df["date"]]
        tbl = pa.Table.from_pydict({
            "venue": ["fred"] * len(df), "event_id": [sid] * len(df), "market_id": [sid] * len(df),
            "outcome": ["OTHER"] * len(df), "ts": ts, "date": df["date"].tolist(), "series_id": [sid] * len(df),
            "value": [None if pd.isna(v) else float(v) for v in df["value"]]}, schema=SCHEMA)
        path = OUT / f"{sid}.parquet"
        pq.write_table(tbl, path, compression="zstd")
        nn = df["value"].notna()
        rec = {"rows": int(len(df)), "non_null": int(nn.sum()), "null_rows": int((~nn).sum()),
               "first_date": str(df["date"].min()), "last_date": str(df["date"].max()),
               "last_value": float(df.loc[nn, "value"].iloc[-1]), "last_non_null_date": str(df.loc[nn, "date"].iloc[-1])}
        manifest["files"][str(path.relative_to(ROOT))] = rec
        staleness = (date.today() - df.loc[nn, "date"].iloc[-1]).days
        if staleness > 7:
            manifest["anomalies"].append(f"{sid}: last non-null observation {rec['last_non_null_date']} is {staleness} days old")
        print(f"[OK] {sid}: {rec}")
        time.sleep(0.5)
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(json.dumps({"anomalies": manifest["anomalies"]}, indent=1))


if __name__ == "__main__":
    main()
