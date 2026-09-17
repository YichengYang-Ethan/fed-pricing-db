"""Pull ZQ daily settlement prices for every delivery month, 2022-01 -> now, from Databento.

Only the statistics schema is bought: it already carries `symbol`, so the (expensive) definition
schema is not needed. Settlement is stat_type 3. CME publishes preliminary and final settlements,
so rows are deduped per (trade date, contract) keeping the LAST by ts_recv, and stat_flags is
retained so the choice can be audited.
"""
import databento as db, pandas as pd, datetime as dt, sys, time
from pathlib import Path

OUT = Path("/Users/ethanyang/Developer/fed-pricing-db/raw/cme/databento")
OUT.mkdir(parents=True, exist_ok=True)
KEY = Path.home() / ".config/databento/key"   # chmod 600; never in the tree
c = db.Historical(KEY.read_text().strip())
SETTLE = int(db.StatType.SETTLEMENT_PRICE)

months, y, m = [], 2022, 1
while (y, m) <= (2026, 9):
    nxt = (y + 1, 1) if m == 12 else (y, m + 1)
    months.append((f"{y}-{m:02d}-01", f"{nxt[0]}-{nxt[1]:02d}-01"))
    y, m = nxt

frames, t0 = [], time.time()
for i, (a, b) in enumerate(months, 1):
    for attempt in range(3):
        try:
            d = c.timeseries.get_range(dataset="GLBX.MDP3", symbols=["ZQ.FUT"], stype_in="parent",
                                       schema="statistics", start=a, end=b)
            df = d.to_df()
            break
        except Exception as e:
            if attempt == 2:
                print(f"  {a[:7]}: FAILED {type(e).__name__}: {e}", flush=True); df = None
            else:
                time.sleep(5)
    if df is None or not len(df):
        continue
    s = df[df.stat_type == SETTLE].copy()
    if not len(s):
        print(f"  {a[:7]}: no settlement rows", flush=True); continue
    s["trade_date"] = pd.to_datetime(s.ts_ref).dt.date
    s = s.reset_index().sort_values("ts_recv").drop_duplicates(["trade_date", "symbol"], keep="last")
    frames.append(s[["trade_date", "symbol", "price", "stat_flags", "instrument_id", "ts_recv"]])
    print(f"  {a[:7]}: {len(s):5d} settlements, {s.symbol.nunique():4d} contracts  "
          f"[{i}/{len(months)}, {time.time()-t0:.0f}s]", flush=True)

allf = pd.concat(frames, ignore_index=True)
# The per-chunk dedup above cannot see a final settlement that CME posts after midnight UTC,
# because it lands in the NEXT month's request window. Dedup again across the whole pull.
allf = (allf.sort_values("ts_recv")
            .drop_duplicates(["trade_date", "symbol"], keep="last")
            .sort_values(["trade_date", "symbol"], ignore_index=True))
allf["delivery_month"] = allf.symbol.str.extract(r"ZQ([FGHJKMNQUVXZ])(\d)$").apply(
    lambda r: None if pd.isna(r[0]) else r[0] + r[1], axis=1)
allf.to_parquet(OUT / "zq_settlements.parquet", index=False)
print(f"\nWROTE {len(allf):,} rows / {allf.symbol.nunique()} contracts -> {OUT/'zq_settlements.parquet'}")
print(f"date range {allf.trade_date.min()} .. {allf.trade_date.max()}, {time.time()-t0:.0f}s")
