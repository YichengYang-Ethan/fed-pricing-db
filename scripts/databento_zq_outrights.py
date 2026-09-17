import pandas as pd
DBN="/Users/ethanyang/Developer/fed-pricing-db/raw/cme/databento/zq_settlements.parquet"
OUT="/Users/ethanyang/Developer/fed-pricing-db/raw/cme/databento/zq_outrights.parquet"
IBK="/Users/ethanyang/Developer/fed-pricing-db/raw/cme/ibkr/zq_contracts_ibkr.parquet"
INV={"F":1,"G":2,"H":3,"J":4,"K":5,"M":6,"N":7,"Q":8,"U":9,"V":10,"X":11,"Z":12}

d=pd.read_parquet(DBN); d["trade_date"]=pd.to_datetime(d["trade_date"])
d=d[d.trade_date.dt.year>=2000]
o=d[d.symbol.str.fullmatch(r"ZQ[FGHJKMNQUVXZ]\d")].copy()
o["m"]=o.symbol.str[2].map(INV); o["y1"]=o.symbol.str[3].astype(int)

# ZQ lists 60 consecutive months; 60 < 120 so the single-digit year is unambiguous.
# Pick the decade whose horizon lands in [-1, 60]; -1 covers the final settlement
# stamped the day after a contract's last trade date.
base=(o.trade_date.dt.year//10)*10
cand=pd.DataFrame({dec:(base+o.y1+dec-o.trade_date.dt.year)*12+(o.m-o.trade_date.dt.month) for dec in (-10,0,10)})
ok=(cand>=-1)&(cand<=60)
n_ok=ok.sum(axis=1)
print("rows with exactly one valid decade:", (n_ok==1).sum(), " zero:", (n_ok==0).sum(), " multiple:", (n_ok>1).sum())
dec=cand.where(ok).idxmin(axis=1)   # exactly one valid, so idxmin == the valid one
o=o[n_ok==1].copy(); dec=dec[n_ok==1]
o["year"]=base[o.index]+o.y1+dec
o["dm"]=o.year.astype(str)+"-"+o.m.map("{:02d}".format)
o["h"]=(o.year-o.trade_date.dt.year)*12+(o.m-o.trade_date.dt.month)
o=o[["trade_date","symbol","dm","year","m","h","price","instrument_id","ts_recv"]].sort_values(["trade_date","dm"])
print(f"kept {len(o):,} / 72,139 outright rows   {o.dm.nunique()} delivery months  {o.trade_date.min().date()}..{o.trade_date.max().date()}")
print("delivery months span:", o.dm.min(), "..", o.dm.max())

ib=pd.read_parquet(IBK); ib["date"]=pd.to_datetime(ib["date"])
j=o.merge(ib[["date","delivery_month","settle"]],left_on=["trade_date","dm"],right_on=["date","delivery_month"],how="inner")
bad=(j.price-j.settle).abs()>1e-9
print(f"\nre-check vs IBKR: {len(j):,} overlaps, mismatches {bad.sum()}, max|d| {(j.price-j.settle).abs().max()*100:.6f} bp")
o.to_parquet(OUT,index=False); print(f"wrote -> {OUT}")
