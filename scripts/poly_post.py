"""Post-pass: re-anchor minute windows on the actual series end, rebuild prices/markets/manifest with QA anomalies."""
import json, os, sys, time, datetime as dt
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from poly_common import *
import pyarrow as pa, pyarrow.parquet as pq, pandas as pd, duckdb

OUT = f'{ROOT}/raw/poly'; PARTS = f'{SCRATCH}/poly_parts'
NOW = int(time.time())
man = json.load(open(f'{OUT}/manifest.json'))
tok_meta = man['tokens']
anomalies = [a for a in man['anomalies']]
mdf = pd.read_parquet(f'{OUT}/markets.parquet')
end_by_mkt = {r.market_id: (date_to_ts(r.end_date) if r.end_date else None) for r in mdf.itertuples()}
closed_by_mkt = {r.market_id: bool(r.closed) for r in mdf.itertuples()}
q_by_mkt = {r.market_id: r.question for r in mdf.itertuples()}
CLOB_SLEEP = 0.35

def pull(tok, start_ts, fidelity):
    res = http_json(f'https://clob.polymarket.com/prices-history?market={tok}&startTs={start_ts}&fidelity={fidelity}')
    time.sleep(CLOB_SLEEP)
    return res.get('history', []) if isinstance(res, dict) else []

def walk_minute(tok, start_ts, window_end):
    pts, calls, cur = {}, 0, start_ts
    while cur < window_end:
        h = pull(tok, cur, 1); calls += 1
        for x in h:
            pts[int(x['t'])] = float(x['p'])
        if not h:
            break
        last_t = int(h[-1]['t'])
        if last_t >= window_end - 1:
            break
        if len(h) < 19900 and last_t < cur + 14 * 86400:
            break
        cur = max(last_t + 1, cur + 14 * 86400) if len(h) >= 19900 else cur + 14 * 86400
    return pts, calls

# ---------------------------------------------------------------- 1. re-anchor minute windows
repulled = []
for tok, tm in tok_meta.items():
    if tm['n_hourly'] == 0:
        continue
    part = f'{PARTS}/{tok}.parquet'
    tbl = pq.read_table(part)
    df = tbl.to_pandas()
    h = df[df.fidelity == 60]
    last_h = int(h.ts.max())
    end_ts = end_by_mkt.get(tm['market_id'])
    closed = closed_by_mkt.get(tm['market_id'], tm.get('closed'))
    if closed and end_ts is not None:
        eff_end = min(end_ts + 86400, last_h + 3600)
    elif closed:
        eff_end = last_h + 3600
    else:
        eff_end = NOW
    new_start = max(eff_end - 45 * 86400, int(h.ts.min()) - 3600)
    old_start = tm['minute_window'][0]
    if new_start < old_start - 3600:
        pts, calls = walk_minute(tok, new_start, eff_end)
        m_old = df[df.fidelity == 1]
        merged = dict(zip(m_old.ts.astype(int), m_old.p.astype(float)))
        merged.update(pts)
        ts_sorted = sorted(merged)
        n_m = len(ts_sorted)
        base = df.iloc[0]
        mt = pa.table({
            'venue': pa.array([base.venue] * n_m, pa.string()), 'event_id': pa.array([base.event_id] * n_m, pa.string()),
            'market_id': pa.array([base.market_id] * n_m, pa.string()), 'token_id': pa.array([tok] * n_m, pa.string()),
            'outcome': pa.array([base.outcome] * n_m, pa.string()), 'outcome_label': pa.array([base.outcome_label] * n_m, pa.string()),
            'ts': pa.array(ts_sorted, pa.int64()), 'p': pa.array([merged[t] for t in ts_sorted], pa.float64()),
            'fidelity': pa.array([1] * n_m, pa.int16())})
        htbl = pa.Table.from_pandas(h, preserve_index=False).cast(mt.schema)
        pq.write_table(pa.concat_tables([htbl, mt]), part, compression='zstd')
        tm['minute_window'] = [new_start, eff_end]
        tm['n_minute'] = n_m
        tm['calls'] = tm['calls'] + calls
        tm['minute_reanchored'] = True
        tm['last_ts'] = max(tm['last_ts'] or 0, max(ts_sorted) if ts_sorted else 0)
        repulled.append((tm['event_id'], tm['outcome'], tm['outcome_label'], tok[:10], dt.datetime.fromtimestamp(old_start, UTC).date().isoformat(),
                         dt.datetime.fromtimestamp(new_start, UTC).date().isoformat(), n_m, calls))
        print('reanchored', repulled[-1], flush=True)
    if closed and end_ts is not None and last_h + 3600 < end_ts - 2 * 86400:
        anomalies.append(f'token {tok[:12]}.. {tm["event_id"]} | {q_by_mkt.get(tm["market_id"], "")!r} [{tm["outcome_label"]}]: series ends '
                         f'{dt.datetime.fromtimestamp(last_h, UTC).date()} , {round((end_ts - last_h) / 86400, 1)} days before nominal end '
                         f'{dt.datetime.fromtimestamp(end_ts, UTC).date()} (dead/early-resolved market); minute window re-anchored on actual last observation')
    if closed and end_ts is not None and last_h > end_ts + 3 * 86400:
        anomalies.append(f'token {tok[:12]}.. {tm["event_id"]} | {q_by_mkt.get(tm["market_id"], "")!r} [{tm["outcome_label"]}]: prices extend '
                         f'{round((last_h - end_ts) / 86400, 1)} days past nominal end {dt.datetime.fromtimestamp(end_ts, UTC).date()} '
                         f'(post-resolution tail kept as-is; trim at fomc_date+1d downstream)')
print(f'reanchored tokens: {len(repulled)}', flush=True)

# ---------------------------------------------------------------- 2. rebuild prices.parquet
parts = [f'{PARTS}/{t}.parquet' for t in tok_meta]
con = duckdb.connect()
con.execute(f"""COPY (SELECT * FROM read_parquet({json.dumps(parts)}) ORDER BY event_id, market_id, token_id, fidelity, ts)
                TO '{OUT}/prices.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)""")
P = f"read_parquet('{OUT}/prices.parquet')"
stats = con.execute(f"""SELECT fidelity, count(*) n, count(DISTINCT token_id), count(DISTINCT market_id), count(DISTINCT event_id), min(ts), max(ts)
                        FROM {P} GROUP BY 1 ORDER BY 1""").fetchall()
dups = con.execute(f"SELECT count(*) FROM (SELECT token_id, fidelity, ts FROM {P} GROUP BY 1,2,3 HAVING count(*)>1)").fetchone()[0]
oob = con.execute(f"SELECT count(*), min(p), max(p) FROM {P} WHERE p<0 OR p>1").fetchone()
if oob[0]:
    anomalies.append(f'prices: {oob[0]} rows have p outside [0,1] (min {oob[1]}, max {oob[2]}); Polymarket midpoint artefact, kept raw')
yn = con.execute(f"""
    WITH h AS (SELECT market_id, outcome_label, ts//3600 AS hr, avg(p) p FROM {P}
               WHERE fidelity=60 AND outcome IN ('CUT50P','CUT25','HOLD','HIKE25','HIKE50P') GROUP BY 1,2,3),
    j AS (SELECT y.market_id, y.hr, y.p+n.p AS s FROM h y JOIN h n USING(market_id,hr) WHERE y.outcome_label='Yes' AND n.outcome_label='No')
    SELECT count(*), count(DISTINCT market_id), median(abs(s-1)), quantile_cont(abs(s-1),0.95), max(abs(s-1)),
           sum(CASE WHEN abs(s-1)>0.05 THEN 1 ELSE 0 END) FROM j""").fetchone()
ynm = con.execute(f"""
    WITH h AS (SELECT market_id, outcome_label, ts//60 AS mn, avg(p) p FROM {P}
               WHERE fidelity=1 AND outcome IN ('CUT50P','CUT25','HOLD','HIKE25','HIKE50P') GROUP BY 1,2,3),
    j AS (SELECT y.market_id, y.mn, y.p+n.p AS s FROM h y JOIN h n USING(market_id,mn) WHERE y.outcome_label='Yes' AND n.outcome_label='No')
    SELECT count(*), count(DISTINCT market_id), median(abs(s-1)), quantile_cont(abs(s-1),0.95), max(abs(s-1)),
           sum(CASE WHEN abs(s-1)>0.05 THEN 1 ELSE 0 END) FROM j""").fetchone()
# per-event coverage summary for manifest
cov = con.execute(f"""
    SELECT event_id, fidelity, count(*) n, count(DISTINCT token_id) tokens, min(ts) t0, max(ts) t1 FROM {P} GROUP BY 1,2 ORDER BY 1,2""").fetchall()
coverage = {}
for e, f, n, tk, t0, t1 in cov:
    coverage.setdefault(e, {})[f'fid{f}'] = dict(rows=n, tokens=tk, first=dt.datetime.fromtimestamp(t0, UTC).isoformat(timespec='minutes'),
                                                 last=dt.datetime.fromtimestamp(t1, UTC).isoformat(timespec='minutes'))

# ---------------------------------------------------------------- 3. markets.parquet: fix has_prices
rows_by_tok = {t: (v['n_hourly'] + v['n_minute']) for t, v in tok_meta.items()}
mdf['yes_token_id_n_rows'] = mdf['yes_token_id'].map(lambda t: rows_by_tok.get(t, 0) if t else 0)
mdf['no_token_id_n_rows'] = mdf['no_token_id'].map(lambda t: rows_by_tok.get(t, 0) if t else 0)
mdf['all_tokens_n_rows'] = mdf['token_ids'].map(lambda s: sum(rows_by_tok.get(t, 0) for t in json.loads(s)))
mdf['has_prices'] = mdf['all_tokens_n_rows'] > 0
mdf['fetched_at'] = NOW
pq.write_table(pa.Table.from_pandas(mdf, preserve_index=False), f'{OUT}/markets.parquet', compression='zstd')

# ---------------------------------------------------------------- 4. extra structural anomalies
for r in mdf[mdf.volume.isna()].itertuples():
    anomalies.append(f'market {r.event_id} | {r.question!r}: inventory vol is null (gamma volumeNum={r.gamma_volume})')
for r in mdf.itertuples():
    if r.fomc_date and r.end_date and r.end_date < r.fomc_date:
        anomalies.append(f'market {r.event_id} | {r.question!r}: end_date {r.end_date} precedes FOMC decision {r.fomc_date}; minute window still covers decision because endTs is ignored')
anomalies.append("event fed-decision-in-september-568: low-volume non-negRisk mirror of fed-decision-in-september-762 (same 5 questions, Sep 2026); "
                 "trading stopped 2026-05-11; keep 762 as the primary Sep-2026 event")
anomalies.append("mapping: 18 '25+ bps hike' legs (2024-03 .. 2026-04 4-leg events) carry canonical HIKE50P with outcome_detail=HIKE25P per parent rule; "
                 "they mean ANY hike (>=25bp), so compare against Kalshi H25+H26, not H26 alone")
anomalies.append("mapping: exact-size legs 'decrease by 50 bps' (CUT50) and 'decrease by 75+ bps' (CUT75P) in Nov-2024/Dec-2024/Jan-2025 both map to CUT50P; "
                 "2022-23 'increase by 50/75/100 bps' legs all map to HIKE50P; aggregate by (event_id, outcome) or use outcome_detail")
anomalies.append("7 events from 2022 (Mar/May/Jun/Jul/Sep/Nov/Dec) are FPMM/AMM markets (enableOrderBook=False): CLOB prices-history is empty for all 19 tokens; "
                 "metadata kept in markets.parquet, no price rows")
anomalies.append("fed-interest-rates-february-2023 / march-2023 / may-2023: CLOB series run to 2023-05-17, i.e. 16-106 days past the decision (post-resolution tail kept)")
anomalies.append("CLOB endpoint: no ~20k-point cap observed (single calls returned up to 191k minute points); endTs confirmed ignored; "
                 "minute pulls therefore mostly needed 1 call + 1 empty confirmation call")
# dedupe anomalies while keeping order
seen, dedup = set(), []
for a in anomalies:
    if a not in seen:
        seen.add(a); dedup.append(a)
anomalies = dedup

# ---------------------------------------------------------------- 5. manifest (slim tokens)
slim = {}
for t, v in tok_meta.items():
    slim[t] = dict(event_id=v['event_id'], market_id=v['market_id'], outcome=v['outcome'], outcome_label=v['outcome_label'],
                   n_hourly=v['n_hourly'], n_minute=v['n_minute'], calls=v['calls'], first_ts=v['first_ts'], last_ts=v['last_ts'],
                   hourly_startTs=v['hourly_startTs'], minute_window=v['minute_window'], minute_reanchored=v.get('minute_reanchored', False),
                   hourly_rechunked=v['hourly_downsampled_rechunked'], enable_order_book=v['enable_order_book'], closed=v['closed'])
by_outcome = mdf.groupby('outcome').size().to_dict()
by_detail = mdf.groupby(['outcome', 'outcome_detail']).size().reset_index().values.tolist()
man.update(dict(
    run_finished_utc=dt.datetime.now(UTC).isoformat(timespec='seconds'),
    collection_policy=dict(man['collection_policy'], minute=('fidelity=1 over the 45 days before the effective end: min(nominal end+1d, last hourly obs+1h) '
                                                             'for closed markets, now for open markets; walked in 14-day chunks only if a response stops short; dedupe by t')),
    counts=dict(events=int(mdf.event_id.nunique()), markets=len(mdf), tokens_planned=len(tok_meta),
                tokens_with_data=sum(1 for v in tok_meta.values() if v['n_hourly'] + v['n_minute'] > 0),
                tokens_empty=sum(1 for v in tok_meta.values() if v['n_hourly'] + v['n_minute'] == 0),
                markets_with_prices=int(mdf.has_prices.sum()), markets_without_prices=int((~mdf.has_prices).sum()),
                events_with_prices=int(mdf[mdf.has_prices].event_id.nunique()),
                prices_rows_total=sum(s[1] for s in stats), prices_rows_hourly=next((s[1] for s in stats if s[0] == 60), 0),
                prices_rows_minute=next((s[1] for s in stats if s[0] == 1), 0), duplicate_keys=dups,
                markets_by_outcome={k: int(v) for k, v in by_outcome.items()}, markets_by_outcome_detail=[[a, b, int(c)] for a, b, c in by_detail],
                clob_calls=sum(v['calls'] for v in tok_meta.values()), gamma_calls=55, minute_windows_reanchored=len(repulled)),
    price_stats=[dict(fidelity=s[0], rows=s[1], tokens=s[2], markets=s[3], events=s[4], min_ts=s[5], max_ts=s[6],
                      min_utc=dt.datetime.fromtimestamp(s[5], UTC).isoformat(), max_utc=dt.datetime.fromtimestamp(s[6], UTC).isoformat()) for s in stats],
    yes_no_consistency=dict(
        hourly=dict(n_pairs=yn[0], markets=yn[1], median_abs_dev=yn[2], p95_abs_dev=yn[3], max_abs_dev=yn[4], n_dev_gt_5c=yn[5]),
        minute=dict(n_pairs=ynm[0], markets=ynm[1], median_abs_dev=ynm[2], p95_abs_dev=ynm[3], max_abs_dev=ynm[4], n_dev_gt_5c=ynm[5])),
    event_coverage=coverage,
    tokens=slim,
    anomalies=anomalies,
))
json.dump(man, open(f'{OUT}/manifest.json', 'w'), indent=1, default=str)
json.dump(tok_meta, open(f'{PARTS}/_meta.json', 'w'))
print('COUNTS', json.dumps(man['counts'], default=str))
print('YN hourly', yn); print('YN minute', ynm)
print('ANOMALIES', len(anomalies))
print('manifest bytes', os.path.getsize(f'{OUT}/manifest.json'))
