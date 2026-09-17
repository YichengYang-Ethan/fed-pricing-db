"""Polymarket Fed collector: Gamma metadata + CLOB price history -> raw/poly/{prices,markets}.parquet + manifest.json.

Resumable: per-token price pulls are cached as parquet parts in SCRATCH/poly_parts/.
"""
import json, os, sys, time, glob, datetime as dt
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from poly_common import *
import pyarrow as pa, pyarrow.parquet as pq

OUT = f'{ROOT}/raw/poly'
PARTS = f'{SCRATCH}/poly_parts'
os.makedirs(OUT, exist_ok=True); os.makedirs(PARTS, exist_ok=True)
NOW = int(time.time())
RUN_STARTED = dt.datetime.now(UTC).isoformat(timespec='seconds')
CANON5 = {'CUT50P', 'CUT25', 'HOLD', 'HIKE25', 'HIKE50P'}
CLOB_SLEEP = 0.35      # ~3 req/s
GAMMA_SLEEP = 0.5
CAP_HINT = 19900       # parent said ~20k cap; verified NOT binding today but guard kept
anomalies = []
def anom(msg):
    anomalies.append(msg); print('ANOMALY:', msg, flush=True)

inv = json.load(open(f'{ROOT}/raw/poly_fed_inventory.json'))

# ------------------------------------------------------------------ 1. Gamma metadata
gamma_path = f'{SCRATCH}/gamma_raw.json'
gamma = json.load(open(gamma_path)) if os.path.exists(gamma_path) else {}
for slug in inv:
    if slug in gamma:
        continue
    res = http_json(f'https://gamma-api.polymarket.com/events?slug={slug}')
    time.sleep(GAMMA_SLEEP)
    if not res:
        anom(f'gamma: no event returned for slug={slug}')
        gamma[slug] = None
        continue
    if len(res) > 1:
        anom(f'gamma: {len(res)} events returned for slug={slug}; using first')
    gamma[slug] = res[0]
    json.dump(gamma, open(gamma_path, 'w'))
json.dump(gamma, open(gamma_path, 'w'))
print('gamma events cached:', sum(1 for v in gamma.values() if v), flush=True)

# ------------------------------------------------------------------ 2. markets table + token plan
def jl(s):
    return json.loads(s) if isinstance(s, str) else (s if s is not None else [])

market_rows, token_plan = [], []
for slug, ev in inv.items():
    g = gamma.get(slug) or {}
    gmk = g.get('markets') or []
    ev_start = ev.get('start') or ''
    g_start = (g.get('startDate') or g.get('creationDate') or '')[:10]
    start_ts = date_to_ts(ev_start) if ev_start else None
    if start_ts is None:
        start_ts = date_to_ts('2021-01-01')
        anom(f'event {slug}: empty start in inventory (gamma startDate={g_start!r}); using 2021-01-01 for hourly startTs')
    fomc = nearest_fomc(ev.get('end'))
    if fomc is None:
        anom(f'event {slug}: no FOMC date within 8 days of end={ev.get("end")}')
    for idx, m in enumerate(ev['markets']):
        q = m['q']
        toks, outs = jl(m.get('tokens')), jl(m.get('outcomes'))
        # match gamma market by tokens, else by question
        gm = None
        for cand in gmk:
            if toks and jl(cand.get('clobTokenIds')) == toks:
                gm = cand; break
        if gm is None:
            for cand in gmk:
                if cand.get('question', '').strip().lower() == q.strip().lower():
                    gm = cand; break
        if gm is None and gmk:
            anom(f'market {slug} | {q!r}: no matching gamma market (by tokens or question)')
        gm = gm or {}
        if not toks:
            gt = jl(gm.get('clobTokenIds'))
            if gt:
                toks = gt
                anom(f'market {slug} | {q!r}: inventory tokens null; recovered clobTokenIds from gamma')
            else:
                anom(f'market {slug} | {q!r}: tokens null in inventory and gamma -> no prices collectable')
        if not outs:
            outs = jl(gm.get('outcomes'))
        can, det, note = map_outcome(q)
        if note and can == 'OTHER' and det in ('UNMAPPED',):
            anom(f'market {slug} | {q!r}: UNMAPPED question -> OTHER')
        elif note and det == 'HIKE25P':
            pass  # summarized once below
        elif note and can == 'OTHER':
            pass  # expected OTHER families; summarized in manifest
        cond = gm.get('conditionId') or ''
        market_id = cond if cond else f'{slug}#{idx}'
        if not cond:
            anom(f'market {slug} | {q!r}: no conditionId from gamma; market_id={market_id}')
        yes_i = outs.index('Yes') if 'Yes' in outs else None
        no_i = outs.index('No') if 'No' in outs else None
        if yes_i is None:
            anom(f'market {slug} | {q!r}: outcomes {outs} have no "Yes"; collecting all tokens with their labels')
        end_date = m.get('end') or ev.get('end')
        end_ts = date_to_ts(end_date) if end_date else None
        g_end = gm.get('endDate') or ''
        closed_time = gm.get('closedTime') or ''
        op = jl(gm.get('outcomePrices'))
        eob = gm.get('enableOrderBook')
        market_rows.append(dict(
            venue='polymarket', event_id=slug, market_id=market_id, outcome=can, outcome_detail=det,
            mapping_note=note or '', ts=end_ts if end_ts is not None else -1,
            question=q, event_title=ev.get('title', ''), gamma_event_id=str(g.get('id', '')),
            gamma_market_id=str(gm.get('id', '')), market_slug=gm.get('slug', ''), condition_id=cond,
            question_id=gm.get('questionID', ''), outcomes=json.dumps(outs), token_ids=json.dumps(toks),
            yes_token_id=toks[yes_i] if (yes_i is not None and yes_i < len(toks)) else '',
            no_token_id=toks[no_i] if (no_i is not None and no_i < len(toks)) else '',
            start_date=ev_start, gamma_start_date=gm.get('startDate', '') or '', end_date=end_date or '',
            gamma_end_date=g_end, closed_time=closed_time, fomc_date=fomc or '',
            closed=bool(m.get('closed')), event_closed=bool(ev.get('closed')),
            volume=float(m['vol']) if m.get('vol') is not None else None,
            gamma_volume=float(gm['volumeNum']) if gm.get('volumeNum') not in (None, '') else None,
            gamma_liquidity=float(gm['liquidityNum']) if gm.get('liquidityNum') not in (None, '') else None,
            enable_order_book=bool(eob) if eob is not None else None,
            neg_risk=bool(g.get('negRisk') or gm.get('negRisk') or False),
            final_outcome_prices=json.dumps(op), uma_resolution_status=gm.get('umaResolutionStatus', '') or '',
            resolution_source=gm.get('resolutionSource', '') or g.get('resolutionSource', '') or '',
            description=(gm.get('description') or '')[:2000],
        ))
        # token plan
        if not toks:
            continue
        for ti, tok in enumerate(toks):
            label = outs[ti] if ti < len(outs) else f'idx{ti}'
            is_yes = (ti == yes_i)
            collect = is_yes or (yes_i is None) or (can in CANON5 and ti == no_i)
            if not collect:
                continue
            token_plan.append(dict(slug=slug, market_id=market_id, token_id=tok, outcome=can, label=label,
                                   start_ts=start_ts, end_ts=end_ts, closed=bool(m.get('closed')),
                                   eob=eob, q=q))

print(f'markets={len(market_rows)} tokens_planned={len(token_plan)}', flush=True)

# ------------------------------------------------------------------ 3. price pulls
def pull(tok, start_ts, fidelity):
    url = f'https://clob.polymarket.com/prices-history?market={tok}&startTs={start_ts}&fidelity={fidelity}'
    res = http_json(url)
    time.sleep(CLOB_SLEEP)
    return res.get('history', []) if isinstance(res, dict) else []

def pull_window(tok, start_ts, window_end, fidelity, step_s, tag):
    """Pull [start_ts, window_end) at given fidelity. endTs is ignored by the API, so we walk forward in
    `step_s` chunks only while the previous response stopped short of the window (dedupe by t)."""
    pts, calls, chunks = {}, 0, []
    cur = start_ts
    while cur < window_end:
        h = pull(tok, cur, fidelity); calls += 1
        n = len(h)
        chunks.append(dict(startTs=cur, n=n, last_t=h[-1]['t'] if h else None))
        for x in h:
            pts[int(x['t'])] = float(x['p'])
        if n == 0:
            break
        last_t = int(h[-1]['t'])
        if last_t >= window_end - 1:          # response already spans the whole window (no cap hit)
            break
        if n < CAP_HINT and last_t < cur + step_s:  # short response that ended early = series ended (market dead)
            break
        cur = max(last_t + 1, cur + step_s) if n >= CAP_HINT else cur + step_s
    return pts, calls, chunks

tok_meta = {}
meta_path = f'{PARTS}/_meta.json'
if os.path.exists(meta_path):
    tok_meta = json.load(open(meta_path))

for i, tp in enumerate(token_plan):
    tok = tp['token_id']
    part = f'{PARTS}/{tok}.parquet'
    if os.path.exists(part) and tok in tok_meta:
        continue
    t0 = time.time()
    # (a) hourly over full life
    hstart = tp['start_ts']
    h_pts, h_calls, h_chunks = {}, 0, []
    h = pull(tok, hstart, 60); h_calls += 1
    for x in h:
        h_pts[int(x['t'])] = float(x['p'])
    h_chunks.append(dict(startTs=hstart, n=len(h), last_t=h[-1]['t'] if h else None))
    downsampled = False
    if len(h) >= CAP_HINT:
        downsampled = True
        h_pts = {}
        cur = hstart
        while cur < NOW:
            hh = pull(tok, cur, 60); h_calls += 1
            h_chunks.append(dict(startTs=cur, n=len(hh), last_t=hh[-1]['t'] if hh else None))
            for x in hh:
                h_pts[int(x['t'])] = float(x['p'])
            cur += 120 * 86400
    # (b) minute over last 45 days before end (or before now if open)
    if tp['closed'] and tp['end_ts'] is not None:
        wend = tp['end_ts'] + 86400          # include the whole end date (endTs ignored anyway)
    else:
        wend = NOW
    mstart = wend - 45 * 86400
    if h_pts:
        mstart = max(mstart, min(h_pts) - 3600)  # do not start before the series exists
    m_pts, m_calls, m_chunks = pull_window(tok, mstart, wend, 1, 14 * 86400, 'min')
    rows_ts, rows_p, rows_f = [], [], []
    for t, p in sorted(h_pts.items()):
        rows_ts.append(t); rows_p.append(p); rows_f.append(60)
    for t, p in sorted(m_pts.items()):
        rows_ts.append(t); rows_p.append(p); rows_f.append(1)
    n = len(rows_ts)
    tbl = pa.table({
        'venue': pa.array(['polymarket'] * n, pa.string()),
        'event_id': pa.array([tp['slug']] * n, pa.string()),
        'market_id': pa.array([tp['market_id']] * n, pa.string()),
        'token_id': pa.array([tok] * n, pa.string()),
        'outcome': pa.array([tp['outcome']] * n, pa.string()),
        'outcome_label': pa.array([tp['label']] * n, pa.string()),
        'ts': pa.array(rows_ts, pa.int64()),
        'p': pa.array(rows_p, pa.float64()),
        'fidelity': pa.array(rows_f, pa.int16()),
    })
    pq.write_table(tbl, part, compression='zstd')
    tok_meta[tok] = dict(
        event_id=tp['slug'], market_id=tp['market_id'], outcome=tp['outcome'], outcome_label=tp['label'],
        n_hourly=len(h_pts), n_minute=len(m_pts), calls=h_calls + m_calls,
        hourly_startTs=hstart, hourly_downsampled_rechunked=downsampled, hourly_chunks=h_chunks,
        minute_window=[mstart, wend], minute_chunks=m_chunks,
        first_ts=min(rows_ts) if rows_ts else None, last_ts=max(rows_ts) if rows_ts else None,
        enable_order_book=tp['eob'], closed=tp['closed'], secs=round(time.time() - t0, 1))
    if n == 0:
        why = 'AMM/FPMM-era market (enableOrderBook=False), no CLOB history' if tp['eob'] is False else 'unknown'
        anom(f'token {tok[:12]}.. {tp["slug"]} | {tp["q"]!r} [{tp["label"]}]: EMPTY price history ({why})')
    if i % 10 == 0 or n == 0:
        json.dump(tok_meta, open(meta_path, 'w'))
    print(f'[{i+1}/{len(token_plan)}] {tp["slug"][:40]:40s} {tp["outcome"]:7s} {tp["label"]:4s} h={len(h_pts):5d} m={len(m_pts):6d} calls={h_calls+m_calls} {time.time()-t0:.1f}s', flush=True)
json.dump(tok_meta, open(meta_path, 'w'))

# ------------------------------------------------------------------ 4. assemble outputs
parts = [f'{PARTS}/{tp["token_id"]}.parquet' for tp in token_plan if os.path.exists(f'{PARTS}/{tp["token_id"]}.parquet')]
import duckdb
con = duckdb.connect()
con.execute(f"""
    COPY (SELECT * FROM read_parquet({json.dumps(parts)}) ORDER BY event_id, market_id, token_id, fidelity, ts)
    TO '{OUT}/prices.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
""")
stats = con.execute(f"""
    SELECT fidelity, count(*) n, count(DISTINCT token_id) tokens, count(DISTINCT market_id) markets,
           count(DISTINCT event_id) events, min(ts) mints, max(ts) maxts
    FROM read_parquet('{OUT}/prices.parquet') GROUP BY 1 ORDER BY 1
""").fetchall()
dups = con.execute(f"SELECT count(*) FROM (SELECT token_id, fidelity, ts FROM read_parquet('{OUT}/prices.parquet') GROUP BY 1,2,3 HAVING count(*)>1)").fetchone()[0]
# yes+no consistency (hourly, bucketed to the hour) for canonical legs with both tokens
yn = con.execute(f"""
    WITH h AS (SELECT market_id, outcome_label, ts//3600 AS hr, avg(p) p FROM read_parquet('{OUT}/prices.parquet')
               WHERE fidelity=60 AND outcome IN ('CUT50P','CUT25','HOLD','HIKE25','HIKE50P') GROUP BY 1,2,3),
    j AS (SELECT y.market_id, y.hr, y.p+n.p AS s FROM h y JOIN h n USING(market_id,hr) WHERE y.outcome_label='Yes' AND n.outcome_label='No')
    SELECT count(*) n_pairs, count(DISTINCT market_id) markets, median(abs(s-1)) med_abs_dev, quantile_cont(abs(s-1),0.95) p95_abs_dev,
           max(abs(s-1)) max_abs_dev, sum(CASE WHEN abs(s-1)>0.05 THEN 1 ELSE 0 END) n_dev_gt_5c FROM j
""").fetchone()

# markets parquet
import pandas as pd
mdf = pd.DataFrame(market_rows)
for tok_col in ('yes_token_id', 'no_token_id'):
    mdf[tok_col + '_n_rows'] = mdf[tok_col].map(lambda t: (tok_meta.get(t, {}).get('n_hourly', 0) + tok_meta.get(t, {}).get('n_minute', 0)) if t else 0)
mdf['has_prices'] = (mdf['yes_token_id_n_rows'] + mdf['no_token_id_n_rows']) > 0
mdf['fetched_at'] = NOW
mtbl = pa.Table.from_pandas(mdf, preserve_index=False)
pq.write_table(mtbl, f'{OUT}/markets.parquet', compression='zstd')

# manifest
by_outcome = mdf.groupby('outcome').size().to_dict()
by_detail = mdf.groupby(['outcome', 'outcome_detail']).size().reset_index().values.tolist()
split_leg_events = sorted({r['event_id'] for r in market_rows} & {
    e for e, grp in mdf[mdf.outcome.isin(CANON5)].groupby('event_id') if grp.outcome.duplicated().any()})
manifest = dict(
    source='polymarket', run_started_utc=RUN_STARTED, run_finished_utc=dt.datetime.now(UTC).isoformat(timespec='seconds'),
    endpoints=dict(prices='https://clob.polymarket.com/prices-history?market={token}&startTs={ts}&fidelity={60|1}',
                   gamma='https://gamma-api.polymarket.com/events?slug={slug}'),
    endpoint_behaviour=dict(
        endTs_ignored=True, interval_max_never_used=True,
        cap_20k_observed=False,
        note=('Probe on 2026-09-17: fidelity=1 from event start returned 180,811 and 191,492 points in single calls, so no '
              '~20k cap was binding; guard kept (>=19,900 hourly points -> 120-day re-chunk). Minute pulls walk 14-day '
              'chunks only when a response stops short of the window; dedupe by t.')),
    files=dict(prices='raw/poly/prices.parquet', markets='raw/poly/markets.parquet'),
    schema=dict(
        prices=['venue', 'event_id(=slug)', 'market_id(=conditionId)', 'token_id', 'outcome(canonical)', 'outcome_label(Yes/No or label)',
                'ts(int64 UTC epoch s)', 'p(float64)', 'fidelity(int16: 60 hourly, 1 minute)'],
        markets_ts='ts = market end date (UTC midnight epoch); -1 if unknown'),
    outcome_mapping=dict(
        canonical_enum=['CUT50P', 'CUT25', 'HOLD', 'HIKE25', 'HIKE50P', 'OTHER'],
        rules={'decrease by 50+ bps': 'CUT50P', 'decrease by 50 bps (exact)': 'CUT50P (detail CUT50)', 'decrease by 75+ bps': 'CUT50P (detail CUT75P)',
               'decrease by 25 bps': 'CUT25', 'no change / raise|increase by 0 bps': 'HOLD', 'increase|raise by 25 bps': 'HIKE25',
               'increase|raise by 25+ bps (2024-26 wording, any hike)': 'HIKE50P (detail HIKE25P) per parent instruction; raw question kept',
               'increase by 50+ / 50 / 75 / 100 bps': 'HIKE50P (detail HIKE50P/HIKE50/HIKE75/HIKE100)',
               'three-decision path, dissent combos, individual dissent, favored-leg, catch-all, cumulative threshold': 'OTHER (see outcome_detail)'},
        events_with_multiple_legs_sharing_a_canonical_outcome=split_leg_events,
        warning=('For events listed above, downstream must aggregate legs by (event_id, outcome) or use outcome_detail; '
                 'HIKE25P legs mean ANY hike, not >=50bp.')),
    collection_policy=dict(yes_token='all markets', no_token='only markets whose canonical outcome is one of the 5 legs',
                           non_yes_no_markets='all tokens collected with outcome_label = the listed outcome name',
                           hourly='fidelity=60 from inventory event start (2021-01-01 if empty) to now',
                           minute='fidelity=1 over [end-45d, end+1d) for closed markets, [now-45d, now) for open ones'),
    counts=dict(events=len(inv), markets=len(market_rows), tokens_planned=len(token_plan), tokens_with_data=sum(1 for v in tok_meta.values() if (v['n_hourly'] + v['n_minute']) > 0),
                tokens_empty=sum(1 for v in tok_meta.values() if (v['n_hourly'] + v['n_minute']) == 0),
                markets_with_prices=int(mdf.has_prices.sum()), markets_without_prices=int((~mdf.has_prices).sum()),
                prices_rows_total=sum(s[1] for s in stats), prices_rows_hourly=next((s[1] for s in stats if s[0] == 60), 0),
                prices_rows_minute=next((s[1] for s in stats if s[0] == 1), 0), duplicate_keys=dups,
                markets_by_outcome=by_outcome, markets_by_outcome_detail=[[a, b, int(c)] for a, b, c in by_detail],
                clob_calls=sum(v['calls'] for v in tok_meta.values()), gamma_calls=len(inv)),
    price_stats=[dict(fidelity=s[0], rows=s[1], tokens=s[2], markets=s[3], events=s[4], min_ts=s[5], max_ts=s[6]) for s in stats],
    yes_no_consistency_hourly=dict(n_pairs=yn[0], markets=yn[1], median_abs_dev=yn[2], p95_abs_dev=yn[3], max_abs_dev=yn[4], n_dev_gt_5c=yn[5]),
    tokens=tok_meta,
    anomalies=anomalies,
)
json.dump(manifest, open(f'{OUT}/manifest.json', 'w'), indent=1, default=str)
print('DONE', json.dumps(manifest['counts'], default=str), flush=True)
print('YES+NO', yn)
print('ANOMALIES', len(anomalies))
