"""Shared helpers for the Polymarket Fed collector. Outputs land in raw/poly/."""
import json, os, re, time, urllib.request, urllib.error, datetime as dt

UTC = dt.timezone.utc
UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/128.0 Safari/537.36')
# ROOT defaults to this checkout; override with FED_DB_ROOT to run it anywhere.
ROOT = os.environ.get('FED_DB_ROOT') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Resumable per-token parquet parts land here. A session scratchpad under /private/tmp gets
# wiped by macOS, which loses a multi-hour pull, so this defaults inside the checkout.
SCRATCH = os.environ.get('FED_DB_SCRATCH') or os.path.join(ROOT, 'raw', '_parts')
FOMC = ["2022-01-26","2022-03-16","2022-05-04","2022-06-15","2022-07-27","2022-09-21","2022-11-02","2022-12-14",
        "2023-02-01","2023-03-22","2023-05-03","2023-06-14","2023-07-26","2023-09-20","2023-11-01","2023-12-13",
        "2024-01-31","2024-03-20","2024-05-01","2024-06-12","2024-07-31","2024-09-18","2024-11-07","2024-12-18",
        "2025-01-29","2025-03-19","2025-05-07","2025-06-18","2025-07-30","2025-09-17","2025-10-29","2025-12-10",
        "2026-01-28","2026-03-18","2026-04-29","2026-06-17","2026-07-29","2026-09-16","2026-10-28","2026-12-09",
        "2027-01-27","2027-03-17","2027-04-28","2027-06-09","2027-07-28","2027-09-15","2027-10-27","2027-12-08",
        "2028-01-26"]


def http_json(url, tries=3, backoff=5.0, timeout=90):
    """GET JSON with browser UA; retry on 429/5xx/network errors."""
    last = None
    for i in range(tries):
        req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last = f'HTTP {e.code}'
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                continue
            raise
        except Exception as e:  # network hiccup
            last = repr(e)
            time.sleep(backoff)
    raise RuntimeError(f'GET failed after {tries} tries: {url} ({last})')


def date_to_ts(s):
    """'YYYY-MM-DD' or ISO datetime -> UTC epoch seconds; None/'' -> None."""
    if not s:
        return None
    s = s.strip()
    if len(s) == 10:
        return int(dt.datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=UTC).timestamp())
    s = s.replace('Z', '+00:00')
    if ' ' in s and 'T' not in s:
        s = s.replace(' ', 'T')
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return int(d.timestamp())


def nearest_fomc(date_str, tol_days=8):
    if not date_str:
        return None
    d = dt.date.fromisoformat(date_str[:10])
    best = min(FOMC, key=lambda f: abs((dt.date.fromisoformat(f) - d).days))
    return best if abs((dt.date.fromisoformat(best) - d).days) <= tol_days else None


_DASH = '[–—-]'


def map_outcome(q):
    """Map a Polymarket question to (canonical, detail, note).

    canonical in {CUT50P, CUT25, HOLD, HIKE25, HIKE50P, OTHER}; detail is the finer raw bucket.
    """
    s = q.lower().replace('≤', '<=').replace('≥', '>=')
    s = re.sub(_DASH, '-', s)
    # --- OTHER families first ---------------------------------------------------------
    if 'dissent' in s or 'fomc decision result in' in s:
        if 'fomc decision result in' in s:
            m = re.search(r'result in (no change|a 25bp cut|any other outcome) ?(?:with (<=2|>2|<2|>=2) dissents)?', s)
            leg = m.group(1) if m else 'unknown'
            dis = m.group(2) if m and m.group(2) else ''
            legc = {'no change': 'HOLD', 'a 25bp cut': 'CUT25', 'any other outcome': 'OTHER'}.get(leg, 'UNK')
            return 'OTHER', f'COMBO_{legc}' + (f'_DISSENT{dis}' if dis else ''), 'decision x dissent combo'
        return 'OTHER', 'DISSENT_INDIVIDUAL', 'individual dissent market'
    if 'next three decisions' in s or re.search(r'\b(cut|pause|hike)-(cut|pause|hike)-(cut|pause|hike)\b', s) \
            or 'decide differently' in s:
        m = re.search(r'\b(cut|pause|hike)-(cut|pause|hike)-(cut|pause|hike)\b', s)
        if m:
            return 'OTHER', 'PATH_' + '_'.join(x.upper() for x in m.groups()), 'three-decision path market'
        return 'OTHER', 'PATH_OTHER', 'three-decision path catch-all'
    if 'favored for fed decision' in s:
        return 'OTHER', 'ODDS_FAVORED_2WAY', 'which-leg-favored market; outcomes are not Yes/No'
    if 'change rates to another level' in s:
        return 'OTHER', 'CATCHALL_OTHER_LEVEL', 'residual catch-all leg'
    if 'set interest rates above' in s:
        m = re.search(r'above ([0-9.]+)%', s)
        m2 = re.search(r'\((\d+) bps or more\)', s)
        det = f'THRESHOLD_ABOVE_{m.group(1)}PCT' if m else 'THRESHOLD_ABOVE'
        if m2:
            det += f'_HIKE_GE{m2.group(1)}'
        return 'OTHER', det, 'cumulative threshold market (not a single-size bucket)'
    # --- canonical families -------------------------------------------------------------
    if re.search(r'\bno change\b', s):
        return 'HOLD', 'HOLD', None
    m = re.search(r'\b(decrease|decreases|cut|cuts|lower|lowers)\b.*?\bby (\d+)(\+?) ?bps?', s)
    if m:
        n, plus = int(m.group(2)), m.group(3) == '+'
        if n == 25 and not plus:
            return 'CUT25', 'CUT25', None
        if n == 25 and plus:
            return 'OTHER', 'CUT25P', 'cut 25+ spans CUT25 and CUT50P'
        if n >= 50:
            return 'CUT50P', f'CUT{n}' + ('P' if plus else ''), None
        return 'OTHER', f'CUT{n}' + ('P' if plus else ''), 'unusual cut size'
    m = re.search(r'\b(increase|increases|raise|raises|hike|hikes)\b.*?\bby (\d+)(\+?) ?bps?', s)
    if m:
        n, plus = int(m.group(2)), m.group(3) == '+'
        if n == 0:
            return 'HOLD', 'HOLD_0BP', None
        if n == 25 and not plus:
            return 'HIKE25', 'HIKE25', None
        if n == 25 and plus:
            # 2024-2026 wording: '25+ bps hike' = any hike; parent instruction: canonical HIKE50P, detail HIKE25P
            return 'HIKE50P', 'HIKE25P', "'25+ bps hike' leg (any hike) mapped to HIKE50P per canonical rule; detail=HIKE25P"
        if n >= 50:
            return 'HIKE50P', f'HIKE{n}' + ('P' if plus else ''), None
        return 'OTHER', f'HIKE{n}' + ('P' if plus else ''), 'unusual hike size'
    return 'OTHER', 'UNMAPPED', 'ambiguous question text'
