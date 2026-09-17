#!/usr/bin/env python3
"""Build raw/meetings.parquet: one row per FOMC decision date 2022-2028 with venue cross-references.

Links each meeting to its Kalshi event (by month), Polymarket slugs (by end date), the ZQ contract for the
meeting month, and the delta = fraction of the month after the effective date. Cross-checks venue close/end
dates against the FOMC date and the Kalshi settled leg against the realized FRED target change.
"""
import calendar, json, re, sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

ROOT = Path("/Users/ethanyang/Developer/fed-pricing-db")
RAW = ROOT / "raw"
FOMC = ["2022-01-26","2022-03-16","2022-05-04","2022-06-15","2022-07-27","2022-09-21","2022-11-02","2022-12-14",
        "2023-02-01","2023-03-22","2023-05-03","2023-06-14","2023-07-26","2023-09-20","2023-11-01","2023-12-13",
        "2024-01-31","2024-03-20","2024-05-01","2024-06-12","2024-07-31","2024-09-18","2024-11-07","2024-12-18",
        "2025-01-29","2025-03-19","2025-05-07","2025-06-18","2025-07-30","2025-09-17","2025-10-29","2025-12-10",
        "2026-01-28","2026-03-18","2026-04-29","2026-06-17","2026-07-29","2026-09-16","2026-10-28","2026-12-09",
        "2027-01-27","2027-03-17","2027-04-28","2027-06-09","2027-07-28","2027-09-15","2027-10-27","2027-12-08",
        "2028-01-26"]
CODES = "FGHJKMNQUVXZ"
MON = {m.upper(): i for i, m in enumerate(calendar.month_abbr) if m}
LEG2CANON = {"C26": "CUT50P", "C25": "CUT25", "H0": "HOLD", "H25": "HIKE25", "H26": "HIKE50P"}
BDAY = CustomBusinessDay(calendar=USFederalHolidayCalendar())
DECISION_TITLE = re.compile(r"^Fed [Dd]ecision in [A-Za-z]+\??$|^Fed Interest Rates:? [A-Za-z]+ \d{4}$")


def kalshi_month(ev: str):
    """'KXFEDDECISION-26OCT' -> ('2026-10', None); 'FEDDECISION-24MAR20' -> ('2024-03', 20)."""
    m = re.match(r"^(?:KX)?FEDDECISION-(\d{2})([A-Z]{3})(\d{2})?$", ev)
    if not m:
        return None, None
    yy, mon, dd = m.groups()
    return f"20{yy}-{MON[mon]:02d}", (int(dd) if dd else None)


def realized_outcome(bp):
    if bp is None or pd.isna(bp):
        return None
    if bp <= -50: return "CUT50P"
    if bp == -25: return "CUT25"
    if bp == 0: return "HOLD"
    if bp == 25: return "HIKE25"
    if bp >= 50: return "HIKE50P"
    return "OTHER"


def main():
    anomalies = []
    kalshi = json.loads((RAW / "kalshi_fed_inventory.json").read_text())
    poly = json.loads((RAW / "poly_fed_inventory.json").read_text())
    meetings = [date.fromisoformat(d) for d in FOMC]
    mset = set(meetings)
    by_month = {}
    for d in meetings:
        by_month.setdefault(f"{d.year:04d}-{d.month:02d}", []).append(d)
    for k, v in by_month.items():
        if len(v) > 1:
            anomalies.append(f"month {k} has {len(v)} FOMC dates; by-month Kalshi mapping ambiguous")

    # Kalshi: month -> event
    k_by_month = {}
    for ev, rec in kalshi.items():
        ym, dd = kalshi_month(ev)
        if ym is None:
            anomalies.append(f"kalshi event {ev}: unparseable month; skipped")
            continue
        if ym in k_by_month:
            anomalies.append(f"kalshi month {ym}: multiple events {k_by_month[ym]} and {ev}; keeping first")
            continue
        k_by_month[ym] = ev
        if ym not in by_month:
            anomalies.append(f"kalshi event {ev} ({ym}) has no FOMC meeting in the provided calendar")
        elif dd is not None and dd != by_month[ym][0].day:
            anomalies.append(f"kalshi event {ev}: ticker day {dd} != FOMC day {by_month[ym][0].day}")

    # Poly: end date -> slugs
    p_by_end = {}
    for slug, ev in poly.items():
        end = ev.get("end")
        try:
            ed = date.fromisoformat(end[:10]) if end else None
        except ValueError:
            ed = None
        if ed is None:
            anomalies.append(f"poly {slug}: missing/invalid end '{end}'; not linked")
            continue
        p_by_end.setdefault(ed, []).append(slug)
        if ed not in mset:
            near = min(meetings, key=lambda m: abs((m - ed).days))
            anomalies.append(f"poly {slug} ('{ev.get('title')}') end {ed} is not an FOMC date (nearest {near}, {(ed-near).days:+d}d); not linked")
        for mk in ev.get("markets", []):
            mend = (mk.get("end") or "")[:10]
            if mend and mend != end[:10]:
                anomalies.append(f"poly {slug} market '{mk.get('q','')[:60]}' end {mend} != event end {end[:10]}")

    # FRED target upper bound for realized change
    tgt = None
    p = RAW / "fred" / "DFEDTARU.parquet"
    if p.exists():
        tgt = pq.read_table(p).to_pandas().set_index("date")["value"]
    else:
        anomalies.append("raw/fred/DFEDTARU.parquet missing; realized_change_bp not computed")

    cme_files = {q.name for q in (RAW / "cme").glob("ZQ*.parquet")}
    zq = None
    if (RAW / "cme" / "ZQ_F.parquet").exists():
        zq = pq.read_table(RAW / "cme" / "ZQ_F.parquet").to_pandas()
        zq_months = set(zq["event_id"])
    else:
        zq_months = set()
        anomalies.append("raw/cme/ZQ_F.parquet missing; continuous coverage unknown")

    rows = []
    for d in meetings:
        ym = f"{d.year:04d}-{d.month:02d}"
        dim = calendar.monthrange(d.year, d.month)[1]
        month_end = date(d.year, d.month, dim)
        eff = (pd.Timestamp(d) + BDAY).date()
        if eff.month != d.month:
            anomalies.append(f"{d}: effective_date {eff} falls in next month; days_post set to 0")
            days_post = 0
        else:
            days_post = (month_end - eff).days + 1
        ev = k_by_month.get(ym)
        legs = kalshi.get(ev, {}).get("legs", {}) if ev else {}
        k_status = None
        k_close = None
        k_result = None
        if ev is not None:
            if not legs:
                k_status = "purged"
            else:
                statuses = {l["status"] for l in legs.values()}
                k_status = statuses.pop() if len(statuses) == 1 else "mixed:" + ",".join(sorted(statuses))
                closes = {l["close"] for l in legs.values()}
                k_close = sorted(closes)[0]
                if len(closes) > 1:
                    anomalies.append(f"kalshi {ev}: legs have differing close times {sorted(closes)}")
                for leg, l in legs.items():
                    if l["close"][:10] != d.isoformat():
                        anomalies.append(f"kalshi {l['ticker']} close {l['close']} != FOMC date {d}")
                    if leg not in LEG2CANON:
                        anomalies.append(f"kalshi {l['ticker']}: unknown leg code {leg}")
                yes = [LEG2CANON.get(leg, "OTHER") for leg, l in legs.items() if l.get("result") == "yes"]
                if len(yes) == 1:
                    k_result = yes[0]
                elif len(yes) > 1:
                    anomalies.append(f"kalshi {ev}: {len(yes)} legs settled yes {yes}")
        slugs = sorted(p_by_end.get(d, []))
        dec = [s for s in slugs if DECISION_TITLE.match(poly[s].get("title", ""))]
        other = [s for s in slugs if s not in dec]
        if len(dec) > 1:
            anomalies.append(f"{d}: {len(dec)} Polymarket single-decision events share this end date: {dec}")
        sym = f"ZQ{CODES[d.month-1]}{d.year % 100:02d}"
        cbt = f"{sym}.CBT.parquet"
        zq_source = "contract" if cbt in cme_files else "continuous_in_month"
        zq_avail = (cbt in cme_files) or (ym in zq_months)
        realized = None
        if tgt is not None and d in tgt.index and eff in tgt.index and pd.notna(tgt[d]) and pd.notna(tgt[eff]):
            realized = int(round((tgt[eff] - tgt[d]) * 100))
        r_out = realized_outcome(realized)
        if k_result is not None and r_out is not None and k_result != r_out:
            anomalies.append(f"{d}: kalshi settled {k_result} but FRED DFEDTARU change {realized}bp implies {r_out}")
        rows.append({
            "meeting_date": d, "meeting_month": ym, "kalshi_event": ev, "kalshi_status": k_status,
            "kalshi_close": k_close, "kalshi_result_outcome": k_result,
            "poly_slugs": slugs, "poly_slugs_decision": dec, "poly_slugs_other": other,
            "zq_contract_symbol": sym, "zq_source": zq_source,
            "zq_source_symbol": f"{sym}.CBT" if zq_source == "contract" else "ZQ=F", "zq_data_available": zq_avail,
            "effective_date": eff, "month_end": month_end, "days_in_month": dim, "days_post": days_post,
            "delta": days_post / dim, "realized_change_bp": realized, "realized_outcome": r_out,
        })
    schema = pa.schema([
        ("meeting_date", pa.date32()), ("meeting_month", pa.string()), ("kalshi_event", pa.string()),
        ("kalshi_status", pa.string()), ("kalshi_close", pa.string()), ("kalshi_result_outcome", pa.string()),
        ("poly_slugs", pa.list_(pa.string())), ("poly_slugs_decision", pa.list_(pa.string())),
        ("poly_slugs_other", pa.list_(pa.string())), ("zq_contract_symbol", pa.string()), ("zq_source", pa.string()),
        ("zq_source_symbol", pa.string()), ("zq_data_available", pa.bool_()), ("effective_date", pa.date32()),
        ("month_end", pa.date32()), ("days_in_month", pa.int32()), ("days_post", pa.int32()), ("delta", pa.float64()),
        ("realized_change_bp", pa.int32()), ("realized_outcome", pa.string()),
    ])
    tbl = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(tbl, RAW / "meetings.parquet", compression="zstd")
    linked_poly = sum(len(r["poly_slugs"]) for r in rows)
    manifest = {
        "file": "raw/meetings.parquet", "rows": len(rows), "schema": [f.name for f in schema],
        "effective_date_rule": "next US federal business day after the meeting (pandas USFederalHolidayCalendar)",
        "delta_rule": "days_post = days from effective_date through month end inclusive; delta = days_post / days_in_month",
        "kalshi_link": "by meeting month from raw/kalshi_fed_inventory.json (status 'purged' when legs are empty)",
        "poly_link": "raw/poly_fed_inventory.json events whose end date == FOMC date; decision vs other split by title regex",
        "zq_source": "'contract' when raw/cme/ZQ<code><yy>.CBT.parquet exists, else 'continuous_in_month' (ZQ=F)",
        "zq_continuous_note": "ZQ=F bars dated within the meeting month ARE that month's contract for the whole month (no roll after the FOMC day; see raw/cme/manifest.json settlement_check), so continuous_in_month covers pre- and post-meeting days",
        "realized_change_bp": "100*(DFEDTARU[effective_date] - DFEDTARU[meeting_date]) from raw/fred; null when FRED has no data yet",
        "counts": {
            "meetings": len(rows), "kalshi_linked": sum(r["kalshi_event"] is not None for r in rows),
            "kalshi_with_legs": sum(r["kalshi_status"] not in (None, "purged") for r in rows),
            "kalshi_purged": sum(r["kalshi_status"] == "purged" for r in rows),
            "poly_linked_meetings": sum(bool(r["poly_slugs"]) for r in rows), "poly_slugs_linked": linked_poly,
            "poly_slugs_total": len(poly), "zq_contract": sum(r["zq_source"] == "contract" for r in rows),
            "realized_available": sum(r["realized_change_bp"] is not None for r in rows),
        }, "anomalies": anomalies}
    (RAW / "meetings_manifest.json").write_text(json.dumps(manifest, indent=1, default=str))
    print(json.dumps(manifest, indent=1, default=str))


if __name__ == "__main__":
    main()
