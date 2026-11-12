"""
Weekly Bronze ingestion of Sodir facilities from the FactMaps REST API (WGS84). Data: NLOD licence.

Each run:
 1. pages through layer 307 "All facilities" (1000 rows per page) with retries;
 2. stores the raw JSON pages unchanged in bronze/reference/sodir/facilities/ingest_date=YYYY-MM-DD/,
    with a _manifest.json (source URL, fetch time, row count, fields): one immutable snapshot per run day;
 3. rebuilds bronze/reference/facilities.csv in the same column layout as the old shapefile download,
    so Silver and the facility filter keep working, and replaces it atomically;
 4. logs added, removed and changed facilities against the previous CSV to quality/sodir_changes.jsonl.
It refuses to replace facilities.csv (exit 1) if the API's fields change or it returns far fewer rows than before.
Silver reads facilities.csv at the start of every run, so a new list is picked up within 2 minutes.
Stdlib only.   Run:  APP_ENV=prod PYTHONPATH=. python reference/fetch_sodir.py
"""
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import csv, json, math, os, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

LAYER_URL = "https://factmaps.sodir.no/api/rest/services/Factmaps/FactMapsWGS84/MapServer/307"
PAGE = 1000
MIN_ROW_RATIO = 0.8            # refuse to publish if the new list is < 80% of the previous one
MOVE_M = 100                   # a facility "moved" if its position changed by more than this
OSLO = ZoneInfo("Europe/Oslo")  # Sodir dates are midnight Oslo time, sent as epoch milliseconds

# API field -> column name of the old shapefile download, in the same order. Silver and the config use these names.
COLUMNS = [
    ("fclNpdidFacility", "idFacility"), ("fclNpdidCurrentOperator", "idOperator"),
    ("fclNpdidCurrentRespCompany", "idCompResp"), ("fclFixedOrMoveable", "FixMov"), ("fclName", "facName"),
    ("fclKind", "facKind"), ("fclPhase", "phaseName"), ("fclNpdidBelongsTo", "idBelongTo"),
    ("fclFunctions", "facFunc"), ("fclGeodeticDatum", "datum"), ("fclNsDeg", "ns_deg"), ("fclNsMin", "ns_min"),
    ("fclNsSec", "ns_second"), ("fclNsCode", "ns_code"), ("fclEwDeg", "ew_deg"), ("fclEwMin", "ew_min"),
    ("fclEwSec", "ew_second"), ("fclEwCode", "ew_code"), ("fclUtmHemisphere", "utmHemis"), ("fclUtmZone", "utm_zone"),
    ("fclNsUtm", "ns_utm"), ("fclEwUtm", "ew_utm"), ("fclRkb", "rkb"), ("fclWaterDepth", "WaterDepth"),
    ("fclStartupDate", "dtStartup"), ("fclDesignLifetime", "lifetime"), ("fclStatus", "statusName"),
    ("fclSurface", "surface"), ("fclBelongsToName", "belong2nm"), ("fclBelongsToKind", "belong2knd"),
    ("fclCurrentOperatorName", "curOperNam"), ("fclCurrentRespCompanyName", "curRespCom"),
    ("fclNationCode2", "natonCode2"), ("fclNationName", "nationName"), ("fclBelongsToFactPageUrl", "belong2url"),
    ("fclFactPageUrl", "FactUrl"), ("fclFactMapUrl", "MapUrl"), ("fclSlotCapacityGross", "SlotCpGros"),
    ("fclSlotCapacityNet", "SlotCpNet"), ("fclDateUpdated", "dtUpdated"), ("fclFormalName1Quadrant", "FormlQuad"),
    ("fclFormalName2Block", "FormlBlock"), ("fclDateRemoved", "yrRemoved"), ("fclDateShutdown", "yrShutdown"),
]
DATE_FIELDS = {"fclStartupDate", "fclDateUpdated", "fclDateRemoved", "fclDateShutdown"}
HEADER = [c for _, c in COLUMNS] + ["centroid_lon", "centroid_lat", "_source", "_ingest_ts"]
WATCH = ["facName", "facKind", "phaseName", "surface", "FixMov"]


def _json(url, tries=4):
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ais-data-pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.load(r)
            if "error" in d:
                raise RuntimeError(f"API error: {d['error']}")
            return d
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(5 * 2 ** k)


def check_fields(fields):
    """Fail loudly if Sodir renamed or dropped a column we rely on."""
    missing = [api for api, _ in COLUMNS if api not in fields]
    if missing:
        raise SystemExit(f"Sodir API fields changed; missing {missing}. facilities.csv NOT replaced.")


def fetch_pages():
    pages, offset = [], 0
    while True:
        q = urllib.parse.urlencode({"where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": 4326,
                                    "orderByFields": "OBJECTID", "resultOffset": offset,
                                    "resultRecordCount": PAGE, "f": "json"})
        d = _json(f"{LAYER_URL}/query?{q}")
        pages.append(d)
        n = len(d.get("features", []))
        offset += n
        if n == 0 or (n < PAGE and not d.get("exceededTransferLimit")):
            return pages


def _date(v):
    if v in (None, ""):
        return ""
    return datetime.fromtimestamp(v / 1000, OSLO).date().isoformat()


def _value(api, v):
    """Dates as YYYY-MM-DD, None as empty, text trimmed (the API pads some names with a trailing space,
    and names are the join key in Silver, Gold and the dashboard)."""
    if api in DATE_FIELDS:
        return _date(v)
    if v is None:
        return ""
    return v.strip() if isinstance(v, str) else v


def to_rows(features, source, fetched_at):
    """API features -> rows in the old CSV layout. Features without a position are skipped, as before."""
    rows = []
    for f in features:
        a, g = f.get("attributes", {}), f.get("geometry") or {}
        if g.get("x") is None or g.get("y") is None:
            continue
        row = {col: _value(api, a.get(api)) for api, col in COLUMNS}
        row.update(centroid_lon=g["x"], centroid_lat=g["y"], _source=source, _ingest_ts=fetched_at)
        rows.append(row)
    return rows


def _dist_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371000.0 * math.asin(math.sqrt(a))


def diff(old, new):
    """Facilities added, removed, or changed (watched attributes or position) between two lists."""
    o = {str(r["idFacility"]): r for r in old}
    n = {str(r["idFacility"]): r for r in new}
    changed = []
    for k in sorted(o.keys() & n.keys()):
        what = {c: [str(o[k].get(c, "")), str(n[k].get(c, ""))] for c in WATCH
                if str(o[k].get(c, "")).strip() != str(n[k].get(c, "")).strip()}
        try:
            moved = _dist_m(float(o[k]["centroid_lat"]), float(o[k]["centroid_lon"]),
                            float(n[k]["centroid_lat"]), float(n[k]["centroid_lon"]))
            if moved > MOVE_M:
                what["position_moved_m"] = round(moved)
        except (TypeError, ValueError, KeyError):
            what["position_moved_m"] = None
        if what:
            changed.append({"id": k, "name": n[k]["facName"], "changed": what})
    return {"added": [{"id": k, "name": n[k]["facName"]} for k in sorted(n.keys() - o.keys())],
            "removed": [{"id": k, "name": o[k]["facName"]} for k in sorted(o.keys() - n.keys())],
            "changed": changed}


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv_atomic(path, rows):
    tmp = os.path.join(os.path.dirname(path), "." + os.path.basename(path) + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def main():
    import pipeline_config
    cfg = pipeline_config.load()
    P = lambda rel: Path(pipeline_config.path(cfg, rel))
    ref, now = P("bronze/reference"), datetime.now(timezone.utc)
    fetched_at = now.isoformat()

    meta = _json(f"{LAYER_URL}?f=json")
    fields = [f["name"] for f in meta.get("fields", [])]
    check_fields(fields)
    pages = fetch_pages()

    snap = ref / "sodir" / "facilities" / f"ingest_date={now:%Y-%m-%d}"
    snap.mkdir(parents=True, exist_ok=True)
    for old_page in snap.glob("page_*.json"):
        old_page.unlink()
    for i, p in enumerate(pages):
        (snap / f"page_{i:03d}.json").write_text(json.dumps(p), encoding="utf-8")
    features = [f for p in pages for f in p.get("features", [])]
    rows = to_rows(features, LAYER_URL, fetched_at)
    (snap / "_manifest.json").write_text(json.dumps(
        {"source": LAYER_URL, "fetched_at": fetched_at, "pages": len(pages), "features": len(features),
         "rows_with_position": len(rows), "fields": fields, "licence": "NLOD, Norwegian Offshore Directorate"},
        indent=2), encoding="utf-8")

    current = ref / "facilities.csv"
    old = read_csv(current)
    if old and len(rows) < MIN_ROW_RATIO * len(old):
        raise SystemExit(f"Only {len(rows)} facilities vs {len(old)} last time; facilities.csv NOT replaced "
                         f"(snapshot kept in {snap}).")
    changes = diff(old, rows) if old else {"added": [], "removed": [], "changed": []}
    write_csv_atomic(str(current), rows)

    summary = {"fetched_at": fetched_at, "rows": len(rows), "previous_rows": len(old), "snapshot": str(snap),
               "added": len(changes["added"]), "removed": len(changes["removed"]), "changed": len(changes["changed"]),
               "details": {k: v[:50] for k, v in changes.items()}}
    qdir = P("quality"); qdir.mkdir(parents=True, exist_ok=True)
    with open(qdir / "sodir_changes.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k != "details"}))
    for kind in ("added", "removed", "changed"):
        for item in changes[kind][:20]:
            print(f"  {kind}: {item}")


if __name__ == "__main__":
    main()
