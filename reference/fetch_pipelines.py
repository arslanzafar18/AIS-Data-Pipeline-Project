"""
Weekly Bronze ingestion of Sodir pipelines from the FactMaps REST API (layer 311 "Pipelines", WGS84). Data: NLOD licence.

Why: loitering or anchoring over a subsea pipeline is an anchor-damage and sabotage risk, so Gold looks for slow vessels
near pipelines, away from installations (spark/gold_pipelines.py). Run by the same weekly timer as the facility list.

Each run:
 1. pages through the layer (1000 features per page) with retries;
 2. stores the raw JSON pages in bronze/reference/sodir/pipelines/ingest_date=YYYY-MM-DD/ with a _manifest.json;
 3. rebuilds bronze/reference/pipelines.csv, one row per straight SEGMENT between two vertices of a pipeline, and
    replaces it atomically. Gold measures each position's distance to these segments.
It refuses to replace pipelines.csv (exit 1) if the fields change or far fewer pipelines come back than last time.
Stdlib only.   Run:  APP_ENV=prod PYTHONPATH=. python reference/fetch_pipelines.py
"""
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import csv, json, os, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path

LAYER_URL = "https://factmaps.sodir.no/api/rest/services/Factmaps/FactMapsWGS84/MapServer/311"
PAGE = 1000
MIN_ROW_RATIO = 0.8
FIELDS = {"pplNpdidPipeline": "pipeline_id", "pplName": "name", "pplMapLabel": "label", "pplBelongsToName": "belongs_to",
          "pplCurrentPhase": "phase", "pplMedium": "medium", "fclNameFrom": "from_facility", "fclNameTo": "to_facility",
          "cmpLongName": "operator", "pplDimension": "dimension_in", "pplWaterDepth": "water_depth_m"}
HEADER = list(FIELDS.values()) + ["seg_no", "lon1", "lat1", "lon2", "lat2", "_source", "_ingest_ts"]


def _json(url, tries=4):
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ais-data-pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.load(r)
            if "error" in d:
                raise RuntimeError(f"API error: {d['error']}")
            return d
        except urllib.error.HTTPError as e:
            if (400 <= e.code < 500 and e.code != 429) or k == tries - 1:   # a rejected request does not get better
                raise
            time.sleep(5 * 2 ** k)                                       # 429 or a server error: back off
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(5 * 2 ** k)


def fetch_pages(get=_json):
    pages, offset = [], 0
    while True:
        q = urllib.parse.urlencode({"where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": 4326,
                                    "orderByFields": "OBJECTID", "resultOffset": offset, "resultRecordCount": PAGE, "f": "json"})
        d = get(f"{LAYER_URL}/query?{q}")
        pages.append(d)
        n = len(d.get("features", []))
        offset += n
        if n == 0 or (n < PAGE and not d.get("exceededTransferLimit")):
            return pages


def check_fields(fields):
    missing = [f for f in FIELDS if f not in fields]
    if missing:
        raise SystemExit(f"Sodir pipeline fields changed; missing {missing}. pipelines.csv NOT replaced.")


def _clean(v):
    if v is None:
        return ""
    return v.strip() if isinstance(v, str) else v


def to_segments(features, source, fetched_at):
    """One row per straight segment of every path of every pipeline; a pipeline without geometry is skipped."""
    rows = []
    for f in features:
        a, paths = f.get("attributes", {}), (f.get("geometry") or {}).get("paths") or []
        base = {col: _clean(a.get(api)) for api, col in FIELDS.items()}
        seg = 0
        for path in paths:
            for (x1, y1), (x2, y2) in zip(path, path[1:]):
                rows.append({**base, "seg_no": seg, "lon1": x1, "lat1": y1, "lon2": x2, "lat2": y2,
                             "_source": source, "_ingest_ts": fetched_at})
                seg += 1
    return rows


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
    features = [f for p in pages for f in p.get("features", [])]

    snap = ref / "sodir" / "pipelines" / f"ingest_date={now:%Y-%m-%d}"
    snap.mkdir(parents=True, exist_ok=True)
    for old_page in snap.glob("page_*.json"):
        old_page.unlink()
    for i, p in enumerate(pages):
        (snap / f"page_{i:03d}.json").write_text(json.dumps(p), encoding="utf-8")
    rows = to_segments(features, LAYER_URL, fetched_at)
    n_pipes = len({r["pipeline_id"] for r in rows})
    (snap / "_manifest.json").write_text(json.dumps(
        {"source": LAYER_URL, "fetched_at": fetched_at, "pages": len(pages), "features": len(features),
         "pipelines_with_geometry": n_pipes, "segments": len(rows), "fields": fields,
         "licence": "NLOD, Norwegian Offshore Directorate"}, indent=2), encoding="utf-8")

    current = ref / "pipelines.csv"
    old = read_csv(current)
    old_pipes = len({r["pipeline_id"] for r in old})
    if old and n_pipes < MIN_ROW_RATIO * old_pipes:
        raise SystemExit(f"Only {n_pipes} pipelines vs {old_pipes} last time; pipelines.csv NOT replaced (snapshot in {snap}).")
    write_csv_atomic(str(current), rows)
    print(json.dumps({"fetched_at": fetched_at, "pipelines": n_pipes, "segments": len(rows), "previous_pipelines": old_pipes,
                      "in_service": len({r["pipeline_id"] for r in rows if r["phase"] == "IN SERVICE"}), "snapshot": str(snap)}))


if __name__ == "__main__":
    main()
