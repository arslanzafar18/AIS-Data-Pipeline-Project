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
import json, urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from reference import common

LAYER_URL = "https://factmaps.sodir.no/api/rest/services/Factmaps/FactMapsWGS84/MapServer/311"
PAGE = 1000
FIELDS = {"pplNpdidPipeline": "pipeline_id", "pplName": "name", "pplMapLabel": "label", "pplBelongsToName": "belongs_to",
          "pplCurrentPhase": "phase", "pplMedium": "medium", "fclNameFrom": "from_facility", "fclNameTo": "to_facility",
          "cmpLongName": "operator", "pplDimension": "dimension_in", "pplWaterDepth": "water_depth_m"}
HEADER = list(FIELDS.values()) + ["seg_no", "lon1", "lat1", "lon2", "lat2", "_source", "_ingest_ts"]


_json = common.get_json    # shared GET with retries (reference/common.py)


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


def write_csv_atomic(path, rows):
    common.write_csv_atomic(path, rows, HEADER)


def main():
    import pipeline_config
    cfg = pipeline_config.load()
    P = lambda rel: Path(pipeline_config.path(cfg, rel))
    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()

    # 1. Download, after checking the API still has the columns we rely on.
    fields = [f["name"] for f in _json(f"{LAYER_URL}?f=json").get("fields", [])]
    check_fields(fields)
    pages = fetch_pages()
    features = [f for p in pages for f in p.get("features", [])]
    rows = to_segments(features, LAYER_URL, fetched_at)
    n_pipes = len({r["pipeline_id"] for r in rows})

    # 2. Keep the raw pages in Bronze.
    snap = P("bronze/reference/sodir/pipelines") / f"ingest_date={now:%Y-%m-%d}"
    common.save_snapshot(snap, pages, {"source": LAYER_URL, "fetched_at": fetched_at, "pages": len(pages),
                                       "features": len(features), "pipelines_with_geometry": n_pipes,
                                       "segments": len(rows), "fields": fields,
                                       "licence": "NLOD, Norwegian Offshore Directorate"})

    # 3. Refuse a list that shrank (counted in pipelines, not segments), then replace pipelines.csv.
    current = P("bronze/reference/pipelines.csv")
    old_pipes = len({r["pipeline_id"] for r in common.read_csv(current)})
    common.refuse_if_shrunk(n_pipes, old_pipes, "pipelines", "pipelines.csv", snap)
    write_csv_atomic(str(current), rows)
    in_service = len({r["pipeline_id"] for r in rows if r["phase"] == "IN SERVICE"})
    print(json.dumps({"fetched_at": fetched_at, "pipelines": n_pipes, "segments": len(rows),
                      "previous_pipelines": old_pipes, "in_service": in_service, "snapshot": str(snap)}))


if __name__ == "__main__":
    main()
