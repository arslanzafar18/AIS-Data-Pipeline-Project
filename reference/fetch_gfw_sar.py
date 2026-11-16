"""
Daily Bronze ingestion of satellite radar (SAR) vessel detections that match NO AIS vessel ("dark" vessels) from the
Global Fishing Watch 4Wings API, dataset public-global-sar-presence. Powered by Global Fishing Watch
(https://globalfishingwatch.org), CC BY-NC 4.0: non-commercial use only, attribution required.

Why: a vessel that switches off AIS is invisible to the rest of the pipeline; radar sees it when a satellite passes.
Data arrives about 5 days late and only for satellite passes, so each run re-reads the last `days` days.

Each run:
 1. asks for daily detections on a 0.01 degree grid (about 1 km) in each configured box, unmatched to AIS only;
 2. stores each raw response in bronze/gfw/sar/ingest_date=YYYY-MM-DD/ with a _manifest.json (dataset version,
    date range, boxes, rate-limit headers, attribution, access date);
 3. merges them into bronze/gfw/sar_detections.csv (one row per day and grid cell; the newest fetch wins) and keeps
    only the lookback window.
The token is read from GFW_TOKEN (production .env) and never written or printed. Without a token the run is skipped.
Stdlib only.   Run:  APP_ENV=prod PYTHONPATH=. python reference/fetch_gfw_sar.py [--days 21]
"""
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import argparse, json, os, time, urllib.error, urllib.parse, urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from reference import common

API = "https://gateway.api.globalfishingwatch.org/v3/4wings/report"
DATASET = "public-global-sar-presence:latest"
UA = "ais-data-pipeline/1.0 (university course project)"      # the API's firewall refuses Python's default agent
BOXES = [[1.5, 56.0, 9.0, 66.0], [9.0, 66.0, 31.5, 72.5]]      # lon_min, lat_min, lon_max, lat_max: North Sea and
                                                                # Norwegian Sea; Barents Sea
HEADER = ["date", "lat", "lon", "detections", "dataset", "fetched_at"]
ATTRIBUTION = "Powered by Global Fishing Watch, https://globalfishingwatch.org (CC BY-NC 4.0)"


def polygon(box):
    x0, y0, x1, y1 = box
    return {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {
        "type": "Polygon", "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]}}]}


def request(box, start, end, token, tries=4):
    q = {"spatial-resolution": "HIGH", "temporal-resolution": "DAILY", "group-by": "FLAG", "datasets[0]": DATASET,
         "filters[0]": "matched='false'", "date-range": f"{start},{end}", "format": "JSON"}
    req = urllib.request.Request(f"{API}?{urllib.parse.urlencode(q)}", data=json.dumps({"geojson": polygon(box)}).encode(),
                                 method="POST", headers={"User-Agent": UA, "Authorization": f"Bearer {token}",
                                                         "Content-Type": "application/json"})
    for k in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                limits = {h: v for h, v in r.headers.items() if h.lower().startswith("x-ratelimit")}
                return json.load(r), limits
        except urllib.error.HTTPError as e:
            if e.code in (401, 403) or (400 <= e.code < 500 and e.code != 429) or k == tries - 1:
                raise SystemExit(f"GFW API refused the request: HTTP {e.code}. Nothing replaced.")
            time.sleep(30 * 2 ** k)                                  # 429 or a server error: back off
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(30 * 2 ** k)


def rows_from(body, fetched_at):
    """Entries -> one row per day and grid cell. The dataset key carries its version (e.g. ...:v5.0)."""
    out = []
    for entry in body.get("entries", []):
        for ds, items in entry.items():
            for it in items or []:
                if it.get("lat") is None or it.get("lon") is None or not it.get("date"):
                    continue
                out.append({"date": it["date"][:10], "lat": round(float(it["lat"]), 4), "lon": round(float(it["lon"]), 4),
                            "detections": int(it.get("detections") or 0), "dataset": ds, "fetched_at": fetched_at})
    return out


def merge(old, new, keep_from):
    """Union by (date, lat, lon), the newest fetch winning; rows before keep_from are dropped."""
    by = {}
    for r in old + new:
        k = (str(r["date"]), str(r["lat"]), str(r["lon"]))
        if k not in by or str(r["fetched_at"]) >= str(by[k]["fetched_at"]):
            by[k] = r
    return sorted((r for r in by.values() if str(r["date"]) >= keep_from.isoformat()), key=lambda r: (str(r["date"]), str(r["lat"])))


def write_csv_atomic(path, rows):
    common.write_csv_atomic(path, rows, HEADER)


def main(argv=None):
    import pipeline_config
    cfg = pipeline_config.load()
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=21)
    a = ap.parse_args(argv)
    token = os.environ.get("GFW_TOKEN", "").strip()
    if not token:
        print("GFW_TOKEN not set: dark-vessel detections skipped")
        return
    P = lambda rel: Path(pipeline_config.path(cfg, rel))
    now = datetime.now(timezone.utc)
    end, start = now.date(), now.date() - timedelta(days=a.days)
    boxes = cfg.get("surveillance", {}).get("gfw_boxes", BOXES)

    # 1. One request per sea area (box).
    bodies, new, limits = [], [], {}
    for box in boxes:
        body, limits = request(box, start, end, token)
        bodies.append(body)
        new += rows_from(body, now.isoformat())
    versions = sorted({r["dataset"] for r in new})

    # 2. Keep the raw responses in Bronze.
    snap = P("bronze/gfw/sar") / f"ingest_date={now:%Y-%m-%d}"
    common.save_snapshot(snap, bodies, {"source": API, "dataset": DATASET, "versions": versions,
                                        "date_range": [str(start), str(end)], "boxes": boxes,
                                        "filter": "matched='false'", "spatial_resolution": "HIGH (0.01 deg)",
                                        "fetched_at": now.isoformat(), "cells": len(new), "rate_limits": limits,
                                        "attribution": ATTRIBUTION,
                                        "licence": "CC BY-NC 4.0, non-commercial use only"},
                         name="box_{:02d}.json")

    # 3. Merge into the stored detections (newest fetch wins) and drop days older than the lookback window.
    out = P("bronze/gfw/sar_detections.csv")
    keep_from = date.today() - timedelta(days=int(cfg.get("lookback_days", 90)))
    merged = merge(common.read_csv(out), new, keep_from)
    write_csv_atomic(str(out), merged)
    print(json.dumps({"fetched_at": now.isoformat(), "date_range": [str(start), str(end)], "new_cells": len(new),
                      "stored_cells": len(merged), "versions": versions,
                      "daily_remaining": limits.get("x-ratelimit-daily-remaining-requests")}))


if __name__ == "__main__":
    main()
