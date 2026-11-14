"""
Weekly Bronze ingestion of the Norwegian fishing vessel register (Fiskeridirektoratet vessel API).

Why: AIS ship types are self-reported and often wrong, so fishing boats are identified by matching their radio call
sign (sent in AIS) against the official register. The Gold dashboard reads bronze/reference/fishing_vessels.csv.

Each run:
 1. pages through https://api.fiskeridir.no/vessel-api/api/v1/vessels (page from 1, per_page rows; the total is in
    the x-totalcount header) with retries;
 2. drops the `owners` list from every record before anything is stored: it names private persons and the pipeline
    does not need it (data minimisation);
 3. stores the pages in bronze/reference/fiskeridir/vessels/ingest_date=YYYY-MM-DD/ with a _manifest.json;
 4. rebuilds bronze/reference/fishing_vessels.csv and replaces it atomically, logging added and removed vessels
    to quality/fishing_register_changes.jsonl.
It refuses to replace the CSV (exit 1) if the fields we use disappear or far fewer vessels come back than last time.
Stdlib only.   Run:  APP_ENV=prod PYTHONPATH=. python reference/fetch_fishing_vessels.py
"""
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import csv, json, os, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "https://api.fiskeridir.no/vessel-api/api/v1/vessels"
PER_PAGE = 500
MAX_PAGES = 1000
MIN_ROW_RATIO = 0.8
NEEDED = ["id", "radioCallSign", "name", "length"]
HEADER = ["vessel_id", "call_sign", "name", "registration_mark", "length_m", "build_year", "_source", "_ingest_ts"]


def _get(url, tries=4):
    """JSON body and the x-totalcount header of one page."""
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "ais-data-pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                total = r.headers.get("x-totalcount")
                return json.load(r), (int(total) if total and total.isdigit() else None)
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(5 * 2 ** k)


def fetch_pages(get=_get, per_page=PER_PAGE):
    """All pages until the reported total is reached or a page comes back empty."""
    pages, seen, total = [], 0, None
    for page in range(1, MAX_PAGES + 1):
        items, t = get(f"{API}?{urllib.parse.urlencode({'page': page, 'per_page': per_page})}")
        total = t if t is not None else total
        if not items:
            break
        pages.append(items)
        seen += len(items)
        if total is not None and seen >= total:
            break
    return pages, total


def strip_personal(items):
    return [{k: v for k, v in it.items() if k != "owners"} for it in items]


def to_rows(items, source, fetched_at):
    rows = []
    for it in items:
        rows.append({"vessel_id": it.get("id") or "", "call_sign": (it.get("radioCallSign") or "").strip(),
                     "name": (it.get("name") or "").strip(), "registration_mark": (it.get("registrationMark") or "").strip(),
                     "length_m": "" if it.get("length") is None else it["length"], "build_year": it.get("buildYear") or "",
                     "_source": source, "_ingest_ts": fetched_at})
    return rows


def check_fields(items):
    if items and any(f not in items[0] for f in NEEDED):
        raise SystemExit(f"Vessel API fields changed: need {NEEDED}, got {sorted(items[0])}. fishing_vessels.csv NOT replaced.")


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

    pages, total = fetch_pages()
    pages = [strip_personal(p) for p in pages]
    items = [it for p in pages for it in p]
    check_fields(items)
    if total is not None and len(items) < MIN_ROW_RATIO * total:
        raise SystemExit(f"Got {len(items)} of {total} vessels the API reports; fishing_vessels.csv NOT replaced.")

    snap = ref / "fiskeridir" / "vessels" / f"ingest_date={now:%Y-%m-%d}"
    snap.mkdir(parents=True, exist_ok=True)
    for old_page in snap.glob("page_*.json"):
        old_page.unlink()
    for i, p in enumerate(pages):
        (snap / f"page_{i:03d}.json").write_text(json.dumps(p), encoding="utf-8")
    rows = to_rows(items, API, fetched_at)
    (snap / "_manifest.json").write_text(json.dumps(
        {"source": API, "fetched_at": fetched_at, "pages": len(pages), "vessels": len(items), "reported_total": total,
         "with_call_sign": sum(1 for r in rows if r["call_sign"]), "removed_fields": ["owners"],
         "publisher": "Fiskeridirektoratet (Norwegian Directorate of Fisheries)"}, indent=2), encoding="utf-8")

    current = ref / "fishing_vessels.csv"
    old = read_csv(current)
    if old and len(rows) < MIN_ROW_RATIO * len(old):
        raise SystemExit(f"Only {len(rows)} vessels vs {len(old)} last time; fishing_vessels.csv NOT replaced (snapshot in {snap}).")
    o, n = {r["vessel_id"] for r in old}, {str(r["vessel_id"]) for r in rows}
    write_csv_atomic(str(current), rows)
    summary = {"fetched_at": fetched_at, "vessels": len(rows), "with_call_sign": sum(1 for r in rows if r["call_sign"]),
               "previous": len(old), "added": len(n - o) if old else 0, "removed": len(o - n) if old else 0, "snapshot": str(snap)}
    qdir = P("quality"); qdir.mkdir(parents=True, exist_ok=True)
    with open(qdir / "fishing_register_changes.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
