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
import json, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path

from reference import common
from reference.common import MIN_ROW_RATIO

API = "https://api.fiskeridir.no/vessel-api/api/v1/vessels"
PAGE_SIZES = (500, 200, 100, 50, 30)   # the API rejects pages that are too large (HTTP 400); the first size it accepts is used
MAX_PAGES = 1000
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
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500 or k == tries - 1:      # a client error does not get better by retrying
                raise
            time.sleep(5 * 2 ** k)
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(5 * 2 ** k)


def page_url(page, per_page):
    return f"{API}?{urllib.parse.urlencode({'page': page, 'per_page': per_page})}"


def choose_page_size(get=_get, sizes=PAGE_SIZES):
    """The largest page size the API accepts, tried on page 1."""
    for size in sizes:
        try:
            get(page_url(1, size))
            return size
        except urllib.error.HTTPError as e:
            if e.code != 400:
                raise
    raise SystemExit(f"The vessel API rejected every page size {sizes}; fishing_vessels.csv NOT replaced.")


def fetch_pages(get=_get, per_page=None):
    """All pages until the reported total is reached or a page comes back empty."""
    per_page = per_page or choose_page_size(get)
    pages, seen, total = [], 0, None
    for page in range(1, MAX_PAGES + 1):
        items, t = get(page_url(page, per_page))
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


def write_csv_atomic(path, rows):
    common.write_csv_atomic(path, rows, HEADER)


def main():
    import pipeline_config
    cfg = pipeline_config.load()
    P = lambda rel: Path(pipeline_config.path(cfg, rel))
    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()

    # 1. Download and drop personal data (owners) before anything is stored.
    pages, total = fetch_pages()
    print(f"fetched {sum(len(p) for p in pages)} vessels in {len(pages)} pages (reported total {total})")
    pages = [strip_personal(p) for p in pages]
    items = [it for p in pages for it in p]
    check_fields(items)
    if total is not None and len(items) < MIN_ROW_RATIO * total:
        raise SystemExit(f"Got {len(items)} of {total} vessels the API reports; fishing_vessels.csv NOT replaced.")
    rows = to_rows(items, API, fetched_at)
    with_call_sign = sum(1 for r in rows if r["call_sign"])

    # 2. Keep the raw pages in Bronze.
    snap = P("bronze/reference/fiskeridir/vessels") / f"ingest_date={now:%Y-%m-%d}"
    common.save_snapshot(snap, pages, {"source": API, "fetched_at": fetched_at, "pages": len(pages),
                                       "vessels": len(items), "reported_total": total,
                                       "with_call_sign": with_call_sign, "removed_fields": ["owners"],
                                       "publisher": "Fiskeridirektoratet (Norwegian Directorate of Fisheries)"})

    # 3. Refuse a list that shrank, then replace fishing_vessels.csv.
    current = P("bronze/reference/fishing_vessels.csv")
    old = common.read_csv(current)
    common.refuse_if_shrunk(len(rows), len(old), "vessels", "fishing_vessels.csv", snap)
    write_csv_atomic(str(current), rows)

    # 4. Log how many vessels joined or left the register.
    o, n = {r["vessel_id"] for r in old}, {str(r["vessel_id"]) for r in rows}
    summary = {"fetched_at": fetched_at, "vessels": len(rows), "with_call_sign": with_call_sign,
               "previous": len(old), "added": len(n - o) if old else 0, "removed": len(o - n) if old else 0,
               "snapshot": str(snap)}
    common.append_jsonl(P("quality/fishing_register_changes.jsonl"), summary)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
