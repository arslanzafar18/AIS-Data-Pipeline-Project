"""
Helpers shared by the reference fetchers (Sodir facilities, Sodir pipelines, fishing register, GFW radar).

Every fetcher follows the same four steps:
 1. download the pages from the API;
 2. save_snapshot(): keep the raw pages and a _manifest.json in a Bronze folder for today (ingest_date=YYYY-MM-DD);
 3. refuse_if_shrunk(): stop if the new list is much shorter than the one we already have;
 4. write_csv_atomic(): replace the current CSV in one step, so Silver and Gold never read half a file.
Stdlib only.
"""
import csv, json, os, time, urllib.error, urllib.request

MIN_ROW_RATIO = 0.8        # refuse to publish if the new list is < 80% of the previous one
USER_AGENT = "ais-data-pipeline/1.0"


def get_json(url, tries=4):
    """GET a JSON page, retrying with back-off on 429 and server errors (5 s, 10 s, 20 s).
    Other 4xx errors are raised at once: a rejected request does not get better by repeating it."""
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.load(r)
            if "error" in d:
                raise RuntimeError(f"API error: {d['error']}")
            return d
        except urllib.error.HTTPError as e:
            if (400 <= e.code < 500 and e.code != 429) or k == tries - 1:
                raise
            time.sleep(5 * 2 ** k)
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(5 * 2 ** k)


def read_csv(path):
    """Rows of a CSV as dicts; an empty list if the file does not exist yet (the first run)."""
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv_atomic(path, rows, header):
    """Write to a hidden temp file next to the target, then rename it over the target in one step."""
    tmp = os.path.join(os.path.dirname(path), "." + os.path.basename(path) + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def save_snapshot(snap, pages, manifest, name="page_{:03d}.json"):
    """Store the raw API pages unchanged plus a _manifest.json describing them. A second run on the same day
    replaces that day's pages, so the folder always holds exactly one complete download."""
    snap.mkdir(parents=True, exist_ok=True)
    for old in snap.glob(name.split("{")[0] + "*.json"):
        old.unlink()
    for i, page in enumerate(pages):
        (snap / name.format(i)).write_text(json.dumps(page), encoding="utf-8")
    (snap / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def refuse_if_shrunk(new_count, old_count, what, csv_name, snap):
    """Stop (exit 1) instead of replacing a good list with a partial download."""
    if old_count and new_count < MIN_ROW_RATIO * old_count:
        raise SystemExit(f"Only {new_count} {what} vs {old_count} last time; {csv_name} NOT replaced "
                         f"(snapshot kept in {snap}).")


def append_jsonl(path, record):
    """Add one JSON line to a log under quality/ (creating the folder on first use)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
