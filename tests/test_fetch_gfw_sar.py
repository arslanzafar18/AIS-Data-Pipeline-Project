"""Global Fishing Watch radar detections: parsing, merging and the skip without a token (no network)."""
import importlib.util
from datetime import date
from pathlib import Path

spec = importlib.util.spec_from_file_location("gfw", Path(__file__).parent.parent / "reference" / "fetch_gfw_sar.py")
G = importlib.util.module_from_spec(spec)
spec.loader.exec_module(G)

BODY = {"total": 1, "entries": [{"public-global-sar-presence:v5.0": [
    {"date": "2026-11-03", "detections": 1, "flag": "", "lat": 60.52, "lon": 3.45, "vesselIDs": 1},
    {"date": "2026-11-08", "detections": 2, "flag": "", "lat": 60.81, "lon": 3.98, "vesselIDs": 1},
    {"date": "2026-11-09", "detections": 1, "lat": None, "lon": 3.0}]}]}


def test_rows_from_the_report_format_seen_on_the_api():
    rows = G.rows_from(BODY, "2026-11-15T06:15:00+00:00")
    assert [(r["date"], r["lat"], r["detections"]) for r in rows] == [("2026-11-03", 60.52, 1), ("2026-11-08", 60.81, 2)]
    assert rows[0]["dataset"] == "public-global-sar-presence:v5.0"


def test_merge_newest_fetch_wins_and_old_days_drop():
    old = [{"date": "2026-08-01", "lat": "60.1", "lon": "3.1", "detections": "1", "dataset": "v", "fetched_at": "2026-08-07"},
           {"date": "2026-11-03", "lat": "60.52", "lon": "3.45", "detections": "1", "dataset": "v", "fetched_at": "2026-11-09"}]
    new = [{"date": "2026-11-03", "lat": "60.52", "lon": "3.45", "detections": "3", "dataset": "v", "fetched_at": "2026-11-15"}]
    out = G.merge(old, new, date(2026, 9, 1))
    assert len(out) == 1 and out[0]["detections"] == "3"


def test_no_token_skips(monkeypatch, capsys):
    monkeypatch.delenv("GFW_TOKEN", raising=False)
    G.main([])
    assert "skipped" in capsys.readouterr().out


def test_polygon_is_closed():
    ring = G.polygon([1.5, 56.0, 9.0, 66.0])["features"][0]["geometry"]["coordinates"][0]
    assert ring[0] == ring[-1] and len(ring) == 5
