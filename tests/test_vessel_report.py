import json, re
from datetime import datetime, timedelta
from spark.gold_logic import build_vessel_tracks
from spark.vessel_report import find_mmsi, place_visits, render, track_km

POS = "mmsi long, event_ts timestamp, latitude double, longitude double, speedOverGround double, courseOverGround double, inst_name string, dist_m double, is_at_installation boolean"


def test_tracks_keep_latest_position_per_slot(spark):
    t0 = datetime(2026, 11, 11, 8, 0)
    rows = [(1, t0 + timedelta(minutes=m), 60.0 + m / 100, 2.0, 5.0, 90.0, None, None, False) for m in (0, 4, 9, 12)]
    out = sorted(build_vessel_tracks(spark.createDataFrame(rows, POS), 600).collect(), key=lambda r: r["slot_ts"])
    assert [(r["slot_ts"], r["lat"]) for r in out] == [(t0, 60.09), (t0 + timedelta(minutes=10), 60.12)]
    assert str(out[0]["dt"]) == "2026-11-11"


def test_track_km_and_ignores_jumps():
    one_deg_lat = [{"lat": 60.0, "lon": 2.0}, {"lat": 61.0, "lon": 2.0}]
    assert 110 < track_km(one_deg_lat) < 112
    assert track_km(one_deg_lat + [{"lat": 70.0, "lon": 2.0}]) < 112      # a 1000 km jump is a bad fix


def test_find_mmsi_by_name(spark):
    v = spark.createDataFrame([(1, "ESVAGT AURORA", datetime(2026, 11, 1)), (2, "ESVAGT STAVANGER", datetime(2026, 11, 2)),
                               (3, "SEA1 RUBY", datetime(2026, 11, 3))], "mmsi long, name string, last_seen timestamp")
    assert find_mmsi(v, name="esvagt aurora") == (1, None)
    assert find_mmsi(v, name="ruby") == (3, None)
    mmsi, choices = find_mmsi(v, name="esvagt")
    assert mmsi is None and sorted(choices) == [(1, "ESVAGT AURORA"), (2, "ESVAGT STAVANGER")]
    assert find_mmsi(v, mmsi="257") == (257, None)


def test_render_embeds_data(tmp_path):
    data = {"vessel": {"mmsi": 1, "name": "X"}, "days": 30, "stats": {}, "track": [], "visits": [], "zones": [],
            "events": [{"note": "</script>"}], "daily": [], "places": []}
    html = open(render(data, str(tmp_path / "v.html"))).read()
    body = re.search(r'id="data">(.*?)</script>', html, re.S).group(1)
    assert json.loads(body)["events"][0]["note"] == "</script>"


def test_place_visits_uses_closest_track_point_in_window():
    t0 = datetime(2026, 11, 11, 8, 0)
    tracks = [{"slot_ts": t0 + timedelta(minutes=m), "lat": 60 + m / 1000, "lon": 2.0, "inst_name": i, "dist_m": d}
              for m, i, d in ((0, "P", 3000.0), (10, "P", 400.0), (20, "P", 900.0), (90, None, None))]
    visits = [{"inst_name": "P", "arrival_ts": t0, "departure_ts": t0 + timedelta(minutes=20)},
              {"inst_name": "P", "arrival_ts": t0 + timedelta(hours=5), "departure_ts": t0 + timedelta(hours=6)}]
    out = place_visits(visits, tracks, "arrival_ts", "departure_ts")
    assert (out[0]["ship_lat"], out[0]["ship_lon"]) == (60.01, 2.0)       # 400 m, the closest point
    assert out[1]["ship_lat"] is None                                      # no track point in that window
