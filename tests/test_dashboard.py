import json, re
from datetime import datetime
from spark import dashboard

T = datetime(2026, 11, 4, 12, 0)
VISITS = "mmsi long, inst_name string, inst_kind string, is_onshore boolean, arrival_ts timestamp, duration_min double, entered_zone int"
HOURLY = "inst_name string, hour_ts timestamp, distinct_vessels long, is_onshore boolean"


def _data(path):
    return json.loads(re.search(r'<script type="application/json" id="data">(.*?)</script>', open(path).read(), re.S).group(1))


def test_dashboard_embeds_visits_and_hours(spark, tmp_path):
    visits = spark.createDataFrame(
        [(1, "PLATFORM", "JACKET 4 LEGS", False, T, 90.0, 1),
         (2, "PLATFORM", "JACKET 4 LEGS", False, T, 10.0, 0),
         (3, "TERMINAL", "ONSHORE FACILITY", True, T, 2000.0, 0)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 2, False), ("TERMINAL", T, 1, True)], HOURLY)
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path)))
    assert [(r[0], r[2]) for r in data["inst"]] == [("PLATFORM", False), ("TERMINAL", True)]
    assert len(data["v"]["t"]) == 3 and sum(data["v"]["z"]) == 1 and sorted(data["v"]["d"]) == [10, 90, 2000]
    assert data["v"]["t"] == [12, 12, 12]                  # hours since the first day's midnight
    assert data["h"] == {"t": [12], "off": [2], "on": [1]}


def test_dashboard_adds_installation_positions(spark, tmp_path):
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 90.0, 1)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 1, False)], HOURLY)
    fac = spark.createDataFrame([("PLATFORM", 61.0, 2.0)], "inst_name string, i_lat double, i_lon double")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path), fac=fac))
    assert data["inst"][0][3:5] == [61.0, 2.0]
