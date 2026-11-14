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


def test_dashboard_embeds_zone_entries_events_and_vessels(spark, tmp_path):
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 90.0, 1)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 1, False)], HOURLY)
    zones = spark.createDataFrame([(1, "PLATFORM", T, 20.0, 11.5, 120.0)],
                                  "mmsi long, inst_name string, entry_ts timestamp, minutes_inside double, max_sog_kn double, min_dist_m double")
    events = spark.createDataFrame(
        [("loitering", "PLATFORM", 1, T, 180.0, 900.0, 0, False, False, False, False, False, False, True),
         ("ais_gap", "OTHER RIG", 2, T, 60.0, 3000.0, 5, True, False, False, False, False, True, False)],
        "event_type string, inst_name string, mmsi long, start_ts timestamp, duration_min double, min_dist_m double, "
        "days_present_before int, regular_visitor boolean, is_facility boolean, censored boolean, feed_outage boolean, "
        "terminal_call boolean, support_craft boolean, worth_a_look boolean")
    vessels = spark.createDataFrame([(1, "ESVAGT AURORA", 99, 87, False), (2, "SMALL", 51, 9, False), (3, "UNRELATED", 70, 100, False)],
                                    "mmsi long, name string, ship_type int, length_m int, is_facility boolean")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path),
                                 zones=zones, events=events, vessels=vessels, rules={"min_days": 3}))
    assert [r[0] for r in data["inst"]] == ["OTHER RIG", "PLATFORM"]          # installations from events are included
    assert data["zn"]["kn"] == [11.5] and data["zn"]["d"] == [20.0]
    bits = data["flag_bits"]
    assert data["ev"]["k"] == [0, 1] and data["ev"]["f"][0] == bits["worth_a_look"]
    assert data["ev"]["f"][1] == bits["regular_visitor"] + bits["support_craft"] and data["ev"]["p"] == [0, 5]
    assert set(data["ves"]) == {"1", "2"} and data["ves"]["1"][0] == "ESVAGT AURORA"   # only vessels with activity
    assert data["rules"] == {"min_days": 3}
