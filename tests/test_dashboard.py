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


def test_visits_show_gold_waiting_and_alongside_minutes(spark, tmp_path):
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 300.0, 1, 45.0, 150.0),
                                    (2, "PLATFORM", "JACKET 4 LEGS", False, T, 10.0, 0, None, None)],
                                   VISITS + ", wait_min double, alongside_min double")
    hourly = spark.createDataFrame([("PLATFORM", T, 2, False)], HOURLY)
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path)))
    assert sorted(zip(data["v"]["m"], data["v"]["w"], data["v"]["a"])) == [(1, 45, 150), (2, None, None)]   # read, not computed


def test_dashboard_embeds_landings_gaps_and_outages(spark, tmp_path):
    from datetime import timedelta
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 60.0, 1)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 1, False), ("PLATFORM", T + timedelta(hours=4), 1, False)], HOURLY)
    landings = spark.createDataFrame([("PLATFORM", T + timedelta(hours=1), 12.0)], "inst_name string, arrival_ts timestamp, minutes_in_zone double")
    standby = spark.createDataFrame([("PLATFORM", T + timedelta(hours=1), 1), ("PLATFORM", T + timedelta(hours=3), 1)],
                                    "inst_name string, start_ts timestamp, hours long")      # Gold standby_gaps
    outages = spark.createDataFrame([(T + timedelta(hours=2),)], "hour_ts timestamp")         # Gold feed_outage_hours
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path),
                                 landings=landings, standby=standby, outages=outages))
    assert data["hl"] == {"i": [0], "t": [13], "d": [12]}
    assert sorted(zip(data["gp"]["t"], data["gp"]["n"])) == [(13, 1), (15, 1)] and data["out"] == [14]


def test_dashboard_shows_the_gold_classification(spark, tmp_path):
    visits = spark.createDataFrame([(m, "PLATFORM", "JACKET 4 LEGS", False, T, 30.0, 0) for m in (1, 2)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 2, False)], HOURLY)
    vessels = spark.createDataFrame(
        [(1, "KYSTFISK", 30, 18, False, "fishing register", "fishing vessel", "fishing vessel (fishing register)"),
         (2, "SUPPLY", 90, 80, False, None, "offshore fleet", None)],
        "mmsi long, name string, ship_type int, length_m int, is_facility boolean, fishing_source string, "
        "vessel_group string, unexpected_reason string")
    zones = spark.createDataFrame([(1, "PLATFORM", T, 20.0, 12.5, 120.0, True, "low", "18 m: small craft", True, 2)],
                                  "mmsi long, inst_name string, entry_ts timestamp, minutes_inside double, max_sog_kn double, "
                                  "min_dist_m double, is_fast boolean, risk_level string, risk_reason string, "
                                  "is_unexpected boolean, earlier_entries int")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00",
                                                  "fishing_register": {"loaded": True}}, str(tmp_path / "out"),
                                 vessels=vessels, zones=zones))
    assert data["ves"]["1"][4:] == ["fishing register", "fishing vessel", "fishing vessel (fishing register)"]
    assert data["ves"]["2"][4:] == [None, "offshore fleet", None] and data["fishing_register"] is True
    zn = data["zn"]
    assert (zn["f"], zn["r"], zn["rw"], zn["u"], zn["b"]) == ([1], [2], ["18 m: small craft"], [1], [2])


def test_pipeline_lines_for_the_map(tmp_path):
    p = tmp_path / "pipelines.csv"
    p.write_text("pipeline_id,name,label,medium,phase,seg_no,lon1,lat1,lon2,lat2\n"
                 "7,Gas A,GA,Gas,IN SERVICE,0,3.0,60.0,3.1,60.0\n7,Gas A,GA,Gas,IN SERVICE,1,3.1,60.0,3.2,60.1\n"
                 "7,Gas A,GA,Gas,IN SERVICE,2,4.0,61.0,4.1,61.0\n8,Old,OL,Oil,DECOMMISSIONED,0,5.0,60.0,5.1,60.0\n")
    lines, segs = dashboard.pipeline_lines(str(p))
    assert [len(l[2]) for l in lines] == [3, 2] and len(segs) == 3          # the jump starts a new line; old pipe left out


def test_dashboard_shows_gold_dark_detections(spark, tmp_path):
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 60.0, 1)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 1, False)], HOURLY)
    dark = spark.createDataFrame([("2026-11-04", 60.004, 3.05, 2, None, None, "Gas A", 445),
                                  ("2026-11-01", 60.6, 3.7, 1, "PLATFORM X", 2446, None, None)],      # before the data: left out
                                 "date string, lat double, lon double, detections long, inst_name string, inst_m int, "
                                 "pipeline string, pipe_m int")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00", "radar": {"newest": "2026-11-04"}},
                                 str(tmp_path), dark=dark))
    assert data["dk"]["p"] == ["Gas A"] and data["dk"]["pm"] == [445] and data["dk"]["n"] == [2] and data["dk"]["t"] == [0]
    assert data["dk_meta"] == {"newest": "2026-11-04"}
