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


def test_visits_carry_waiting_and_alongside_minutes(spark, tmp_path):
    from datetime import timedelta
    visits = spark.createDataFrame(
        [(1, "PLATFORM", "JACKET 4 LEGS", False, T, 300.0, 1),            # waits 45 min, then two zone entries
         (2, "PLATFORM", "JACKET 4 LEGS", False, T, 10.0, 0),             # passing: never inside 500 m
         (1, "PLATFORM", "JACKET 4 LEGS", False, T + timedelta(days=1), 60.0, 1)],   # a later visit by the same ship
        VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 2, False)], HOURLY)
    zones = spark.createDataFrame(
        [(1, "PLATFORM", T + timedelta(minutes=45), 120.0, 3.0, 80.0),
         (1, "PLATFORM", T + timedelta(minutes=200), 30.0, 2.0, 90.0),
         (1, "PLATFORM", T + timedelta(days=1, minutes=5), 50.0, 4.0, 60.0)],
        "mmsi long, inst_name string, entry_ts timestamp, minutes_inside double, max_sog_kn double, min_dist_m double")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path), zones=zones))
    got = sorted(zip(data["v"]["m"], data["v"]["t"], data["v"]["w"], data["v"]["a"]), key=lambda r: (r[0], r[1]))
    assert got == [(1, 12, 45, 150), (1, 36, 5, 50), (2, 12, None, None)]


def test_standby_gaps_skip_feed_outages_and_onshore(spark):
    from datetime import timedelta
    h = lambda n: T + timedelta(hours=n)
    # PLATFORM has a vessel at hours 0, 1, 5 and 9; TERMINAL (onshore) has none after hour 0
    hourly = spark.createDataFrame([("PLATFORM", h(n), 1, False) for n in (0, 1, 5, 9)] + [("TERMINAL", h(0), 1, True)], HOURLY)
    # the feed is normal every hour except hour 3 (outage: 10 messages against a median of 1000)
    coverage = spark.createDataFrame([(h(n), 10 if n == 3 else 1000, 50) for n in range(10)], "hour_ts timestamp, msgs long, vessels long")
    gaps = {(r["start_ts"], r["hours"]) for r in dashboard.standby_gaps(hourly, coverage, 0.5, h(0), h(9)).collect()}
    assert gaps == {(h(2), 1), (h(4), 1), (h(6), 3)}      # hour 3 is unknown, so 2..4 is two gaps, not one of three hours


def test_dashboard_embeds_landings_gaps_and_outages(spark, tmp_path):
    from datetime import timedelta
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 60.0, 1)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 1, False), ("PLATFORM", T + timedelta(hours=4), 1, False)], HOURLY)
    coverage = spark.createDataFrame([(T + timedelta(hours=n), 5 if n == 2 else 1000, 50) for n in range(5)], "hour_ts timestamp, msgs long, vessels long")
    landings = spark.createDataFrame([("PLATFORM", T + timedelta(hours=1), 12.0)], "inst_name string, arrival_ts timestamp, minutes_in_zone double")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path),
                                 landings=landings, coverage=coverage))
    assert data["hl"] == {"i": [0], "t": [13], "d": [12]}
    assert sorted(zip(data["gp"]["t"], data["gp"]["n"])) == [(13, 1), (15, 1)] and data["out"] == [14]


def test_fishing_register_is_matched_on_call_sign(spark, tmp_path):
    reg = tmp_path / "fishing_vessels.csv"
    reg.write_text("call_sign,name,length_m\nLK-2345,KYSTFISK,18.2\n,NO SIGN,10\n", encoding="utf-8")
    signs = dashboard.load_fishing_call_signs(str(reg))
    assert signs == {"LK2345"} and dashboard.load_fishing_call_signs(str(tmp_path / "missing.csv")) is None
    visits = spark.createDataFrame([(1, "PLATFORM", "JACKET 4 LEGS", False, T, 90.0, 1),
                                    (2, "PLATFORM", "JACKET 4 LEGS", False, T, 30.0, 0)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 2, False)], HOURLY)
    vessels = spark.createDataFrame([(1, "KYSTFISK", 99, 18, False, "lk 2345"), (2, "SUPPLY", 90, 80, False, "LAXX")],
                                    "mmsi long, name string, ship_type int, length_m int, is_facility boolean, call_sign string")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path / "out"),
                                 vessels=vessels, fishing_call_signs=signs))
    assert data["ves"]["1"][4] == 1 and data["ves"]["2"][4] == 0 and data["fishing_register"] is True



def test_dashboard_reads_the_fishing_match_from_gold(spark, tmp_path):
    visits = spark.createDataFrame([(m, "PLATFORM", "JACKET 4 LEGS", False, T, 30.0, 0) for m in (1, 2, 3)], VISITS)
    hourly = spark.createDataFrame([("PLATFORM", T, 3, False)], HOURLY)
    vessels = spark.createDataFrame([(1, "A", 30, 18, False, "LK1", "strong"), (2, "B", 30, 18, False, "LK2", "weak"),
                                     (3, "C", 30, 18, False, "LK3", None)],
                                    "mmsi long, name string, ship_type int, length_m int, is_facility boolean, call_sign string, "
                                    "fishing_match string")
    data = _data(dashboard.build(visits, hourly, {"finished_at": "2026-11-04T13:00:00+00:00"}, str(tmp_path / "out"),
                                 vessels=vessels, fishing_call_signs={"LK3"}))
    assert [data["ves"][k][4] for k in ("1", "2", "3")] == [2, 1, 0]    # Gold's match wins over the raw call-sign list

def test_pipeline_lines_and_dark_detections(tmp_path):
    p = tmp_path / "pipelines.csv"
    p.write_text("pipeline_id,name,label,medium,phase,seg_no,lon1,lat1,lon2,lat2\n"
                 "7,Gas A,GA,Gas,IN SERVICE,0,3.0,60.0,3.1,60.0\n7,Gas A,GA,Gas,IN SERVICE,1,3.1,60.0,3.2,60.1\n"
                 "7,Gas A,GA,Gas,IN SERVICE,2,4.0,61.0,4.1,61.0\n8,Old,OL,Oil,DECOMMISSIONED,0,5.0,60.0,5.1,60.0\n")
    lines, segs = dashboard.pipeline_lines(str(p))
    assert [len(l[2]) for l in lines] == [3, 2] and len(segs) == 3          # the jump starts a new line; old pipe left out
    dets = [{"date": "d", "lat": 60.004, "lon": 3.05, "n": 1}, {"date": "d", "lat": 60.598, "lon": 3.726, "n": 1},
            {"date": "d", "lat": 63.0, "lon": 8.0, "n": 1}]
    near = dashboard.dark_near_infrastructure(dets, [("PLATFORM X", 60.62, 3.726)], segs, 5000, 2000)
    assert [(r["pipe"], r["pipe_m"], r["inst"]) for r in near] == [("Gas A", 445, None), (None, None, "PLATFORM X")]
    assert near[1]["inst_m"] == 2446                                         # found across a grid cell edge
