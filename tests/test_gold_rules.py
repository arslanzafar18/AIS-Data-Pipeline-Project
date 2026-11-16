"""Rules that used to run in the dashboard and now live in Gold: waiting and alongside per visit, the vessel
classification (offshore fleet, rescue craft, fishing, group, unexpected) and the zone-entry assessment (risk,
earlier entries, unexpected)."""
from datetime import datetime, timedelta
from spark.gold_logic import assess_zone_entries, classify_vessels, fleet_stats, visit_zone_times

T = datetime(2026, 11, 4, 12, 0)
VISITS = "mmsi long, inst_name string, is_onshore boolean, arrival_ts timestamp, duration_min double"
ZONES = "mmsi long, inst_name string, is_onshore boolean, entry_ts timestamp, minutes_inside double, max_sog_kn double"
VESSELS = "mmsi long, name string, ship_type int, length_m int, is_facility boolean, fishing_match string"


def test_waiting_and_alongside_per_visit(spark):
    visits = spark.createDataFrame([(1, "P", False, T, 300.0), (2, "P", False, T, 10.0), (1, "P", False, T + timedelta(days=1), 60.0)],
                                   VISITS)
    zones = spark.createDataFrame([(1, "P", False, T + timedelta(minutes=45), 120.0, 3.0),
                                   (1, "P", False, T + timedelta(minutes=200), 30.0, 2.0),
                                   (1, "P", False, T + timedelta(days=1, minutes=5), 50.0, 4.0)], ZONES)
    out = visit_zone_times(visits, zones)
    assert out.count() == 3                                             # no visit added or dropped
    got = sorted((r["mmsi"], r["arrival_ts"], r["wait_min"], r["alongside_min"]) for r in out.collect())
    assert got == [(1, T, 45.0, 150.0), (1, T + timedelta(days=1), 5.0, 50.0), (2, T, None, None)]


def test_offshore_fleet_by_behaviour_not_ship_type(spark):
    day = lambda d, h=0: T + timedelta(days=d, hours=h)
    zones = spark.createDataFrame(
        [(1, "A", False, day(0), 10.0, 1.0), (1, "B", False, day(1), 10.0, 1.0)]                 # 2 installations
        + [(2, "A", False, day(0), 90.0, 1.0), (2, "A", False, day(3), 70.0, 1.0)]              # served A on 2 days
        + [(3, "A", False, day(0), 10.0, 1.0)]                                                 # once: not fleet
        + [(4, "T", True, day(d), 10.0, 1.0) for d in range(6)], ZONES)                       # onshore only: not counted
    visits = spark.createDataFrame([(5, "A", False, day(d), 300.0) for d in range(10)] + [(6, "A", False, day(0), 2880.0)], VISITS)
    fleet = {r["mmsi"]: r["is_offshore_fleet"] for r in fleet_stats(zones, visits).collect()}
    assert fleet == {1: True, 2: True, 3: False, 5: True, 6: True}   # 5: 10 visits, 6: 48 h within 5 km
    assert 4 not in fleet                                              # onshore entries never make a vessel "fleet"


def test_classification_rescue_fishing_group_and_unexpected(spark):
    vessels = spark.createDataFrame([
        (1, "ESVAGT FRB 1", 70, 9, False, None),        # rescue boat by name
        (2, "SAR BOAT", 51, 15, False, None),           # search-and-rescue type, under 24 m
        (3, "KYSTFISK", 70, 18, False, "weak"),         # register, call sign only
        (4, "TRAWLER", 30, 40, False, None),            # AIS type fishing
        (5, "TANKER", 80, 250, False, None),            # rarely near installations
        (6, "PLATFORM X", 0, 0, True, None),            # an installation broadcasting AIS
        (7, "SUPPLY ONE", 90, 80, False, None)], VESSELS)
    zones = spark.createDataFrame([(7, "A", False, T, 10.0, 1.0), (7, "B", False, T, 10.0, 1.0),
                                   (5, "A", False, T, 10.0, 1.0)], ZONES)
    visits = spark.createDataFrame([(5, "A", False, T, 30.0)], VISITS)
    out = {r["mmsi"]: r for r in classify_vessels(vessels, zones, visits).collect()}
    assert out[1]["is_rescue_craft"] and out[2]["is_rescue_craft"] and not out[7]["is_rescue_craft"]
    assert out[3]["fishing_source"] == "fishing register, call sign only" and out[4]["fishing_source"] == "AIS type fishing"
    assert {m: out[m]["vessel_group"] for m in out} == {1: "rescue craft", 2: "rescue craft", 3: "fishing vessel",
                                                        4: "fishing vessel", 5: "rarely near installations",
                                                        6: "rarely near installations", 7: "offshore fleet"}
    assert {m for m in out if out[m]["unexpected_in_zone"]} == {3, 4, 5}      # not rescue, not a platform, not fleet
    assert out[4]["unexpected_reason"] == "fishing vessel (AIS type fishing)"
    assert out[5]["unexpected_reason"].startswith("rarely near installations: 1 zone entries at 1 installation(s), 1 visits")
    assert out[7]["unexpected_reason"] is None and len(out) == 7


def test_zone_entry_risk_earlier_entries_and_unexpected(spark):
    vessels = spark.createDataFrame([(1, "BIG", 80, 120, False, False, True), (2, "MID", 90, 50, False, False, False),
                                     (3, "SMALL", 70, 15, False, False, True), (4, "FRB", 70, 8, False, True, False),
                                     (5, "NOLEN", 70, None, False, False, True)],
                                    "mmsi long, name string, ship_type int, length_m int, is_facility boolean, "
                                    "is_rescue_craft boolean, unexpected_in_zone boolean")
    zones = spark.createDataFrame([
        (1, "A", False, T, 10.0, 12.0), (1, "A", False, T + timedelta(days=1), 10.0, 4.0),
        (2, "A", False, T, 10.0, 16.0), (2, "B", False, T, 10.0, 11.0),
        (3, "A", False, T, 10.0, 25.0), (4, "A", False, T, 10.0, 28.0), (5, "A", False, T, 10.0, 11.0),
        (1, "TERM", True, T, 10.0, 3.0), (9, "A", False, T, 10.0, 2.0)], ZONES)
    out = {(r["mmsi"], r["inst_name"], r["entry_ts"]): r for r in assess_zone_entries(zones, vessels, 10).collect()}
    assert len(out) == 9                                                # one row per zone entry
    r = lambda m, i="A", t=T: out[(m, i, t)]
    assert (r(1)["risk_level"], r(1)["risk_reason"]) == ("high", "120 m at 12.0 kn: large and fast")
    assert r(2)["risk_level"] == "high" and r(2, "B")["risk_level"] == "medium"          # mid-size: 16 kn vs 11 kn
    assert r(3)["risk_level"] == "low" and r(4)["risk_reason"] == "rescue craft, built for these speeds"
    assert r(5)["risk_level"] == "medium" and r(1, "A", T + timedelta(days=1))["is_fast"] is False
    assert r(1, "A", T + timedelta(days=1))["risk_level"] is None
    assert r(1)["earlier_entries"] == 0 and r(1, "A", T + timedelta(days=1))["earlier_entries"] == 1
    assert r(1)["is_unexpected"] and not r(2)["is_unexpected"] and not r(1, "TERM")["is_unexpected"]   # onshore never
    assert r(9)["is_unexpected"]                                       # a vessel missing from the table counts as unknown


def test_standby_gaps_skip_feed_outages_and_onshore(spark):
    from spark.gold_logic import standby_gaps
    h = lambda n: T + timedelta(hours=n)
    # PLATFORM has a vessel at hours 0, 1, 5 and 9; TERMINAL (onshore) has none after hour 0
    hourly = spark.createDataFrame([("PLATFORM", h(n), 1, False) for n in (0, 1, 5, 9)] + [("TERMINAL", h(0), 1, True)],
                                   "inst_name string, hour_ts timestamp, distinct_vessels long, is_onshore boolean")
    # the feed is normal every hour except hour 3 (outage: 10 messages against a median of 1000)
    coverage = spark.createDataFrame([(h(n), 10 if n == 3 else 1000, 50) for n in range(10)], "hour_ts timestamp, msgs long, vessels long")
    gaps = {(r["inst_name"], r["start_ts"], r["hours"]) for r in standby_gaps(hourly, coverage, 0.5, h(0), h(9)).collect()}
    assert gaps == {("PLATFORM", h(2), 1), ("PLATFORM", h(4), 1), ("PLATFORM", h(6), 3)}   # hour 3 unknown: two gaps, not one


def test_feed_outage_hours(spark):
    from spark.gold_logic import feed_outage_hours
    h = lambda n: T + timedelta(hours=n)
    coverage = spark.createDataFrame([(h(n), 10 if n in (2, 5) else 1000, 50) for n in range(6)], "hour_ts timestamp, msgs long, vessels long")
    out = sorted(r["hour_ts"] for r in feed_outage_hours(coverage, 0.5, h(5)).collect())
    assert out == [h(2)]                                    # hour 5 is the current hour, still filling up: never an outage


def test_dark_detections_near_installations_and_pipelines(spark):
    from spark.gold_logic import dark_near_infrastructure
    det = spark.createDataFrame([("2026-11-04", 60.004, 3.05, 1), ("2026-11-04", 60.598, 3.726, 2), ("2026-11-04", 63.0, 8.0, 1)],
                                "date string, lat double, lon double, detections long")
    inst = spark.createDataFrame([("PLATFORM X", 60.62, 3.726)], "inst_name string, i_lat double, i_lon double")
    seg = spark.createDataFrame([("7", "Gas A", "GA", "Gas", "IN SERVICE", "0", 3.0, 60.0, 3.1, 60.0),
                                 ("7", "Gas A", "GA", "Gas", "IN SERVICE", "1", 3.1, 60.0, 3.2, 60.1)],
                                "pipeline_id string, name string, label string, medium string, phase string, seg_no string, "
                                "lon1 double, lat1 double, lon2 double, lat2 double")
    got = {(r["lat"], r["lon"]): r for r in dark_near_infrastructure(det, inst, seg, 5000, 2000).collect()}
    assert set(got) == {(60.004, 3.05), (60.598, 3.726)}                     # the open-sea detection is left out
    a, b = got[(60.004, 3.05)], got[(60.598, 3.726)]
    assert (a["pipeline"], a["pipe_m"], a["inst_name"]) == ("Gas A", 445, None)
    assert (b["inst_name"], b["inst_m"], b["pipeline"], b["detections"]) == ("PLATFORM X", 2446, None, 2)
