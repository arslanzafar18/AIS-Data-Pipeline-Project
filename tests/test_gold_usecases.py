from datetime import datetime, timedelta
from spark.gold_logic import (build_ais_gaps, build_coverage, build_loitering, build_zone_entries, covered_hours,
                              flag_censored, flag_facility, flag_ordinary, flag_outage, flag_regular,
                              mark_worth_a_look)

SCHEMA = ("mmsi long, inst_name string, inst_kind string, is_onshore boolean, event_ts timestamp, dist_m double, "
          "speedOverGround double, name string, shipType int, is_at_installation boolean, is_near_installation boolean")
T0 = datetime(2026, 11, 11, 8, 0)


def row(mmsi, minutes, dist=300.0, sog=5.0, inst="PLATFORM", near=True):
    return (mmsi, inst if near else None, "JACKET" if near else None, False, T0 + timedelta(minutes=minutes),
            dist if near else None, sog, "SHIP", 90, near and dist <= 500, near)


def test_zone_entries_split_on_gaps_and_measure_speed(spark):
    rows = [row(1, m, sog=s) for m, s in ((0, 3.0), (10, 7.0), (20, 2.0))] + [row(1, 70, sog=1.0)]   # 50 min gap
    rows += [row(1, 80, dist=2000.0)]                                                                  # outside 500 m
    out = sorted(build_zone_entries(spark.createDataFrame(rows, SCHEMA), 1800).collect(), key=lambda r: r["entry_ts"])
    assert len(out) == 2
    assert out[0]["minutes_inside"] == 20.0 and out[0]["max_sog_kn"] == 7.0 and out[0]["n_msgs"] == 3
    assert out[1]["n_msgs"] == 1


def test_loitering_needs_slow_and_long_enough(spark):
    slow_3h = [row(1, m, dist=3000.0, sog=0.5) for m in range(0, 181, 10)]
    slow_1h = [row(2, m, dist=3000.0, sog=0.5) for m in range(0, 61, 10)]
    broken = [row(3, m, dist=3000.0, sog=0.5) for m in range(0, 61, 10)] + [row(3, 70, dist=3000.0, sog=9.0)] + \
             [row(3, m, dist=3000.0, sog=0.5) for m in range(80, 141, 10)]
    ev = build_loitering(spark.createDataFrame(slow_3h + slow_1h + broken, SCHEMA), 1800, 2.0, 120).collect()
    assert [(r["mmsi"], r["duration_min"], r["event_type"]) for r in ev] == [(1, 180.0, "loitering")]


def test_ais_gap_only_when_silent_everywhere_and_within_bounds(spark):
    rows = [row(1, 0), row(1, 60)]                                   # 60 min silent near PLATFORM -> gap
    rows += [row(2, 0), row(2, 30, near=False), row(2, 60)]          # message elsewhere in between -> no gap
    rows += [row(3, 0), row(3, 14 * 60)]                             # 14 h apart -> longer than 12 h max
    rows += [row(4, 0), row(4, 20)]                                  # 20 min -> shorter than 30 min
    ev = build_ais_gaps(spark.createDataFrame(rows, SCHEMA), 1800, 12 * 3600, "2026-11-11 00:00:00").collect()
    assert [(r["mmsi"], r["duration_min"], r["start_inst_name"]) for r in ev] == [(1, 60.0, "PLATFORM")]
    assert build_ais_gaps(spark.createDataFrame(rows, SCHEMA), 1800, 12 * 3600, "2026-11-11 09:30:00").count() == 0


def test_regular_visitor_counts_days_present(spark):
    d = lambda m, day, h=9: datetime(2026, m, day, h)
    visits = spark.createDataFrame(
        [(1, "PLATFORM", d(11, day), d(11, day, 10)) for day in (1, 1, 5, 9)] +   # present 3 distinct days
        [(2, "PLATFORM", d(11, day), d(11, day, 10)) for day in (5, 9)] +         # 2 days
        [(4, "PLATFORM", d(10, 20), d(11, 10))] +                                 # one visit, stationed 22 days
        [(1, "PLATFORM", d(9, 1), d(9, 1, 10))],                                  # outside the 30 days
        "mmsi long, inst_name string, arrival_ts timestamp, departure_ts timestamp")
    events = spark.createDataFrame([(m, "PLATFORM", T0) for m in (1, 2, 3, 4)],
                                   "mmsi long, inst_name string, start_ts timestamp")
    out = {r["mmsi"]: (r["days_present_before"], r["regular_visitor"])
           for r in flag_regular(events, visits, "start_ts", 3, 30).collect()}
    assert out == {1: (3, True), 2: (2, False), 3: (0, False), 4: (22, True)}


def test_facility_flag_matches_sodir_names_loosely(spark):
    fac = spark.createDataFrame([("ÅSGARD B", "SEMISUB STEEL", "FIXED"), ("OSEBERG A", "CONDEEP 4 SHAFTS", "FIXED"),
                                 ("NOBLE INVINCIBLE", "JACK-UP 3 LEGS", "MOVEABLE"), ("GOLIAT FPSO", "FPSO", "FIXED"),
                                 ("AB", "JACKET 4 LEGS", "FIXED")], "fac string, kind string, fixmov string")
    ev = spark.createDataFrame([(1, "ASGARD B", 0.1), (2, "Noble Invincible", 0.2), (3, "HAVEN", 0.0), (4, "AB", 0.0),
                                (5, None, 0.0), (6, "GOLIAT FPSO", 0.3), (7, "GOLIAT FPSO", 8.5),
                                (8, "OSEBERG A", 6.0), (9, "NOBLE INVINCIBLE", 7.0)],
                               "mmsi long, vessel_name string, avg_sog_kn double")
    out = {r["mmsi"]: r["is_facility"] for r in flag_facility(ev, fac, speed_col="avg_sog_kn").collect()}
    # floating units (semisub, jack-up, FPSO) count only when stationary; a fixed Condeep always counts
    assert out == {1: True, 2: True, 3: False, 4: False, 5: False, 6: True, 7: False, 8: True, 9: False}
    assert {r["mmsi"]: r["is_facility"] for r in flag_facility(ev.drop("avg_sog_kn"), fac.select("fac")).collect()}[2]


def test_censored_and_feed_outage_follow_hourly_coverage(spark):
    h = lambda hh, mm=0: datetime(2026, 11, 11, hh, mm)
    pos = spark.createDataFrame([(i, h(hh, i % 60)) for hh, n in ((8, 100), (9, 100), (10, 100), (11, 10), (12, 100))
                                 for i in range(n)], "mmsi long, event_ts timestamp")
    ok = covered_hours(build_coverage(pos), 0.5, None)
    loiter = spark.createDataFrame([(1, h(9, 30)), (2, h(8, 10))], "mmsi long, start_ts timestamp")
    assert {r["mmsi"]: r["censored"] for r in flag_censored(loiter, ok, "start_ts").collect()} == {1: False, 2: True}
    gaps = spark.createDataFrame([(1, h(10, 30), h(12, 10)), (2, h(9, 10), h(10, 50))],
                                 "mmsi long, start_ts timestamp, end_ts timestamp")
    assert {r["mmsi"]: r["feed_outage"] for r in flag_outage(gaps, ok, "start_ts", "end_ts").collect()} == {1: True, 2: False}


def test_terminal_calls_and_support_craft_are_ordinary(spark):
    ev = spark.createDataFrame([
        (1, "STI ROTHERHITHE", "MONGSTAD", True, 183),       # large tanker at an onshore terminal
        (2, "SMALL BOAT", "MONGSTAD", True, 15),              # small vessel at a terminal: still checked
        (3, "UNKNOWN", "MONGSTAD", True, None),              # no length: not treated as large
        (4, "DRAUGEN MOB BOAT", "DRAUGEN", False, 8),         # carries the installation's own name
        (5, "ESVAGT FRB76", "EKOFISK K", False, 9),           # rescue boat, other name: still checked
        (6, "AASTA HANSTEEN MOB", "AASTA HANSTEEN SPAR", False, 9),   # type word in the installation name ignored
        (7, "GOLIAT MOB", "GOLIAT FPSO", False, 8),
    ], "mmsi long, vessel_name string, inst_name string, is_onshore boolean, length_m int")
    out = {r["mmsi"]: (r["terminal_call"], r["support_craft"]) for r in flag_ordinary(ev, 100).collect()}
    assert out == {1: (True, False), 2: (False, False), 3: (False, False), 4: (False, True), 5: (False, False),
                   6: (False, True), 7: (False, True)}


def test_worth_a_look_only_when_no_ordinary_explanation(spark):
    cols = "mmsi long, regular_visitor boolean, is_facility boolean, censored boolean, terminal_call boolean, support_craft boolean"
    ev = spark.createDataFrame([(1, False, False, False, False, False), (2, True, False, False, False, False),
                                (3, False, False, True, False, False), (4, False, False, False, False, True)], cols)
    assert {r["mmsi"]: r["worth_a_look"] for r in mark_worth_a_look(ev, "censored").collect()} == \
           {1: True, 2: False, 3: False, 4: False}
