from datetime import datetime, timedelta
from spark.gold_logic import build_ais_gaps, build_loitering, build_zone_entries, flag_regular

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


def test_regular_visitor_flag_counts_distinct_previous_days(spark):
    visits = spark.createDataFrame(
        [(1, "PLATFORM", datetime(2026, 11, d, 9)) for d in (1, 1, 5, 9)] +       # 3 distinct days
        [(2, "PLATFORM", datetime(2026, 11, d, 9)) for d in (5, 9)] +             # 2 days
        [(1, "PLATFORM", datetime(2026, 9, 1, 9))],                               # outside 30 days
        "mmsi long, inst_name string, arrival_ts timestamp")
    events = spark.createDataFrame([(1, "PLATFORM", T0), (2, "PLATFORM", T0), (3, "PLATFORM", T0)],
                                   "mmsi long, inst_name string, start_ts timestamp")
    out = {r["mmsi"]: (r["visit_days_before"], r["regular_visitor"])
           for r in flag_regular(events, visits, "start_ts", 3, 30).collect()}
    assert out == {1: (3, True), 2: (2, False), 3: (0, False)}
