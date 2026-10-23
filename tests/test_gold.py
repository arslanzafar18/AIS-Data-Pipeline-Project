from datetime import datetime, timedelta
from pyspark.sql import functions as F
from spark.gold_logic import build_hourly, build_visits

T0 = datetime(2026, 10, 11, 10, 0, 0)
SCHEMA = ("mmsi long, inst_name string, inst_kind string, event_ts timestamp, name string, shipType int, "
          "dist_m double, is_at_installation boolean, speedOverGround double")

def rows(spark, spec):
    data = [(m, "A", "PLATFORM", T0 + timedelta(minutes=mins), "V", 70, float(d), d <= 500, 1.0) for m, mins, d in spec]
    return spark.createDataFrame(data, SCHEMA)

def test_gap_over_threshold_starts_new_visit(spark):
    near = rows(spark, [(1, 0, 4000), (1, 10, 300), (1, 20, 4000), (1, 90, 4500), (1, 100, 200)])
    v = build_visits(near, 1800).orderBy("arrival_ts").collect()
    assert len(v) == 2
    assert v[0]["n_msgs"] == 3 and v[0]["duration_min"] == 20 and v[0]["entered_zone"] == 1
    assert v[1]["n_msgs"] == 2 and v[1]["min_dist_m"] == 200

def test_visits_and_hourly_reconcile_with_input(spark):
    near = rows(spark, [(1, 0, 400), (2, 5, 4000), (1, 70, 450), (2, 80, 100)])
    n = near.count()
    assert build_visits(near, 1800).agg(F.sum("n_msgs")).collect()[0][0] == n
    h = build_hourly(near)
    assert h.agg(F.sum("position_msgs")).collect()[0][0] == n
    assert h.where(F.col("hour_ts") == T0).collect()[0]["distinct_vessels"] == 2
