"""Sodir pipeline segments and loitering over pipelines."""
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("fp", Path(__file__).parent.parent / "reference" / "fetch_pipelines.py")
FP = importlib.util.module_from_spec(spec)
spec.loader.exec_module(FP)


def test_features_become_segments():
    f = {"attributes": {"pplNpdidPipeline": 7, "pplName": "Gas pipe ", "pplCurrentPhase": "IN SERVICE", "pplMedium": "Gas"},
         "geometry": {"paths": [[[3.0, 60.0], [3.1, 60.0], [3.2, 60.1]], [[4.0, 61.0], [4.1, 61.0]]]}}
    rows = FP.to_segments([f, {"attributes": {"pplNpdidPipeline": 8}, "geometry": None}], "src", "now")
    assert [r["seg_no"] for r in rows] == [0, 1, 2] and rows[0]["name"] == "Gas pipe" and rows[2]["lon1"] == 4.0
    with pytest.raises(SystemExit):
        FP.check_fields(["pplName"])


def test_point_segment_distance():
    from spark.gold_pipelines import point_segment_m_py as d
    # a pipeline running east along 60 N; a point 0.004 deg north of its middle is about 445 m away
    assert abs(d(60.004, 3.05, 60.0, 3.0, 60.0, 3.1) - 445.3) < 1
    assert abs(d(60.0, 2.99, 60.0, 3.0, 60.0, 3.1) - 556.6) < 1      # beyond the west end: distance to the end point
    assert d(60.0, 3.0, 60.0, 3.0, 60.0, 3.0) == 0.0                  # a zero-length segment


def test_loitering_over_a_pipeline(spark):
    from spark.gold_pipelines import build_pipeline_loitering
    seg = spark.createDataFrame([("7", "Gas pipe", "GP", "Gas", "IN SERVICE", "0", 3.0, 60.0, 3.1, 60.0)],
                                "pipeline_id string, name string, label string, medium string, phase string, seg_no string, "
                                "lon1 double, lat1 double, lon2 double, lat2 double")
    t0 = datetime(2026, 11, 10, 8, 0)
    rows = [(1, t0 + timedelta(minutes=10 * i), 60.002, 3.05, 0.5, 20000.0) for i in range(16)]       # 2.5 h, 220 m away
    rows += [(2, t0 + timedelta(minutes=10 * i), 60.002, 3.05, 0.5, 20000.0) for i in range(6)]       # only 50 min
    rows += [(3, t0 + timedelta(minutes=10 * i), 60.002, 3.05, 0.5, 900.0) for i in range(16)]        # next to an installation
    rows += [(4, t0 + timedelta(minutes=10 * i), 60.002, 3.05, 9.0, 20000.0) for i in range(16)]      # passing at 9 kn
    rows += [(5, t0 + timedelta(minutes=10 * i), 60.02, 3.05, 0.5, 20000.0) for i in range(16)]       # 2.2 km away
    tracks = spark.createDataFrame(rows, "mmsi long, slot_ts timestamp, lat double, lon double, sog_kn double, dist_m double")
    out = build_pipeline_loitering(tracks, seg, 500, 5000, 2.0, 120, 1800).collect()
    assert [r["mmsi"] for r in out] == [1]
    r = out[0]
    assert r["pipeline_id"] == "7" and r["duration_min"] == 150 and r["n_positions"] == 16 and abs(r["min_dist_m"] - 222.6) < 1
    # the same vessel next to an onshore terminal (8 km away) is left out by the shore rule; 20 km away it is kept
    near_shore = spark.createDataFrame([(60.07, 3.05)], "i_lat double, i_lon double")
    far_shore = spark.createDataFrame([(60.18, 3.05)], "i_lat double, i_lon double")
    assert build_pipeline_loitering(tracks, seg, 500, 5000, 2.0, 120, 1800, shore=near_shore, min_shore_m=15000).count() == 0
    assert build_pipeline_loitering(tracks, seg, 500, 5000, 2.0, 120, 1800, shore=far_shore, min_shore_m=15000).count() == 1



def test_segments_with_inch_marks_are_read_correctly(spark, tmp_path):
    """Pipeline names such as 36" Gas: Python's csv doubles the quote, and Spark must read it back as one field."""
    from spark.gold_pipelines import segments_df
    rows = FP.to_segments([{"attributes": {"pplNpdidPipeline": 1, "pplName": '36" Gas, EKOFISK', "pplCurrentPhase": "IN SERVICE"},
                            "geometry": {"paths": [[[3.0, 56.5], [3.1, 56.6]]]}}], "src", "now")
    path = tmp_path / "pipelines.csv"
    FP.write_csv_atomic(str(path), rows)
    seg = segments_df(spark, str(path), ["IN SERVICE"]).collect()
    assert len(seg) == 1 and seg[0]["name"] == '36" Gas, EKOFISK' and seg[0]["lat2"] == 56.6
