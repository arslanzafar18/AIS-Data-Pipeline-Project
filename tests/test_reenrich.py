from datetime import datetime
from spark.reenrich import reenrich_day

KM = 1000 / 111195.0
CFG = {"near_m": 5000, "at_m": 500, "stationary_kn": 0.5, "near_m_onshore": 1500}


def test_reenrich_replaces_installation_columns_and_keeps_rows(spark):
    fac = spark.createDataFrame([("PLATFORM", "JACKET 4 LEGS", False, 61.0, 2.0)],
                                "inst_name string, inst_kind string, is_onshore boolean, i_lat double, i_lon double")
    t = datetime(2026, 10, 30, 12, 0)
    old = spark.createDataFrame(
        [(257000001, t, 61.0 + 1 * KM, 2.0, 3.0, "TEMPLATE", 40.0, True),   # was a well template, now the platform
         (257000002, t, 63.0, 2.0, 3.0, "WELL", 20.0, True)],               # facility filtered out: no longer near
        "mmsi long, event_ts timestamp, latitude double, longitude double, speedOverGround double, "
        "inst_name string, dist_m double, is_near_installation boolean")
    out = {r["mmsi"]: r for r in reenrich_day(old, fac, CFG).collect()}
    assert len(out) == 2
    assert out[257000001]["inst_name"] == "PLATFORM" and 900 < out[257000001]["dist_m"] < 1100
    assert out[257000002]["inst_name"] is None and out[257000002]["is_near_installation"] is False
