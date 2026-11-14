from datetime import datetime
from spark.reenrich import refresh_flags


def test_refresh_flags_on_old_silver_rows(spark):
    t = datetime(2026, 10, 30, 12, 0)
    old = spark.createDataFrame(
        [(257000001, t, 12.0), (257000002, t, 94.0), (111257501, t, 94.0), (257000003, t, None)],
        "mmsi long, event_ts timestamp, speedOverGround double")          # no altitude / is_aircraft / sog_glitch yet
    out = {r["mmsi"]: r for r in refresh_flags(old, 50.0).collect()}
    assert out[257000001]["speedOverGround"] == 12.0 and not out[257000001]["sog_glitch"] and not out[257000001]["is_aircraft"]
    assert out[257000002]["speedOverGround"] is None and out[257000002]["sog_glitch"]        # ship at 94 kn: nulled
    assert out[111257501]["is_aircraft"] and out[111257501]["speedOverGround"] == 94.0      # helicopter keeps its speed
    assert not out[257000003]["sog_glitch"] and out[257000003]["altitude"] is None


def test_refresh_flags_keeps_existing_flags(spark):
    t = datetime(2026, 11, 14, 12, 0)
    new = spark.createDataFrame([(257000002, t, None, True, 120, False)],
                                "mmsi long, event_ts timestamp, speedOverGround double, sog_glitch boolean, altitude int, is_aircraft boolean")
    r = refresh_flags(new, 50.0).collect()[0]
    assert r["sog_glitch"] and r["is_aircraft"] and r["altitude"] == 120    # glitch stays flagged; altitude marks an aircraft
