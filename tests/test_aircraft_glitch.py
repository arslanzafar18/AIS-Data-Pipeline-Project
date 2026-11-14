import json
from datetime import datetime
from spark import silver_logic as L
from spark.gold_logic import build_vessel_days, build_vessels, with_position_flags


def _bronze(spark, payloads):
    rows = [("live", "2026-11-11T08:00:05+00:00", "run", "f.jsonl.gz", json.dumps(p)) for p in payloads]
    return spark.createDataFrame(rows, "_source string, _ingest_ts string, _run_id string, _file string, payload string")


def _msg(mmsi, sog, **kw):
    return dict(mmsi=mmsi, msgtime="2026-11-11T08:00:00+00:00", latitude=60.5, longitude=2.8, speedOverGround=sog, **kw)


def test_aircraft_flagged_and_impossible_ship_speed_nulled(spark):
    out = {r["mmsi"]: r for r in L.standardize(L.parse(_bronze(spark, [
        _msg(257000001, 12.0),                     # normal ship
        _msg(257000002, 94.0),                     # ship at 94 kn: glitch, nulled
        _msg(111257501, 94.0),                     # SAR helicopter MMSI: aircraft, speed kept
        _msg(257000004, 80.0, altitude=150),       # altitude reported: aircraft, speed kept
        _msg(257000005, 102.3),                    # the sentinel: corrected as before, not a glitch
    ])), 50.0).collect()}
    assert out[257000001]["speedOverGround"] == 12.0 and not out[257000001]["is_aircraft"]
    assert out[257000002]["speedOverGround"] is None and out[257000002]["sog_glitch"]
    assert out[111257501]["is_aircraft"] and out[111257501]["speedOverGround"] == 94.0 and not out[111257501]["sog_glitch"]
    assert out[257000004]["is_aircraft"] and out[257000004]["altitude"] == 150
    assert out[257000005]["fix_sog"] and not out[257000005]["sog_glitch"]


def test_quality_counts_glitches_and_aircraft(spark):
    q = L.quality_agg(L.validate(L.standardize(L.parse(_bronze(spark, [
        _msg(257000002, 94.0), _msg(257000003, 60.0), _msg(111257501, 90.0), _msg(257000001, 10.0)])))))
    assert (q["corrected_sog_glitch"], q["aircraft_rows"]) == (2, 1)


def test_old_silver_gets_flags_and_faulty_transponder_is_found(spark):
    t = datetime(2026, 11, 11, 8)
    pos = spark.createDataFrame(
        [(111257501, t, "2026-11-11", False, False, None)] +
        [(257000002, t.replace(minute=m), "2026-11-11", False, False, None) for m in range(20)],
        "mmsi long, event_ts timestamp, dt string, is_near_installation boolean, is_at_installation boolean, inst_name string")
    flagged = with_position_flags(pos)                       # Silver from before the change: no columns yet
    assert {r["mmsi"]: r["is_aircraft"] for r in flagged.select("mmsi", "is_aircraft").distinct().collect()} == \
           {111257501: True, 257000002: False}
    glitchy = flagged.withColumn("sog_glitch", (flagged.mmsi == 257000002) & (flagged.event_ts.cast("long") % 1200 < 900))
    for col in ("name", "shipType", "callSign", "imo", "shipLength", "destination"):
        glitchy = glitchy.withColumn(col, glitchy.mmsi.cast("string") if col == "name" else glitchy.mmsi * 0)
    v = {r["mmsi"]: r for r in build_vessels(build_vessel_days(glitchy), 10, 0.01).collect()}
    assert v[257000002]["sog_glitches"] == 15 and v[257000002]["faulty_speed"]
    assert v[111257501]["is_aircraft"] and not v[111257501]["faulty_speed"]
