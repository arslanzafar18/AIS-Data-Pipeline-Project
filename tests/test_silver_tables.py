import json
import pytest
from pyspark.sql import SparkSession, functions as F
from spark.silver_logic import parse, standardize, validate, BRONZE_SCHEMA
from spark.silver_tables import (classify_mmsi, flag_suspect, stage_rows, fate_counts,
                                 vessel_static_observations, build_vessels)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s


def test_classify_mmsi(spark):
    rows = [(257123456, None), (111257514, None), (992571234, None), (970123456, None), (2573000, None),
            (25730000, None), (123456789, None), (12345, None), (None, None), (1000000000, None), (257000001, 500)]
    df = spark.createDataFrame(rows, "mmsi long, altitude int")
    got = {r["mmsi"]: r["mmsi_class"] for r in classify_mmsi(df).collect()}
    assert got[257123456] == "vessel"
    assert got[111257514] == "aircraft"
    assert got[992571234] == "aton"
    assert got[970123456] == "beacon"
    assert got[2573000] == "coast_station"       # 002573000: leading zeros are lost in a Long
    assert got[25730000] == "group_id"
    assert got[123456789] == "other_station"           # not a real MID
    assert got[12345] == "invalid"
    assert got[None] == "invalid"
    assert got[1000000000] == "invalid"
    assert got[257000001] == "aircraft"          # altitude reported


def test_flag_suspect(spark):
    # one vessel, a fix every 10 s moving ~0.001 deg (about 22 kn); point 4 is a lone spike 1 degree away
    base = "2026-10-12 10:00:%02d"
    rows = [(257000001, base % 0, 60.000, 3.000), (257000001, base % 10, 60.001, 3.000), (257000001, base % 20, 60.002, 3.000),
            (257000001, base % 30, 61.002, 3.000), (257000001, base % 40, 60.004, 3.000), (257000001, base % 50, 60.005, 3.000),
            (257000002, base % 0, 0.0, 0.0), (257000003, base % 0, 10.0, 3.0),
            # a fast but real transit (about 40 kn): must NOT be flagged
            (257000004, base % 0, 60.0, 3.0), (257000004, base % 10, 60.00185, 3.0), (257000004, base % 20, 60.0037, 3.0)]
    df = (spark.createDataFrame(rows, "mmsi long, t string, latitude double, longitude double")
               .withColumn("event_ts", F.to_timestamp("t")))
    got = {(r["mmsi"], r["t"]): r["suspect_reason"] for r in flag_suspect(df).collect()}
    assert got[(257000001, base % 30)] == "position_spike"
    assert got[(257000001, base % 20)] is None and got[(257000001, base % 40)] is None   # neighbours not blamed
    assert got[(257000002, base % 0)] == "null_island"
    assert got[(257000003, base % 0)] == "out_of_area"
    assert all(v is None for (m, _), v in got.items() if m == 257000004)


def _bronze(spark, msgs):
    rows = []
    for i, (payload, ingest) in enumerate(msgs):
        rows.append(("live", ingest, "run1", f"f{i % 2}.gz", payload if isinstance(payload, str) else json.dumps(payload)))
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def test_stage_rows_partitions_every_input_row(spark):
    ing = "2026-10-12T10:05:00Z"
    ok = lambda mmsi, t, lat=60.0, lon=3.0, **kw: ({"mmsi": mmsi, "msgtime": f"2026-10-12T10:00:{t:02d}Z", "latitude": lat, "longitude": lon, **kw}, ing)
    msgs = [ok(257000001, 0), ok(257000001, 10, 60.001), ok(257000001, 20, 60.002), ok(257000001, 30, 61.5), ok(257000001, 40, 60.004),
            ok(257000001, 50, 60.005),
            ok(257000001, 10, 60.001),                                  # exact duplicate of the 10 s fix
            ok(111257514, 0, altitude=500),                             # aircraft -> non_vessel
            ok(2573000, 0),                                             # coast station -> non_vessel (was a bad_mmsi reject)
            ok(12345, 0), ok(109110268, 0),                                           # invalid MMSI -> rejected
            ok(257000002, 0, 91.0, 181.0),                              # sentinel position -> rejected
            ok(257000003, 0, 0.0, 0.0),                                 # null island -> suspect
            ("{not json", ing),                                         # parse_failed -> rejected
            ({"mmsi": 257000009, "msgtime": "2013-01-01T00:00:00Z", "latitude": 60.0, "longitude": 3.0}, ing)]  # stale -> rejected
    flagged = validate(classify_mmsi(standardize(parse(_bronze(spark, msgs)))))
    st = stage_rows(flagged)
    counts = {(r["outcome"], r["reason"]): r["count"] for r in fate_counts(st).collect()}
    assert sum(counts.values()) == len(msgs)                            # every input row has exactly one fate
    assert counts[("duplicate", None)] == 1
    assert counts[("non_vessel", "aircraft")] == 1 and counts[("non_vessel", "coast_station")] == 1
    assert counts[("rejected", "bad_mmsi")] == 1 and counts[("rejected", "sentinel_position")] == 1
    assert counts[("rejected", "parse_failed")] == 1 and counts[("rejected", "stale_timestamp")] == 1
    assert counts[("suspect", "null_island")] == 1 and counts[("suspect", "position_spike")] == 1
    assert counts[("position", None)] == 5
    assert counts[("non_vessel", "other_station")] == 1


def test_old_validate_behaviour_without_mmsi_class(spark):
    df = _bronze(spark, [({"mmsi": 2573000, "msgtime": "2026-10-12T10:00:00Z", "latitude": 60.0, "longitude": 3.0}, "2026-10-12T10:05:00Z")])
    got = validate(standardize(parse(df))).collect()[0]["reject_reason"]
    assert got == "bad_mmsi"                                            # unchanged when classify_mmsi was not run


def test_vessel_dimension(spark):
    rows = [(257000001, "2026-10-01 00:00:00", "OLD NAME", 70, None), (257000001, "2026-10-01 00:00:10", "OLD NAME", 70, None),
            (257000001, "2026-10-05 00:00:00", "NEW NAME", None, 9876543),
            (257000002, "2026-10-02 00:00:00", None, None, None)]                 # no static data at all
    df = (spark.createDataFrame(rows, "mmsi long, t string, name string, shipType int, imo long")
               .withColumn("event_ts", F.to_timestamp("t")).withColumn("callSign", F.lit(None).cast("string"))
               .withColumn("shipLength", F.lit(None).cast("int")).withColumn("shipWidth", F.lit(None).cast("int")))
    obs = vessel_static_observations(df)
    assert obs.count() == 2                                              # (OLD,70) and (NEW,imo); the empty vessel is skipped
    v = {r["mmsi"]: r for r in build_vessels(obs).collect()}
    assert v[257000001]["name"] == "NEW NAME" and v[257000001]["ship_type"] == 70    # latest non-null per field
    assert v[257000001]["imo"] == 9876543 and v[257000001]["name_variants"] == 2
    assert 257000002 not in v
