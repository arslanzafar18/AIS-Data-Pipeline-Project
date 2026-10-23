import json
from spark import silver_logic as L

def bronze(spark, payloads, ing="2026-10-11T10:00:05+00:00", file="a.jsonl.gz"):
    rows = [("src", ing, "run", file, p if isinstance(p, str) else json.dumps(p)) for p in payloads]
    return spark.createDataFrame(rows, L.BRONZE_SCHEMA)

def good(**kw):
    m = {"mmsi": 257111000, "msgtime": "2026-10-11T10:00:00+00:00", "latitude": 60.5, "longitude": 3.6,
         "speedOverGround": 5.0, "courseOverGround": 90.0, "trueHeading": 90, "rateOfTurn": 0.0,
         "name": " test ship ", "shipType": 70}
    m.update(kw); return m

def run(spark, payloads, **kw):
    return L.validate(L.standardize(L.parse(bronze(spark, payloads, **kw)))).collect()

def test_valid_row_is_standardized(spark):
    r = run(spark, [good()])[0]
    assert r["reject_reason"] is None and r["name"] == "TEST SHIP"
    assert str(r["event_ts"]) == "2026-10-11 10:00:00" and r["latency_s"] == 5

def test_sentinels_corrected_not_rejected(spark):
    r = run(spark, [good(speedOverGround=102.3, trueHeading=511, rateOfTurn=-128, shipType=0, name="")])[0]
    assert r["reject_reason"] is None
    assert r["speedOverGround"] is None and r["trueHeading"] is None and r["rateOfTurn"] is None
    assert r["shipType"] is None and r["name"] is None and r["fix_sog"] and r["fix_heading"]

def test_reject_reasons(spark):
    cases = {"parse_failed": '{"mmsi": 2571', "bad_mmsi": good(mmsi=12345), "bad_timestamp": good(msgtime="garbage"),
             "missing_position": good(latitude=None), "sentinel_position": good(latitude=91, longitude=181),
             "out_of_range": good(latitude=120.0), "future_timestamp": good(msgtime="2026-10-11T11:00:00+00:00")}
    for want, payload in cases.items():
        assert run(spark, [payload])[0]["reject_reason"] == want, want

def test_unknown_field_flags_schema_drift(spark):
    r = run(spark, [good(), good(newField=1)])
    assert sorted(x["unknown_fields"] for x in r) == [False, True]

def test_dedupe_keeps_earliest_ingest(spark):
    a = L.validate(L.standardize(L.parse(bronze(spark, [good()], ing="2026-10-11T10:00:09+00:00", file="b"))))
    b = L.validate(L.standardize(L.parse(bronze(spark, [good(), good(msgtime="2026-10-11T10:00:10+00:00")], file="a"))))
    out = L.dedupe(a.unionByName(b)).orderBy("event_ts").collect()
    assert len(out) == 2 and out[0]["_file"] == "a"

def fac(spark):
    return spark.createDataFrame([("PLAT_A", "PLATFORM", 60.5, 3.6), ("PLAT_B", "PLATFORM", 60.52, 3.6)],
                                 "inst_name string, inst_kind string, i_lat double, i_lon double")

def test_enrich_nearest_and_zone_flags(spark):
    pts = [good(mmsi=257000001, latitude=60.5 + 0.002, longitude=3.6),          # ~222 m from A
           good(mmsi=257000002, latitude=60.5 + 0.03, longitude=3.6),           # ~3.3 km from A, 2.2 km from B
           good(mmsi=257000003, latitude=61.5, longitude=5.0)]                  # far
    v = L.dedupe(L.validate(L.standardize(L.parse(bronze(spark, pts)))).where("reject_reason is null"))
    out = {r["mmsi"]: r for r in L.enrich(v, fac(spark), 5000, 500, 0.5).collect()}
    assert out[257000001]["inst_name"] == "PLAT_A" and out[257000001]["is_at_installation"]
    assert out[257000002]["inst_name"] == "PLAT_B" and not out[257000002]["is_at_installation"]
    assert not out[257000003]["is_near_installation"] and out[257000003]["inst_name"] is None

def test_quality_agg_counts_add_up(spark):
    pay = [good(), good(), good(mmsi=1), '{"bad', good(speedOverGround=102.3, mmsi=257000009)]
    q = L.quality_agg(L.validate(L.standardize(L.parse(bronze(spark, pay)))))
    assert q["input_rows"] == 5 and q["valid_rows"] + q["rejected_rows"] == 5
    assert q["parse_failed"] == 1 and q["rej_bad_mmsi"] == 1 and q["corrected_sog"] == 1
    assert q["valid_rows"] == 3
