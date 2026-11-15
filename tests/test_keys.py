"""Keys: Sodir's facility number as the installation key, and the uniqueness check used by the Gold gate."""
import csv
from datetime import datetime, timedelta
from spark import silver_logic as L
from spark.common import load_facilities
from spark.gold_logic import build_installations, build_visits, conform_installations, duplicate_keys

T0 = datetime(2026, 10, 11, 10, 0, 0)
CFG = {"facility_name_col": "facName", "facility_kind_col": "facKind", "facility_id_col": "idFacility",
       "onshore_kinds": ["ONSHORE FACILITY"], "facility_filter": None}


def _csv(tmp_path):
    p = tmp_path / "facilities.csv"
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["idFacility", "facName", "facKind", "centroid_lat", "centroid_lon"])
        w.writerow(["272000", "PLAT_A", "JACKET 4 LEGS", 60.5, 3.6])
        w.writerow(["272001", "MONGSTAD", "ONSHORE FACILITY", 60.81, 5.03])
    return str(p)


def test_facility_number_is_read_and_carried_into_silver(spark, tmp_path):
    fac = load_facilities(spark, CFG, _csv(tmp_path))
    ids = {r["inst_name"]: r["inst_id"] for r in fac.collect()}
    assert ids == {"PLAT_A": 272000, "MONGSTAD": 272001}
    v = spark.createDataFrame([(257000001, T0, 60.502, 3.6, 1.0), (257000002, T0, 62.0, 3.6, 1.0)],
                              "mmsi long, event_ts timestamp, latitude double, longitude double, speedOverGround double")
    out = {r["mmsi"]: r for r in L.enrich(v, fac, 5000, 500, 0.5).collect()}
    assert out[257000001]["inst_id"] == 272000 and out[257000001]["inst_name"] == "PLAT_A"
    assert out[257000002]["inst_id"] is None
    assert "inst_id" in L.SILVER_COLUMNS


def test_facility_list_without_numbers_still_works(spark):
    fac = spark.createDataFrame([("PLAT_A", "PLATFORM", 60.5, 3.6)], "inst_name string, inst_kind string, i_lat double, i_lon double")
    v = spark.createDataFrame([(257000001, T0, 60.502, 3.6, 1.0)],
                              "mmsi long, event_ts timestamp, latitude double, longitude double, speedOverGround double")
    r = L.enrich(v, fac, 5000, 500, 0.5).collect()[0]
    assert r["inst_name"] == "PLAT_A" and r["inst_id"] is None


def test_renamed_installation_keeps_one_history(spark):
    # Sodir renamed facility 10 from OLD NAME to NEW NAME. Older Silver rows carry the old name (one even has no
    # number, written before Silver stored it); newer rows carry the new name. One visit must come out, not two.
    inst = spark.createDataFrame([(10, "NEW NAME"), (11, "OTHER")], "inst_id long, inst_name string")
    pos = spark.createDataFrame([
        (1, 10, "OLD NAME", T0, 400.0, True),
        (1, None, "NEW NAME", T0 + timedelta(minutes=10), 300.0, True),
        (1, 10, "NEW NAME", T0 + timedelta(minutes=20), 200.0, True),
        (2, None, None, T0, None, False),
    ], "mmsi long, inst_id long, inst_name string, event_ts timestamp, dist_m double, is_at_installation boolean")
    out = conform_installations(pos, inst)
    assert out.count() == pos.count()                                   # the joins never add or drop rows
    near = out.where("inst_name IS NOT NULL").selectExpr("*", "'PLATFORM' AS inst_kind", "'V' AS name",
                                                          "70 AS shipType", "1.0 AS speedOverGround")
    assert {(r["inst_id"], r["inst_name"]) for r in near.collect()} == {(10, "NEW NAME")}
    v = build_visits(near, 1800).collect()
    assert len(v) == 1 and v[0]["inst_id"] == 10 and v[0]["n_msgs"] == 3
    other = out.where("mmsi = 2").collect()[0]
    assert other["inst_id"] is None and other["inst_name"] is None


def test_duplicate_keys_counts_repeated_keys(spark):
    df = spark.createDataFrame([(1, "A"), (1, "A"), (1, "B"), (2, "A"), (2, "A"), (2, "A")], "mmsi long, inst_name string")
    assert duplicate_keys(df, ["mmsi", "inst_name"]) == 2
    assert duplicate_keys(df.dropDuplicates(), ["mmsi", "inst_name"]) == 0


def test_installations_dimension_has_the_number(spark, tmp_path):
    rows = {r["inst_name"]: r for r in build_installations(spark, CFG, _csv(tmp_path)).collect()}
    assert rows["PLAT_A"]["inst_id"] == 272000 and rows["MONGSTAD"]["is_onshore"] is True
