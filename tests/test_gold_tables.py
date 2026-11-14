import csv
from datetime import date, datetime
from spark.gold_logic import build_installations, build_vessel_days, build_vessels

POS = ("mmsi long, dt date, event_ts timestamp, is_near_installation boolean, is_at_installation boolean, "
       "inst_name string, name string, shipType int, callSign string, imo long, shipLength int, destination string")


def _pos(spark):
    t = lambda d, h: datetime(2026, 11, d, h, 0)
    return spark.createDataFrame([
        (1, date(2026, 11, 11), t(11, 8), True, True, "PLATFORM", "OLD NAME", 90, "LAXX", 9000001, 80, "BERGEN"),
        (1, date(2026, 11, 11), t(11, 9), True, False, "PLATFORM", None, None, None, None, None, None),
        (1, date(2026, 11, 12), t(12, 7), False, False, None, "NEW NAME", 90, "LAXX", 9000001, 80, None),
        (2, date(2026, 11, 12), t(12, 6), True, False, "TERMINAL", "TANKER", 80, None, None, 250, "MONGSTAD"),
    ], POS)


def test_vessel_days_counts_and_latest_identity(spark):
    rows = {(r["mmsi"], str(r["dt"])): r for r in build_vessel_days(_pos(spark)).collect()}
    a = rows[(1, "2026-11-11")]
    assert (a["n_msgs"], a["n_near"], a["n_zone"], a["installations"]) == (2, 2, 1, 1)
    assert a["name"] == "OLD NAME" and a["destination"] == "BERGEN"      # latest NON-null value
    assert rows[(1, "2026-11-12")]["installations"] == 0


def test_vessels_take_latest_identity_and_totals(spark):
    v = {r["mmsi"]: r for r in build_vessels(build_vessel_days(_pos(spark))).collect()}
    assert v[1]["name"] == "NEW NAME" and v[1]["days_seen"] == 2 and v[1]["n_msgs"] == 3
    assert v[1]["first_seen"] == datetime(2026, 11, 11, 8) and v[1]["last_seen"] == datetime(2026, 11, 12, 7)
    assert v[2]["length_m"] == 250


def test_installations_apply_filter_and_carry_field(spark, tmp_path):
    p = tmp_path / "facilities.csv"
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["facName", "facKind", "surface", "phaseName", "FixMov", "belong2nm", "centroid_lat", "centroid_lon"])
        w.writerow(["OSEBERG A", "CONDEEP 4 SHAFTS", "Y", "IN SERVICE", "FIXED", "OSEBERG", 60.49, 2.83])
        w.writerow(["TEMPLATE", "MULTI WELL TEMPLATE", "N", "IN SERVICE", "FIXED", "OSEBERG", 60.5, 2.8])
        w.writerow(["MONGSTAD", "ONSHORE FACILITY", "Y", "IN SERVICE", "FIXED", "", 60.81, 5.03])
    cfg = {"facility_name_col": "facName", "facility_kind_col": "facKind", "onshore_kinds": ["ONSHORE FACILITY"],
           "facility_filter": {"include": {"surface": ["Y"], "phaseName": ["IN SERVICE"]},
                               "fixed_col": "FixMov", "moveable_kinds_allowed": ["FPSO"]}}
    rows = {r["inst_name"]: r for r in build_installations(spark, cfg, str(p)).collect()}
    assert set(rows) == {"OSEBERG A", "MONGSTAD"}
    assert rows["OSEBERG A"]["field"] == "OSEBERG" and rows["MONGSTAD"]["is_onshore"] is True
