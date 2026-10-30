import csv
from datetime import datetime
from spark import silver_logic as L
from spark.common import load_facilities

FILTER = {"include": {"surface": ["Y"], "phaseName": ["IN SERVICE", "INSTALLATION"]},
          "exclude_kinds": ["LANDFALL", "OFFSHORE WIND TURBINE"],
          "fixed_col": "FixMov", "moveable_kinds_allowed": ["FPSO", "FSU"]}
CFG = {"facility_name_col": "facName", "facility_kind_col": "facKind",
       "onshore_kinds": ["ONSHORE FACILITY"], "facility_filter": FILTER}
ROWS = [  # name, kind, surface, phase, FixMov
    ("PLATFORM", "JACKET 4 LEGS", "Y", "IN SERVICE", "FIXED"),
    ("TEMPLATE", "MULTI WELL TEMPLATE", "N", "IN SERVICE", "FIXED"),
    ("TERMINAL", "ONSHORE FACILITY", "Y", "IN SERVICE", "FIXED"),
    ("LANDFALL", "LANDFALL", "Y", "IN SERVICE", "FIXED"),
    ("RIG", "SEMISUB STEEL", "Y", "IN SERVICE", "MOVEABLE"),
    ("FLOATER", "FPSO", "Y", "IN SERVICE", "MOVEABLE"),
    ("GONE", "JACKET 4 LEGS", "Y", "REMOVED", "FIXED"),
    ("NEW", "JACKET 8 LEGS", "Y", "INSTALLATION", "FIXED"),
    ("TURBINE", "OFFSHORE WIND TURBINE", "Y", "IN SERVICE", "FIXED"),
]
KM = 1000 / 111195.0   # degrees of latitude per km (haversine uses R = 6371 km)


def _csv(tmp_path):
    p = tmp_path / "facilities.csv"
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["facName", "facKind", "surface", "phaseName", "FixMov", "centroid_lat", "centroid_lon"])
        for i, r in enumerate(ROWS):
            w.writerow([*r, 60 + i, 2.0])
    return str(p)


def test_filter_keeps_platforms_floaters_onshore_and_new(spark, tmp_path):
    out = {r["inst_name"]: r["is_onshore"] for r in load_facilities(spark, CFG, _csv(tmp_path)).collect()}
    assert out == {"PLATFORM": False, "TERMINAL": True, "FLOATER": False, "NEW": False}


def test_no_filter_keeps_everything(spark, tmp_path):
    assert load_facilities(spark, dict(CFG, facility_filter=None), _csv(tmp_path)).count() == len(ROWS)


def test_onshore_uses_smaller_radius(spark):
    fac = spark.createDataFrame(
        [("TERMINAL", "ONSHORE FACILITY", True, 60.0, 5.0), ("PLATFORM", "JACKET 4 LEGS", False, 61.0, 2.0)],
        "inst_name string, inst_kind string, is_onshore boolean, i_lat double, i_lon double")
    t = datetime(2026, 10, 30, 12, 0)
    v = spark.createDataFrame(
        [(257000001, t, 60.0 + 1 * KM, 5.0, 3.0),    # 1 km from terminal  -> near (onshore radius 1.5 km)
         (257000002, t, 60.0 + 3 * KM, 5.0, 3.0),    # 3 km from terminal  -> NOT near
         (257000003, t, 61.0 + 3 * KM, 2.0, 3.0)],   # 3 km from platform  -> near (offshore radius 5 km)
        "mmsi long, event_ts timestamp, latitude double, longitude double, speedOverGround double")
    out = {r["mmsi"]: r for r in L.enrich(v, fac, 5000, 500, 0.5, near_m_onshore=1500).collect()}
    assert out[257000001]["inst_name"] == "TERMINAL" and out[257000001]["is_onshore"] is True
    assert out[257000002]["is_near_installation"] is False
    assert out[257000003]["inst_name"] == "PLATFORM" and out[257000003]["is_onshore"] is False
