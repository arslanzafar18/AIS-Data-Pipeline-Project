import importlib.util, pathlib
import pytest

_spec = importlib.util.spec_from_file_location(
    "fetch_sodir", pathlib.Path(__file__).resolve().parent.parent / "reference" / "fetch_sodir.py")
fs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fs)

OLD_HEADER = ("idFacility,idOperator,idCompResp,FixMov,facName,facKind,phaseName,idBelongTo,facFunc,datum,ns_deg,"
              "ns_min,ns_second,ns_code,ew_deg,ew_min,ew_second,ew_code,utmHemis,utm_zone,ns_utm,ew_utm,rkb,WaterDepth,"
              "dtStartup,lifetime,statusName,surface,belong2nm,belong2knd,curOperNam,curRespCom,natonCode2,nationName,"
              "belong2url,FactUrl,MapUrl,SlotCpGros,SlotCpNet,dtUpdated,FormlQuad,FormlBlock,yrRemoved,yrShutdown,"
              "centroid_lon,centroid_lat,_source,_ingest_ts").split(",")

OSEBERG = {"attributes": {"fclNpdidFacility": 271245, "fclFixedOrMoveable": "FIXED", "fclName": "OSEBERG A",
                          "fclKind": "CONDEEP 4 SHAFTS", "fclPhase": "IN SERVICE", "fclSurface": "Y",
                          "fclBelongsToName": "OSEBERG", "fclStartupDate": 596934000000,
                          "fclDateUpdated": 1771542000000, "fclDateRemoved": None, "fclWaterDepth": 109.0},
           "geometry": {"x": 2.825646592000055, "y": 60.49134963800003}}


def test_header_matches_the_old_shapefile_csv():
    assert fs.HEADER == OLD_HEADER


def test_to_rows_maps_names_dates_and_position():
    rows = fs.to_rows([OSEBERG, {"attributes": {"fclName": "NO POSITION"}, "geometry": None}], "src", "t")
    assert len(rows) == 1
    r = rows[0]
    assert (r["facName"], r["facKind"], r["phaseName"], r["surface"], r["FixMov"]) == \
           ("OSEBERG A", "CONDEEP 4 SHAFTS", "IN SERVICE", "Y", "FIXED")
    assert r["dtStartup"] == "1988-12-01" and r["dtUpdated"] == "2026-02-20" and r["yrRemoved"] == ""
    assert abs(r["centroid_lon"] - 2.8256) < 1e-3 and abs(r["centroid_lat"] - 60.4913) < 1e-3
    assert r["_source"] == "src" and r["idCompResp"] == ""


def test_diff_finds_added_removed_and_changed():
    base = fs.to_rows([OSEBERG], "s", "t")[0]
    old = [dict(base), dict(base, idFacility="1", facName="GONE")]
    new = [dict(base, phaseName="SHUT DOWN", centroid_lat=base["centroid_lat"] + 0.01),
           dict(base, idFacility="2", facName="NEW ONE")]
    d = fs.diff(old, new)
    assert [x["name"] for x in d["added"]] == ["NEW ONE"]
    assert [x["name"] for x in d["removed"]] == ["GONE"]
    c = d["changed"][0]
    assert (c["id"], c["name"]) == ("271245", "OSEBERG A")
    assert c["changed"]["phaseName"] == ["IN SERVICE", "SHUT DOWN"] and 1000 < c["changed"]["position_moved_m"] < 1200


def test_unchanged_list_has_no_differences():
    rows = fs.to_rows([OSEBERG], "s", "t")
    as_text = [{k: str(v) for k, v in rows[0].items()}]       # as read back from the CSV
    assert fs.diff(as_text, rows) == {"added": [], "removed": [], "changed": []}


def test_missing_api_field_stops_the_run():
    with pytest.raises(SystemExit):
        fs.check_fields(["OBJECTID", "fclName"])


def test_csv_written_atomically_with_old_header(tmp_path):
    p = tmp_path / "facilities.csv"
    fs.write_csv_atomic(str(p), fs.to_rows([OSEBERG], "s", "t"))
    assert p.read_text().splitlines()[0].split(",") == OLD_HEADER
    assert not list(tmp_path.glob(".*tmp"))


def test_text_is_trimmed_so_names_still_match():
    padded = {"attributes": dict(OSEBERG["attributes"], fclName="CASTORO 10 ", fclKind=" VESSEL"),
              "geometry": OSEBERG["geometry"]}
    r = fs.to_rows([padded], "s", "t")[0]
    assert (r["facName"], r["facKind"]) == ("CASTORO 10", "VESSEL")
