"""Pipeline health tab: the Gold run log and the per-day Silver summary (plain Python, no Spark session needed)."""
import json
from datetime import datetime, timedelta, timezone
from spark import gold_job
from spark.dashboard import pipeline_health

T0 = datetime(2026, 11, 10)


def _write(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write((r if isinstance(r, str) else json.dumps(r)) + "\n")


def test_run_log_records_published_and_blocked_runs(tmp_path):
    P = lambda rel: str(tmp_path / rel)
    ok = {"run_id": "a", "started_at": "2026-11-10T10:00:00+00:00", "finished_at": "2026-11-10T10:01:40+00:00",
          "runtime_s": 100.0, "status": "READY", "mode": "incremental", "silver_rows": 5,
          "checks": [{"check": "x", "passed": True}], "warnings": [], "keys": {"repeated_keys": {"vessel_tracks": 0}}}
    bad = dict(ok, run_id="b", status="FAILED", checks=[{"check": "silver_fresh", "passed": False}],
               keys={"repeated_keys": {"vessel_tracks": 2, "vessel_days": 1}}, warnings=["repeated keys ..."])
    gold_job.log_run(P, ok)
    gold_job.log_run(P, bad)
    lines = [json.loads(l) for l in open(tmp_path / "quality" / "gold_runs.jsonl")]
    assert [l["status"] for l in lines] == ["READY", "FAILED"]
    assert lines[1]["failed"] == ["silver_fresh"] and lines[1]["repeated_keys"] == 3 and lines[0]["checks"] == 1


def test_health_sums_silver_per_day_and_reads_runs(tmp_path):
    q, m = tmp_path / "quality", tmp_path / "metrics"
    q.mkdir(); m.mkdir()
    batch = lambda ts, **kw: {"processed_at": ts, "input_rows": 100, "accepted_rows": 90, "in_batch_duplicates": 2,
                              "cross_batch_duplicates": 1, "latency_p95_s": kw.get("p95", 5), "runtime_s": kw.get("rt", 3.0),
                              "rej_bad_mmsi": 4, "rej_sentinel_position": 6}
    _write(q / "silver_metrics.jsonl", [
        batch("2026-11-09T23:00:00+00:00"),                       # before t0: only counts as the last batch seen
        batch("2026-11-10T01:00:00+00:00", rt=2.0),
        batch("2026-11-10T02:00:00+00:00", rt=4.0, p95=9),
        "not json",
        batch("2026-11-11T05:00:00+00:00")])
    _write(q / "gold_runs.jsonl", [
        {"finished_at": "2026-11-09T05:00:00+00:00", "status": "READY"},   # before t0: left out
        {"finished_at": "2026-11-10T03:02:00+00:00", "status": "FAILED", "mode": "incremental", "runtime_s": 90,
         "failed": ["silver_fresh"], "warnings": [], "repeated_keys": 0},
        {"finished_at": "2026-11-11T06:00:00+00:00", "status": "ERROR", "warnings": ["Gold crashed"]}])
    (m / "heartbeat.json").write_text(json.dumps({"ts": "2026-11-11T06:58:00+00:00"}))
    h = pipeline_health(T0, str(q), str(m), str(tmp_path), now=T0 + timedelta(hours=31))
    sv = h["sv"]
    assert sv["t"] == [0, 24] and sv["b"] == [2, 1] and sv["in"] == [200, 100] and sv["dup"] == [6, 3]
    assert sv["rt"] == [3.0, 3.0] and sv["rtx"] == [4.0, 3.0] and sv["p95"] == [9, 5]
    assert sv["rej"]["sentinel_position"] == [12, 6] and sv["rej"]["parse_failed"] == [0, 0]
    assert [(r["t"], r["s"], r["f"]) for r in h["runs"]] == [(3, "FAILED", ["silver_fresh"]), (30, "ERROR", [])]
    assert h["heartbeat"] == "2026-11-11T06:58:00Z" and h["silver_batch"] == "2026-11-11T05:00:00Z"
    assert 0 <= h["disk_free_pct"] <= 100


def test_health_without_files_is_empty_not_an_error(tmp_path):
    h = pipeline_health(T0, str(tmp_path / "none"), None, None, now=datetime.now(timezone.utc).replace(tzinfo=None))
    assert h["sv"]["t"] == [] and h["runs"] == [] and h["heartbeat"] is None and h["disk_free_pct"] is None


def test_bronze_columns_finds_contract_gaps_new_and_absent_fields(spark):
    from spark import silver_logic as L
    good = '{"mmsi": 257000001, "msgtime": "2026-11-10T10:00:00+00:00", "latitude": 60.5, "longitude": 3.6, "newField": 1}'
    rows = [("live", "2026-11-10T10:00:05+00:00", "r1", "f1", good),
            ("live", "2026-11-10T10:00:05+00:00", None, "f1", good),          # broken record contract
            ("live", "2026-11-10T10:00:05+00:00", "r1", "f1", '{"bad')]       # unreadable: no keys, never an error
    out = L.bronze_columns(spark.createDataFrame(rows, L.BRONZE_SCHEMA))
    assert out["bronze_contract_bad_rows"] == 1
    assert out["unknown_keys"] == {"newField": 2}
    assert "altitude" in out["absent_keys"] and "shipType" in out["absent_keys"]
    assert not set(L.ESSENTIAL_FIELDS) & set(out["absent_keys"])


def test_health_summarises_bronze_columns_per_day_and_reference_headers(tmp_path):
    q = tmp_path / "quality"
    q.mkdir()
    b = lambda ts, **kw: dict({"processed_at": ts, "input_rows": 10, "accepted_rows": 10}, **kw)
    _write(q / "silver_metrics.jsonl", [
        b("2026-11-10T01:00:00+00:00"),                                      # written before the check existed
        b("2026-11-10T02:00:00+00:00", bronze_contract_bad_rows=1, unknown_keys={"x": 3}, absent_keys=["altitude", "imoNumber"]),
        b("2026-11-10T03:00:00+00:00", bronze_contract_bad_rows=0, unknown_keys={"x": 2}, absent_keys=["altitude"])])
    ref = tmp_path / "pipelines.csv"
    ref.write_text("pipeline_id,name,lat1\n1,A,60\n")
    h = pipeline_health(T0, str(q), None, None, now=T0 + timedelta(hours=5),
                        reference=[("Pipelines", str(ref), ["pipeline_id", "name", "lat1", "lat2"]),
                                   ("Radar", str(tmp_path / "none.csv"), ["date"])])
    sv = h["sv"]
    assert sv["cb"] == [2] and sv["bad"] == [1] and sv["unk"] == [{"x": 5}] and sv["abs"] == [["altitude"]]
    assert h["ref"][0]["missing"] == ["lat2"] and h["ref"][0]["columns"] == 3 and h["ref"][0]["updated"].endswith("Z")
    assert h["ref"][1]["columns"] is None and h["ref"][1]["missing"] == []


def test_bronze_files_counts_expected_received_and_missing(tmp_path):
    from spark.dashboard import bronze_files
    ev = tmp_path / "events.jsonl"
    rows = [{"ts": "2026-11-10T00:00:05+00:00", "event": "start"}]
    for k in range(1, 30):                                     # 00:02 .. 00:58, one file per 2 minutes
        if k not in (10, 11):                                  # two windows lost
            rows.append({"ts": f"2026-11-10T00:{2 * k:02d}:00+00:00", "event": "flush", "file": "f", "messages": 100})
    rows.append({"ts": "2026-11-10T00:40:00+00:00", "event": "reader_error"})
    _write(ev, rows)
    out = bronze_files(str(ev), T0, T0 + timedelta(hours=1), flush_seconds=120)
    assert out["t"] == [0] and out["exp"] == [29] and out["files"] == [27] and out["msgs"] == [2700]
    assert out["errors"] == [1] and out["starts"] == [1] and out["empty"] == [0]
    assert bronze_files(str(tmp_path / "none.jsonl"), T0, T0) == {k: [] for k in ("t", "exp", "files", "msgs", "empty", "errors", "starts")}


def test_health_sums_silver_corrections(tmp_path):
    q = tmp_path / "quality"
    q.mkdir()
    _write(q / "silver_metrics.jsonl", [
        {"processed_at": "2026-11-10T01:00:00+00:00", "input_rows": 100, "accepted_rows": 90, "rejected_rows": 6,
         "corrected_sog": 1, "corrected_heading": 2, "corrected_rot": 3, "corrected_shiptype": 4, "corrected_sog_glitch": 5, "aircraft_rows": 7},
        {"processed_at": "2026-11-10T02:00:00+00:00", "input_rows": 50, "accepted_rows": 50, "rejected_rows": 0}])
    sv = pipeline_health(T0, str(q), None, None, now=T0 + timedelta(hours=3))["sv"]
    assert sv["rj"] == [6] and sv["fix"] == [10] and sv["gl"] == [5] and sv["air"] == [7]
