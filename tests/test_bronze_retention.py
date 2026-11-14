"""Bronze retention deletes only old files that Silver has processed."""
import importlib.util, json
from datetime import date
from pathlib import Path

spec = importlib.util.spec_from_file_location("bronze_retention", Path(__file__).parent.parent / "scripts" / "bronze_retention.py")
R = importlib.util.module_from_spec(spec)
spec.loader.exec_module(R)


def touch(p):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x" * 10)
    return p


def test_only_old_and_processed_files_go(tmp_path):
    raw = tmp_path / "raw"
    old_done = touch(raw / "dt=2026-10-01" / "hour=10" / "ais_a_1.jsonl.gz")
    old_pending = touch(raw / "dt=2026-10-01" / "hour=11" / "ais_a_2.jsonl.gz")
    recent = touch(raw / "dt=2026-11-13" / "hour=10" / "ais_a_3.jsonl.gz")
    src = tmp_path / "sources" / "0"
    src.mkdir(parents=True)
    (src / "9.compact").write_text("v1\n" + "\n".join(json.dumps({"path": f"file://{p}", "batchId": 1})
                                                       for p in (old_done, recent)) + "\n")
    done = R.processed_files(tmp_path / "sources")
    assert done == {"ais_a_1.jsonl.gz", "ais_a_3.jsonl.gz"}
    delete, keep = R.plan(raw, date(2026, 10, 15), done)
    assert delete == [old_done] and keep == [old_pending]
    for f in delete:
        f.unlink()
    R.remove_empty_dirs(raw, date(2026, 10, 15))
    assert not (raw / "dt=2026-10-01" / "hour=10").exists() and old_pending.exists() and recent.exists()


def test_missing_checkpoint_deletes_nothing(tmp_path):
    touch(tmp_path / "raw" / "dt=2026-01-01" / "hour=00" / "ais_b.jsonl.gz")
    delete, keep = R.plan(tmp_path / "raw", date(2026, 11, 1), R.processed_files(tmp_path / "nope"))
    assert delete == [] and len(keep) == 1
