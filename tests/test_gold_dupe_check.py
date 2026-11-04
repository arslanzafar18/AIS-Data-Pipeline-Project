import json
from datetime import datetime, timedelta, timezone
from spark.gold_job import _dupe_check_due


def _write(p, when):
    p.write_text(json.dumps({"checked_at": when.isoformat()}))


def test_due_when_never_checked(tmp_path):
    assert _dupe_check_due(str(tmp_path / "missing.json"), 24)


def test_not_due_after_recent_check(tmp_path):
    p = tmp_path / "c.json"; _write(p, datetime.now(timezone.utc) - timedelta(hours=2))
    assert not _dupe_check_due(str(p), 24)


def test_due_after_old_check(tmp_path):
    p = tmp_path / "c.json"; _write(p, datetime.now(timezone.utc) - timedelta(hours=25))
    assert _dupe_check_due(str(p), 24)


def test_zero_hours_means_every_run(tmp_path):
    p = tmp_path / "c.json"; _write(p, datetime.now(timezone.utc))
    assert _dupe_check_due(str(p), 0) and _dupe_check_due(str(p), None)
