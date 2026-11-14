"""Monitor checks: disk space and hourly message volume (stdlib, no Spark)."""
import importlib.util, json
from datetime import datetime, timedelta, timezone
from pathlib import Path

spec = importlib.util.spec_from_file_location("check_heartbeat", Path(__file__).parent.parent / "scripts" / "check_heartbeat.py")
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)

NOW = datetime(2026, 11, 14, 18, 20, tzinfo=timezone.utc)


def flushes(per_hour):
    """events.jsonl lines: each hour split over 30 flushes of 2 minutes."""
    out = []
    for h, n in per_hour.items():
        for i in range(30):
            ts = h + timedelta(minutes=2 * i)
            out.append(json.dumps({"ts": ts.isoformat(), "event": "flush", "messages": n // 30}))
    out.append(json.dumps({"ts": NOW.isoformat(), "event": "reader_error", "error": "x"}))
    return out


def hours(n_last, usual=30000, history=24):
    last = NOW.replace(minute=0) - timedelta(hours=1)
    d = {last - timedelta(hours=i): usual for i in range(1, history + 1)}
    d[last] = n_last
    return d


def test_normal_hour_passes():
    msg, problem = M.volume_check(M.hourly_messages(flushes(hours(27000))), NOW, 0.5)
    assert problem is None and "usual 30000" in msg


def test_hour_with_half_the_usual_messages_alerts():
    _, problem = M.volume_check(M.hourly_messages(flushes(hours(9000))), NOW, 0.5)
    assert problem and "message volume low" in problem


def test_silent_hour_alerts_and_current_hour_is_ignored():
    d = hours(0)
    d[NOW.replace(minute=0)] = 30000            # the running hour is incomplete and never judged
    _, problem = M.volume_check(M.hourly_messages(flushes(d)), NOW, 0.5)
    assert problem


def test_no_alert_without_history():
    _, problem = M.volume_check(M.hourly_messages(flushes(hours(0, history=3))), NOW, 0.5)
    assert problem is None


def test_disk_check(tmp_path):
    msg, problem = M.disk_check(tmp_path, 0)
    assert problem is None and "disk free" in msg
    _, problem = M.disk_check(tmp_path, 100.1)
    assert problem and "disk almost full" in problem
