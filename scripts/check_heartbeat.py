"""Monitor workflow check. Fails (=> the monitor workflow opens a pipeline-alert issue) if:
  - the collector heartbeat is stale (HEARTBEAT_URL, MAX_AGE_SECONDS);
  - GOLD_URL is set and the published Gold manifest is not READY or too old (GOLD_MAX_AGE_MIN);
  - DATA_DIR is set and its disk has less than MIN_FREE_PCT percent free;
  - EVENTS_PATH is set and the last full hour brought far fewer messages than usual
    (below VOLUME_MIN_FRACTION of the median of the previous 24 full hours).
Stdlib only."""
import json, os, shutil, statistics, sys, urllib.request
from datetime import datetime, timedelta, timezone


def age_s(ts):
    return (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds()


def disk_check(path, min_free_pct):
    u = shutil.disk_usage(path)
    free = 100.0 * u.free / u.total
    msg = f"disk free {free:.1f}% ({u.free / 1e9:.1f} of {u.total / 1e9:.1f} GB) at {path}"
    return msg, (f"disk almost full: {free:.1f}% free (min {min_free_pct}%)" if free < min_free_pct else None)


def hourly_messages(lines):
    """Messages per UTC hour from collector 'flush' events (metrics events.jsonl lines)."""
    per_hour = {}
    for line in lines:
        try:
            e = json.loads(line)
        except ValueError:
            continue
        if e.get("event") != "flush":
            continue
        h = datetime.fromisoformat(e["ts"]).astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        per_hour[h] = per_hour.get(h, 0) + int(e.get("messages") or 0)
    return per_hour


def volume_check(per_hour, now, min_fraction, history_hours=24):
    last = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    history = [per_hour.get(last - timedelta(hours=i), 0) for i in range(1, history_hours + 1)]
    seen = [n for n in history if n > 0]
    got = per_hour.get(last, 0)
    if len(seen) < history_hours // 2:          # not enough history yet (new install): never alert
        return f"volume: {got} messages in {last:%H}:00 UTC (history too short to judge)", None
    usual = statistics.median(seen)
    msg = f"volume: {got} messages in {last:%Y-%m-%d %H}:00 UTC, usual {usual:.0f}/h"
    low = got < min_fraction * usual
    return msg, (f"message volume low: {got} in the last full hour vs usual {usual:.0f}" if low else None)


def tail_lines(path, max_bytes=2_000_000):
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        f.seek(max(0, f.tell() - max_bytes))
        return f.read().decode("utf-8", "replace").splitlines()[1:]   # first line may be cut


def main(env=os.environ):
    fail = []
    hb = json.load(urllib.request.urlopen(env["HEARTBEAT_URL"], timeout=20))
    a = age_s(hb["ts"])
    print(f"collector heartbeat age {a:.0f}s, last_file={hb.get('last_file')}")
    if a > int(env.get("MAX_AGE_SECONDS", "600")):
        fail.append(f"collector heartbeat stale ({a:.0f}s)")
    if env.get("GOLD_URL"):
        g = json.load(urllib.request.urlopen(env["GOLD_URL"], timeout=20))
        ga = age_s(g["finished_at"])
        print(f"gold manifest status={g['status']} age {ga / 60:.0f} min")
        if g["status"] != "READY":
            fail.append("gold manifest not READY")
        if ga > int(env.get("GOLD_MAX_AGE_MIN", "180")) * 60:
            fail.append("gold manifest too old")
    if env.get("DATA_DIR"):
        msg, problem = disk_check(env["DATA_DIR"], float(env.get("MIN_FREE_PCT", "15")))
        print(msg)
        fail += [problem] if problem else []
    if env.get("EVENTS_PATH"):
        msg, problem = volume_check(hourly_messages(tail_lines(env["EVENTS_PATH"])), datetime.now(timezone.utc),
                                    float(env.get("VOLUME_MIN_FRACTION", "0.5")))
        print(msg)
        fail += [problem] if problem else []
    if fail:
        sys.exit("; ".join(fail))


if __name__ == "__main__":
    main()
