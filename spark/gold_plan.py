"""
Planning and publishing helpers for incremental Gold. Pure Python (no Spark), so they are unit-tested directly.

Incremental rule: recompute from the earliest CHANGED day S (the last `recent_days` days, plus any older day whose
Silver files changed since the last successful Gold run) up to the newest day. Visits still open at S are
rebuilt by gold_job from their own arrival (see there). Partitions are swapped in one day at a time; days older than the lookback are dropped.
"""
import calendar, json, os, shutil
from datetime import date, datetime, timedelta, timezone


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def silver_day_mtimes(silver_dir):
    """{day: newest file modification time (epoch s)} for each dt=YYYY-MM-DD partition of Silver."""
    out = {}
    if not os.path.isdir(silver_dir):
        return out
    for name in os.listdir(silver_dir):
        if not name.startswith("dt="):
            continue
        try:
            day = date.fromisoformat(name[3:])
        except ValueError:
            continue
        newest = 0.0
        for root, _, files in os.walk(os.path.join(silver_dir, name)):
            for f in files:
                if not f.startswith((".", "_")):
                    newest = max(newest, os.path.getmtime(os.path.join(root, f)))
        out[day] = newest
    return out


def plan_start(day_mtimes, max_dt, window_start, recent_days, last_success_epoch, slack_s=300):
    """Earliest day to recompute: the last `recent_days` days, plus any day whose Silver changed since the last
    successful run (with `slack_s` of safety margin). No previous run means everything changed."""
    days = [d for d in day_mtimes if window_start <= d <= max_dt]
    recent = [d for d in days if d > max_dt - timedelta(days=recent_days)]
    changed = [d for d in days if last_success_epoch is None or day_mtimes[d] > last_success_epoch - slack_s]
    candidates = recent + changed
    return min(candidates) if candidates else max_dt


def day_start_s(d):
    return calendar.timegm(d.timetuple())


def days_between(first, last):
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def has_data(path):
    """True if a table folder holds at least one data file (an empty Spark write leaves only _SUCCESS)."""
    if not os.path.isdir(path):
        return False
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if not d.startswith(("_", "."))]
        if any(f.endswith(".parquet") for f in files):
            return True
    return False


def is_partitioned(path):
    return os.path.isdir(path) and any(n.startswith("dt=") for n in os.listdir(path))


def swap_dir(new, final):
    """Replace a whole table directory (used for --full runs and the small dimension tables)."""
    prev = os.path.join(os.path.dirname(final), "_prev_" + os.path.basename(final))
    shutil.rmtree(prev, ignore_errors=True)
    if os.path.exists(final):
        shutil.move(final, prev)
    if os.path.exists(new):
        shutil.move(new, final)
    shutil.rmtree(prev, ignore_errors=True)


def swap_partitions(stage_table, live_table, days):
    """Replace the given day partitions of a live table with the staged ones. A day with no staged
    partition is removed (it has no rows any more). Other days are untouched."""
    os.makedirs(live_table, exist_ok=True)
    for d in days:
        name = f"dt={d.isoformat()}"
        live, new = os.path.join(live_table, name), os.path.join(stage_table, name)
        prev = os.path.join(live_table, "_prev_" + name)   # leading "_": Spark ignores it while it exists
        shutil.rmtree(prev, ignore_errors=True)
        if os.path.exists(live):
            os.rename(live, prev)
        if os.path.exists(new):
            shutil.move(new, live)
        shutil.rmtree(prev, ignore_errors=True)


def drop_partitions_before(live_table, first_day):
    """Retention: remove day partitions older than the lookback window."""
    gone = []
    if not os.path.isdir(live_table):
        return gone
    for n in sorted(os.listdir(live_table)):
        if n.startswith("dt="):
            try:
                if date.fromisoformat(n[3:]) < first_day:
                    shutil.rmtree(os.path.join(live_table, n)); gone.append(n)
            except ValueError:
                pass
    return gone
