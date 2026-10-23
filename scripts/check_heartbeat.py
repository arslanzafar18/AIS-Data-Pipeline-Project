"""Monitor workflow check. Fails (=> GitHub notifies the owner) if the collector heartbeat is stale or,
when GOLD_URL is set, if the published Gold manifest is not READY or too old. Stdlib only."""
import json, os, sys, urllib.request
from datetime import datetime, timezone

def age_s(ts): return (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds()
fail = []
hb = json.load(urllib.request.urlopen(os.environ["HEARTBEAT_URL"], timeout=20))
a = age_s(hb["ts"]); print(f"collector heartbeat age {a:.0f}s, last_file={hb.get('last_file')}")
if a > int(os.environ.get("MAX_AGE_SECONDS", "600")): fail.append(f"collector heartbeat stale ({a:.0f}s)")
if os.environ.get("GOLD_URL"):
    g = json.load(urllib.request.urlopen(os.environ["GOLD_URL"], timeout=20))
    ga = age_s(g["finished_at"]); print(f"gold manifest status={g['status']} age {ga/60:.0f} min")
    if g["status"] != "READY": fail.append("gold manifest not READY")
    if ga > int(os.environ.get("GOLD_MAX_AGE_MIN", "180")) * 60: fail.append("gold manifest too old")
if fail: sys.exit("; ".join(fail))
