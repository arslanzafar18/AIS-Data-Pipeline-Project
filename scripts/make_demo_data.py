"""
SYNTHETIC demo data (NOT real AIS) so the whole Bronze->Silver->Gold chain can be exercised and verified
without live credentials. Writes Bronze files in exactly the collector's format via collector.make_record,
a demo installation reference CSV, and demo_expected.json with the ground truth (injected defects).
Usage: APP_ENV=dev python scripts/make_demo_data.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import csv, gzip, json, math, random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pipeline_config
from collector.collector import make_record

random.seed(42)
cfg = pipeline_config.load()
ROOT = Path(cfg["data_root"])
BASE = datetime(2026, 10, 11, 10, 0, 0, tzinfo=timezone.utc)
MINUTES, WIN = 90, 120
INST = [("DEMO_PLATFORM_A", "PLATFORM", 60.50, 3.60), ("DEMO_PLATFORM_B", "PLATFORM", 60.80, 3.90),
        ("DEMO_FPSO_C", "FPSO", 61.10, 2.90)]
KN = 0.514444


def offset(lat, lon, d_m, bearing_deg):
    b = math.radians(bearing_deg)
    return (lat + d_m * math.cos(b) / 111320.0, lon + d_m * math.sin(b) / (111320.0 * math.cos(math.radians(lat))))


def msg(mmsi, t, lat, lon, sog, name, cog=90.0):
    return {"mmsi": mmsi, "msgtime": t.isoformat(), "latitude": round(lat, 6), "longitude": round(lon, 6),
            "speedOverGround": round(sog, 1), "courseOverGround": cog,
            "trueHeading": 511 if random.random() < 0.3 else int(cog), "rateOfTurn": 0.0,
            "name": name, "shipType": 70, "navigationalStatus": 0, "destination": "DEMO"}


events = []   # (event_time, dict)
for k in range(10):                                   # visitors: approach 6 km, loiter 20 min (<=300 m), leave
    name, _, ilat, ilon = INST[k % 3]
    bearing, start = random.uniform(0, 360), BASE + timedelta(seconds=random.randint(0, 1800))
    appr = 6000 / (12 * KN)
    for s in range(0, int(appr * 2 + 1200), 10):
        t = start + timedelta(seconds=s)
        if s < appr:      d, sog = 6000 - 12 * KN * s, 12.0
        elif s < appr + 1200:  d, sog = random.uniform(50, 300), 0.3
        else:             d, sog = 12 * KN * (s - appr - 1200), 12.0
        b = bearing if (s < appr or s >= appr + 1200) else random.uniform(0, 360)
        lat, lon = offset(ilat, ilon, d, b)
        events.append((t, msg(257000001 + k, t, lat, lon, sog, f"DEMO SUPPLY {k}")))
for k in range(20):                                   # transit traffic far from installations
    lat, lon = 63.0 + 0.05 * k, 1.0
    for s in range(0, MINUTES * 60, 60):
        t = BASE + timedelta(seconds=s)
        lat2, lon2 = offset(lat, lon, 14 * KN * s, 90)
        events.append((t, msg(258000001 + k, t, lat2, lon2, 14.0, f"DEMO TRANSIT {k}")))
events.sort(key=lambda e: e[0])

exp = dict(malformed=0, bad_mmsi=0, sentinel=0, duplicates=0, valid_unique=0, corrected_sog=0, unknown_field_rows=0)
buckets = {}
def bucket_of(ing):
    return int((ing - BASE).total_seconds() // WIN)
def put(ing, line):
    buckets.setdefault(bucket_of(ing), []).append((ing.isoformat(), line))

for t, m in events:
    ing = t + timedelta(seconds=random.randint(1, 6))
    r = random.random()
    if r < 0.003:
        exp["malformed"] += 1; put(ing, json.dumps(m)[:25]); continue
    if r < 0.008:
        m["mmsi"] = 12345; exp["bad_mmsi"] += 1; put(ing, json.dumps(m)); continue
    if r < 0.018:
        m["latitude"], m["longitude"] = 91, 181; exp["sentinel"] += 1; put(ing, json.dumps(m)); continue
    if random.random() < 0.005: m["speedOverGround"] = 102.3; exp["corrected_sog"] += 1
    if random.random() < 0.005: m["extraField"] = 1; exp["unknown_field_rows"] += 1
    exp["valid_unique"] += 1
    line = json.dumps(m); put(ing, line)
    if random.random() < 0.02:                       # duplicate in same file or in the NEXT file (reconnect overlap)
        exp["duplicates"] += 1
        if "extraField" in m: exp["unknown_field_rows"] += 1      # the copy is an input row too
        if random.random() < 0.5: put(ing + timedelta(seconds=1), line)
        else:
            nxt = BASE + timedelta(seconds=(bucket_of(ing) + 1) * WIN + 1); put(nxt, line)

run_id = "demo00000001"
for b in sorted(buckets):
    end = BASE + timedelta(seconds=(b + 1) * WIN)
    d = ROOT / "bronze/raw" / f"dt={end:%Y-%m-%d}" / f"hour={end:%H}"
    d.mkdir(parents=True, exist_ok=True)
    name = f"ais_{run_id}_{end:%Y%m%dT%H%M%SZ}.jsonl.gz"
    with gzip.open(d / name, "wt") as f:
        for ts, line in buckets[b]:
            f.write(json.dumps(make_record(ts, line, name, run_id)) + "\n")

ref = ROOT / "bronze/reference"; ref.mkdir(parents=True, exist_ok=True)
with open(ref / "facilities.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["fclName", "fclKind", "centroid_lat", "centroid_lon", "_source", "_ingest_ts"])
    for n, k, la, lo in INST: w.writerow([n, k, la, lo, "SYNTHETIC_DEMO", BASE.isoformat()])
exp["files"] = len(buckets); exp["total_lines"] = sum(len(v) for v in buckets.values()); exp["visitors"] = 10
(ROOT / "demo_expected.json").write_text(json.dumps(exp, indent=2))
print(json.dumps(exp))
