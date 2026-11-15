"""Gold dashboard: one self-contained HTML page, rebuilt after every successful Gold publish.

Five tabs, all driven by one date-range slider (the first four also by an All / Offshore / Onshore switch):
  Traffic        visits and hourly vessels per installation (use case 1)
  Safety zones   500 m zone entries, fast entries, minutes inside (use case 2)
  Surveillance   loitering and AIS gaps, 'worth a look' first, each with a plain-language Why (use case 3)
  Vessel         search any ship by name or MMSI: its visits, zone entries and events (use case 4);
                 the full route map comes from spark/vessel_report.py
  Pipeline health  the pipeline itself: freshness, messages per hour with feed outages, Silver rejects and batch
                 times, and every Gold run (published, blocked by the gate, or crashed) from quality/gold_runs.jsonl
The Gold tables are embedded compactly (one entry per visit / entry / event / hour) and the browser recomputes
every number for the chosen range. Writes <gold>/dashboard/index.html atomically; the page reloads itself every
5 minutes while nobody is using it.
View it:   python3 -m http.server 8050 --bind 127.0.0.1 --directory <data_root>/gold/dashboard
Preview from any Gold folder (read-only on the tables; writes ./dashboard-preview unless --out is given):
           PYTHONPATH=. python -m spark.dashboard --gold-dir data/prod/gold [--out folder]
"""
import argparse, json, math, os, shutil
from datetime import datetime, timezone
from pyspark.sql import functions as F

BUCKETS = [(15, "Under 15 min"), (60, "15 to 60 min"), (180, "1 to 3 h"), (720, "3 to 12 h"),
           (1440, "12 to 24 h"), (None, "Over 24 h")]
# bit flags of a surveillance event, decoded by the page
FLAG_BITS = {"regular_visitor": 1, "is_facility": 2, "censored": 4, "feed_outage": 8, "terminal_call": 16,
             "support_craft": 32, "worth_a_look": 64}


def _onshore(df):
    c = F.col("is_onshore") if "is_onshore" in df.columns else F.lit(None)
    return F.when(F.coalesce(c.cast("boolean"), F.lit(False)), "onshore").otherwise("offshore")


def _iso(o):
    if hasattr(o, "isoformat"):
        s = o.isoformat()
        return s + "Z" if "T" in s and getattr(o, "tzinfo", 1) is None else s
    return float(o) if hasattr(o, "__float__") else str(o)


def _col(df, c):
    return F.col(c) if c in df.columns else F.lit(None)


def visit_zone_times(visits, zones):
    """Per visit: minutes from arrival within the near radius until the first 500 m zone entry (wait_min) and minutes
    inside the zone in total (alongside_min). Zone entries are matched to the visit of the same vessel and installation
    that contains them; visits that never entered the zone get nulls."""
    if zones is None:
        return visits.withColumn("wait_min", F.lit(None).cast("double")).withColumn("alongside_min", F.lit(None).cast("double"))
    v = visits.alias("v")
    z = zones.select("mmsi", "inst_name", "entry_ts", "minutes_inside").alias("z")
    dep = (F.col("v.departure_ts") if "departure_ts" in visits.columns
           else (F.col("v.arrival_ts").cast("long") + F.col("v.duration_min") * 60).cast("timestamp"))
    hit = (v.join(z, (F.col("v.mmsi") == F.col("z.mmsi")) & (F.col("v.inst_name") == F.col("z.inst_name"))
                  & F.col("z.entry_ts").between(F.col("v.arrival_ts"), dep))
            .groupBy(F.col("v.mmsi").alias("mmsi"), F.col("v.inst_name").alias("inst_name"), F.col("v.arrival_ts").alias("arrival_ts"))
            .agg(F.min("z.entry_ts").alias("_first"), F.sum("z.minutes_inside").alias("alongside_min")))
    return (visits.join(hit, ["mmsi", "inst_name", "arrival_ts"], "left")
                  .withColumn("wait_min", (F.col("_first").cast("long") - F.col("arrival_ts").cast("long")) / 60.0)
                  .drop("_first"))


def standby_gaps(hourly, coverage, min_fraction, start, end):
    """Runs of consecutive hours in which no vessel was within the near radius of an OFFSHORE installation, counting only
    hours the feed covered (an hour with under min_fraction of the median message volume is unknown and splits a run).
    One row per run: inst_name, start hour, hours."""
    from spark.gold_logic import covered_hours
    from pyspark.sql import Window
    off = hourly.where(~F.coalesce(F.col("is_onshore").cast("boolean"), F.lit(False))) if "is_onshore" in hourly.columns else hourly
    grid = (off.select("inst_name").distinct()
               .withColumn("hour_ts", F.explode(F.sequence(F.lit(start), F.lit(end), F.expr("INTERVAL 1 HOUR")))))
    ok = covered_hours(coverage, min_fraction, end).withColumnRenamed("_h", "hour_ts")
    present = off.select("inst_name", F.date_trunc("hour", "hour_ts").alias("hour_ts")).distinct().withColumn("_p", F.lit(True))
    empty = (grid.join(F.broadcast(ok), "hour_ts").join(present, ["inst_name", "hour_ts"], "left").where(F.col("_p").isNull())
                 .withColumn("_i", (F.col("hour_ts").cast("long") / 3600).cast("long")))
    run = F.col("_i") - F.row_number().over(Window.partitionBy("inst_name").orderBy("_i"))
    return (empty.withColumn("_run", run).groupBy("inst_name", "_run")
                 .agg(F.min("hour_ts").alias("start_ts"), F.count("*").alias("hours")).drop("_run"))


def norm_call_sign(c):
    return "".join(ch for ch in str(c or "").upper() if ch.isalnum())


def load_fishing_call_signs(path):
    """Radio call signs of registered Norwegian fishing vessels (bronze/reference/fishing_vessels.csv, column call_sign)."""
    import csv
    if not path or not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return {norm_call_sign(r.get("call_sign")) for r in csv.DictReader(f)} - {""}


def pipeline_lines(path, phases=("IN SERVICE",)):
    """Sodir pipeline segments (bronze/reference/pipelines.csv) -> map polylines and the segment list.
    Consecutive segments that join end to start form one line; a jump starts a new one."""
    import csv
    if not path or not os.path.exists(path):
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        segs = [r for r in csv.DictReader(f) if not phases or r.get("phase") in phases]
    segs.sort(key=lambda r: (r["pipeline_id"], int(r["seg_no"])))
    lines, cur, key = [], None, None
    out = []
    for r in segs:
        a, b = (float(r["lat1"]), float(r["lon1"])), (float(r["lat2"]), float(r["lon2"]))
        out.append((r["name"] or r["label"], r.get("medium") or "", a[0], a[1], b[0], b[1]))
        if cur is None or key != r["pipeline_id"] or cur[2][-1] != [round(a[0], 4), round(a[1], 4)]:
            cur = [r["name"] or r["label"], r.get("medium") or "", [[round(a[0], 4), round(a[1], 4)]]]
            lines.append(cur)
            key = r["pipeline_id"]
        cur[2].append([round(b[0], 4), round(b[1], 4)])
    return lines, out


def dark_near_infrastructure(detections, installations, segments, inst_m, pipe_m, cell=0.1):
    """Radar detections within inst_m of an offshore installation or pipe_m of a pipeline, each with the nearest of
    both. A grid of `cell` degrees keeps the number of distance checks small."""
    import math
    from spark.gold_pipelines import point_segment_m_py

    def hav(a1, o1, a2, o2):
        p1, p2 = math.radians(a1), math.radians(a2)
        h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(o2 - o1) / 2) ** 2
        return 2 * 6371000.0 * math.asin(math.sqrt(h))

    grid = {}
    reach = max(inst_m, pipe_m) / 111320.0 + cell
    def add(kind, item, la0, lo0, la1, lo1):
        for gy in range(math.floor((la0 - reach) / cell), math.floor((la1 + reach) / cell) + 1):
            for gx in range(math.floor((lo0 - 2 * reach) / cell), math.floor((lo1 + 2 * reach) / cell) + 1):
                grid.setdefault((gy, gx), ([], []))[kind].append(item)
    for it in installations:
        add(0, it, it[1], it[2], it[1], it[2])
    for sg in segments:
        add(1, sg, min(sg[2], sg[4]), min(sg[3], sg[5]), max(sg[2], sg[4]), max(sg[3], sg[5]))
    out = []
    for d in detections:
        lat, lon = d["lat"], d["lon"]
        insts, segs = grid.get((math.floor(lat / cell), math.floor(lon / cell)), ([], []))
        bi = min(((hav(lat, lon, i[1], i[2]), i[0]) for i in insts), default=(None, None))
        bp = min(((point_segment_m_py(lat, lon, s_[2], s_[3], s_[4], s_[5]), s_[0]) for s_ in segs), default=(None, None))
        near_i = bi[0] is not None and bi[0] <= inst_m
        near_p = bp[0] is not None and bp[0] <= pipe_m
        if near_i or near_p:
            out.append({**d, "inst": bi[1] if near_i else None, "inst_m": round(bi[0]) if near_i else None,
                        "pipe": bp[1] if near_p else None, "pipe_m": round(bp[0]) if near_p else None})
    return out


def read_sar(path):
    import csv
    if not path or not os.path.exists(path):
        return None, None
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    dets = [{"date": r["date"], "lat": float(r["lat"]), "lon": float(r["lon"]), "n": int(r["detections"] or 0)} for r in rows]
    meta = {"versions": sorted({r["dataset"] for r in rows}), "accessed": max((r["fetched_at"] for r in rows), default=None)}
    return dets, meta


REJECT_REASONS = ("parse_failed", "bad_mmsi", "bad_timestamp", "missing_position", "sentinel_position", "out_of_range",
                  "future_timestamp", "stale_timestamp")


def _utc_naive(s):
    """ISO text with or without offset -> naive UTC datetime (the dashboard's t0 is naive UTC); None if unreadable."""
    try:
        d = datetime.fromisoformat(str(s))
    except (TypeError, ValueError):
        return None
    return d.astimezone(timezone.utc).replace(tzinfo=None) if d.tzinfo else d


def _jsonl(path):
    if not path or not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                yield json.loads(line)
            except ValueError:
                continue


SAR_COLUMNS = ["date", "lat", "lon", "detections", "dataset", "fetched_at"]          # read by read_sar
FISHING_COLUMNS = ["call_sign"]                                                    # read by load_fishing_call_signs


def reference_columns(files):
    """files: [(label, path, required columns)] -> one row per file: does its header still have every column the
    pipeline reads? (A fetch already refuses to replace a file whose source changed its fields; this shows it.)"""
    import csv
    out = []
    for label, path, required in files:
        row = {"file": label, "required": len(required), "columns": None, "missing": [], "updated": None}
        if path and os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as f:
                header = next(csv.reader(f), [])
            row.update(columns=len(header), missing=[c for c in required if c not in header],
                       updated=datetime.fromtimestamp(os.path.getmtime(path), timezone.utc).replace(tzinfo=None).isoformat() + "Z")
        out.append(row)
    return out


def pipeline_health(t0, quality_dir=None, metrics_dir=None, gold_dir=None, now=None, reference=None):
    """Plain-Python health data for the Pipeline health tab, from files the pipeline already writes:
    quality/silver_metrics.jsonl (one line per Silver batch) summed per day, quality/gold_runs.jsonl (one line per
    Gold run), the collector heartbeat and the free disk space. Times are hours since t0 like the rest of the page."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    hrs = lambda d: int((d - t0).total_seconds() // 3600)
    days, last_batch = {}, None
    for m in _jsonl(quality_dir and os.path.join(quality_dir, "silver_metrics.jsonl")):
        ts = _utc_naive(m.get("processed_at"))
        if ts is None:
            continue
        last_batch = ts if last_batch is None or ts > last_batch else last_batch
        if ts < t0:
            continue
        d = days.setdefault(ts.date(), {"b": 0, "in": 0, "ok": 0, "dup": 0, "p95": 0, "rt": 0.0, "rtx": 0.0,
                                        "rej": dict.fromkeys(REJECT_REASONS, 0), "cb": 0, "bad": 0, "unk": {}, "abs": None, "lat": []})
        if "absent_keys" in m:                       # Bronze column check (batches written since it exists)
            d["cb"] += 1
            d["bad"] += m.get("bronze_contract_bad_rows") or 0
            for k, n in (m.get("unknown_keys") or {}).items():
                d["unk"][k] = d["unk"].get(k, 0) + n
            a = set(m.get("absent_keys") or [])
            d["abs"] = a if d["abs"] is None else d["abs"] & a        # absent from EVERY checked batch of the day
        d["b"] += 1
        d["in"] += m.get("input_rows") or 0
        d["ok"] += m.get("accepted_rows") or 0
        d["dup"] += (m.get("in_batch_duplicates") or 0) + (m.get("cross_batch_duplicates") or 0)
        if m.get("source", "live") == "live":              # backfill batches are weeks old by design
            d["p95"] = max(d["p95"], m.get("latency_p95_s") or 0)
            d["lat"].append(m.get("latency_p95_s") or 0)
        d["rt"] += m.get("runtime_s") or 0
        d["rtx"] = max(d["rtx"], m.get("runtime_s") or 0)
        for r in REJECT_REASONS:
            d["rej"][r] += m.get("rej_" + r) or 0
    order = sorted(days)
    sv = {"t": [(k - t0.date()).days * 24 for k in order]}
    for f in ("b", "in", "ok", "dup", "p95", "rtx"):
        sv[f] = [round(days[k][f], 1) for k in order]
    med = lambda a: sorted(a)[len(a) // 2] if a else 0
    sv["p95m"] = [med(days[k]["lat"]) for k in order]      # typical batch; p95 keeps the worst batch of the day
    sv["rt"] = [round(days[k]["rt"] / days[k]["b"], 1) for k in order]
    sv["rej"] = {r: [days[k]["rej"][r] for k in order] for r in REJECT_REASONS}
    sv["cb"] = [days[k]["cb"] for k in order]
    sv["bad"] = [days[k]["bad"] for k in order]
    sv["unk"] = [days[k]["unk"] for k in order]
    sv["abs"] = [sorted(days[k]["abs"] or []) for k in order]
    runs = []
    for r in _jsonl(quality_dir and os.path.join(quality_dir, "gold_runs.jsonl")):
        fin = _utc_naive(r.get("finished_at"))
        if fin is None or fin < t0:
            continue
        runs.append({"t": hrs(fin), "at": fin.isoformat() + "Z", "s": r.get("status"), "m": r.get("mode"),
                     "rt": r.get("runtime_s"), "n": r.get("checks"), "f": r.get("failed") or [], "w": r.get("warnings") or [],
                     "k": r.get("repeated_keys"), "rows": r.get("silver_rows"), "from": r.get("recompute_from")})
    hb = None
    hb_path = metrics_dir and os.path.join(metrics_dir, "heartbeat.json")
    if hb_path and os.path.exists(hb_path):
        try:
            hb = _utc_naive(json.load(open(hb_path)).get("ts"))
        except (OSError, ValueError):
            hb = None
    disk = None
    if gold_dir and os.path.isdir(gold_dir):
        u = shutil.disk_usage(gold_dir)
        disk = round(100.0 * u.free / u.total, 1)
    iso = lambda d: None if d is None else d.isoformat() + "Z"
    return {"built": iso(now), "heartbeat": iso(hb), "silver_batch": iso(last_batch), "disk_free_pct": disk,
            "sv": sv, "runs": runs, "ref": reference_columns(reference or [])}


def summarise(visits, hourly, zones=None, events=None, vessels=None, landings=None, gaps=None, outage_hours=None,
              fishing_call_signs=None, extra_mmsi=None):
    """Compact, column-wise data: one entry per visit, zone entry, event and hour, so the browser can recompute
    every number for any date range. Times are whole hours since t0 (midnight of the first day)."""
    end = hourly.agg(F.max("hour_ts")).collect()[0][0]
    if end is None:
        return None
    on_v, on_h = _onshore(visits) == "onshore", _onshore(hourly) == "onshore"
    names = {r["inst_name"]: (r["inst_kind"], bool(r["onshore"])) for r in
             visits.groupBy("inst_name").agg(F.first("inst_kind", ignorenulls=True).alias("inst_kind"),
                                             F.max(on_v).alias("onshore")).collect()}
    vr = (visit_zone_times(visits, zones)
          .select("inst_name", "mmsi", "arrival_ts", "duration_min", "entered_zone", "wait_min", "alongside_min").collect())
    hr = (hourly.groupBy("hour_ts")
                .agg(F.sum(F.when(on_h, 0).otherwise(F.col("distinct_vessels"))).alias("off"),
                     F.sum(F.when(on_h, F.col("distinct_vessels")).otherwise(0)).alias("on"))
                .orderBy("hour_ts").collect())
    zr = (zones.select("inst_name", "mmsi", "entry_ts", "minutes_inside", _col(zones, "max_sog_kn").alias("kn"),
                       "min_dist_m").collect() if zones is not None else [])
    er = []
    if events is not None:
        flags = sum((F.coalesce(_col(events, k).cast("int"), F.lit(0)) * bit) for k, bit in FLAG_BITS.items())
        er = events.select("event_type", "inst_name", "mmsi", "start_ts", "duration_min", "min_dist_m",
                           _col(events, "days_present_before").alias("days"), flags.alias("f")).collect()
    lr = landings.select("inst_name", "arrival_ts", "minutes_in_zone").collect() if landings is not None else []
    gr = gaps.select("inst_name", "start_ts", "hours").collect() if gaps is not None else []
    for r in zr + er + lr + gr:
        names.setdefault(r["inst_name"], (None, False))
    order = sorted(n for n in names if n is not None)
    idx = {n: k for k, n in enumerate(order)}
    firsts = [r["arrival_ts"] for r in vr] + ([hr[0]["hour_ts"]] if hr else [])
    t0 = min(firsts).replace(hour=0, minute=0, second=0, microsecond=0)
    hrs = lambda ts: int((ts - t0).total_seconds() // 3600)
    seen = {r["mmsi"] for r in vr} | {r["mmsi"] for r in zr} | {r["mmsi"] for r in er} | set(extra_mmsi or [])
    ves = {}
    if vessels is not None:
        fish = fishing_call_signs or set()
        for r in vessels.select("mmsi", _col(vessels, "name").alias("name"), _col(vessels, "ship_type").alias("st"),
                                _col(vessels, "call_sign").alias("cs"),
                                _col(vessels, "length_m").alias("len"), _col(vessels, "is_facility").alias("fac")).collect():
            if r["mmsi"] in seen:
                ves[str(r["mmsi"])] = [r["name"], r["st"], r["len"], bool(r["fac"]), int(norm_call_sign(r["cs"]) in fish)]
    return {
        "t0": t0, "end": end, "buckets": [b[1] for b in BUCKETS],
        "inst": [[n, names[n][0], names[n][1], None, None, None] for n in order],
        "v": {"i": [idx[r["inst_name"]] for r in vr], "m": [r["mmsi"] for r in vr],
              "t": [hrs(r["arrival_ts"]) for r in vr], "d": [int(round(r["duration_min"] or 0)) for r in vr],
              "z": [int(r["entered_zone"] or 0) for r in vr],
              "w": [None if r["wait_min"] is None else int(round(r["wait_min"])) for r in vr],
              "a": [None if r["alongside_min"] is None else int(round(r["alongside_min"])) for r in vr]},
        "h": {"t": [hrs(r["hour_ts"]) for r in hr], "off": [int(r["off"] or 0) for r in hr],
              "on": [int(r["on"] or 0) for r in hr]},
        "zn": {"i": [idx[r["inst_name"]] for r in zr], "m": [r["mmsi"] for r in zr], "t": [hrs(r["entry_ts"]) for r in zr],
               "d": [round(r["minutes_inside"] or 0, 1) for r in zr],
               "kn": [None if r["kn"] is None else round(r["kn"], 1) for r in zr],
               "x": [None if r["min_dist_m"] is None else int(r["min_dist_m"]) for r in zr]},
        "ev": {"k": [1 if r["event_type"] == "ais_gap" else 0 for r in er], "i": [idx[r["inst_name"]] for r in er],
               "m": [r["mmsi"] for r in er], "t": [hrs(r["start_ts"]) for r in er],
               "d": [int(round(r["duration_min"] or 0)) for r in er],
               "x": [None if r["min_dist_m"] is None else int(r["min_dist_m"]) for r in er],
               "p": [r["days"] for r in er], "f": [int(r["f"] or 0) for r in er]},
        "hl": {"i": [idx[r["inst_name"]] for r in lr], "t": [hrs(r["arrival_ts"]) for r in lr],
               "d": [int(round(r["minutes_in_zone"] or 0)) for r in lr]},
        "gp": {"i": [idx[r["inst_name"]] for r in gr], "t": [hrs(r["start_ts"]) for r in gr], "n": [int(r["hours"]) for r in gr]},
        "out": sorted(hrs(h) for h in (outage_hours or []) if h >= t0),
        "ves": ves, "flag_bits": FLAG_BITS,
    }


def build(visits, hourly, manifest, out_dir, fac=None, zones=None, events=None, vessels=None, installations=None,
          rules=None, landings=None, coverage=None, min_fraction=0.5, fishing_call_signs=None, pipeline_loiter=None,
          pipelines_csv=None, sar_csv=None, dark_inst_m=5000, dark_pipe_m=2000, quality_dir=None, metrics_dir=None,
          gold_dir=None, reference=None):
    gaps, outage = None, None
    if coverage is not None:
        from spark.gold_logic import covered_hours
        span = hourly.agg(F.min("hour_ts"), F.max("hour_ts")).collect()[0]
        if span[0] is not None:
            start = span[0].replace(hour=0, minute=0, second=0, microsecond=0)
            gaps = standby_gaps(hourly, coverage, min_fraction, start, span[1])
            ok = covered_hours(coverage, min_fraction, span[1]).select("_h")
            hours = coverage.select("hour_ts")
            outage = [r["hour_ts"] for r in hours.join(ok, hours["hour_ts"] == ok["_h"], "left_anti")
                                                 .where(F.col("hour_ts") >= F.lit(start)).collect()]
    pl = (pipeline_loiter.select("mmsi", "start_ts", "duration_min", "pipeline_name", "medium", "min_dist_m", "avg_sog_kn",
                                 "lat", "lon").collect() if pipeline_loiter is not None else [])
    data = summarise(visits, hourly, zones, events, vessels, landings, gaps, outage, fishing_call_signs,
                     extra_mmsi={r["mmsi"] for r in pl})
    if data is None:
        return None
    data["fishing_register"] = fishing_call_signs is not None
    t0 = data["t0"]
    hrs = lambda ts: int((ts - t0).total_seconds() // 3600)
    data["pl"] = {"m": [r["mmsi"] for r in pl], "t": [hrs(r["start_ts"]) for r in pl], "d": [int(round(r["duration_min"])) for r in pl],
                  "p": [r["pipeline_name"] for r in pl], "md": [r["medium"] for r in pl], "x": [int(round(r["min_dist_m"])) for r in pl],
                  "kn": [round(r["avg_sog_kn"] or 0, 1) for r in pl], "lat": [round(r["lat"], 4) for r in pl], "lon": [round(r["lon"], 4) for r in pl]}
    lines, segs = pipeline_lines(pipelines_csv)
    data["pp"] = lines
    dets, meta = read_sar(sar_csv)
    if dets is not None and fac is not None:
        offshore = [(r["inst_name"], r["i_lat"], r["i_lon"]) for r in fac.select("inst_name", "i_lat", "i_lon", "is_onshore").collect()
                    if not r["is_onshore"] and r["i_lat"] is not None and r["i_lon"] is not None
                    and math.isfinite(r["i_lat"]) and math.isfinite(r["i_lon"])]
        first = t0.date().isoformat()
        near = dark_near_infrastructure([d for d in dets if d["date"] >= first], offshore, segs, dark_inst_m, dark_pipe_m)
        day = lambda s_: (datetime.fromisoformat(s_) - t0).days
        data["dk"] = {"t": [day(d["date"]) * 24 for d in near], "lat": [d["lat"] for d in near], "lon": [d["lon"] for d in near],
                      "n": [d["n"] for d in near], "i": [d["inst"] for d in near], "im": [d["inst_m"] for d in near],
                      "p": [d["pipe"] for d in near], "pm": [d["pipe_m"] for d in near]}
        data["dk_meta"] = {**meta, "cells": len(dets), "newest": max((d["date"] for d in dets), default=None),
                           "inst_m": dark_inst_m, "pipe_m": dark_pipe_m}
    pos = {}
    if fac is not None:                              # positions from the Sodir reference file
        pos = {r["inst_name"]: (r["i_lat"], r["i_lon"], None) for r in fac.select("inst_name", "i_lat", "i_lon").collect()}
    if installations is not None:                    # Gold installations table: position and field
        for r in installations.select("inst_name", "lat", "lon", _col(installations, "field").alias("field")).collect():
            pos[r["inst_name"]] = (r["lat"], r["lon"], r["field"])
    for row in data["inst"]:
        row[3], row[4], row[5] = pos.get(row[0], (None, None, None))
    data["manifest"] = {k: manifest.get(k) for k in ("run_id", "finished_at", "status", "silver_rows", "tables", "mode")}
    if coverage is not None:                         # messages per hour over all positions, for the health tab
        fc = coverage.where(F.col("hour_ts") >= F.lit(t0)).select("hour_ts", "msgs").orderBy("hour_ts").collect()
        data["fc"] = {"t": [hrs(r["hour_ts"]) for r in fc], "n": [int(r["msgs"] or 0) for r in fc]}
    data["ph"] = pipeline_health(t0, quality_dir, metrics_dir, gold_dir, reference=reference)
    data["rules"] = rules or {}
    page = PAGE.replace("__DATA__", json.dumps(data, default=_iso, separators=(",", ":")).replace("</", "<\\/"))
    os.makedirs(out_dir, exist_ok=True)
    tmp, final = os.path.join(out_dir, ".index.html.tmp"), os.path.join(out_dir, "index.html")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(page)
    os.replace(tmp, final)
    return final


def rules_from_config(cfg):
    rv, sc = cfg.get("regular_visitor", {}), cfg.get("surveillance", {})
    return {"min_days": rv.get("min_days", 3), "window_days": rv.get("window_days", 30),
            "large_ship_min_m": sc.get("large_ship_min_m", 100), "loiter_max_kn": sc.get("loiter_max_kn", 2.0),
            "loiter_min_minutes": sc.get("loiter_min_minutes", 120),
            "ais_gap_min_minutes": sc.get("ais_gap_min_minutes", 30), "fast_kn": sc.get("zone_fast_kn", 10),
            "gap_alert_h": sc.get("standby_gap_alert_hours", 3), "dark_inst_m": sc.get("dark_inst_m", 5000),
            "dark_pipe_m": sc.get("dark_pipe_m", 2000)}


def build_from_gold(spark, gold, manifest, out_dir, cfg, fac=None):
    """Reads the published Gold tables (any that exist) and builds the page."""
    from spark.gold_plan import has_data
    read = lambda t: spark.read.parquet(os.path.join(gold, t)) if has_data(os.path.join(gold, t)) else None
    loiter, gaps = read("loitering_events"), read("ais_gap_events")
    events = loiter if gaps is None else (gaps if loiter is None else loiter.unionByName(gaps, allowMissingColumns=True))
    return build(read("vessel_visits"), read("installation_activity_hourly"), manifest, out_dir, fac=fac,
                 zones=read("zone_entries"), events=events, vessels=read("vessels"),
                 installations=read("installations"), rules=rules_from_config(cfg), landings=read("helicopter_landings"),
                 coverage=read("feed_coverage_hourly"), min_fraction=cfg.get("surveillance", {}).get("coverage_min_fraction", 0.5),
                 fishing_call_signs=load_fishing_call_signs(os.path.join(gold, "..", "bronze", "reference", "fishing_vessels.csv")),
                 pipeline_loiter=read("pipeline_loitering"),
                 pipelines_csv=os.path.join(gold, "..", "bronze", "reference", "pipelines.csv"),
                 sar_csv=os.path.join(gold, "..", "bronze", "gfw", "sar_detections.csv"),
                 dark_inst_m=cfg.get("surveillance", {}).get("dark_inst_m", 5000),
                 dark_pipe_m=cfg.get("surveillance", {}).get("dark_pipe_m", 2000),
                 quality_dir=os.path.join(gold, "..", "quality"), metrics_dir=cfg.get("metrics_dir"), gold_dir=gold,
                 reference=reference_files(gold, cfg))


def reference_files(gold, cfg):
    """The Bronze reference files and the columns the pipeline reads from each."""
    from spark.common import facility_columns
    from spark.gold_pipelines import SEGMENT_COLUMNS
    ref = os.path.join(gold, "..", "bronze", "reference")
    return [("Sodir facilities", os.path.join(ref, "facilities.csv"), facility_columns(cfg)),
            ("Sodir pipelines", os.path.join(ref, "pipelines.csv"), SEGMENT_COLUMNS),
            ("Fishing-vessel register", os.path.join(ref, "fishing_vessels.csv"), FISHING_COLUMNS),
            ("Global Fishing Watch radar", os.path.join(gold, "..", "bronze", "gfw", "sar_detections.csv"), SAR_COLUMNS)]


def main():
    import pipeline_config
    from spark.session import get_spark
    from spark.common import load_facilities
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold-dir"); ap.add_argument("--out"); ap.add_argument("--facilities")
    a = ap.parse_args()
    cfg = pipeline_config.load()
    gold = a.gold_dir or pipeline_config.path(cfg, "gold")
    out = a.out or ("dashboard-preview" if a.gold_dir else os.path.join(gold, "dashboard"))
    spark = get_spark(cfg, "gold-dashboard")
    fac_path = a.facilities or os.path.join(gold, "..", "bronze", "reference", "facilities.csv")
    fac = load_facilities(spark, dict(cfg, facility_filter=None), fac_path) if os.path.exists(fac_path) else None
    print(build_from_gold(spark, gold, json.load(open(os.path.join(gold, "_READY.json"))), out, cfg, fac))


PAGE = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vessel activity at Norwegian oil and gas installations</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.js"></script>
<style>
:root{--bg:#f2f5f6;--surface:#fff;--ink:#14303b;--muted:#5d7480;--line:#d5dee2;--accent:#13808a;--signal:#e09a12;--warn:#b4441c;--onshore:#7a5ea8;
--font:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;color-scheme:light}
@media (prefers-color-scheme:dark){:root{--bg:#0d181d;--surface:#14232a;--ink:#e2ebee;--muted:#93a8b1;--line:#253841;--accent:#4fb3bc;--signal:#f0b33c;--warn:#f08a5d;--onshore:#b39ddb;color-scheme:dark}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 var(--font)}
main{max-width:1180px;margin:0 auto;padding:28px 20px 48px}
h1{font-size:1.75rem;font-weight:600;line-height:1.2;margin:0 0 6px;max-width:30ch}
.status{color:var(--muted);margin:0}.status.stale{color:var(--warn);font-weight:500}
.tabs{display:flex;flex-wrap:wrap;gap:4px;margin:20px 0 0;border-bottom:1px solid var(--line)}
.tabs button{font:inherit;font-weight:500;color:var(--muted);background:none;border:0;border-bottom:3px solid transparent;padding:8px 14px;cursor:pointer}
.tabs button[aria-selected="true"]{color:var(--ink);border-bottom-color:var(--accent)}
.tabs button:focus-visible{outline:2px solid var(--signal);outline-offset:-2px}
.bar{display:flex;flex-wrap:wrap;gap:16px 32px;align-items:end;justify-content:space-between;margin:20px 0 8px}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:8px;overflow:hidden;background:var(--surface)}
.seg button{font:inherit;color:var(--ink);background:none;border:0;padding:6px 14px;cursor:pointer}
.seg button[aria-pressed="true"]{background:var(--accent);color:var(--surface)}
.seg button:focus-visible{outline:2px solid var(--signal);outline-offset:-2px}
.range{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:14px 18px 10px;margin-top:16px}
.range-head{display:flex;flex-wrap:wrap;gap:8px 16px;align-items:baseline;justify-content:space-between}
.range-head strong{font-weight:600}
.presets button{font:inherit;font-size:.875rem;color:var(--accent);background:none;border:1px solid var(--line);border-radius:6px;padding:2px 10px;margin-left:6px;cursor:pointer}
.dual{position:relative;height:30px;margin-top:6px}
.dual .track,.dual .fill{position:absolute;top:13px;height:4px;border-radius:2px}
.dual .track{left:0;right:0;background:var(--line)}.dual .fill{background:var(--accent)}
.dual input{position:absolute;left:0;top:4px;width:100%;margin:0;height:22px;background:none;pointer-events:none;-webkit-appearance:none;appearance:none}
.dual input::-webkit-slider-runnable-track{background:none;height:22px}.dual input::-moz-range-track{background:none}
.dual input::-webkit-slider-thumb{-webkit-appearance:none;pointer-events:auto;width:18px;height:18px;margin-top:2px;border-radius:50%;background:var(--accent);border:2px solid var(--surface);box-shadow:0 0 0 1px var(--accent);cursor:grab}
.dual input::-moz-range-thumb{pointer-events:auto;width:14px;height:14px;border-radius:50%;background:var(--accent);border:2px solid var(--surface);box-shadow:0 0 0 1px var(--accent);cursor:grab}
.ends{display:flex;justify-content:space-between;color:var(--muted);font-size:.8125rem}
.kpis{display:flex;flex-wrap:wrap;gap:8px 36px;margin:0;padding:0;list-style:none}
.kpis b{display:block;font-size:1.6rem;font-weight:600;line-height:1.1;font-variant-numeric:tabular-nums}
.kpis span{color:var(--muted);font-size:.875rem}
section{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:18px 18px 12px;margin-top:16px}
h2{font-size:1.05rem;font-weight:600;margin:0 0 2px}
.note{color:var(--muted);font-size:.875rem;margin:0 0 12px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}.grid section{margin-top:0}
@media (max-width:820px){.grid{grid-template-columns:1fr}}
.chart{position:relative;height:300px}.chart.tall{height:440px}
.map{height:460px;border-radius:8px}.leaflet-container{font:inherit}
.scroll{overflow-x:auto;max-height:640px;overflow-y:auto}
table{border-collapse:collapse;width:100%;font-size:.875rem}
th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap;vertical-align:top}
th{color:var(--muted);font-weight:500;position:sticky;top:0;background:var(--surface)}
td.n{text-align:right;font-variant-numeric:tabular-nums}
tr.click{cursor:pointer}tr.click:hover td{background:color-mix(in srgb,var(--accent) 8%,transparent)}
a.v{color:var(--accent);text-decoration:none;cursor:pointer}a.v:hover{text-decoration:underline}
.tag{display:inline-block;font-size:.75rem;padding:0 6px;border-radius:4px;border:1px solid var(--line);margin-right:4px}
.tag.look{border-color:var(--warn);color:var(--warn);font-weight:500}
.why{white-space:normal;min-width:300px;max-width:480px;font-size:.8125rem;color:var(--muted);line-height:1.4}
.search{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
button.plain{font:inherit;color:var(--accent);background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:6px 12px;cursor:pointer}
.help summary{cursor:pointer;font-weight:500;color:var(--accent);margin:4px 0}
.defs{display:grid;grid-template-columns:max-content 1fr;gap:4px 16px;margin:10px 0;font-size:.875rem}
.defs dt{font-weight:600}.defs dd{margin:0;color:var(--muted)}
@media (max-width:600px){.defs{grid-template-columns:1fr}.defs dd{margin-bottom:6px}}
.example{font-size:.875rem;margin:6px 0;padding:6px 10px;border-left:3px solid var(--signal);background:color-mix(in srgb,var(--signal) 8%,transparent)}
.howto{font-size:.875rem;color:var(--muted);margin:6px 0 4px;padding-left:20px}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;list-style:none;margin:0 0 10px;padding:0;font-size:.8125rem;color:var(--muted);align-items:center}
.legend li{display:flex;align-items:center;gap:6px}
.dot{width:12px;height:12px;border-radius:50%;display:inline-block;opacity:.75}
.dot.ring{background:transparent;border:3px solid var(--warn);opacity:1}
.sizes{display:inline-flex;align-items:center;gap:2px}.sizes span{display:inline-block;border-radius:50%;border:1.5px solid var(--muted)}
.sizes span:nth-child(1){width:6px;height:6px}.sizes span:nth-child(2){width:10px;height:10px}.sizes span:nth-child(3){width:14px;height:14px}
.risk{display:inline-block;font-size:.75rem;font-weight:600;padding:0 8px;border-radius:4px}
.risk.h{background:color-mix(in srgb,var(--warn) 18%,transparent);color:var(--warn)}.risk.m{background:color-mix(in srgb,var(--signal) 18%,transparent);color:var(--ink)}.risk.l{background:color-mix(in srgb,var(--line) 60%,transparent);color:var(--muted)}
.legend .line{display:inline-block;width:18px;height:0;border-top:2px solid var(--onshore)}
.attr{font-size:.75rem;color:var(--muted);margin:8px 0 0}.attr a{color:var(--accent)}
.glance{margin:12px 0 0;max-width:90ch}.glance:empty{display:none}
.search input{font:inherit;padding:7px 10px;border:1px solid var(--line);border-radius:8px;background:var(--surface);color:var(--ink);min-width:320px}
code{font-size:.8125rem;background:color-mix(in srgb,var(--line) 50%,transparent);padding:1px 5px;border-radius:4px}
[hidden]{display:none!important}
footer{color:var(--muted);font-size:.8125rem;margin-top:20px;max-width:80ch}
</style>
</head>
<body>
<main>
<h1>Vessel activity at Norwegian oil and gas installations</h1>
<p class="status" id="status"></p>
<div class="tabs" role="tablist">
  <button type="button" role="tab" data-tab="traffic" aria-selected="true">Traffic</button>
  <button type="button" role="tab" data-tab="safety" aria-selected="false">Safety zones</button>
  <button type="button" role="tab" data-tab="surv" aria-selected="false">Surveillance</button>
  <button type="button" role="tab" data-tab="vessel" aria-selected="false">Vessel</button>
  <button type="button" role="tab" data-tab="health" aria-selected="false">Pipeline health</button>
</div>
<div class="range">
  <div class="range-head">
    <span>Showing <strong id="rangeLabel"></strong></span>
    <span class="presets"><button type="button" data-days="1">Last day</button><button type="button" data-days="7">7 days</button><button type="button" data-days="30">30 days</button><button type="button" data-days="0">All</button></span>
  </div>
  <div class="dual"><div class="track"></div><div class="fill" id="fill"></div>
    <input type="range" id="from" min="0" step="1" aria-label="First day"><input type="range" id="to" min="0" step="1" aria-label="Last day"></div>
  <div class="ends"><span id="first"></span><span id="last"></span></div>
</div>
<div class="bar">
  <ul class="kpis" id="kpis"></ul>
  <div class="seg" role="group" aria-label="Installations shown" id="grpSeg">
    <button type="button" data-g="all" aria-pressed="true">All</button><button type="button" data-g="offshore" aria-pressed="false">Offshore</button><button type="button" data-g="onshore" aria-pressed="false">Onshore</button>
  </div>
</div>

<div data-panel="traffic">
  <section><h2>Supply and service traffic at installations</h2>
    <details class="help" open><summary>How to read and use this page</summary>
      <dl class="defs">
        <dt>Visit</dt><dd>A vessel within 5 km of an offshore installation (1.5 km for onshore terminals). 30 minutes without a position starts a new visit.</dd>
        <dt>Service call</dt><dd>A visit that came inside the installation's 500 m safety zone. Only vessels with permission may enter the zone, so entering it is a good sign of supply, standby or crew work. It is inferred from positions: the data does not say what the work was.</dd>
        <dt>Installation transmitters</dt><dd>Platforms, FPSOs and rigs that broadcast AIS under their own Sodir name are not vessels serving anyone, so they are left out of this tab.</dd>
        <dt>Passing visit</dt><dd>A visit that stayed outside 500 m: usually a ship sailing past.</dd>
        <dt>Waiting</dt><dd>Time within 5 km before entering the 500 m zone, for example waiting for permission, the crane or the weather. Long waits point to queues or delays.</dd>
        <dt>Alongside</dt><dd>Time inside the 500 m zone, where the work happens. Long times point to heavy jobs or standby duty.</dd>
      </dl>
      <p class="example">Example of one service call: arrives within 5 km 08:00 → waits 40 min → enters the zone 08:40 → alongside 3 h → leaves 11:40.</p>
      <ol class="howto">
        <li>Choose the days with the slider at the top.</li>
        <li>Pick an installation: click it on the map, type its name below, or click its bar or table row. Every number on this tab then shows that installation only.</li>
        <li>Read its workload per day, when vessels arrive, and how long they wait and stay.</li>
        <li>The table lists the vessels serving it and the other installations they serve. Click a vessel name for its full history.</li>
      </ol>
    </details>
    <div class="search"><input id="iq" list="ilist" placeholder="Pick an installation, e.g. HEIDRUN" aria-label="Installation"><datalist id="ilist"></datalist>
      <button type="button" id="iclear" class="plain">All installations</button></div>
    <p class="glance" id="glance"></p></section>
  <section><h2>Where service calls happen</h2><p class="note">Click a circle to pick that installation.</p>
    <ul class="legend" aria-label="Map legend"><li><span class="dot" style="background:var(--accent)"></span>Offshore installation</li>
      <li><span class="dot" style="background:var(--onshore)"></span>Onshore terminal</li>
      <li><span class="dot ring"></span>Picked installation</li><li><span class="sizes"><span></span><span></span><span></span></span>Larger circle = more service calls</li></ul>
    <div class="map" id="mapT"></div></section>
  <section id="secTop"><h2>Busiest installations</h2><p class="note">Top 15 by service calls in the selected days. Click a bar to pick that installation.</p><div class="chart tall"><canvas id="cTop"></canvas></div></section>
  <section><h2 id="hDaily">Service calls per day</h2><p class="note" id="nDaily"></p><div class="chart"><canvas id="cDaily"></canvas></div></section>
  <div class="grid">
    <section><h2>When vessels arrive</h2><p class="note">Service calls by hour of arrival within 5 km, Norwegian time</p><div class="chart"><canvas id="cArr"></canvas></div></section>
    <section><h2>Waiting and time alongside</h2><p class="note">Per service call: minutes within 5 km before entering the zone, and minutes inside it</p><div class="chart"><canvas id="cDur"></canvas></div></section>
  </div>
  <section><h2 id="hTable"></h2><p class="note" id="nTable"></p><div class="scroll" id="tTraffic"></div></section>
</div>

<div data-panel="safety" hidden>
  <section><h2>Safety monitoring and emergency preparedness</h2>
    <details class="help" open><summary>How to read and use this page</summary>
      <dl class="defs">
        <dt>Zone entry</dt><dd>A vessel inside an installation's 500 m safety zone; 30 minutes without a position inside starts a new entry. Only vessels with permission may enter.</dd>
        <dt>Fast entry</dt><dd>A zone entry with a speed over <span class="rv" data-r="fast_kn"></span> kn recorded inside 500 m. Speeds above 50 kn are GPS errors and already removed; a single fast reading can still be noise, repeated ones are worth checking.</dd>
        <dt>Risk</dt><dd>Collision energy grows with a vessel's mass and the square of its speed, so fast entries are ranked by size and speed together.
          <b>High</b>: 80 m or longer at over 10 kn, or 24 to 80 m at over 15 kn. <b>Medium</b>: 24 to 80 m at 10 to 15 kn, or length unknown.
          <b>Low</b>: under 24 m, or a rescue craft. These thresholds are this dashboard's own judgement, not an industry standard.</dd>
        <dt>Rescue craft</dt><dd>A fast rescue boat or daughter craft launched from a standby vessel for drills, crew transfers and call-outs: AIS type "search and rescue" under 24 m, or a name containing FRB, FRC, MOB, DC (with or without a number) or DAUGHTER. They are built to run at 25 to 30 kn, so they rank as low risk.</dd>
        <dt>Offshore fleet</dt><dd>Vessels that entered the 500 m zone of at least 2 different offshore installations, or at least 5 times in total, or made at least 10 visits within 5 km of offshore installations, or spent at least 48 hours in total within 5 km of them (standby and field vessels that wait outside the zone), or stayed at least 1 hour inside the zone of the same installation on 2 or more different days (a supply vessel serving one field), in the whole data period. A vessel that only meets the last rule but has no business there still shows on the Surveillance tab as loitering. Supply, standby, diving and crew vessels fall in this group whatever ship type their AIS reports (many supply vessels report "cargo").</dd>
        <dt>Unexpected entry</dt><dd>An entry into an offshore installation's 500 m zone by a fishing vessel, or by a vessel outside the offshore fleet: a ship that almost never comes inside any safety zone.</dd>
        <dt>Fishing vessel</dt><dd>Registered in the Norwegian fishing vessel register (Fiskeridirektoratet, matched on radio call sign), or reporting AIS ship type "fishing". Foreign fishing vessels are only found by their AIS type, and boats under 15 m often carry no AIS.</dd>
        <dt>Vessel presence</dt><dd>The share of hours with at least one vessel within 5 km of an offshore installation. Hours when the whole feed was thin are left out. Offshore Norge guideline 064 sets rescue times (a person overboard picked up within 8 min; a ditched helicopter or evacuation to sea rescued within 120 min) but no fixed distance for standby vessels, and one vessel often covers several installations. So presence is context, not a test of those requirements.</dd>
        <dt>Helideck landing</dt><dd>A search-and-rescue aircraft (MMSI 111xxxxxx, or an aircraft reporting altitude) inside the 500 m zone.</dd>
      </dl>
      <ol class="howto">
        <li>Choose the days with the slider at the top.</li>
        <li>Pick an installation on the map, by name below, or from the preparedness table; the lists then show that installation only. The pick is shared with the Traffic tab.</li>
        <li>Start with the unexpected entries and fishing vessels, then the fast entries. The presence table shows how often each installation had a vessel nearby.</li>
      </ol>
    </details>
    <div class="search"><input id="sq" list="ilist" placeholder="Pick an installation, e.g. HEIDRUN" aria-label="Installation"><button type="button" id="sclear" class="plain">All installations</button></div>
    <p class="glance" id="sglance"></p></section>
  <section><h2>Entries into 500 m safety zones</h2><p class="note">Click a circle to pick that installation.</p>
    <ul class="legend" aria-label="Map legend"><li><span class="dot" style="background:var(--accent)"></span>Offshore installation</li>
      <li><span class="dot" style="background:var(--onshore)"></span>Onshore terminal</li><li><span class="dot ring"></span>Fast entries recorded</li>
      <li><span class="sizes"><span></span><span></span><span></span></span>Larger circle = more zone entries</li></ul>
    <div class="map" id="mapZ"></div></section>
  <section><h2>Unexpected zone entries</h2><p class="note">Fishing vessels and vessels outside the offshore fleet inside 500 m of an offshore installation, newest first.</p><div class="scroll" id="tUnexp"></div></section>
  <section><h2>Fishing vessels near installations</h2><p class="note" id="nFish"></p><div class="scroll" id="tFish"></div></section>
  <section><h2>Fast zone entries by risk</h2><p class="note" id="noteFast"></p><div class="scroll" id="tFast"></div></section>
  <section><h2 id="hPrep">Vessel presence and helideck landings</h2><p class="note" id="nPrep"></p><div class="scroll" id="tPrep"></div></section>
</div>

<div data-panel="surv" hidden>
  <section><h2>Security of offshore infrastructure</h2>
    <details class="help" open><summary>How to read and use this page</summary>
      <dl class="defs">
        <dt>Loitering</dt><dd>A vessel under 2 kn within 5 km of an installation for at least 2 hours.</dd>
        <dt>AIS gap</dt><dd>A vessel near an installation that sent nothing anywhere for at least 30 minutes, then reappeared.</dd>
        <dt>Worth a look</dt><dd>An event with no ordinary explanation: not a regular visitor, not a facility's own transmitter, not a large ship at a terminal, not support craft, and not during a feed outage.</dd>
        <dt>Loitering over a pipeline</dt><dd>A vessel under 2 kn within 500 m of an in-service pipeline (Sodir) for at least 2 hours, at least 5 km from any installation. Anchoring or dragging gear there is a damage and sabotage risk. Cables are not in the Sodir data.</dd>
        <dt>Dark vessel detection</dt><dd>A vessel seen by satellite radar (Sentinel-1) that matches no AIS signal, within <span class="rv" data-r="dark_inst_m"></span> m of an offshore facility or <span class="rv" data-r="dark_pipe_m"></span> m of a pipeline. Positions are 0.01 degree grid cells (about 1 km), data arrive about 5 days late and only when a satellite passes, and very small boats are not detected. A detection right next to an installation can be a rig or another structure rather than a vessel.</dd>
      </dl>
      <ol class="howto">
        <li>Choose the days with the slider at the top; live AIS data only.</li>
        <li>Start with "Worth a look", then the pipeline loitering and the dark detections.</li>
        <li>Click a row to see it on the map, a vessel name to open its profile.</li>
      </ol>
    </details></section>
  <section><h2>Events near installations and pipelines</h2><p class="note">Click a marker for details.</p>
    <ul class="legend" aria-label="Map legend"><li><span class="dot" style="background:var(--warn)"></span>Events worth a look at an installation</li>
      <li><span class="dot" style="background:var(--muted)"></span>Ordinary events</li><li><span class="dot" style="background:var(--signal)"></span>Loitering over a pipeline</li>
      <li><span class="dot" style="background:var(--ink)"></span>Dark vessel detection (radar)</li><li><span class="line"></span>Pipeline in service</li></ul>
    <div class="map" id="mapS"></div><p class="attr" id="attrS"></p></section>
  <section><h2>Events</h2>
    <div class="bar" style="margin-top:0"><p class="note" id="noteS" style="margin:0"></p>
      <div class="seg" id="survSeg"><button type="button" data-s="look" aria-pressed="true">Worth a look</button><button type="button" data-s="all" aria-pressed="false">All events</button></div></div>
    <div class="scroll" id="tSurv"></div></section>
  <section><h2>Loitering over pipelines</h2><p class="note" id="notePL"></p><div class="scroll" id="tPL"></div></section>
  <section><h2>Dark vessel detections near infrastructure</h2><p class="note" id="noteDK"></p><div class="scroll" id="tDK"></div><p class="attr" id="attrDK"></p></section>
</div>

<div data-panel="health" hidden>
  <section><h2>Pipeline health</h2>
    <p class="note">How the pipeline itself is doing in the selected days: is data arriving, how much Silver rejects, and whether each hourly Gold run published.</p>
    <details class="help"><summary>How to read this tab</summary>
      <ul class="howto">
        <li><b>Freshness</b> says when each layer last did its work. Anything marked <span class="tag look">Check</span> is older than normal.</li>
        <li><b>Messages per hour</b> shows the live feed. Orange hours are feed outages: under half the median volume. Events in those hours are flagged, not trusted.</li>
        <li><b>Bronze columns</b> checks that what arrives still has the expected shape. For live AIS, each Silver batch counts records missing one of the five Bronze columns, new fields in the AIS messages, and expected fields no message carried. For the reference files, it checks the columns the pipeline reads.</li>
        <li><b>Rejected rows</b> are rows Silver refused, by reason. They are kept in <code>silver/rejects</code> with the raw message. A sudden rise usually means a change at the source.</li>
        <li><b>Gold runs</b>: a blocked run (the quality gate failed) or a crash leaves the previous Gold published. The log names the failed checks and any warning, such as repeated keys.</li>
      </ul>
      <p class="note">Sources: the collector heartbeat, <code>quality/silver_metrics.jsonl</code> (one line per Silver batch), the Gold table <code>feed_coverage_hourly</code> and <code>quality/gold_runs.jsonl</code> (one line per Gold run).</p>
    </details></section>
  <section><h2>Freshness</h2><p class="note" id="nFresh"></p><div class="scroll" id="tFresh"></div></section>
  <section><h2>Bronze columns: live AIS</h2><p class="note" id="nBronze"></p><div class="scroll" id="tBronze"></div></section>
  <section><h2>Bronze columns: reference files</h2><p class="note">Does each reference file still have every column the pipeline reads? A weekly or daily fetch refuses to replace a file whose source changed its fields, so a problem here also shows as an old update time.</p><div class="scroll" id="tRef"></div></section>
  <section><h2>Messages per hour</h2><p class="note" id="nFeed"></p><div class="chart"><canvas id="cFeed"></canvas></div></section>
  <div class="grid">
    <section><h2>Rejected rows per day</h2><p class="note" id="nRej"></p><div class="chart"><canvas id="cRej"></canvas></div></section>
    <section><h2>Silver batch times per day</h2><p class="note" id="nSil"></p><div class="chart"><canvas id="cSil"></canvas></div></section>
  </div>
  <section><h2>Gold runs</h2><p class="note" id="nRuns"></p><div class="chart"><canvas id="cRuns"></canvas></div></section>
  <section><h2>Gold run log</h2><p class="note">Newest first. A blocked or crashed run leaves the previous Gold published.</p><div class="scroll" id="tRuns"></div></section>
</div>

<div data-panel="vessel" hidden>
  <section><h2>Find a vessel</h2><div class="search"><input id="vq" list="vlist" placeholder="Name or MMSI, e.g. ESVAGT AURORA"><datalist id="vlist"></datalist></div>
    <p class="note" id="vnote" style="margin-top:10px"></p></section>
  <div id="vbody" hidden>
    <section><h2 id="vname"></h2><p class="note" id="vident"></p><ul class="kpis" id="vkpis"></ul>
      <p class="note" style="margin-top:12px">Route map and day-by-day history: <code id="vcmd"></code></p></section>
    <section><h2>Surveillance events</h2><div class="scroll" id="vEv"></div></section>
    <section><h2>Safety-zone entries</h2><div class="scroll" id="vZn"></div></section>
    <section><h2>Visits to installations</h2><div class="scroll" id="vVi"></div></section>
  </div>
</div>
<footer>Rebuilt from the published Gold tables after each hourly Gold run (also after a run the quality gate blocked, so the Pipeline health tab shows it); this page reloads every 5 minutes while you are not using it.</footer>
</main>
<script type="application/json" id="data">__DATA__</script>
<script>
(function(){
const D = JSON.parse(document.getElementById('data').textContent);
const $ = id => document.getElementById(id), css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const num = n => n == null ? '–' : Math.round(n).toLocaleString('en-GB');
const dur = m => m == null ? '–' : m < 60 ? Math.round(m) + ' min' : m < 2880 ? (m / 60).toFixed(1) + ' h' : (m / 1440).toFixed(1) + ' d';
const HOUR = 36e5, DAY = 24 * HOUR, T0 = Date.parse(D.t0), END = Date.parse(D.end);
const NDAYS = Math.max(1, Math.floor((END - T0) / DAY) + 1);
const when = s => new Date(s).toLocaleString('en-GB', {weekday:'short', day:'numeric', month:'short', hour:'2-digit', minute:'2-digit', timeZone:'UTC'}) + ' UTC';
const at = h => new Date(T0 + h * HOUR).toLocaleString('en-GB', {day:'numeric', month:'short', hour:'2-digit', minute:'2-digit', timeZone:'UTC'});
const dayName = k => new Date(T0 + k * DAY).toLocaleDateString('en-GB', {day:'numeric', month:'short', year:'numeric', timeZone:'UTC'});
const I = D.inst, V = D.v, H = D.h, Z = D.zn || {i:[],m:[],t:[],d:[],kn:[],x:[]}, E = D.ev || {k:[],i:[],m:[],t:[],d:[],x:[],p:[],f:[]};
const VES = D.ves || {}, B = D.flag_bits || {};
const R = Object.assign({min_days:3, window_days:30, large_ship_min_m:100, loiter_max_kn:2, loiter_min_minutes:120, ais_gap_min_minutes:30, fast_kn:10, gap_alert_h:3, dark_inst_m:5000, dark_pipe_m:2000}, D.rules || {});
const vname = m => (VES[m] && VES[m][0]) || ('MMSI ' + m);
let tab = 'traffic', grp = 'all', survMode = 'look', from = Math.max(0, NDAYS - 7), to = NDAYS - 1, vessel = null;
const charts = {}, maps = {};
function chart(id, cfg){ if (charts[id]) charts[id].destroy(); charts[id] = new Chart($(id), cfg); }
const keep = on => grp === 'all' || (grp === 'onshore') === !!on;
const inRange = t => t >= from * 24 && t < (to + 1) * 24;
const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const has = (f, k) => (f & (B[k] || 0)) !== 0;

function el(tag, text, cls){ const e = document.createElement(tag); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
function vlink(m){ const a = el('a', vname(m), 'v'); a.addEventListener('click', ev => { ev.stopPropagation(); openVessel(m); }); return a; }
function table(box, rows, cols, onRow){
  box = typeof box === 'string' ? $(box) : box; box.replaceChildren();
  if (!rows.length) { box.append(el('p', 'Nothing in these days for this selection.', 'note')); return; }
  const tb = el('table'), th = el('thead'), hr = el('tr');
  for (const [h, , n] of cols) { const c = el('th', h); if (n) c.style.textAlign = 'right'; hr.append(c); }
  th.append(hr); tb.append(th); const body = el('tbody');
  for (const r of rows.slice(0, 500)) {
    const tr = el('tr'); if (onRow) { tr.className = 'click'; tr.addEventListener('click', () => onRow(r)); }
    for (const [, f, n] of cols) { const td = el('td', null, n ? 'n' : ''); const v = f(r); if (v instanceof Node) td.append(v); else td.textContent = v; tr.append(td); }
    body.append(tr);
  }
  tb.append(body); box.append(tb);
  if (rows.length > 500) box.append(el('p', 'Showing the first 500 of ' + rows.length.toLocaleString('en-GB') + '.', 'note'));
}
function kpis(box, items){ box = $(box); box.replaceChildren();
  for (const [v, l] of items) { const li = el('li'); li.append(el('b', v), el('span', l)); box.append(li); } }
function getMap(id){
  if (typeof L === 'undefined') { $(id).textContent = 'The map needs the Leaflet library from cdnjs.cloudflare.com, which did not load.'; return null; }
  if (!maps[id]) {
    const m = L.map(id, {scrollWheelZoom: false}).setView([62.5, 4], 5);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 13, attribution: '&copy; OpenStreetMap contributors'}).addTo(m);
    maps[id] = {map: m, layer: L.layerGroup().addTo(m), fitted: false, markers: {}};
  }
  const M = maps[id]; M.layer.clearLayers(); M.markers = {}; setTimeout(() => M.map.invalidateSize(), 0); return M;
}
function fit(M, pts){ if (pts.length && !M.fitted) { M.map.fitBounds(pts, {padding: [24, 24]}); M.fitted = true; } }
function popup(lines){ const d = el('div'); d.append(el('b', lines[0])); for (const l of lines.slice(1)) d.append(el('div', l)); return d; }

// ---------- Traffic (logistics planner) ----------
const BIN = [15, 30, 60, 120, 240, 480, 1440], BIN_L = ['Under 15 min', '15–30 min', '30–60 min', '1–2 h', '2–4 h', '4–8 h', '8–24 h', 'Over 24 h'];
const bin = m => { const j = BIN.findIndex(x => m < x); return j < 0 ? BIN.length : j; };
const osloHour = t => +new Date(T0 + t * HOUR).toLocaleString('en-GB', {hour:'2-digit', hourCycle:'h23', timeZone:'Europe/Oslo'});
const SHIP = c => c == null ? '–' : c === 30 ? 'fishing' : c >= 31 && c <= 32 ? 'towing' : c === 50 ? 'pilot' : c === 51 ? 'search and rescue'
  : c === 52 ? 'tug' : c === 53 ? 'port tender' : c === 55 ? 'law enforcement' : c >= 60 && c < 70 ? 'passenger' : c >= 70 && c < 80 ? 'cargo'
  : c >= 80 && c < 90 ? 'tanker' : c >= 90 && c < 100 ? 'other (often offshore supply)' : 'code ' + c;
let sel = null;                                       // index into I of the picked installation, or null for all
const isFac = m => !!(VES[m] && VES[m][3]);          // an installation broadcasting AIS itself (Gold: vessels.is_facility)
function pick(ii){ sel = ii; $('iq').value = ii == null ? '' : I[ii][0]; render();
  const M = maps[tab === 'safety' ? 'mapZ' : 'mapT']; if (ii != null && M && M.markers[I[ii][0]]) M.map.setView(M.markers[I[ii][0]].getLatLng(), 8); }
function traffic(){
  if (sel != null && !keep(I[sel][2])) sel = null;
  const per = new Map(), calls = [], passing = [], facLeft = new Set();
  for (let k = 0; k < V.t.length; k++) {
    if (!inRange(V.t[k])) continue; const ii = V.i[k]; if (!keep(I[ii][2])) continue;
    if (isFac(V.m[k])) { facLeft.add(V.m[k]); continue; }
    let p = per.get(ii); if (!p) { p = {calls:0, pass:0, ves:new Set(), a:[], w:[]}; per.set(ii, p); }
    const r = {ii, m:V.m[k], t:V.t[k], d:V.d[k], w:V.w ? V.w[k] : null, a:V.a ? V.a[k] : null};
    if (V.z[k]) { p.calls++; p.ves.add(r.m); if (r.a != null) p.a.push(r.a); if (r.w != null) p.w.push(r.w); calls.push(r); }
    else { p.pass++; passing.push(r); }
  }
  const inst = [...per].map(([ii, p]) => ({ii, name:I[ii][0], kind:I[ii][1], on:I[ii][2], lat:I[ii][3], lon:I[ii][4], field:I[ii][5],
    calls:p.calls, pass:p.pass, vessels:p.ves.size, along:median(p.a), wait:median(p.w)})).sort((x, y) => y.calls - x.calls || y.pass - x.pass);
  const C = sel == null ? calls : calls.filter(r => r.ii === sel), P = sel == null ? passing : passing.filter(r => r.ii === sel);
  const days = to - from + 1, nVes = new Set(C.map(r => r.m)).size;
  const A = C.map(r => r.a).filter(x => x != null), W = C.map(r => r.w).filter(x => x != null);
  kpis('kpis', [[num(C.length), 'service calls'], [num(nVes), 'vessels serving'], [(C.length / days).toFixed(1), 'calls per day'],
    [dur(median(A)), 'median alongside'], [dur(median(W)), 'median wait'], [num(P.length), 'passing visits']]);
  // installation picker
  $('iclear').hidden = sel == null; $('secTop').hidden = sel != null;
  // daily series and arrival hours
  const perDay = new Array(days).fill(0), arr = new Array(24).fill(0);
  for (const r of C) { perDay[Math.floor(r.t / 24) - from]++; arr[osloHour(r.t)]++; }
  const g = $('glance');
  if (sel == null) g.textContent = '';
  else { const me = inst.find(r => r.ii === sel) || {calls:0, vessels:0}, best = perDay.indexOf(Math.max(...perDay)), peak = arr.indexOf(Math.max(...arr));
    g.textContent = I[sel][0] + ((I[sel][1] || '') ? ' (' + I[sel][1].toLowerCase() + (I[sel][5] && I[sel][5] !== I[sel][0] ? ', field ' + I[sel][5] : '') + ')' : '') + ': ' +
      (C.length ? num(C.length) + ' service calls by ' + num(nVes) + ' vessels in ' + days + (days === 1 ? ' day' : ' days') +
        '; busiest day ' + dayName(from + best) + ' (' + perDay[best] + '); most arrivals ' + String(peak).padStart(2, '0') + ':00–' + String((peak + 1) % 24).padStart(2, '0') + ':00 Norwegian time.'
       : 'no service calls in these days' + (P.length ? ', only ' + num(P.length) + ' passing visits.' : '.')); }
  // map
  const M = getMap('mapT');
  if (M) { const maxC = Math.max(1, ...inst.map(r => r.calls)), pts = [];
    for (const r of inst.filter(r => r.lat != null)) {
      const c = css(r.on ? '--onshore' : '--accent'), me = r.ii === sel;
      const mk = L.circleMarker([r.lat, r.lon], {radius: 3 + 20 * Math.sqrt(r.calls / maxC), color: me ? css('--warn') : c, weight: me ? 3 : 1.5,
          fillColor: c, fillOpacity: sel == null || me ? 0.45 : 0.15})
        .bindTooltip(r.name + ': ' + num(r.calls) + ' service calls, ' + num(r.pass) + ' passing').on('click', () => pick(r.ii));
      M.layer.addLayer(mk); M.markers[r.name] = mk; pts.push([r.lat, r.lon]); }
    fit(M, pts); }
  // table: installations, or the vessels serving the picked one
  if (sel == null) {
    $('hTable').textContent = 'Installations';
    $('nTable').textContent = 'Click a row to pick an installation.' + (facLeft.size ? ' ' + num(facLeft.size) +
      (facLeft.size === 1 ? ' installation broadcasting its own AIS is' : ' installations broadcasting their own AIS are') +
      ' left out of these numbers (for example an FPSO sending its own name).' : '');
    table('tTraffic', inst, [['Installation', r => r.name], ['Type', r => (r.kind || '').toLowerCase() + (r.on ? ' (onshore)' : '')],
      ['Field', r => r.field || '–'], ['Service calls', r => num(r.calls), 1], ['Vessels serving', r => num(r.vessels), 1],
      ['Median alongside', r => dur(r.along), 1], ['Median wait', r => dur(r.wait), 1], ['Passing visits', r => num(r.pass), 1]], r => pick(r.ii));
  } else {
    const byV = new Map(), elsewhere = new Map();
    for (let k = 0; k < V.t.length; k++) {               // every installation, whatever the Offshore/Onshore switch shows
      if (!V.z[k] || V.i[k] === sel || !inRange(V.t[k]) || isFac(V.m[k])) continue;
      let s = elsewhere.get(V.m[k]); if (!s) elsewhere.set(V.m[k], s = new Map()); s.set(V.i[k], (s.get(V.i[k]) || 0) + 1); }
    for (const r of C) { let v = byV.get(r.m); if (!v) byV.set(r.m, v = {m:r.m, n:0, a:0, w:[], last:-1}); v.n++; v.a += r.a || 0; if (r.w != null) v.w.push(r.w); v.last = Math.max(v.last, r.t); }
    const rows = [...byV.values()].sort((x, y) => y.n - x.n || y.a - x.a);
    const others = m => { const s = elsewhere.get(m); if (!s) return '–';
      const top = [...s].sort((x, y) => y[1] - x[1]); return top.slice(0, 3).map(([ii, n]) => I[ii][0] + ' (' + n + ')').join(', ') + (top.length > 3 ? ' +' + (top.length - 3) : ''); };
    $('hTable').textContent = 'Vessels serving ' + I[sel][0];
    $('nTable').textContent = 'Service calls in the selected days. "Also serves" lists other installations the vessel made service calls to, which shows shared supply routes.';
    table('tTraffic', rows, [['Vessel', r => vlink(r.m)], ['Type', r => SHIP(VES[r.m] && VES[r.m][1])], ['Length', r => VES[r.m] && VES[r.m][2] ? VES[r.m][2] + ' m' : '–', 1],
      ['Calls', r => num(r.n), 1], ['Alongside', r => dur(r.a), 1], ['Median wait', r => dur(median(r.w)), 1], ['Last call (UTC)', r => at(r.last)],
      ['Also serves', r => others(r.m)]]);
  }
  $('hDaily').textContent = sel == null ? 'Service calls per day, all installations' : 'Service calls per day at ' + I[sel][0];
  $('nDaily').textContent = 'Days in UTC. ' + (sel == null ? 'Pick an installation to see its own workload.' : 'Compare with the vessels table below to see who did the work.');
  if (typeof Chart === 'undefined') return;
  const top = inst.filter(r => r.calls).slice(0, 15);
  if (sel == null) chart('cTop', {type:'bar', data:{labels: top.map(r => r.name), datasets:[
      {label:'Service calls', data: top.map(r => r.calls), backgroundColor: top.map(r => css(r.on ? '--onshore' : '--accent'))}]},
    options:{indexAxis:'y', maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}},
      onClick: (e, els) => { if (els.length) pick(top[els[0].index].ii); }, scales:{y:{grid:{display:false}}}}});
  chart('cDaily', {type:'bar', data:{labels: perDay.map((_, k) => new Date(T0 + (from + k) * DAY).toLocaleDateString('en-GB', {day:'numeric', month:'short', timeZone:'UTC'})),
      datasets:[{label:'Service calls', data: perDay, backgroundColor: css('--accent')}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}}, scales:{x:{ticks:{maxTicksLimit:14}, grid:{display:false}}, y:{beginAtZero:true, ticks:{precision:0}}}}});
  chart('cArr', {type:'bar', data:{labels: arr.map((_, h) => String(h).padStart(2, '0')), datasets:[{label:'Arrivals', data: arr, backgroundColor: css('--accent')}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}}, scales:{x:{grid:{display:false}}, y:{beginAtZero:true, ticks:{precision:0}}}}});
  const bw = new Array(BIN_L.length).fill(0), ba = new Array(BIN_L.length).fill(0);
  for (const w of W) bw[bin(w)]++; for (const x of A) ba[bin(x)]++;
  chart('cDur', {type:'bar', data:{labels: BIN_L, datasets:[{label:'Waiting before entering', data: bw, backgroundColor: css('--signal')},
      {label:'Alongside (inside 500 m)', data: ba, backgroundColor: css('--accent')}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{position:'bottom'}}, scales:{x:{grid:{display:false}}, y:{beginAtZero:true, ticks:{precision:0}}}}});
}
$('iq').addEventListener('change', () => { const v = $('iq').value.trim().toUpperCase(); const ii = I.findIndex(r => r[0] === v); if (ii >= 0) pick(ii); });
$('iclear').addEventListener('click', () => pick(null));
$('ilist').replaceChildren(...I.map(r => { const o = el('option'); o.value = r[0]; return o; }));
document.querySelectorAll('details.help').forEach((hd, n) => {               // remember a collapsed help box (per browser)
  try { if (localStorage.getItem('helpClosed' + n) === '1') hd.open = false; } catch (e) {}
  hd.addEventListener('toggle', () => { try { localStorage.setItem('helpClosed' + n, hd.open ? '0' : '1'); } catch (e) {} }); });
function focus(mapId, name){ const M = maps[mapId]; const m = M && M.markers[name]; if (!m) return;
  $(mapId).scrollIntoView({behavior:'smooth', block:'center'}); M.map.setView(m.getLatLng(), 9); m.openPopup(); }

// ---------- Safety zones and emergency preparedness ----------
const HL = D.hl || {i:[], t:[], d:[]}, GP = D.gp || {i:[], t:[], n:[]}, OUT = new Set(D.out || []);
const RESCUE = /\b(FRB|FRC|MOB|DC\d*|DAUGHTER)\b/;
const vlen = m => VES[m] && VES[m][2] ? VES[m][2] : null;
const isRescue = m => RESCUE.test(((VES[m] && VES[m][0]) || '').toUpperCase()) || (VES[m] && VES[m][1] === 51 && (vlen(m) == null || vlen(m) < 24));
function risk(r){                            // [level 0 high .. 2 low, label, reason]
  const L = vlen(r.m), kn = r.kn;
  if (isRescue(r.m)) return [2, 'Low', 'rescue craft, built for these speeds'];
  if (L == null) return [1, 'Medium', 'length unknown at ' + kn.toFixed(1) + ' kn'];
  if (L < 24) return [2, 'Low', L + ' m: small craft'];
  if (L >= 80) return [0, 'High', L + ' m at ' + kn.toFixed(1) + ' kn: large and fast'];
  return kn > 15 ? [0, 'High', L + ' m at ' + kn.toFixed(1) + ' kn: mid-size and very fast'] : [1, 'Medium', L + ' m at ' + kn.toFixed(1) + ' kn: mid-size'];
}
const fishReg = m => !!(VES[m] && VES[m][4]), fishAis = m => !!(VES[m] && VES[m][1] === 30), isFishing = m => fishReg(m) || fishAis(m);
const FLEET = (() => {                       // zone entries per vessel at offshore installations over the whole data period
  const n = new Map(), inst = new Map(), served = new Map();   // served: vessel -> installation -> days with >= 1 h inside
  for (let k = 0; k < Z.t.length; k++) { if (I[Z.i[k]][2]) continue; const m = Z.m[k];
    n.set(m, (n.get(m) || 0) + 1); if (!inst.has(m)) inst.set(m, new Set()); inst.get(m).add(Z.i[k]);
    if (Z.d[k] >= 60) { if (!served.has(m)) served.set(m, new Map()); const s = served.get(m);
      if (!s.has(Z.i[k])) s.set(Z.i[k], new Set()); s.get(Z.i[k]).add(Math.floor(Z.t[k] / 24)); } }
  const repeat = m => [...(served.get(m) || new Map()).values()].some(days => days.size >= 2);
  const near = new Map(), mins = new Map();  // visits and minutes within 5 km of offshore installations
  for (let k = 0; k < V.t.length; k++) if (!I[V.i[k]][2]) {
    near.set(V.m[k], (near.get(V.m[k]) || 0) + 1); mins.set(V.m[k], (mins.get(V.m[k]) || 0) + (V.d[k] || 0)); }
  const STANDBY_MIN = 48 * 60;               // two days near installations: a standby or field vessel at work
  return {n, inst, near, mins, has: m => (inst.get(m) || new Set()).size >= 2 || (n.get(m) || 0) >= 5 ||
                                         (near.get(m) || 0) >= 10 || (mins.get(m) || 0) >= STANDBY_MIN || repeat(m)};
})();
const fishSource = m => fishReg(m) ? 'fishing register' : fishAis(m) ? 'AIS type fishing' : '';
const unexpectedWhy = m => isFishing(m) ? 'fishing vessel (' + fishSource(m) + ')'
  : 'rarely near installations: ' + (FLEET.n.get(m) || 0) + ' zone entries at ' + (FLEET.inst.get(m) || new Set()).size +
    ' installation(s), ' + (FLEET.near.get(m) || 0) + ' visits and ' + dur(FLEET.mins.get(m) || 0) + ' within 5 km in all data';
function zoneRows(filterMmsi){
  const rows = [];
  for (let k = 0; k < Z.t.length; k++) {
    if (!inRange(Z.t[k])) continue; const ii = Z.i[k];
    if (filterMmsi != null ? Z.m[k] !== filterMmsi : !keep(I[ii][2])) continue;
    rows.push({ii, inst:I[ii][0], on:I[ii][2], m:Z.m[k], t:Z.t[k], d:Z.d[k], kn:Z.kn[k], x:Z.x[k]});
  }
  return rows;
}
function earlierEntries(){                         // per vessel+installation: entry hours, to count earlier entries
  const m = new Map(); for (let k = 0; k < Z.t.length; k++) { const key = Z.m[k] + '|' + Z.i[k]; if (!m.has(key)) m.set(key, []); m.get(key).push(Z.t[k]); } return m; }
function preparedness(){
  const a = from * 24, b = (to + 1) * 24, per = new Map();
  let known = 0; for (let h = a; h < b && h < (END - T0) / HOUR + 1; h++) if (!OUT.has(h)) known++;
  const get = ii => { let p = per.get(ii); if (!p) per.set(ii, p = {gapH:0, longest:0, longT:null, big:0, land:0, lastLand:-1}); return p; };
  for (let k = 0; k < V.t.length; k++) if (!I[V.i[k]][2] && inRange(V.t[k])) get(V.i[k]);   // offshore installations with traffic
  for (let k = 0; k < GP.t.length; k++) {
    const s0 = Math.max(GP.t[k], a), e0 = Math.min(GP.t[k] + GP.n[k], b); if (e0 <= s0 || I[GP.i[k]][2]) continue;
    const p = get(GP.i[k]), n = e0 - s0; p.gapH += n; if (n > p.longest) { p.longest = n; p.longT = s0; } if (n >= R.gap_alert_h) p.big++; }
  for (let k = 0; k < HL.t.length; k++) { if (!inRange(HL.t[k]) || I[HL.i[k]][2]) continue; const p = get(HL.i[k]); p.land++; p.lastLand = Math.max(p.lastLand, HL.t[k]); }
  return [...per].map(([ii, p]) => ({ii, name:I[ii][0], kind:I[ii][1], ...p, cover: known ? 100 * (1 - p.gapH / known) : null}))
                 .sort((x, y) => y.longest - x.longest || y.gapH - x.gapH);
}
function safety(){
  if (sel != null && !keep(I[sel][2])) sel = null;
  $('sq').value = sel == null ? '' : I[sel][0]; $('sclear').hidden = sel == null;
  document.querySelectorAll('.rv').forEach(e => e.textContent = R[e.dataset.r]);
  const all = zoneRows(null), rows = sel == null ? all : all.filter(r => r.ii === sel), per = new Map();
  const isFast = r => r.kn != null && r.kn > R.fast_kn, fast = rows.filter(isFast).map(r => ({...r, risk: risk(r)}));
  const prior = earlierEntries();
  const unexp = rows.filter(r => !r.on && !isRescue(r.m) && !isFac(r.m) && (isFishing(r.m) || !FLEET.has(r.m)))
                    .map(r => ({...r, before: prior.get(r.m + '|' + r.ii).filter(t => t < r.t).length}));
  const fishVisits = [];
  for (let k = 0; k < V.t.length; k++) { const ii = V.i[k];
    if (!inRange(V.t[k]) || I[ii][2] || !isFishing(V.m[k]) || (sel != null && ii !== sel)) continue;
    fishVisits.push({ii, inst:I[ii][0], m:V.m[k], t:V.t[k], d:V.d[k], z:V.z[k]}); }
  for (const r of all) { let p = per.get(r.ii); if (!p) { p = {n:0, fast:0, ves:new Set()}; per.set(r.ii, p); } p.n++; p.ves.add(r.m); if (isFast(r)) p.fast++; }
  const prep = preparedness().filter(r => sel == null || r.ii === sel);
  const landings = prep.reduce((s, r) => s + r.land, 0);
  kpis('kpis', [[num(rows.length), 'zone entries'], [num(unexp.length), 'unexpected entries'], [num(new Set(fishVisits.map(r => r.m)).size), 'fishing vessels within 5 km'],
    [num(fast.filter(r => r.risk[0] === 0).length), 'high-risk fast entries'], [num(landings), 'helideck landings']]);
  const g = $('sglance');
  if (sel == null) g.textContent = '';
  else { const p = prep[0];
    g.textContent = I[sel][0] + ': ' + num(rows.length) + ' zone entries (' + num(fast.length) + ' fast, ' + num(unexp.length) + ' unexpected)' +
      (p ? '; a vessel within 5 km in ' + (p.cover == null ? '–' : p.cover.toFixed(1) + '%') + ' of hours, longest time without ' + (p.longest ? p.longest + ' h' : 'none') +
           '; ' + num(p.land) + (p.land === 1 ? ' helideck landing.' : ' helideck landings.') : '.'); }
  const list = [...per].map(([ii, p]) => ({ii, name:I[ii][0], lat:I[ii][3], lon:I[ii][4], on:I[ii][2], ...p}));
  const M = getMap('mapZ');
  if (M) { const mx = Math.max(1, ...list.map(r => r.n)), pts = [];
    for (const r of list.filter(r => r.lat != null)) {
      const c = css(r.on ? '--onshore' : '--accent'), me = r.ii === sel;
      const mk = L.circleMarker([r.lat, r.lon], {radius: 4 + 18 * Math.sqrt(r.n / mx), color: r.fast || me ? css('--warn') : c, weight: me ? 4 : r.fast ? 2.5 : 1.5,
          fillColor: c, fillOpacity: sel == null || me ? 0.45 : 0.15})
        .bindTooltip(r.name + ': ' + num(r.n) + ' entries, ' + num(r.fast) + ' fast').on('click', () => pick(r.ii));
      M.layer.addLayer(mk); M.markers[r.name] = mk; pts.push([r.lat, r.lon]); }
    fit(M, pts); }
  $('nPrep').textContent = 'Offshore installations: share of hours with at least one vessel within 5 km (hours with a thin feed left out), the longest ' +
    'time without one, and helideck landings. Context only: guideline 064 sets rescue times, not a standby distance, and one vessel can cover several ' +
    'installations. Lowest presence first; click a row to pick the installation.';
  table('tPrep', prep.sort((x, y) => (x.cover ?? 101) - (y.cover ?? 101)), [['Installation', r => r.name], ['Type', r => (r.kind || '').toLowerCase()],
    ['Vessel within 5 km', r => r.cover == null ? '–' : r.cover.toFixed(1) + '% of hours', 1], ['Longest time without', r => r.longest ? r.longest + ' h' : 'none', 1],
    ['It started (UTC)', r => r.longT == null ? '–' : at(r.longT)],
    ['Helideck landings', r => num(r.land), 1], ['Last landing (UTC)', r => r.lastLand < 0 ? '–' : at(r.lastLand)]], r => pick(r.ii));
  $('noteFast').textContent = 'Entries with a speed above ' + R.fast_kn + ' kn inside 500 m, highest risk first, then fastest. Risk combines size and speed (see How to read this page).';
  const ty = r => SHIP(VES[r.m] && VES[r.m][1]), len = r => vlen(r.m) ? vlen(r.m) + ' m' : '–';
  const tag = r => el('span', r.risk[1], 'risk ' + 'hml'[r.risk[0]]);
  table('tFast', fast.sort((a, b) => a.risk[0] - b.risk[0] || b.kn - a.kn), [['Risk', tag], ['Vessel', r => vlink(r.m)], ['Type', ty], ['Length', len, 1],
    ['Installation', r => r.inst], ['Entry (UTC)', r => at(r.t)], ['Top speed', r => r.kn.toFixed(1) + ' kn', 1], ['Closest', r => num(r.x) + ' m', 1],
    ['Inside', r => dur(r.d), 1], ['Why this risk', r => r.risk[2]]], r => pick(r.ii));
  table('tUnexp', unexp.sort((a, b) => b.t - a.t), [['Vessel', r => vlink(r.m)], ['Type', ty], ['Length', len, 1], ['Installation', r => r.inst],
    ['Entry (UTC)', r => at(r.t)], ['Inside', r => dur(r.d), 1], ['Closest', r => num(r.x) + ' m', 1],
    ['Top speed', r => r.kn == null ? '–' : r.kn.toFixed(1) + ' kn', 1], ['Earlier entries here', r => r.before ? num(r.before) : 'first time', 1],
    ['Why unexpected', r => unexpectedWhy(r.m)]], r => pick(r.ii));
  $('nFish').textContent = 'Visits by fishing vessels within 5 km of offshore installations, newest first. ' + (D.fishing_register
    ? 'Norwegian vessels are identified from the fishing vessel register; others by AIS type.' : 'The fishing vessel register is not loaded yet, so only AIS type "fishing" is used.');
  table('tFish', fishVisits.sort((a, b) => b.t - a.t), [['Vessel', r => vlink(r.m)], ['Identified by', r => fishSource(r.m)], ['Length', len, 1],
    ['Installation', r => r.inst], ['Arrived (UTC)', r => at(r.t)], ['Stayed', r => dur(r.d), 1], ['Inside 500 m', r => r.z ? 'yes' : 'no']], r => pick(r.ii));
}
$('sq').addEventListener('change', () => { const v = $('sq').value.trim().toUpperCase(); const ii = I.findIndex(r => r[0] === v); if (ii >= 0) pick(ii); });
$('sclear').addEventListener('click', () => pick(null));

// ---------- Surveillance ----------
function eventRows(filterMmsi){
  const rows = [];
  for (let k = 0; k < E.t.length; k++) {
    if (!inRange(E.t[k])) continue; const ii = E.i[k];
    if (filterMmsi != null ? E.m[k] !== filterMmsi : !keep(I[ii][2])) continue;
    rows.push({ii, inst:I[ii][0], on:I[ii][2], lat:I[ii][3], lon:I[ii][4], gap:E.k[k] === 1, m:E.m[k], t:E.t[k], d:E.d[k], x:E.x[k], p:E.p[k], f:E.f[k]});
  }
  return rows;
}
function what(r){
  return r.gap ? 'silent for ' + dur(r.d) + ' (at least ' + R.ais_gap_min_minutes + ' min) between two positions near ' + r.inst + ', sending nothing anywhere in between'
               : 'stayed under ' + R.loiter_max_kn + ' kn within 5 km of ' + r.inst + ' for ' + dur(r.d) + ' (at least ' + dur(R.loiter_min_minutes) + ')';
}
function why(r){
  const regular = 'present at ' + r.inst + ' on ' + (r.p == null ? 'an unknown number' : r.p) + ' of the previous ' + R.window_days + ' days';
  if (has(r.f, 'worth_a_look'))
    return 'Worth a look: it ' + what(r) + '. None of the ordinary explanations applies: ' + regular + ' (' + R.min_days +
      ' needed to count as a regular visitor), not an installation or rig broadcasting AIS, not a large ship (' + R.large_ship_min_m +
      ' m or more) at an onshore terminal, not a craft named after the installation, and the AIS feed was complete.';
  const out = [];
  if (has(r.f, 'regular_visitor')) out.push('regular visitor: ' + regular);
  if (has(r.f, 'is_facility')) out.push('the vessel is itself an installation or rig in Sodir\'s list, stationary at the time');
  if (has(r.f, 'terminal_call')) out.push('a large ship at an onshore terminal, typically waiting to berth');
  if (has(r.f, 'support_craft')) out.push('its name contains the installation\'s name, so it belongs to it');
  if (has(r.f, 'censored')) out.push('it began right after a gap in the feed or the start of the data');
  if (has(r.f, 'feed_outage')) out.push('the whole AIS feed was quiet during the gap');
  return 'Ordinary: ' + (out.join('; ') || 'no flag recorded') + '.';
}
function flagTags(r){
  const w = el('span');
  if (has(r.f, 'worth_a_look')) w.append(el('span', 'worth a look', 'tag look'));
  for (const [k, l] of [['regular_visitor', 'regular'], ['is_facility', 'facility'], ['terminal_call', 'terminal call'], ['support_craft', 'support craft'], ['censored', 'censored'], ['feed_outage', 'feed outage']])
    if (has(r.f, k)) w.append(el('span', l, 'tag'));
  return w;
}
const evCols = withVessel => [['Type', r => r.gap ? 'AIS gap' : 'loitering']].concat(withVessel ? [['Vessel', r => vlink(r.m)]] : [])
  .concat([['Installation', r => r.inst], ['Start (UTC)', r => at(r.t)], ['Duration', r => dur(r.d), 1], ['Closest', r => num(r.x) + ' m', 1],
           ['Flags', flagTags], ['Why', r => el('div', why(r), 'why')]]);
const PL = D.pl || {m:[], t:[], d:[], p:[], md:[], x:[], kn:[], lat:[], lon:[]}, PP = D.pp || [];
const DK = D.dk || {t:[], lat:[], lon:[], n:[], i:[], im:[], p:[], pm:[]}, DKM = D.dk_meta || null;
function gfwAttribution(box){
  box = $(box); box.replaceChildren(); if (!DKM) return;
  const a = el('a', 'Powered by Global Fishing Watch'); a.href = 'https://globalfishingwatch.org'; a.target = '_blank'; a.rel = 'noopener';
  box.append(a, document.createTextNode('. ' + (DKM.versions || []).join(', ') + ', radar detections not matched to AIS; accessed ' +
    (DKM.accessed || '').slice(0, 10) + ' at https://globalfishingwatch.org/our-apis/. CC BY-NC 4.0, non-commercial use only.'));
}
function surveillance(){
  const all = eventRows(null), look = all.filter(r => has(r.f, 'worth_a_look'));
  const pl = []; for (let k = 0; k < PL.t.length; k++) if (inRange(PL.t[k])) pl.push({m:PL.m[k], t:PL.t[k], d:PL.d[k], p:PL.p[k], md:PL.md[k], x:PL.x[k], kn:PL.kn[k], lat:PL.lat[k], lon:PL.lon[k]});
  const dk = []; for (let k = 0; k < DK.t.length; k++) if (inRange(DK.t[k])) dk.push({t:DK.t[k], lat:DK.lat[k], lon:DK.lon[k], n:DK.n[k], i:DK.i[k], im:DK.im[k], p:DK.p[k], pm:DK.pm[k]});
  kpis('kpis', [[num(look.length), 'events worth a look'], [num(look.filter(r => !r.gap).length), 'loitering worth a look'],
                [num(look.filter(r => r.gap).length), 'AIS gaps worth a look'], [num(pl.length), 'loitering over pipelines'],
                [DKM ? num(dk.reduce((s, r) => s + r.n, 0)) : '–', 'dark vessel detections']]);
  document.querySelectorAll('.rv').forEach(e => e.textContent = num(R[e.dataset.r]));
  const rows = (survMode === 'look' ? look : all).sort((a, b) => b.d - a.d);
  $('noteS').textContent = (survMode === 'look' ? 'Events with no ordinary explanation' : 'All events') + ', longest first. Click a row to see it on the map, a vessel name to open its profile.';
  const M = getMap('mapS');
  if (M) { const pts = [], byInst = new Map();
    for (const r of all.filter(r => r.lat != null)) { const k = r.inst; const b = byInst.get(k) || {r, look:0, n:0}; b.n++; if (has(r.f, 'worth_a_look')) b.look++; byInst.set(k, b); }
    for (const [name, b] of byInst) {
      if (survMode === 'look' && !b.look) continue;
      const c = b.look ? css('--warn') : css('--muted');
      const mk = L.circleMarker([b.r.lat, b.r.lon], {radius: 5 + 3 * Math.sqrt(b.look || b.n), color: c, weight: 2, fillColor: c, fillOpacity: b.look ? 0.35 : 0.15})
        .bindPopup(popup([name, num(b.look) + ' events worth a look', num(b.n) + ' events in total'])).bindTooltip(name);
      M.layer.addLayer(mk); M.markers[name] = mk; pts.push([b.r.lat, b.r.lon]); }
    for (const [name, medium, path] of PP)
      M.layer.addLayer(L.polyline(path, {color: css('--onshore'), weight: 1.5, opacity: 0.7}).bindTooltip(name + (medium ? ' (' + medium.toLowerCase() + ')' : '')));
    for (const r of pl) {
      const mk = L.circleMarker([r.lat, r.lon], {radius: 6, color: css('--signal'), weight: 2, fillColor: css('--signal'), fillOpacity: 0.4})
        .bindPopup(popup([vname(r.m), 'Under ' + R.loiter_max_kn + ' kn for ' + dur(r.d) + ' over ' + r.p, num(r.x) + ' m from the pipeline, ' + at(r.t) + ' UTC']));
      M.layer.addLayer(mk); M.markers['pl-' + r.m + '-' + r.t] = mk; }
    for (const r of dk) {
      const mk = L.circleMarker([r.lat, r.lon], {radius: 4, color: css('--ink'), weight: 1, fillColor: css('--ink'), fillOpacity: 0.7})
        .bindPopup(popup(['Dark vessel detection, ' + dayName(Math.floor(r.t / 24)), (r.i ? num(r.im) + ' m from ' + r.i : '') + (r.i && r.p ? '; ' : '') + (r.p ? num(r.pm) + ' m from ' + r.p : ''),
                          'Radar (Sentinel-1) via Global Fishing Watch']));
      M.layer.addLayer(mk); M.markers['dk-' + r.t + '-' + r.lat + '-' + r.lon] = mk; }
    fit(M, pts); }
  gfwAttribution('attrS');
  table('tSurv', rows, evCols(true), r => focus('mapS', r.inst));
  const group = m => isFishing(m) ? 'fishing vessel' : isRescue(m) ? 'rescue craft' : FLEET.has(m) ? 'offshore fleet' : 'rarely near installations';
  $('notePL').textContent = 'Vessels under ' + R.loiter_max_kn + ' kn within 500 m of an in-service pipeline for at least ' + dur(R.loiter_min_minutes) +
    ', at least 5 km from any installation; live data, from 10-minute track positions. Longest first; click a row to see it on the map.' +
    (PP.length ? '' : ' The pipeline list is not loaded yet.');
  table('tPL', pl.sort((a, b) => b.d - a.d), [['Vessel', r => vlink(r.m)], ['Type', r => SHIP(VES[r.m] && VES[r.m][1])], ['Vessel group', r => group(r.m)],
    ['Pipeline', r => r.p], ['Carries', r => (r.md || '–').toLowerCase()], ['Start (UTC)', r => at(r.t)], ['Duration', r => dur(r.d), 1],
    ['Closest', r => num(r.x) + ' m', 1], ['Average speed', r => r.kn.toFixed(1) + ' kn', 1]], r => focus('mapS', 'pl-' + r.m + '-' + r.t));
  $('noteDK').textContent = DKM
    ? 'Satellite radar detections with no matching AIS signal, within ' + num(DKM.inst_m) + ' m of an offshore facility or ' + num(DKM.pipe_m) +
      ' m of an in-service pipeline. Newest radar data: ' + (DKM.newest || '–') + ' (about 5 days behind, and only when a satellite passed). Newest first.'
    : 'Radar detections are not loaded yet (they need a Global Fishing Watch token on the server).';
  table('tDK', dk.sort((a, b) => b.t - a.t || b.n - a.n), [['Day (UTC)', r => dayName(Math.floor(r.t / 24))], ['Detections', r => num(r.n), 1],
    ['Nearest facility', r => r.i ? r.i + ' (' + num(r.im) + ' m)' : '–'], ['Nearest pipeline', r => r.p ? r.p + ' (' + num(r.pm) + ' m)' : '–'],
    ['Position', r => r.lat.toFixed(2) + ' N, ' + r.lon.toFixed(2) + ' E']], r => focus('mapS', 'dk-' + r.t + '-' + r.lat + '-' + r.lon));
  gfwAttribution('attrDK');
}

// ---------- Vessel ----------
function openVessel(m){ vessel = m; $('vq').value = vname(m) + ' (' + m + ')'; setTab('vessel'); }
function vesselPanel(){
  const dl = $('vlist'); if (!dl.childElementCount) for (const [m, v] of Object.entries(VES)) { const o = el('option'); o.value = (v[0] || 'MMSI') + ' (' + m + ')'; dl.append(o); }
  if (vessel == null) { $('vbody').hidden = true; $('vnote').textContent = Object.keys(VES).length.toLocaleString('en-GB') + ' vessels with activity near installations. Type a name or MMSI, or click a vessel name in any table.';
    kpis('kpis', []); return; }
  const m = vessel, info = VES[m] || [];
  const vv = []; for (let k = 0; k < V.t.length; k++) if (V.m[k] === m && inRange(V.t[k])) vv.push({inst:I[V.i[k]][0], t:V.t[k], d:V.d[k], z:V.z[k]});
  const zz = zoneRows(m), ee = eventRows(m);
  $('vbody').hidden = false; $('vnote').textContent = '';
  $('vname').textContent = info[0] || 'Unnamed vessel';
  $('vident').textContent = ['MMSI ' + m, info[1] != null ? 'ship type ' + info[1] : null, info[2] ? info[2] + ' m' : null,
                             info[3] ? 'installation or rig broadcasting AIS' : null].filter(Boolean).join(' · ');
  $('vcmd').textContent = 'python -m spark.vessel_report --mmsi ' + m;
  const vk = [[num(vv.length), 'visits'], [num(new Set(vv.map(r => r.inst)).size), 'installations'], [num(zz.length), 'zone entries'],
              [dur(zz.reduce((s, r) => s + r.d, 0)), 'inside 500 m zones'], [num(ee.length), 'surveillance events'],
              [num(ee.filter(r => has(r.f, 'worth_a_look')).length), 'worth a look']];
  kpis('vkpis', vk); kpis('kpis', []);
  table('vEv', ee.sort((a, b) => b.t - a.t), evCols(false));
  table('vZn', zz.sort((a, b) => b.t - a.t), [['Installation', r => r.inst], ['Entry (UTC)', r => at(r.t)], ['Inside', r => dur(r.d), 1],
    ['Closest', r => num(r.x) + ' m', 1], ['Top speed', r => r.kn == null ? '–' : r.kn.toFixed(1) + ' kn', 1]]);
  table('vVi', vv.sort((a, b) => b.t - a.t), [['Installation', r => r.inst], ['Arrival (UTC)', r => at(r.t)], ['Stay', r => dur(r.d), 1],
    ['Entered zone', r => r.z ? 'yes' : 'no']]);
}
$('vq').addEventListener('change', () => {
  const q = $('vq').value.trim(), mm = q.match(/\((\d{6,9})\)\s*$/) || q.match(/^(\d{6,9})$/);
  let m = mm ? +mm[1] : null;
  if (m == null) { const up = q.toUpperCase(); const hit = Object.entries(VES).find(([, v]) => (v[0] || '').toUpperCase() === up) ||
                                                           Object.entries(VES).find(([, v]) => (v[0] || '').toUpperCase().includes(up)); if (hit) m = +hit[0]; }
  if (m == null || !VES[m]) { vessel = null; render(); $('vnote').textContent = 'No vessel matches "' + q + '".'; return; }
  vessel = m; render();
});

// ---------- Pipeline health ----------
const REASONS = [['parse_failed', 'unreadable message'], ['bad_mmsi', 'invalid MMSI'], ['bad_timestamp', 'invalid time'],
  ['missing_position', 'no position'], ['sentinel_position', 'position 91/181 (not available)'], ['out_of_range', 'position out of range'],
  ['future_timestamp', 'time in the future'], ['stale_timestamp', 'time over a year old']];
const ESSENTIAL = ['mmsi', 'msgtime', 'latitude', 'longitude'], OPTIONAL = ['altitude'];   // altitude: aircraft only
const ageMin = (ts, ref) => ts ? (ref - Date.parse(ts)) / 6e4 : null;
function health(){
  const P = D.ph || {}, SV = P.sv || {t: [], rej: {}}, ALL = P.runs || [], RU = ALL.filter(r => inRange(r.t));
  const FC = D.fc || {t: [], n: []}, OUT = new Set(D.out || []), built = Date.parse(P.built || new Date().toISOString());
  const ks = SV.t.map((_, k) => k).filter(k => inRange(SV.t[k]));
  const sum = a => ks.reduce((s, k) => s + ((a || [])[k] || 0), 0);
  const input = sum(SV.in), rej = REASONS.reduce((s, [r]) => s + sum(SV.rej[r]), 0);
  const bad = RU.filter(r => r.s !== 'READY');
  const bz = ks.map(k => ({t: SV.t[k], cb: (SV.cb || [])[k] || 0, bad: (SV.bad || [])[k] || 0, unk: (SV.unk || [])[k] || {}, abs: (SV.abs || [])[k] || []}))
    .map(r => Object.assign(r, {issue: r.cb > 0 && (r.bad > 0 || Object.keys(r.unk).length > 0 || r.abs.some(a => !OPTIONAL.includes(a)))}));
  const refBad = (P.ref || []).filter(r => r.missing.length);
  kpis('kpis', [[num(RU.length - bad.length) + ' of ' + num(RU.length), 'Gold runs published'], [num(bad.length), 'Gold runs blocked or crashed'],
    [input ? (100 * rej / input).toFixed(2) + '%' : '–', 'Silver reject rate'], [num(sum(SV.dup)), 'duplicates removed in Silver'],
    [num((D.out || []).filter(inRange).length), 'feed outage hours'],
    [num(bz.filter(r => r.issue).length + refBad.length), 'Bronze column issues']]);
  const checked = bz.filter(r => r.cb > 0);
  $('nBronze').textContent = checked.length
    ? 'One row per day, newest first. Field names are the AIS message fields as BarentsWatch sends them. A field absent from one small batch is normal, so absence counts only when no batch of the day had it (altitude only comes from aircraft).'
    : 'No batch in these days was checked yet: the check starts with this version of the pipeline.';
  table('tBronze', bz.filter(r => r.cb > 0).reverse(), [['', r => el('span', r.issue ? 'Check' : 'OK', r.issue ? 'tag look' : 'tag')],
    ['Day', r => dayName(r.t / 24)], ['Batches checked', r => num(r.cb), 1],
    ['Records missing Bronze columns', r => num(r.bad), 1],
    ['New fields in messages', r => el('div', Object.entries(r.unk).map(([k, n]) => k + ' (' + num(n) + ' rows)').join(', ') || 'none', 'why')],
    ['Expected fields absent all day', r => el('div', r.abs.map(a => a + (ESSENTIAL.includes(a) ? ' (essential)' : OPTIONAL.includes(a) ? ' (aircraft only)' : '')).join(', ') || 'none', 'why')]]);
  table('tRef', P.ref || [], [['File', r => r.file], ['Updated', r => r.updated ? when(r.updated) : 'no file'],
    ['Columns', r => r.columns == null ? '–' : num(r.columns), 1], ['Columns the pipeline reads', r => num(r.required), 1],
    ['Missing', r => el('div', r.missing.join(', ') || (r.columns == null ? '–' : 'none'), 'why')],
    ['', r => r.columns == null ? '–' : el('span', r.missing.length ? 'Check' : 'OK', r.missing.length ? 'tag look' : 'tag')]]);

  const lastRun = ALL.length ? ALL[ALL.length - 1] : null, mf = D.manifest || {};
  const fresh = [
    ['Collector', 'last Bronze file written', P.heartbeat, 10],
    ['Silver', 'last batch processed', P.silver_batch, 15],
    ['Gold', 'last published', mf.finished_at, 120],
    ['Gold', 'last run' + (lastRun ? ' (' + lastRun.s.toLowerCase() + ')' : ' (logged from the next deploy)'), lastRun && lastRun.at, 120, lastRun && lastRun.s !== 'READY'],
  ].map(([layer, what, ts, lim, failed]) => ({layer, what, ts, age: ageMin(ts, built), lim, failed, none: !ts && layer === 'Gold' && !ALL.length}));
  table('tFresh', fresh, [['Layer', r => r.layer], ['What', r => r.what], ['When', r => r.ts ? when(r.ts) : 'no record'],
    ['Age', r => r.age == null ? '–' : dur(r.age), 1],
    ['', r => r.none ? '–' : el('span', r.age == null || r.age > r.lim || r.failed ? 'Check' : 'OK', r.age == null || r.age > r.lim || r.failed ? 'tag look' : 'tag')]]);
  $('nFresh').textContent = 'As of ' + when(P.built) + ', when this page was built (after each Gold run), so the ages do not grow while you watch. '
    + (P.disk_free_pct == null ? '' : 'Free disk space: ' + P.disk_free_pct + '%' + (P.disk_free_pct < 15 ? ', below the 15% the monitor alerts on.' : '.'));

  const fi = FC.t.map((_, k) => k).filter(k => inRange(FC.t[k]));
  const nOut = fi.filter(k => OUT.has(FC.t[k])).length;
  $('nFeed').textContent = 'All AIS messages per hour that reached Silver. ' + (nOut ? nOut + ' hour' + (nOut === 1 ? ' is' : 's are') + ' marked as feed outage (orange).' : 'No feed outage in these days.');
  chart('cFeed', {type: 'bar', data: {labels: fi.map(k => at(FC.t[k])), datasets: [{label: 'Messages', data: fi.map(k => FC.n[k]),
      backgroundColor: fi.map(k => OUT.has(FC.t[k]) ? css('--signal') : css('--accent')), barPercentage: 1, categoryPercentage: 1}]},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {display: false}}, scales: {x: {ticks: {maxTicksLimit: 12}, grid: {display: false}}, y: {beginAtZero: true}}}});

  const dl = ks.map(k => dayName(SV.t[k] / 24));
  const pal = [css('--warn'), css('--signal'), css('--onshore'), css('--accent'), css('--muted'), css('--ink'), css('--line')];
  const used = REASONS.map((r, j) => [r, j]).filter(([[r]]) => sum(SV.rej[r]) > 0);
  $('nRej').textContent = num(rej) + ' of ' + num(input) + ' rows rejected in these days' + (used.length ? '; rejected rows are kept with the raw message in silver/rejects.' : '.');
  chart('cRej', {type: 'bar', data: {labels: dl, datasets: used.map(([[r, l], j]) => ({label: l, data: ks.map(k => SV.rej[r][k]), backgroundColor: pal[j]}))},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {position: 'bottom'}},
              scales: {x: {stacked: true, ticks: {maxTicksLimit: 10}, grid: {display: false}}, y: {stacked: true, beginAtZero: true, ticks: {precision: 0}}}}});
  const old = ks.filter(k => SV.p95[k] > 86400);
  $('nSil').textContent = 'Average and slowest batch runtime, and the typical live batch\'s 95th-percentile message age when stored (seconds; historic backfill batches left out).'
    + (old.length ? ' Very old messages (some batch\'s p95 over a day): ' + old.map(k => dayName(SV.t[k] / 24) + ' (' + dur(SV.p95[k] / 60) + ')').join(', ') + '.' : '');
  chart('cSil', {type: 'line', data: {labels: dl, datasets: [
      {label: 'Average batch (s)', data: ks.map(k => SV.rt[k]), borderColor: css('--accent'), backgroundColor: css('--accent'), pointRadius: 2},
      {label: 'Slowest batch (s)', data: ks.map(k => SV.rtx[k]), borderColor: css('--signal'), backgroundColor: css('--signal'), pointRadius: 2},
      {label: 'Message age p95, typical batch (s)', data: ks.map(k => (SV.p95m || SV.p95)[k]), borderColor: css('--onshore'), backgroundColor: css('--onshore'), pointRadius: 2}]},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {position: 'bottom'}}, scales: {x: {ticks: {maxTicksLimit: 10}, grid: {display: false}}, y: {beginAtZero: true}}}});

  $('nRuns').textContent = RU.length ? 'Runtime of each run in seconds. Teal: published; orange: full rebuild; red: blocked by the gate or crashed.'
    : 'No Gold runs logged in these days. The run log starts with this version of the pipeline.';
  const short = s => new Date(s).toLocaleString('en-GB', {day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC'});
  chart('cRuns', {type: 'bar', data: {labels: RU.map(r => short(r.at)), datasets: [{label: 'Runtime (s)', data: RU.map(r => r.rt),
      backgroundColor: RU.map(r => r.s !== 'READY' ? css('--warn') : r.m === 'full' ? css('--signal') : css('--accent'))}]},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {display: false}}, scales: {x: {ticks: {maxTicksLimit: 12}, grid: {display: false}}, y: {beginAtZero: true}}}});
  table('tRuns', RU.slice().reverse(), [['Finished (UTC)', r => short(r.at)],
    ['Status', r => el('span', r.s === 'READY' ? 'published' : r.s === 'FAILED' ? 'blocked by gate' : 'crashed', r.s === 'READY' ? 'tag' : 'tag look')],
    ['Mode', r => r.m || '–'], ['Runtime', r => r.rt == null ? '–' : r.rt < 120 ? Math.round(r.rt) + ' s' : (r.rt / 60).toFixed(1) + ' min', 1], ['Silver rows', r => num(r.rows), 1],
    ['Repeated keys', r => r.k == null ? '–' : num(r.k), 1],
    ['Failed checks and warnings', r => el('div', [...(r.f || []).map(c => 'failed: ' + c), ...(r.w || [])].join('; ') || '–', 'why')]]);
}

// ---------- shared ----------
function status(){
  const mf = D.manifest || {}, s = $('status'), hours = (Date.now() - Date.parse(mf.finished_at)) / HOUR;
  s.textContent = 'Gold published ' + when(mf.finished_at) + '. Data from ' + dayName(0) + ' to ' + when(D.end) + '.';
  if (hours > 2) { s.textContent += ' Gold is ' + Math.round(hours) + ' hours old: check the pipeline.'; s.classList.add('stale'); }
}
function render(){
  if (typeof Chart !== 'undefined') { Chart.defaults.color = css('--muted'); Chart.defaults.font.family = css('--font'); Chart.defaults.borderColor = css('--line'); }
  ({traffic, safety, surv: surveillance, vessel: vesselPanel, health})[tab]();
}
function setTab(t){
  tab = t;
  document.querySelectorAll('.tabs button').forEach(b => b.setAttribute('aria-selected', String(b.dataset.tab === t)));
  document.querySelectorAll('[data-panel]').forEach(p => p.hidden = p.dataset.panel !== t);
  $('grpSeg').hidden = t === 'vessel' || t === 'health';
  try { history.replaceState(null, '', '#' + t + (t === 'vessel' && vessel != null ? '=' + vessel : '')); } catch (e) {}
  render();
}
document.querySelectorAll('.tabs button').forEach(b => b.addEventListener('click', () => setTab(b.dataset.tab)));
const fEl = $('from'), tEl = $('to'); fEl.max = tEl.max = NDAYS - 1;
$('first').textContent = dayName(0); $('last').textContent = dayName(NDAYS - 1);
let pending = false;
function syncRange(){
  fEl.value = from; tEl.value = to; const span = Math.max(1, NDAYS - 1);
  $('fill').style.left = (from / span * 100) + '%'; $('fill').style.width = ((to - from) / span * 100) + '%';
  fEl.style.zIndex = from >= NDAYS - 1 ? 3 : 2;
  const n = to - from + 1; $('rangeLabel').textContent = (n === 1 ? dayName(from) : dayName(from) + ' to ' + dayName(to)) + ' (' + n + (n === 1 ? ' day)' : ' days)');
  if (!pending) { pending = true; requestAnimationFrame(() => { pending = false; render(); }); }
}
fEl.addEventListener('input', () => { from = Math.min(+fEl.value, to); syncRange(); });
tEl.addEventListener('input', () => { to = Math.max(+tEl.value, from); syncRange(); });
document.querySelectorAll('.presets button').forEach(b => b.addEventListener('click', () => { const n = +b.dataset.days; to = NDAYS - 1; from = n ? Math.max(0, NDAYS - n) : 0; syncRange(); }));
document.querySelectorAll('#grpSeg button').forEach(b => b.addEventListener('click', () => { grp = b.dataset.g;
  document.querySelectorAll('#grpSeg button').forEach(x => x.setAttribute('aria-pressed', String(x === b))); render(); }));
document.querySelectorAll('#survSeg button').forEach(b => b.addEventListener('click', () => { survMode = b.dataset.s;
  document.querySelectorAll('#survSeg button').forEach(x => x.setAttribute('aria-pressed', String(x === b))); render(); }));
let lastTouch = Date.now();
['pointerdown', 'keydown', 'wheel', 'input'].forEach(e => addEventListener(e, () => { lastTouch = Date.now(); }, {passive: true}));
setInterval(() => { if (Date.now() - lastTouch > 120000) location.reload(); }, 300000);
const h = (location.hash || '').slice(1).split('=');
if (h[0] === 'vessel' && h[1] && VES[h[1]]) vessel = +h[1];
status(); syncRange();
setTab(['traffic', 'safety', 'surv', 'vessel', 'health'].includes(h[0]) ? h[0] : 'traffic');
})();
</script>
</body>
</html>
'''

if __name__ == "__main__":
    main()
