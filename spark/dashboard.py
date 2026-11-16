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
import argparse, json, os, shutil
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

BUCKETS = [(15, "Under 15 min"), (60, "15 to 60 min"), (180, "1 to 3 h"), (720, "3 to 12 h"),
           (1440, "12 to 24 h"), (None, "Over 24 h")]
# bit flags of a surveillance event, decoded by the page
RISK = {"high": 0, "medium": 1, "low": 2}       # Gold risk_level -> sort order on the page
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


def pipeline_lines(path, phases=("IN SERVICE",)):
    """Sodir pipeline segments (bronze/reference/pipelines.csv) -> map polylines (drawing only) and the segment list.
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


SAR_COLUMNS = ["date", "lat", "lon", "detections", "dataset", "fetched_at"]          # read by Gold (dark_detections)
FISHING_COLUMNS = ["vessel_id", "call_sign", "name", "length_m"]                 # read by Gold (match_fishing_register)


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


def bronze_files(events_path, t0, now, flush_seconds=120):
    """Completeness of live Bronze per day, from the collector's own log (metrics/<env>/events.jsonl): files written
    against files expected (one every flush_seconds, counted from the first logged event and up to `now` on the
    last day), messages, empty windows, reader errors and restarts."""
    days, first = {}, None
    for e in _jsonl(events_path):
        ts = _utc_naive(e.get("ts"))
        if ts is None:
            continue
        first = ts if first is None or ts < first else first
        if ts < t0:
            continue
        d = days.setdefault(ts.date(), {"files": 0, "msgs": 0, "empty": 0, "errors": 0, "starts": 0})
        kind = e.get("event")
        if kind == "flush":
            if e.get("file"):
                d["files"] += 1
                d["msgs"] += int(e.get("messages") or 0)
            else:
                d["empty"] += 1
        elif kind == "reader_error":
            d["errors"] += 1
        elif kind == "start":
            d["starts"] += 1
    out = {k: [] for k in ("t", "exp", "files", "msgs", "empty", "errors", "starts")}
    for day in sorted(days):
        start = datetime.combine(day, datetime.min.time())
        begin, end = max(start, first), min(start + timedelta(days=1), now)
        out["t"].append((day - t0.date()).days * 24)
        out["exp"].append(max(0, int((end - begin).total_seconds() // flush_seconds)))
        for k in ("files", "msgs", "empty", "errors", "starts"):
            out[k].append(days[day][k])
    return out


def pipeline_health(t0, quality_dir=None, metrics_dir=None, gold_dir=None, now=None, reference=None, flush_seconds=120):
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
                                        "rej": dict.fromkeys(REJECT_REASONS, 0), "cb": 0, "bad": 0, "unk": {}, "abs": None, "lat": [],
                                        "rj": 0, "fix": 0, "gl": 0, "air": 0})
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
        d["rj"] += m.get("rejected_rows") or 0
        d["fix"] += sum(m.get(k) or 0 for k in ("corrected_sog", "corrected_heading", "corrected_rot", "corrected_shiptype"))
        d["gl"] += m.get("corrected_sog_glitch") or 0
        d["air"] += m.get("aircraft_rows") or 0
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
    for f in ("b", "in", "ok", "dup", "p95", "rtx", "rj", "fix", "gl", "air"):
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
            "sv": sv, "runs": runs, "ref": reference_columns(reference or []),
            "bf": bronze_files(metrics_dir and os.path.join(metrics_dir, "events.jsonl"), t0, now, flush_seconds)}


def summarise(visits, hourly, zones=None, events=None, vessels=None, landings=None, gaps=None, outage_hours=None,
              extra_mmsi=None):
    """Compact, column-wise data: one entry per visit, zone entry, event and hour, so the browser can recompute
    every number for any date range. Times are whole hours since t0 (midnight of the first day)."""
    end = hourly.agg(F.max("hour_ts")).collect()[0][0]
    if end is None:
        return None
    on_v, on_h = _onshore(visits) == "onshore", _onshore(hourly) == "onshore"
    names = {r["inst_name"]: (r["inst_kind"], bool(r["onshore"])) for r in
             visits.groupBy("inst_name").agg(F.first("inst_kind", ignorenulls=True).alias("inst_kind"),
                                             F.max(on_v).alias("onshore")).collect()}
    vr = visits.select("inst_name", "mmsi", "arrival_ts", "duration_min", "entered_zone", _col(visits, "wait_min").alias("wait_min"),
                       _col(visits, "alongside_min").alias("alongside_min")).collect()     # both computed in Gold
    hr = (hourly.groupBy("hour_ts")
                .agg(F.sum(F.when(on_h, 0).otherwise(F.col("distinct_vessels"))).alias("off"),
                     F.sum(F.when(on_h, F.col("distinct_vessels")).otherwise(0)).alias("on"))
                .orderBy("hour_ts").collect())
    zr = (zones.select("inst_name", "mmsi", "entry_ts", "minutes_inside", _col(zones, "max_sog_kn").alias("kn"), "min_dist_m",
                       _col(zones, "is_fast").alias("fast"), _col(zones, "risk_level").alias("rl"),
                       _col(zones, "risk_reason").alias("rw"), _col(zones, "is_unexpected").alias("u"),
                       _col(zones, "earlier_entries").alias("b")).collect() if zones is not None else [])   # Gold safety_zone_entries
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
    if vessels is not None:      # every classification below is a Gold column (spark/gold_logic.classify_vessels)
        for r in vessels.select("mmsi", _col(vessels, "name").alias("name"), _col(vessels, "ship_type").alias("st"),
                                _col(vessels, "length_m").alias("len"), _col(vessels, "is_facility").alias("fac"),
                                _col(vessels, "fishing_source").alias("fs"), _col(vessels, "vessel_group").alias("grp"),
                                _col(vessels, "unexpected_reason").alias("why")).collect():
            if r["mmsi"] in seen:
                ves[str(r["mmsi"])] = [r["name"], r["st"], r["len"], bool(r["fac"]), r["fs"], r["grp"], r["why"]]
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
               "x": [None if r["min_dist_m"] is None else int(r["min_dist_m"]) for r in zr],
               "f": [int(bool(r["fast"])) for r in zr], "r": [RISK.get(r["rl"]) for r in zr], "rw": [r["rw"] for r in zr],
               "u": [int(bool(r["u"])) for r in zr], "b": [r["b"] for r in zr]},
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
          rules=None, landings=None, coverage=None, pipeline_loiter=None, pipelines_csv=None, standby=None,
          outages=None, dark=None, quality_dir=None, metrics_dir=None, gold_dir=None, reference=None, flush_seconds=120):
    """Puts published Gold tables into the page. No rule is applied here: every flag, class, risk, gap and
    match is a Gold column or table (spark/gold_logic.py); this only selects, encodes and filters to the window."""
    gaps = standby                                                    # Gold standby_gaps
    outage = [r["hour_ts"] for r in outages.collect()] if outages is not None else None   # Gold feed_outage_hours
    pl = (pipeline_loiter.select("mmsi", "start_ts", "duration_min", "pipeline_name", "medium", "min_dist_m", "avg_sog_kn",
                                 "lat", "lon").collect() if pipeline_loiter is not None else [])
    data = summarise(visits, hourly, zones, events, vessels, landings, gaps, outage, extra_mmsi={r["mmsi"] for r in pl})
    if data is None:
        return None
    data["fishing_register"] = bool((manifest.get("fishing_register") or {}).get("loaded"))
    t0 = data["t0"]
    hrs = lambda ts: int((ts - t0).total_seconds() // 3600)
    data["pl"] = {"m": [r["mmsi"] for r in pl], "t": [hrs(r["start_ts"]) for r in pl], "d": [int(round(r["duration_min"])) for r in pl],
                  "p": [r["pipeline_name"] for r in pl], "md": [r["medium"] for r in pl], "x": [int(round(r["min_dist_m"])) for r in pl],
                  "kn": [round(r["avg_sog_kn"] or 0, 1) for r in pl], "lat": [round(r["lat"], 4) for r in pl], "lon": [round(r["lon"], 4) for r in pl]}
    lines, _ = pipeline_lines(pipelines_csv)
    data["pp"] = lines
    radar = manifest.get("radar")
    if dark is not None and radar:                                   # Gold dark_detections, near infrastructure only
        first = t0.date().isoformat()
        near = [r for r in dark.collect() if r["date"] >= first]
        day = lambda s_: (datetime.fromisoformat(s_) - t0).days
        data["dk"] = {"t": [day(d["date"]) * 24 for d in near], "lat": [d["lat"] for d in near], "lon": [d["lon"] for d in near],
                      "n": [d["detections"] for d in near], "i": [d["inst_name"] for d in near], "im": [d["inst_m"] for d in near],
                      "p": [d["pipeline"] for d in near], "pm": [d["pipe_m"] for d in near]}
        data["dk_meta"] = radar
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
    data["ph"] = pipeline_health(t0, quality_dir, metrics_dir, gold_dir, reference=reference, flush_seconds=flush_seconds)
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
    zones = read("safety_zone_entries")                 # zone entries with Gold's safety assessment
    zones = read("zone_entries") if zones is None else zones
    events = loiter if gaps is None else (gaps if loiter is None else loiter.unionByName(gaps, allowMissingColumns=True))
    return build(read("vessel_visits"), read("installation_activity_hourly"), manifest, out_dir, fac=fac,
                 zones=zones, events=events, vessels=read("vessels"),
                 installations=read("installations"), rules=rules_from_config(cfg), landings=read("helicopter_landings"),
                 coverage=read("feed_coverage_hourly"), standby=read("standby_gaps"), outages=read("feed_outage_hours"),
                 dark=read("dark_detections"),
                 pipeline_loiter=read("pipeline_loitering"),
                 pipelines_csv=os.path.join(gold, "..", "bronze", "reference", "pipelines.csv"),
                 quality_dir=os.path.join(gold, "..", "quality"), metrics_dir=cfg.get("metrics_dir"), gold_dir=gold,
                 reference=reference_files(gold, cfg), flush_seconds=int(cfg.get("flush_seconds", 120)))


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


PAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_page")


def _page_template():
    """The page lives in spark/dashboard_page/ (page.html, page.css, page.js) so each part can be read and edited with
    its own syntax highlighting; here they are joined into one self-contained file. __DATA__ is filled in by build()."""
    read = lambda name: open(os.path.join(PAGE_DIR, name), encoding="utf-8").read()
    return read("page.html").replace("/*__CSS__*/", read("page.css")).replace("//__JS__", read("page.js"))


PAGE = _page_template()


if __name__ == "__main__":
    main()
