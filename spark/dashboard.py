"""Gold dashboard: one self-contained HTML page, rebuilt after every successful Gold publish.

Four tabs, all driven by one date-range slider and an All / Offshore / Onshore switch:
  Traffic        visits and hourly vessels per installation (use case 1)
  Safety zones   500 m zone entries, fast entries, minutes inside (use case 2)
  Surveillance   loitering and AIS gaps, 'worth a look' first, each with a plain-language Why (use case 3)
  Vessel         search any ship by name or MMSI: its visits, zone entries and events (use case 4);
                 the full route map comes from spark/vessel_report.py
The Gold tables are embedded compactly (one entry per visit / entry / event / hour) and the browser recomputes
every number for the chosen range. Writes <gold>/dashboard/index.html atomically; the page reloads itself every
5 minutes while nobody is using it.
View it:   python3 -m http.server 8050 --bind 127.0.0.1 --directory <data_root>/gold/dashboard
Preview from any Gold folder (read-only on the tables; writes ./dashboard-preview unless --out is given):
           PYTHONPATH=. python -m spark.dashboard --gold-dir data/prod/gold [--out folder]
"""
import argparse, json, os
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


def summarise(visits, hourly, zones=None, events=None, vessels=None):
    """Compact, column-wise data: one entry per visit, zone entry, event and hour, so the browser can recompute
    every number for any date range. Times are whole hours since t0 (midnight of the first day)."""
    end = hourly.agg(F.max("hour_ts")).collect()[0][0]
    if end is None:
        return None
    on_v, on_h = _onshore(visits) == "onshore", _onshore(hourly) == "onshore"
    names = {r["inst_name"]: (r["inst_kind"], bool(r["onshore"])) for r in
             visits.groupBy("inst_name").agg(F.first("inst_kind", ignorenulls=True).alias("inst_kind"),
                                             F.max(on_v).alias("onshore")).collect()}
    vr = visits.select("inst_name", "mmsi", "arrival_ts", "duration_min", "entered_zone").collect()
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
    for r in zr + er:
        names.setdefault(r["inst_name"], (None, False))
    order = sorted(n for n in names if n is not None)
    idx = {n: k for k, n in enumerate(order)}
    firsts = [r["arrival_ts"] for r in vr] + ([hr[0]["hour_ts"]] if hr else [])
    t0 = min(firsts).replace(hour=0, minute=0, second=0, microsecond=0)
    hrs = lambda ts: int((ts - t0).total_seconds() // 3600)
    seen = {r["mmsi"] for r in vr} | {r["mmsi"] for r in zr} | {r["mmsi"] for r in er}
    ves = {}
    if vessels is not None:
        for r in vessels.select("mmsi", _col(vessels, "name").alias("name"), _col(vessels, "ship_type").alias("st"),
                                _col(vessels, "length_m").alias("len"), _col(vessels, "is_facility").alias("fac")).collect():
            if r["mmsi"] in seen:
                ves[str(r["mmsi"])] = [r["name"], r["st"], r["len"], bool(r["fac"])]
    return {
        "t0": t0, "end": end, "buckets": [b[1] for b in BUCKETS],
        "inst": [[n, names[n][0], names[n][1], None, None, None] for n in order],
        "v": {"i": [idx[r["inst_name"]] for r in vr], "m": [r["mmsi"] for r in vr],
              "t": [hrs(r["arrival_ts"]) for r in vr], "d": [int(round(r["duration_min"] or 0)) for r in vr],
              "z": [int(r["entered_zone"] or 0) for r in vr]},
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
        "ves": ves, "flag_bits": FLAG_BITS,
    }


def build(visits, hourly, manifest, out_dir, fac=None, zones=None, events=None, vessels=None, installations=None,
          rules=None):
    data = summarise(visits, hourly, zones, events, vessels)
    if data is None:
        return None
    pos = {}
    if fac is not None:                              # positions from the Sodir reference file
        pos = {r["inst_name"]: (r["i_lat"], r["i_lon"], None) for r in fac.select("inst_name", "i_lat", "i_lon").collect()}
    if installations is not None:                    # Gold installations table: position and field
        for r in installations.select("inst_name", "lat", "lon", _col(installations, "field").alias("field")).collect():
            pos[r["inst_name"]] = (r["lat"], r["lon"], r["field"])
    for row in data["inst"]:
        row[3], row[4], row[5] = pos.get(row[0], (None, None, None))
    data["manifest"] = {k: manifest.get(k) for k in ("run_id", "finished_at", "status", "silver_rows", "tables", "mode")}
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
            "ais_gap_min_minutes": sc.get("ais_gap_min_minutes", 30), "fast_kn": sc.get("zone_fast_kn", 10)}


def build_from_gold(spark, gold, manifest, out_dir, cfg, fac=None):
    """Reads the published Gold tables (any that exist) and builds the page."""
    from spark.gold_plan import has_data
    read = lambda t: spark.read.parquet(os.path.join(gold, t)) if has_data(os.path.join(gold, t)) else None
    loiter, gaps = read("loitering_events"), read("ais_gap_events")
    events = loiter if gaps is None else (gaps if loiter is None else loiter.unionByName(gaps, allowMissingColumns=True))
    return build(read("vessel_visits"), read("installation_activity_hourly"), manifest, out_dir, fac=fac,
                 zones=read("zone_entries"), events=events, vessels=read("vessels"),
                 installations=read("installations"), rules=rules_from_config(cfg))


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
  <section><h2>Where the traffic is</h2><p class="note">Circle size shows visits; teal is offshore, violet is onshore. Click a circle or a table row for details.</p><div class="map" id="mapT"></div></section>
  <section><h2>Busiest installations</h2><p class="note">Top 15 by visits, split by whether the vessel entered the 500 m safety zone</p><div class="chart tall"><canvas id="cTop"></canvas></div></section>
  <div class="grid">
    <section><h2>Vessels near installations, per hour</h2><p class="note">Each vessel counted once per installation it is near (UTC)</p><div class="chart"><canvas id="cHourly"></canvas></div></section>
    <section><h2>How long visits last</h2><p class="note">Visits that started in the selected days</p><div class="chart"><canvas id="cDur"></canvas></div></section>
  </div>
  <section><h2>All installations with traffic</h2><div class="scroll" id="tTraffic"></div></section>
</div>

<div data-panel="safety" hidden>
  <section><h2>Entries into 500 m safety zones</h2><p class="note" id="noteZ"></p><div class="map" id="mapZ"></div></section>
  <section><h2>Installations with most zone entries</h2><p class="note" id="noteZtop"></p><div class="chart tall"><canvas id="cZone"></canvas></div></section>
  <section><h2>Fast zone entries</h2><p class="note" id="noteFast"></p><div class="scroll" id="tFast"></div></section>
</div>

<div data-panel="surv" hidden>
  <section><h2>Surveillance events near installations</h2><p class="note">Loitering (slow within 5 km for hours) and AIS gaps (silent near an installation). Live data only. Orange: worth a look; grey: ordinary.</p><div class="map" id="mapS"></div></section>
  <section><h2>Events</h2>
    <div class="bar" style="margin-top:0"><p class="note" id="noteS" style="margin:0"></p>
      <div class="seg" id="survSeg"><button type="button" data-s="look" aria-pressed="true">Worth a look</button><button type="button" data-s="all" aria-pressed="false">All events</button></div></div>
    <div class="scroll" id="tSurv"></div></section>
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
<footer>A visit is a vessel within 5 km of an installation (1.5 km for onshore facilities); a gap of more than 30 minutes starts a new visit. Rebuilt from the Gold tables after each successful hourly publish; this page reloads every 5 minutes while you are not using it.</footer>
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
const VES = D.ves || {}, B = D.flag_bits || {}, LIM = [15, 60, 180, 720, 1440];
const R = Object.assign({min_days:3, window_days:30, large_ship_min_m:100, loiter_max_kn:2, loiter_min_minutes:120, ais_gap_min_minutes:30, fast_kn:10}, D.rules || {});
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

// ---------- Traffic ----------
function traffic(){
  const a = from * 24, b = (to + 1) * 24, per = new Map(), ships = new Set(), ds = [], buckets = D.buckets.map(() => 0);
  for (let k = 0; k < V.t.length; k++) {
    if (!inRange(V.t[k])) continue; const ii = V.i[k]; if (!keep(I[ii][2])) continue;
    const d = V.d[k], m = V.m[k]; ships.add(m); ds.push(d);
    let j = LIM.findIndex(x => d < x); buckets[j < 0 ? LIM.length : j]++;
    let p = per.get(ii); if (!p) { p = {visits:0, ves:new Set(), ds:[], zone:0}; per.set(ii, p); }
    p.visits++; p.ves.add(m); p.ds.push(d); p.zone += V.z[k];
  }
  const inst = [...per].map(([ii, p]) => ({ii, name:I[ii][0], kind:I[ii][1], on:I[ii][2], lat:I[ii][3], lon:I[ii][4], field:I[ii][5],
    visits:p.visits, vessels:p.ves.size, median:median(p.ds), zone:p.zone})).sort((x, y) => y.visits - x.visits);
  kpis('kpis', [[num(ds.length), 'visits'], [num(ships.size), 'vessels'], [num(per.size), 'installations with traffic'], [dur(median(ds)), 'median visit']]);
  const M = getMap('mapT');
  if (M) { const maxV = Math.max(1, ...inst.map(r => r.visits)), pts = [];
    for (const r of inst.filter(r => r.lat != null)) {
      const c = css(r.on ? '--onshore' : '--accent');
      const mk = L.circleMarker([r.lat, r.lon], {radius: 4 + 20 * Math.sqrt(r.visits / maxV), color: c, weight: 1.5, fillColor: c, fillOpacity: 0.45})
        .bindPopup(popup([r.name, (r.kind || '').toLowerCase() + (r.field ? ' · ' + r.field : ''), num(r.visits) + ' visits by ' + num(r.vessels) + ' vessels',
                          'Median stay ' + dur(r.median), num(r.zone) + ' visits entered the 500 m zone'])).bindTooltip(r.name);
      M.layer.addLayer(mk); M.markers[r.name] = mk; pts.push([r.lat, r.lon]); }
    fit(M, pts); }
  table('tTraffic', inst, [['Installation', r => r.name], ['Type', r => (r.kind || '').toLowerCase() + (r.on ? ' (onshore)' : '')],
    ['Field', r => r.field || '–'], ['Visits', r => num(r.visits), 1], ['Vessels', r => num(r.vessels), 1], ['Median stay', r => dur(r.median), 1],
    ['Entered 500 m zone', r => num(r.zone), 1]], r => focus('mapT', r.name));
  if (typeof Chart === 'undefined') return;
  const top = inst.slice(0, 15);
  chart('cTop', {type:'bar', data:{labels: top.map(r => r.name), datasets:[
      {label:'Entered 500 m zone', data: top.map(r => r.zone), backgroundColor: css('--signal')},
      {label:'Stayed outside 500 m', data: top.map(r => r.visits - r.zone), backgroundColor: css('--accent')}]},
    options:{indexAxis:'y', maintainAspectRatio:false, animation:false, plugins:{legend:{position:'bottom'}}, scales:{x:{stacked:true}, y:{stacked:true, grid:{display:false}}}}});
  const hx = [], hy = [];
  for (let k = 0; k < H.t.length; k++) { if (!inRange(H.t[k])) continue; hx.push(H.t[k]);
    hy.push(grp === 'offshore' ? H.off[k] : grp === 'onshore' ? H.on[k] : H.off[k] + H.on[k]); }
  const long = to - from >= 3;
  chart('cHourly', {type:'line', data:{labels: hx.map(t => new Date(T0 + t * HOUR).toLocaleString('en-GB', long ? {day:'numeric', month:'short', timeZone:'UTC'} : {weekday:'short', hour:'2-digit', timeZone:'UTC'})),
      datasets:[{data: hy, borderColor: css('--accent'), pointRadius:0, borderWidth: long ? 1 : 2, tension:0.2}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}}, scales:{x:{ticks:{maxTicksLimit:8}, grid:{display:false}}, y:{beginAtZero:true}}}});
  chart('cDur', {type:'bar', data:{labels: D.buckets, datasets:[{data: buckets, backgroundColor: css('--accent')}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}}, scales:{x:{grid:{display:false}}, y:{beginAtZero:true}}}});
}
function focus(mapId, name){ const M = maps[mapId]; const m = M && M.markers[name]; if (!m) return;
  $(mapId).scrollIntoView({behavior:'smooth', block:'center'}); M.map.setView(m.getLatLng(), 9); m.openPopup(); }

// ---------- Safety zones ----------
function zoneRows(filterMmsi){
  const rows = [];
  for (let k = 0; k < Z.t.length; k++) {
    if (!inRange(Z.t[k])) continue; const ii = Z.i[k];
    if (filterMmsi != null ? Z.m[k] !== filterMmsi : !keep(I[ii][2])) continue;
    rows.push({ii, inst:I[ii][0], on:I[ii][2], m:Z.m[k], t:Z.t[k], d:Z.d[k], kn:Z.kn[k], x:Z.x[k]});
  }
  return rows;
}
function safety(){
  const rows = zoneRows(null), per = new Map(), fast = rows.filter(r => r.kn != null && r.kn > R.fast_kn);
  for (const r of rows) { let p = per.get(r.ii); if (!p) { p = {n:0, fast:0, mins:0, ves:new Set()}; per.set(r.ii, p); }
    p.n++; p.mins += r.d; p.ves.add(r.m); if (r.kn != null && r.kn > R.fast_kn) p.fast++; }
  kpis('kpis', [[num(rows.length), 'zone entries'], [num(new Set(rows.map(r => r.m)).size), 'vessels'],
    [dur(median(rows.map(r => r.d))), 'median time inside'], [num(fast.length), 'entries faster than ' + R.fast_kn + ' kn']]);
  $('noteZ').textContent = 'Circle size shows entries into each 500 m zone; a ring marks installations with entries faster than ' + R.fast_kn + ' kn.';
  $('noteZtop').textContent = 'Top 15 by entries, split by whether the top speed inside the zone exceeded ' + R.fast_kn + ' kn';
  $('noteFast').textContent = 'Entries where a speed above ' + R.fast_kn + ' kn was recorded inside 500 m, fastest first. A single fast reading can be GPS noise; repeated ones are worth checking.';
  const list = [...per].map(([ii, p]) => ({ii, name:I[ii][0], lat:I[ii][3], lon:I[ii][4], on:I[ii][2], ...p})).sort((a, b) => b.n - a.n);
  const M = getMap('mapZ');
  if (M) { const mx = Math.max(1, ...list.map(r => r.n)), pts = [];
    for (const r of list.filter(r => r.lat != null)) {
      const c = css(r.on ? '--onshore' : '--accent');
      const mk = L.circleMarker([r.lat, r.lon], {radius: 4 + 18 * Math.sqrt(r.n / mx), color: r.fast ? css('--warn') : c, weight: r.fast ? 2.5 : 1.5, fillColor: c, fillOpacity: 0.45})
        .bindPopup(popup([r.name, num(r.n) + ' entries by ' + num(r.ves.size) + ' vessels', dur(r.mins) + ' inside in total', num(r.fast) + ' entries faster than ' + R.fast_kn + ' kn']))
        .bindTooltip(r.name);
      M.layer.addLayer(mk); M.markers[r.name] = mk; pts.push([r.lat, r.lon]); }
    fit(M, pts); }
  table('tFast', fast.sort((a, b) => b.kn - a.kn), [['Vessel', r => vlink(r.m)], ['Installation', r => r.inst], ['Entry (UTC)', r => at(r.t)],
    ['Top speed', r => r.kn.toFixed(1) + ' kn', 1], ['Inside', r => dur(r.d), 1], ['Closest', r => num(r.x) + ' m', 1]], r => focus('mapZ', r.inst));
  if (typeof Chart === 'undefined') return;
  const top = list.slice(0, 15);
  chart('cZone', {type:'bar', data:{labels: top.map(r => r.name), datasets:[
      {label:'Faster than ' + R.fast_kn + ' kn inside', data: top.map(r => r.fast), backgroundColor: css('--warn')},
      {label:'Other entries', data: top.map(r => r.n - r.fast), backgroundColor: css('--accent')}]},
    options:{indexAxis:'y', maintainAspectRatio:false, animation:false, plugins:{legend:{position:'bottom'}}, scales:{x:{stacked:true}, y:{stacked:true, grid:{display:false}}}}});
}

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
function surveillance(){
  const all = eventRows(null), look = all.filter(r => has(r.f, 'worth_a_look'));
  kpis('kpis', [[num(all.length), 'events'], [num(look.length), 'worth a look'], [num(look.filter(r => !r.gap).length), 'loitering worth a look'],
                [num(look.filter(r => r.gap).length), 'AIS gaps worth a look'], [num(new Set(look.map(r => r.m)).size), 'vessels worth a look']]);
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
    fit(M, pts); }
  table('tSurv', rows, evCols(true), r => focus('mapS', r.inst));
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

// ---------- shared ----------
function status(){
  const mf = D.manifest || {}, s = $('status'), hours = (Date.now() - Date.parse(mf.finished_at)) / HOUR;
  s.textContent = 'Gold published ' + when(mf.finished_at) + '. Data from ' + dayName(0) + ' to ' + when(D.end) + '.';
  if (hours > 2) { s.textContent += ' Gold is ' + Math.round(hours) + ' hours old: check the pipeline.'; s.classList.add('stale'); }
}
function render(){
  if (typeof Chart !== 'undefined') { Chart.defaults.color = css('--muted'); Chart.defaults.font.family = css('--font'); Chart.defaults.borderColor = css('--line'); }
  ({traffic, safety, surv: surveillance, vessel: vesselPanel})[tab]();
}
function setTab(t){
  tab = t;
  document.querySelectorAll('.tabs button').forEach(b => b.setAttribute('aria-selected', String(b.dataset.tab === t)));
  document.querySelectorAll('[data-panel]').forEach(p => p.hidden = p.dataset.panel !== t);
  $('grpSeg').hidden = t === 'vessel';
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
setTab(['traffic', 'safety', 'surv', 'vessel'].includes(h[0]) ? h[0] : 'traffic');
})();
</script>
</body>
</html>
'''

if __name__ == "__main__":
    main()
