"""Gold dashboard: one self-contained HTML page with a map, rebuilt after every successful Gold publish.

Embeds the Gold tables compactly (one entry per visit and per hour) plus each installation's position from the
Sodir reference file; the reader picks any date range and the page recomputes everything in the browser.
Writes <gold>/dashboard/index.html atomically.
The page reloads itself every 5 minutes while nobody is using it.
View it:   python3 -m http.server 8050 --bind 127.0.0.1 --directory <data_root>/gold/dashboard
Preview from any Gold folder (read-only on the tables; writes ./dashboard-preview unless --out is given):
           PYTHONPATH=. python -m spark.dashboard --gold-dir data/prod/gold [--out folder]
"""
import argparse, json, os
from pyspark.sql import functions as F

BUCKETS = [(15, "Under 15 min"), (60, "15 to 60 min"), (180, "1 to 3 h"), (720, "3 to 12 h"),
           (1440, "12 to 24 h"), (None, "Over 24 h")]


def _onshore(df):
    c = F.col("is_onshore") if "is_onshore" in df.columns else F.lit(None)
    return F.when(F.coalesce(c.cast("boolean"), F.lit(False)), "onshore").otherwise("offshore")


def _rows(df):
    return [r.asDict() for r in df.collect()]


def _iso(o):
    if hasattr(o, "isoformat"):
        s = o.isoformat()
        return s + "Z" if "T" in s and getattr(o, "tzinfo", 1) is None else s
    return float(o) if hasattr(o, "__float__") else str(o)


def summarise(visits, hourly):
    """Compact, column-wise data for the page: one entry per visit and per hour, so the browser can
    recompute every number for any date range the reader picks."""
    end = hourly.agg(F.max("hour_ts")).collect()[0][0]
    if end is None:
        return None
    on_v, on_h = _onshore(visits) == "onshore", _onshore(hourly) == "onshore"
    inst = visits.groupBy("inst_name").agg(F.first("inst_kind", ignorenulls=True).alias("inst_kind"),
                                           F.max(on_v).alias("onshore")).orderBy("inst_name").collect()
    idx = {r["inst_name"]: k for k, r in enumerate(inst)}
    vr = visits.select("inst_name", "mmsi", "arrival_ts", "duration_min", "entered_zone").collect()
    hr = (hourly.groupBy("hour_ts")
                .agg(F.sum(F.when(on_h, 0).otherwise(F.col("distinct_vessels"))).alias("off"),
                     F.sum(F.when(on_h, F.col("distinct_vessels")).otherwise(0)).alias("on"))
                .orderBy("hour_ts").collect())
    firsts = [r["arrival_ts"] for r in vr] + ([hr[0]["hour_ts"]] if hr else [])
    t0 = min(firsts).replace(hour=0, minute=0, second=0, microsecond=0)
    hrs = lambda ts: int((ts - t0).total_seconds() // 3600)
    return {
        "t0": t0, "end": end, "buckets": [b[1] for b in BUCKETS],
        "inst": [[r["inst_name"], r["inst_kind"], bool(r["onshore"]), None, None] for r in inst],
        "v": {"i": [idx[r["inst_name"]] for r in vr], "m": [r["mmsi"] for r in vr],
              "t": [hrs(r["arrival_ts"]) for r in vr], "d": [int(round(r["duration_min"] or 0)) for r in vr],
              "z": [int(r["entered_zone"] or 0) for r in vr]},
        "h": {"t": [hrs(r["hour_ts"]) for r in hr], "off": [int(r["off"] or 0) for r in hr],
              "on": [int(r["on"] or 0) for r in hr]},
    }


def build(visits, hourly, manifest, out_dir, fac=None):
    data = summarise(visits, hourly)
    if data is None:
        return None
    if fac is not None:                              # positions for the map, from the Sodir reference file
        pos = {r["inst_name"]: (r["i_lat"], r["i_lon"]) for r in fac.select("inst_name", "i_lat", "i_lon").collect()}
        for row in data["inst"]:
            row[3], row[4] = pos.get(row[0], (None, None))
    data["manifest"] = {k: manifest.get(k) for k in ("run_id", "finished_at", "status", "silver_rows", "tables")}
    page = PAGE.replace("__DATA__", json.dumps(data, default=_iso, separators=(",", ":")).replace("</", "<\\/"))
    os.makedirs(out_dir, exist_ok=True)
    tmp, final = os.path.join(out_dir, ".index.html.tmp"), os.path.join(out_dir, "index.html")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(page)
    os.replace(tmp, final)
    return final


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
    print(build(spark.read.parquet(os.path.join(gold, "vessel_visits")),
                spark.read.parquet(os.path.join(gold, "installation_activity_hourly")),
                json.load(open(os.path.join(gold, "_READY.json"))), out, fac))


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
.status{color:var(--muted);margin:0}
.status.stale{color:var(--warn);font-weight:500}
.bar{display:flex;flex-wrap:wrap;gap:16px 32px;align-items:end;justify-content:space-between;margin:24px 0 8px}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:8px;overflow:hidden;background:var(--surface)}
.seg button{font:inherit;color:var(--ink);background:none;border:0;padding:6px 14px;cursor:pointer}
.seg button[aria-pressed="true"]{background:var(--accent);color:var(--surface)}
.seg button:focus-visible{outline:2px solid var(--signal);outline-offset:-2px}
.range{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:14px 18px 10px;margin-top:16px}
.range-head{display:flex;flex-wrap:wrap;gap:8px 16px;align-items:baseline;justify-content:space-between}
.range-head strong{font-weight:600}
.presets button{font:inherit;font-size:.875rem;color:var(--accent);background:none;border:1px solid var(--line);border-radius:6px;padding:2px 10px;margin-left:6px;cursor:pointer}
.presets button:focus-visible{outline:2px solid var(--signal)}
.dual{position:relative;height:30px;margin-top:6px}
.dual .track,.dual .fill{position:absolute;top:13px;height:4px;border-radius:2px}
.dual .track{left:0;right:0;background:var(--line)}
.dual .fill{background:var(--accent)}
.dual input{position:absolute;left:0;top:4px;width:100%;margin:0;height:22px;background:none;pointer-events:none;-webkit-appearance:none;appearance:none}
.dual input::-webkit-slider-runnable-track{background:none;height:22px}
.dual input::-moz-range-track{background:none}
.dual input::-webkit-slider-thumb{-webkit-appearance:none;pointer-events:auto;width:18px;height:18px;margin-top:2px;border-radius:50%;background:var(--accent);border:2px solid var(--surface);box-shadow:0 0 0 1px var(--accent);cursor:grab}
.dual input::-moz-range-thumb{pointer-events:auto;width:14px;height:14px;border-radius:50%;background:var(--accent);border:2px solid var(--surface);box-shadow:0 0 0 1px var(--accent);cursor:grab}
.dual input:focus-visible::-webkit-slider-thumb{outline:2px solid var(--signal);outline-offset:2px}
.dual input:focus-visible::-moz-range-thumb{outline:2px solid var(--signal);outline-offset:2px}
.ends{display:flex;justify-content:space-between;color:var(--muted);font-size:.8125rem}
.kpis{display:flex;flex-wrap:wrap;gap:8px 36px;margin:0;padding:0;list-style:none}
.kpis b{display:block;font-size:1.6rem;font-weight:600;line-height:1.1;font-variant-numeric:tabular-nums}
.kpis span{color:var(--muted);font-size:.875rem}
section{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:18px 18px 12px;margin-top:16px}
h2{font-size:1.05rem;font-weight:600;margin:0 0 2px}
.note{color:var(--muted);font-size:.875rem;margin:0 0 12px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}
.grid section{margin-top:0}
@media (max-width:820px){.grid{grid-template-columns:1fr}}
.chart{position:relative;height:300px}
.chart.tall{height:440px}
#map{height:480px;border-radius:8px}
.leaflet-container{font:inherit}
.scroll{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:.9rem}
th,td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
th:nth-child(-n+2),td:nth-child(-n+2){text-align:left}
th{color:var(--muted);font-weight:500}
td{font-variant-numeric:tabular-nums}
tbody tr{cursor:pointer}
footer{color:var(--muted);font-size:.8125rem;margin-top:20px;max-width:80ch}
</style>
</head>
<body>
<main>
<h1>Vessel activity at Norwegian oil and gas installations</h1>
<p class="status" id="status"></p>
<div class="bar">
  <ul class="kpis" id="kpis"></ul>
  <div class="seg" role="group" aria-label="Installations shown">
    <button type="button" data-g="all" aria-pressed="true">All</button>
    <button type="button" data-g="offshore" aria-pressed="false">Offshore</button>
    <button type="button" data-g="onshore" aria-pressed="false">Onshore</button>
  </div>
</div>
<div class="range">
  <div class="range-head">
    <span>Showing <strong id="rangeLabel"></strong></span>
    <span class="presets" aria-label="Quick ranges"><button type="button" data-days="1">Last day</button><button type="button" data-days="7">7 days</button><button type="button" data-days="30">30 days</button><button type="button" data-days="0">All</button></span>
  </div>
  <div class="dual"><div class="track"></div><div class="fill" id="fill"></div>
    <input type="range" id="from" min="0" step="1" aria-label="First day">
    <input type="range" id="to" min="0" step="1" aria-label="Last day">
  </div>
  <div class="ends"><span id="first"></span><span id="last"></span></div>
</div>
<section>
  <h2>Where the traffic is</h2>
  <p class="note" id="mapnote"></p>
  <div id="map"></div>
</section>
<section>
  <h2>Busiest installations</h2>
  <p class="note" id="topnote"></p>
  <div class="chart tall"><canvas id="top"></canvas></div>
</section>
<div class="grid">
  <section>
    <h2>Vessels near installations, per hour</h2>
    <p class="note">Each vessel counted once per installation it is near (UTC)</p>
    <div class="chart"><canvas id="hourly"></canvas></div>
  </section>
  <section>
    <h2>How long visits last</h2>
    <p class="note">Visits that started in the selected days</p>
    <div class="chart"><canvas id="durations"></canvas></div>
  </section>
</div>
<section>
  <h2>All installations with traffic</h2>
  <div class="scroll"><table><thead><tr><th>Installation</th><th>Type</th><th>Visits</th><th>Vessels</th><th>Median stay</th><th>Entered 500 m zone</th></tr></thead><tbody id="rows"></tbody></table></div>
</section>
<footer>A visit is a vessel within 5 km of an installation (1.5 km for onshore facilities); a gap of more than 30 minutes starts a new visit. Rebuilt from the Gold tables after each successful hourly publish; this page reloads every 5 minutes while you are not using it.</footer>
</main>
<script type="application/json" id="data">__DATA__</script>
<script>
(function(){
const D = JSON.parse(document.getElementById('data').textContent);
const $ = id => document.getElementById(id);
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const num = n => n == null ? '–' : Math.round(n).toLocaleString('en-GB');
const stay = m => m == null ? '–' : m < 60 ? Math.round(m) + ' min' : (m / 60).toFixed(1) + ' h';
const when = s => new Date(s).toLocaleString('en-GB', {weekday:'short', day:'numeric', month:'short', hour:'2-digit', minute:'2-digit', timeZone:'UTC'}) + ' UTC';
const HOUR = 36e5, DAY = 24 * HOUR, T0 = Date.parse(D.t0), END = Date.parse(D.end);
const NDAYS = Math.max(1, Math.floor((END - T0) / DAY) + 1);
const dayName = k => new Date(T0 + k * DAY).toLocaleDateString('en-GB', {day:'numeric', month:'short', year:'numeric', timeZone:'UTC'});
const I = D.inst, V = D.v, H = D.h, LIM = [15, 60, 180, 720, 1440];
let grp = 'all', from = Math.max(0, NDAYS - 7), to = NDAYS - 1;
const charts = {};
function chart(id, cfg){ if (charts[id]) charts[id].destroy(); charts[id] = new Chart($(id), cfg); }
const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const bucketOf = d => { for (let j = 0; j < LIM.length; j++) if (d < LIM[j]) return j; return LIM.length; };
const keep = on => grp === 'all' || (grp === 'onshore') === on;

function compute(){
  const a = from * 24, b = (to + 1) * 24, per = new Map(), ships = new Set(), ds = [], buckets = D.buckets.map(() => 0);
  for (let k = 0; k < V.t.length; k++) {
    const t = V.t[k]; if (t < a || t >= b) continue;
    const ii = V.i[k]; if (!keep(I[ii][2])) continue;
    const d = V.d[k], m = V.m[k];
    ships.add(m); ds.push(d); buckets[bucketOf(d)]++;
    let p = per.get(ii); if (!p) { p = {visits:0, ves:new Set(), ds:[], zone:0}; per.set(ii, p); }
    p.visits++; p.ves.add(m); p.ds.push(d); p.zone += V.z[k];
  }
  const inst = [...per.entries()].map(([ii, p]) => ({name:I[ii][0], kind:I[ii][1], on:I[ii][2], lat:I[ii][3], lon:I[ii][4],
    visits:p.visits, vessels:p.ves.size, median:median(p.ds), zone:p.zone})).sort((x, y) => y.visits - x.visits);
  const hx = [], hy = [];
  for (let k = 0; k < H.t.length; k++) {
    const t = H.t[k]; if (t < a || t >= b) continue;
    hx.push(t); hy.push(grp === 'offshore' ? H.off[k] : grp === 'onshore' ? H.on[k] : H.off[k] + H.on[k]);
  }
  return {kpi:{visits:ds.length, vessels:ships.size, installations:per.size, median:median(ds)}, inst, buckets, hx, hy};
}

function status(){
  const m = D.manifest || {}, el = $('status');
  const hours = (Date.now() - Date.parse(m.finished_at)) / HOUR;
  el.textContent = 'Gold published ' + when(m.finished_at) + '. Data from ' + dayName(0) + ' to ' + when(D.end) + '.';
  if (hours > 2) { el.textContent += ' Gold is ' + Math.round(hours) + ' hours old: check the pipeline.'; el.classList.add('stale'); }
}
function kpis(k){
  const ul = $('kpis'); ul.replaceChildren();
  for (const [v, label] of [[num(k.visits), 'visits'], [num(k.vessels), 'vessels'], [num(k.installations), 'installations with traffic'], [stay(k.median), 'median visit']]) {
    const li = document.createElement('li'), b = document.createElement('b'), s = document.createElement('span');
    b.textContent = v; s.textContent = label; li.append(b, s); ul.append(li);
  }
}
let map = null, layer = null, markers = {};
function drawMap(inst){
  const note = $('mapnote');
  if (typeof L === 'undefined') { note.textContent = 'The map needs the Leaflet library from cdnjs.cloudflare.com, which did not load.'; return; }
  if (!map) {
    map = L.map('map', {scrollWheelZoom: false}).setView([62.5, 4], 5);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 13, attribution: '&copy; OpenStreetMap contributors'}).addTo(map);
    layer = L.layerGroup().addTo(map);
  }
  layer.clearLayers(); markers = {};
  const pts = inst.filter(r => r.lat != null && r.lon != null);
  const maxV = Math.max(1, ...pts.map(r => r.visits));
  for (const r of pts) {
    const colour = css(r.on ? '--onshore' : '--accent');
    const m = L.circleMarker([r.lat, r.lon], {radius: 4 + 20 * Math.sqrt(r.visits / maxV), color: colour, weight: 1.5, fillColor: colour, fillOpacity: 0.45});
    const div = document.createElement('div'), b = document.createElement('b');
    b.textContent = r.name; div.append(b);
    for (const t of [(r.kind || '').toLowerCase(), num(r.visits) + ' visits by ' + num(r.vessels) + ' vessels',
                     'Median stay ' + stay(r.median), num(r.zone) + ' visits entered the 500 m zone']) {
      const line = document.createElement('div'); line.textContent = t; div.append(line);
    }
    m.bindPopup(div); m.bindTooltip(r.name);
    layer.addLayer(m); markers[r.name] = m;
  }
  if (pts.length && !map._fitted) { map.fitBounds(pts.map(r => [r.lat, r.lon]), {padding: [24, 24]}); map._fitted = true; }
  const missing = inst.length - pts.length;
  note.textContent = 'Circle size shows visits; teal is offshore, violet is onshore. Click a circle or a table row for details.'
    + (missing ? ' ' + missing + ' installations have no position and appear only in the table.' : '');
}
function focusInst(name){
  const m = markers[name]; if (!m) return;
  $('map').scrollIntoView({behavior: 'smooth', block: 'center'});
  map.setView(m.getLatLng(), 9); m.openPopup();
}
function table(inst){
  const tb = $('rows'); tb.replaceChildren();
  for (const r of inst) {
    const tr = document.createElement('tr');
    for (const v of [r.name, (r.kind || '').toLowerCase() + (r.on ? ' (onshore)' : ''), num(r.visits), num(r.vessels), stay(r.median), num(r.zone)]) {
      const td = document.createElement('td'); td.textContent = v; tr.append(td);
    }
    tr.addEventListener('click', () => focusInst(r.name));
    tb.append(tr);
  }
  if (!inst.length) { const tr = document.createElement('tr'), td = document.createElement('td'); td.colSpan = 6; td.textContent = 'No visits in these days for this selection. Widen the date range or pick All.'; tr.append(td); tb.append(tr); }
}
function render(){
  const R = compute();
  kpis(R.kpi); table(R.inst); drawMap(R.inst);
  const note = $('topnote');
  if (typeof Chart === 'undefined') { note.textContent = 'Charts need the Chart.js library from cdnjs.cloudflare.com, which did not load. The table below still works.'; return; }
  Chart.defaults.color = css('--muted'); Chart.defaults.font.family = css('--font'); Chart.defaults.borderColor = css('--line');
  const top = R.inst.slice(0, 15);
  note.textContent = 'Top ' + top.length + ' by visits, split by whether the vessel entered the 500 m safety zone';
  chart('top', {type:'bar', data:{labels: top.map(r => r.name), datasets:[
      {label:'Entered 500 m zone', data: top.map(r => r.zone), backgroundColor: css('--signal')},
      {label:'Stayed outside 500 m', data: top.map(r => r.visits - r.zone), backgroundColor: css('--accent')}]},
    options:{indexAxis:'y', maintainAspectRatio:false, animation:false, plugins:{legend:{position:'bottom'}},
      scales:{x:{stacked:true, title:{display:true, text:'Visits'}}, y:{stacked:true, grid:{display:false}}}}});
  const long = to - from >= 3;
  const lab = t => new Date(T0 + t * HOUR).toLocaleString('en-GB', long ? {day:'numeric', month:'short', timeZone:'UTC'} : {weekday:'short', hour:'2-digit', timeZone:'UTC'});
  chart('hourly', {type:'line', data:{labels: R.hx.map(lab),
      datasets:[{label:'Vessels', data: R.hy, borderColor: css('--accent'), backgroundColor: css('--accent'), pointRadius:0, borderWidth: long ? 1 : 2, tension:0.2}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}}, scales:{x:{ticks:{maxTicksLimit:8, autoSkip:true}, grid:{display:false}}, y:{beginAtZero:true}}}});
  chart('durations', {type:'bar', data:{labels: D.buckets, datasets:[{label:'Visits', data: R.buckets, backgroundColor: css('--accent')}]},
    options:{maintainAspectRatio:false, animation:false, plugins:{legend:{display:false}}, scales:{x:{grid:{display:false}}, y:{beginAtZero:true, title:{display:true, text:'Visits'}}}}});
}

const fEl = $('from'), tEl = $('to');
fEl.max = tEl.max = NDAYS - 1;
$('first').textContent = dayName(0); $('last').textContent = dayName(NDAYS - 1);
let pending = false;
function syncRange(){
  fEl.value = from; tEl.value = to;
  const span = Math.max(1, NDAYS - 1);
  $('fill').style.left = (from / span * 100) + '%';
  $('fill').style.width = ((to - from) / span * 100) + '%';
  fEl.style.zIndex = from >= NDAYS - 1 ? 3 : 2; tEl.style.zIndex = 2;
  const n = to - from + 1;
  $('rangeLabel').textContent = (n === 1 ? dayName(from) : dayName(from) + ' to ' + dayName(to)) + ' (' + n + (n === 1 ? ' day)' : ' days)');
  if (!pending) { pending = true; requestAnimationFrame(() => { pending = false; render(); }); }
}
fEl.addEventListener('input', () => { from = Math.min(+fEl.value, to); syncRange(); });
tEl.addEventListener('input', () => { to = Math.max(+tEl.value, from); syncRange(); });
document.querySelectorAll('.presets button').forEach(b => b.addEventListener('click', () => {
  const n = +b.dataset.days; to = NDAYS - 1; from = n ? Math.max(0, NDAYS - n) : 0; syncRange();
}));
document.querySelectorAll('.seg button').forEach(b => b.addEventListener('click', () => {
  grp = b.dataset.g;
  document.querySelectorAll('.seg button').forEach(x => x.setAttribute('aria-pressed', String(x === b)));
  render();
}));
let lastTouch = Date.now();
['pointerdown', 'keydown', 'wheel', 'input'].forEach(e => addEventListener(e, () => { lastTouch = Date.now(); }, {passive: true}));
setInterval(() => { if (Date.now() - lastTouch > 120000) location.reload(); }, 300000);
status(); syncRange();
})();
</script>
</body>
</html>
'''

if __name__ == "__main__":
    main()
