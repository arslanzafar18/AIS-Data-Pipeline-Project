"""
Vessel profile (use case 4): one self-contained HTML page about ONE ship, built from the Gold tables.

  APP_ENV=prod PYTHONPATH=. python -m spark.vessel_report --mmsi 219017214 [--days 30] [--out reports]
  APP_ENV=prod PYTHONPATH=. python -m spark.vessel_report --name "esvagt aurora"
  --gold-dir points at another Gold folder (e.g. a read-only test copy).

The page shows the ship's identity, headline numbers, a map of its route (vessel_tracks) with the installations it
visited and its zone entries and surveillance events, and tables of visits, zone entries, events and days.
Only reads Gold; writes <out>/vessel_<mmsi>.html.
"""
import argparse, json, math, os, sys
from datetime import datetime, timedelta
from pyspark.sql import functions as F


def _rows(df, cols=None, order=None, limit=None):
    if df is None:
        return []
    if cols:
        df = df.select(*[c for c in cols if c in df.columns])
    if order and order in df.columns:
        df = df.orderBy(F.col(order).desc())
    if limit:
        df = df.limit(limit)
    return [r.asDict() for r in df.collect()]


def _iso(o):
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return float(o) if hasattr(o, "__float__") else str(o)


def _read(spark, gold, t):
    from spark.gold_plan import has_data
    p = os.path.join(gold, t)
    return spark.read.parquet(p) if has_data(p) else None


def track_km(points):
    """Distance sailed along a thinned track (haversine between consecutive points), in km."""
    km, prev = 0.0, None
    for p in points:
        if p.get("lat") is None or p.get("lon") is None:
            continue
        if prev:
            a1, a2 = math.radians(prev["lat"]), math.radians(p["lat"])
            h = math.sin((a2 - a1) / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(math.radians(p["lon"] - prev["lon"]) / 2) ** 2
            step = 2 * 6371.0 * math.asin(math.sqrt(h))
            if step < 200:                     # ignore jumps from bad fixes or long gaps
                km += step
        prev = p
    return km


def find_mmsi(vessels, mmsi=None, name=None):
    """Exactly one vessel by MMSI, or by name (case-insensitive, exact match first, then 'contains')."""
    if mmsi:
        return int(mmsi), None
    up = name.strip().upper()
    exact = vessels.where(F.upper("name") == up).select("mmsi", "name").collect()
    hits = exact or vessels.where(F.upper("name").contains(up)).select("mmsi", "name", "last_seen") \
                           .orderBy(F.desc("last_seen")).limit(10).collect()
    if len(hits) == 1:
        return int(hits[0]["mmsi"]), None
    return None, [(int(h["mmsi"]), h["name"]) for h in hits]


def collect(spark, gold, mmsi, days):
    v = _read(spark, gold, "vessels")
    ident = (v.where(F.col("mmsi") == mmsi).collect() if v is not None else [])
    if not ident:
        return None
    ident = ident[0].asDict()
    since = (ident["last_seen"] - timedelta(days=days)) if ident.get("last_seen") else datetime(1970, 1, 1)
    sel = lambda t, ts: (lambda df: None if df is None else
                         df.where((F.col("mmsi") == mmsi) & (F.col(ts) >= F.lit(since))))(_read(spark, gold, t))
    tracks = _rows(sel("vessel_tracks", "slot_ts"), ["slot_ts", "lat", "lon", "sog_kn", "inst_name"])
    tracks.sort(key=lambda r: r["slot_ts"])
    visits = _rows(sel("vessel_visits", "arrival_ts"), ["inst_name", "inst_kind", "is_onshore", "arrival_ts",
                   "departure_ts", "duration_min", "min_dist_m", "entered_zone"], "arrival_ts")
    zones = _rows(sel("zone_entries", "entry_ts"), ["inst_name", "entry_ts", "exit_ts", "minutes_inside",
                  "min_dist_m", "max_sog_kn", "avg_sog_kn"], "entry_ts")
    ev_cols = ["event_type", "inst_name", "start_ts", "end_ts", "duration_min", "min_dist_m", "regular_visitor",
               "is_facility", "terminal_call", "support_craft", "worth_a_look"]
    events = (_rows(sel("loitering_events", "start_ts"), ev_cols) + _rows(sel("ais_gap_events", "start_ts"), ev_cols))
    events.sort(key=lambda r: r["start_ts"], reverse=True)
    days_t = _rows(sel("vessel_days", "first_ts"), ["dt", "n_msgs", "n_near", "n_zone", "installations",
                   "first_ts", "last_ts", "destination"], "dt")
    inst = _read(spark, gold, "installations")
    names = sorted({r["inst_name"] for r in visits if r.get("inst_name")})
    places = _rows(inst.where(F.col("inst_name").isin(names)), ["inst_name", "inst_kind", "field", "is_onshore",
                   "lat", "lon"]) if inst is not None and names else []
    for p in places:
        p["visits"] = sum(1 for r in visits if r["inst_name"] == p["inst_name"])
    pos = {p["inst_name"]: (p["lat"], p["lon"]) for p in places}
    for r in zones + events:
        r["lat"], r["lon"] = pos.get(r.get("inst_name"), (None, None))
    stats = {"km": round(track_km(tracks)), "visits": len(visits), "installations": len(names),
             "zone_entries": len(zones), "zone_minutes": round(sum(r.get("minutes_inside") or 0 for r in zones)),
             "events": len(events), "worth_a_look": sum(1 for r in events if r.get("worth_a_look")),
             "days_active": len(days_t)}
    return {"vessel": ident, "since": since, "days": days, "stats": stats, "track": tracks, "visits": visits,
            "zones": zones, "events": events, "daily": days_t, "places": places}


def render(data, path):
    page = PAGE.replace("__DATA__", json.dumps(data, default=_iso, separators=(",", ":")).replace("</", "<\\/"))
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = os.path.join(os.path.dirname(path) or ".", "." + os.path.basename(path) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(page)
    os.replace(tmp, path)
    return path


def main():
    import pipeline_config
    from spark.session import get_spark
    ap = argparse.ArgumentParser(description="One-ship report from the Gold tables")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--mmsi"); g.add_argument("--name")
    ap.add_argument("--days", type=int, default=30, help="period before the vessel was last seen (default 30)")
    ap.add_argument("--gold-dir"); ap.add_argument("--out", default="reports")
    a = ap.parse_args()
    cfg = pipeline_config.load()
    cfg["driver_memory"] = "2g"
    gold = a.gold_dir or pipeline_config.path(cfg, "gold")
    spark = get_spark(cfg, "vessel-report")
    vessels = _read(spark, gold, "vessels")
    if vessels is None:
        sys.exit(f"no vessels table in {gold}")
    mmsi, choices = find_mmsi(vessels, a.mmsi, a.name)
    if mmsi is None:
        sys.exit("no vessel with that name" if not choices else
                 "several matches, use --mmsi:\n" + "\n".join(f"  {m}  {n}" for m, n in choices))
    data = collect(spark, gold, mmsi, a.days)
    if data is None:
        sys.exit(f"MMSI {mmsi} not found in {gold}/vessels")
    print(render(data, os.path.join(a.out, f"vessel_{mmsi}.html")))


PAGE = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vessel profile</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.js"></script>
<style>
:root{--bg:#f2f5f6;--surface:#fff;--ink:#14303b;--muted:#5d7480;--line:#d5dee2;--accent:#13808a;--signal:#e09a12;--warn:#b4441c;--onshore:#7a5ea8;
--font:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;color-scheme:light}
@media (prefers-color-scheme:dark){:root{--bg:#0d181d;--surface:#14232a;--ink:#e2ebee;--muted:#93a8b1;--line:#253841;--accent:#4fb3bc;--signal:#f0b33c;--warn:#f08a5d;--onshore:#b39ddb;color-scheme:dark}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 var(--font)}
main{max-width:1180px;margin:0 auto;padding:28px 20px 48px}
h1{font-size:1.75rem;font-weight:600;margin:0}.sub{color:var(--muted);margin:2px 0 0}
.kpis{display:flex;flex-wrap:wrap;gap:8px 36px;margin:20px 0 0;padding:0;list-style:none}
.kpis b{display:block;font-size:1.5rem;font-weight:600;font-variant-numeric:tabular-nums}.kpis span{color:var(--muted);font-size:.875rem}
section{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:18px;margin-top:16px}
h2{font-size:1.05rem;font-weight:600;margin:0 0 4px}.note{color:var(--muted);font-size:.875rem;margin:0 0 12px}
#map{height:520px;border-radius:8px}.leaflet-container{font:inherit}
.scroll{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:.875rem}
th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{color:var(--muted);font-weight:500}td.n{text-align:right;font-variant-numeric:tabular-nums}
.tag{display:inline-block;font-size:.75rem;padding:0 6px;border-radius:4px;border:1px solid var(--line);margin-right:4px}
.tag.look{border-color:var(--warn);color:var(--warn);font-weight:500}
</style></head><body><main>
<h1 id="name"></h1><p class="sub" id="ident"></p>
<ul class="kpis" id="kpis"></ul>
<section><h2>Route</h2><p class="note" id="mapnote"></p><div id="map"></div></section>
<section><h2>Surveillance events</h2><p class="note">Loitering and AIS gaps; flags show the ordinary explanation, if any</p><div class="scroll" id="events"></div></section>
<section><h2>Safety-zone entries</h2><div class="scroll" id="zones"></div></section>
<section><h2>Visits to installations</h2><div class="scroll" id="visits"></div></section>
<section><h2>Day by day</h2><div class="scroll" id="daily"></div></section>
</main>
<script type="application/json" id="data">__DATA__</script>
<script>
(function(){
const D = JSON.parse(document.getElementById('data').textContent), V = D.vessel;
const $ = id => document.getElementById(id), css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const num = n => n == null ? '–' : Math.round(n).toLocaleString('en-GB');
const dur = m => m == null ? '–' : m < 60 ? Math.round(m) + ' min' : m < 2880 ? (m / 60).toFixed(1) + ' h' : (m / 1440).toFixed(1) + ' d';
const t = s => s ? new Date(s.endsWith('Z') || s.includes('+') ? s : s + 'Z').toLocaleString('en-GB', {day:'numeric', month:'short', hour:'2-digit', minute:'2-digit', timeZone:'UTC'}) : '–';
document.title = (V.name || 'MMSI ' + V.mmsi) + ' – vessel profile';
$('name').textContent = V.name || 'Unnamed vessel';
$('ident').textContent = ['MMSI ' + V.mmsi, V.imo ? 'IMO ' + V.imo : null, V.call_sign ? 'call sign ' + V.call_sign : null,
  V.ship_type != null ? 'ship type ' + V.ship_type : null, V.length_m ? V.length_m + ' m' : null,
  V.is_facility ? 'installation or rig broadcasting AIS' : null,
  'seen ' + t(V.first_seen) + ' to ' + t(V.last_seen) + ' UTC; showing the last ' + D.days + ' days'].filter(Boolean).join(' · ');
const S = D.stats;
for (const [v, l] of [[num(S.km) + ' km', 'sailed'], [num(S.visits), 'visits'], [num(S.installations), 'installations'],
                      [num(S.zone_entries), 'zone entries'], [dur(S.zone_minutes), 'inside 500 m zones'],
                      [num(S.events), 'surveillance events'], [num(S.worth_a_look), 'worth a look'], [num(S.days_active), 'days seen']]) {
  const li = document.createElement('li'), b = document.createElement('b'), s = document.createElement('span');
  b.textContent = v; s.textContent = l; li.append(b, s); $('kpis').append(li);
}
function table(id, rows, cols){
  const box = $(id); if (!rows.length) { box.textContent = 'None in this period.'; return; }
  const tb = document.createElement('table'), hr = document.createElement('tr');
  for (const [h] of cols) { const th = document.createElement('th'); th.textContent = h; hr.append(th); }
  const thead = document.createElement('thead'); thead.append(hr); tb.append(thead);
  const body = document.createElement('tbody');
  for (const r of rows) {
    const tr = document.createElement('tr');
    for (const [, f, isNum] of cols) { const td = document.createElement('td'); const v = f(r);
      if (v instanceof Node) td.append(v); else td.textContent = v; if (isNum) td.className = 'n'; tr.append(td); }
    body.append(tr);
  }
  tb.append(body); box.append(tb);
}
function flags(r){
  const w = document.createElement('span');
  const add = (txt, cls) => { const s = document.createElement('span'); s.className = 'tag' + (cls ? ' ' + cls : ''); s.textContent = txt; w.append(s); };
  if (r.worth_a_look) add('worth a look', 'look');
  for (const [k, l] of [['regular_visitor', 'regular visitor'], ['is_facility', 'facility'], ['terminal_call', 'terminal call'], ['support_craft', 'support craft']])
    if (r[k]) add(l);
  return w;
}
table('events', D.events, [['Type', r => r.event_type === 'ais_gap' ? 'AIS gap' : 'loitering'], ['Installation', r => r.inst_name],
  ['Start (UTC)', r => t(r.start_ts)], ['Duration', r => dur(r.duration_min), 1], ['Closest', r => num(r.min_dist_m) + ' m', 1], ['Flags', flags]]);
table('zones', D.zones, [['Installation', r => r.inst_name], ['Entry (UTC)', r => t(r.entry_ts)], ['Inside', r => dur(r.minutes_inside), 1],
  ['Closest', r => num(r.min_dist_m) + ' m', 1], ['Top speed', r => r.max_sog_kn == null ? '–' : r.max_sog_kn.toFixed(1) + ' kn', 1]]);
table('visits', D.visits, [['Installation', r => r.inst_name], ['Type', r => (r.inst_kind || '').toLowerCase() + (r.is_onshore ? ' (onshore)' : '')],
  ['Arrival (UTC)', r => t(r.arrival_ts)], ['Departure', r => t(r.departure_ts)], ['Stay', r => dur(r.duration_min), 1],
  ['Closest', r => num(r.min_dist_m) + ' m', 1], ['Entered zone', r => r.entered_zone ? 'yes' : 'no']]);
table('daily', D.daily, [['Day', r => r.dt], ['Messages', r => num(r.n_msgs), 1], ['Near installations', r => num(r.n_near), 1],
  ['In zones', r => num(r.n_zone), 1], ['Installations', r => num(r.installations), 1], ['Destination', r => r.destination || '–']]);
if (typeof L === 'undefined') { $('mapnote').textContent = 'The map needs the Leaflet library from cdnjs.cloudflare.com, which did not load.'; return; }
const map = L.map('map', {scrollWheelZoom: false});
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 14, attribution: '&copy; OpenStreetMap contributors'}).addTo(map);
const pts = D.track.filter(p => p.lat != null && p.lon != null), bounds = [];
let seg = [];
const flush = () => { if (seg.length > 1) L.polyline(seg, {color: css('--accent'), weight: 2, opacity: 0.8}).addTo(map); seg = []; };
for (let i = 0; i < pts.length; i++) {        // break the line across gaps longer than 2 hours
  if (i && Date.parse(pts[i].slot_ts) - Date.parse(pts[i - 1].slot_ts) > 7.2e6) flush();
  seg.push([pts[i].lat, pts[i].lon]); bounds.push([pts[i].lat, pts[i].lon]);
}
flush();
if (pts.length) {
  const last = pts[pts.length - 1];
  L.circleMarker([last.lat, last.lon], {radius: 7, color: css('--ink'), fillColor: css('--accent'), fillOpacity: 1, weight: 2})
    .bindTooltip('Last position ' + t(last.slot_ts)).addTo(map);
}
for (const p of D.places) {
  const c = css(p.is_onshore ? '--onshore' : '--ink');
  L.circleMarker([p.lat, p.lon], {radius: 5 + Math.min(10, p.visits), color: c, weight: 1.5, fillColor: c, fillOpacity: 0.25})
    .bindTooltip(p.inst_name + ': ' + p.visits + ' visit' + (p.visits === 1 ? '' : 's') + (p.field ? ' (' + p.field + ')' : '')).addTo(map);
  bounds.push([p.lat, p.lon]);
}
for (const e of D.events.filter(e => e.lat != null && e.worth_a_look))
  L.circleMarker([e.lat, e.lon], {radius: 9, color: css('--warn'), weight: 2, fill: false})
    .bindTooltip((e.event_type === 'ais_gap' ? 'AIS gap ' : 'Loitering ') + dur(e.duration_min) + ' at ' + e.inst_name + ', ' + t(e.start_ts)).addTo(map);
if (bounds.length) map.fitBounds(bounds, {padding: [24, 24]}); else map.setView([62.5, 4], 5);
$('mapnote').textContent = pts.length + ' track points (one per 10 minutes); line breaks where the vessel was silent for over 2 hours. ' +
  'Circles are installations it visited (violet: onshore); orange rings mark events worth a look.';
})();
</script></body></html>
'''

if __name__ == "__main__":
    main()
