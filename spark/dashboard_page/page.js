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
const vlen = m => VES[m] && VES[m][2] ? VES[m][2] : null;
// Every classification comes from Gold (spark/gold_logic.classify_vessels, assess_zone_entries); the page only shows it.
// VES[m] = [name, ship type, length, is_facility, fishing_source, vessel_group, unexpected_reason]
const isFishing = m => !!(VES[m] && VES[m][4]), fishSource = m => (VES[m] && VES[m][4]) || '';
const group = m => (VES[m] && VES[m][5]) || 'rarely near installations';
const unexpectedWhy = m => (VES[m] && VES[m][6]) || 'not in the vessel list';
const RISK_L = ['High', 'Medium', 'Low'];
function zoneRows(filterMmsi){
  const rows = [];
  for (let k = 0; k < Z.t.length; k++) {
    if (!inRange(Z.t[k])) continue; const ii = Z.i[k];
    if (filterMmsi != null ? Z.m[k] !== filterMmsi : !keep(I[ii][2])) continue;
    rows.push({ii, inst:I[ii][0], on:I[ii][2], m:Z.m[k], t:Z.t[k], d:Z.d[k], kn:Z.kn[k], x:Z.x[k],
               fast:!!(Z.f && Z.f[k]), rl:Z.r ? Z.r[k] : null, rw:Z.rw ? Z.rw[k] : null, u:!!(Z.u && Z.u[k]), before:Z.b ? Z.b[k] : null});
  }
  return rows;
}
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
  const isFast = r => r.fast, fast = rows.filter(isFast).map(r => ({...r, risk: [r.rl ?? 1, RISK_L[r.rl ?? 1], r.rw || '']}));
  const unexp = rows.filter(r => r.u);                       // Gold: safety_zone_entries.is_unexpected
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
    ? 'Norwegian vessels are identified from the fishing vessel register by call sign, confirmed by name or length ("call sign only" = not confirmed); others by AIS type.' : 'The fishing vessel register is not loaded yet, so only AIS type "fishing" is used.');
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
  const BF = P.bf || {t: []};
  const ks = SV.t.map((_, k) => k).filter(k => inRange(SV.t[k]));
  const sum = a => ks.reduce((s, k) => s + ((a || [])[k] || 0), 0);
  const input = sum(SV.in), rej = REASONS.reduce((s, [r]) => s + sum(SV.rej[r]), 0);
  const bad = RU.filter(r => r.s !== 'READY');
  const fileRows = BF.t.map((t, k) => ({t, exp: BF.exp[k], got: BF.files[k], msgs: BF.msgs[k], empty: BF.empty[k], errors: BF.errors[k], starts: BF.starts[k]}))
    .filter(r => inRange(r.t)).map(r => Object.assign(r, {miss: Math.max(0, r.exp - r.got - r.empty)}))
    .map(r => Object.assign(r, {issue: r.exp > 0 && r.miss > Math.max(2, 0.01 * r.exp)}));
  const bz = ks.map(k => ({t: SV.t[k], cb: (SV.cb || [])[k] || 0, bad: (SV.bad || [])[k] || 0, unk: (SV.unk || [])[k] || {}, abs: (SV.abs || [])[k] || []}))
    .map(r => Object.assign(r, {issue: r.cb > 0 && (r.bad > 0 || Object.keys(r.unk).length > 0 || r.abs.some(a => !OPTIONAL.includes(a)))}));
  const refBad = (P.ref || []).filter(r => r.missing.length);
  kpis('kpis', [[num(RU.length - bad.length) + ' of ' + num(RU.length), 'Gold runs published'], [num(bad.length), 'Gold runs blocked or crashed'],
    [input ? (100 * rej / input).toFixed(2) + '%' : '–', 'Silver reject rate'], [num(sum(SV.dup)), 'duplicates removed in Silver'],
    [num((D.out || []).filter(inRange).length), 'feed outage hours'],
    [num(fileRows.filter(r => r.issue).length + bz.filter(r => r.issue).length + refBad.length), 'Bronze issues']]);
  const tag = (issue, none) => none ? '–' : el('span', issue ? 'Check' : 'OK', issue ? 'tag look' : 'tag');

  // freshness: the two Gold rows differ only when the newest run was blocked or crashed
  const lastRun = ALL.length ? ALL[ALL.length - 1] : null, mf = D.manifest || {};
  const fresh = [
    ['Collector', 'last Bronze file written', P.heartbeat, 10],
    ['Silver', 'last batch processed', P.silver_batch, 15],
    ['Gold data', 'last published: what the dashboard and users see', mf.finished_at, 120],
    ['Gold job', 'last run, whatever its result' + (lastRun ? ': ' + (lastRun.s === 'READY' ? 'published' : lastRun.s === 'FAILED' ? 'blocked by the gate' : 'crashed') : ' (logged from the next deploy)'),
     lastRun && lastRun.at, 120, lastRun && lastRun.s !== 'READY'],
  ].map(([layer, what, ts, lim, failed]) => ({layer, what, ts, age: ageMin(ts, built), lim, failed, none: !ts && layer === 'Gold job' && !ALL.length}));
  table('tFresh', fresh, [['', r => tag(r.age == null || r.age > r.lim || r.failed, r.none)], ['Layer', r => r.layer], ['What', r => r.what],
    ['When', r => r.ts ? when(r.ts) : 'no record'], ['Age', r => r.age == null ? '–' : dur(r.age), 1]]);
  $('nFresh').textContent = 'As of ' + when(P.built) + ', when this page was built (after each Gold run), so the ages do not grow while you watch. '
    + 'The two Gold rows show the same run unless the newest run was blocked or crashed; then the data users see is older than the last attempt. '
    + (P.disk_free_pct == null ? '' : 'Free disk space: ' + P.disk_free_pct + '%' + (P.disk_free_pct < 15 ? ', below the 15% the monitor alerts on.' : '.'));

  // Bronze checks
  $('nFiles').textContent = fileRows.length ? 'Files the collector wrote against the files expected (one every 2 minutes since it started that day). '
      + 'Empty windows (no message in 2 minutes) are logged but write no file. Restarts include deploys.'
    : 'No collector log in these days.';
  table('tFiles', fileRows.slice().reverse(), [['', r => tag(r.issue)], ['Day', r => dayName(r.t / 24)], ['Expected files', r => num(r.exp), 1],
    ['Received', r => num(r.got), 1], ['Empty windows', r => num(r.empty), 1], ['Missing', r => num(r.miss), 1], ['Messages', r => num(r.msgs), 1],
    ['Reader errors', r => num(r.errors), 1], ['Restarts', r => num(r.starts), 1]]);
  const fi = FC.t.map((_, k) => k).filter(k => inRange(FC.t[k]));
  const nOut = fi.filter(k => OUT.has(FC.t[k])).length;
  $('nFeed').textContent = 'All AIS messages per hour that reached Silver. ' + (nOut ? nOut + ' hour' + (nOut === 1 ? ' is' : 's are') + ' marked as feed outage (orange).' : 'No feed outage in these days.');
  chart('cFeed', {type: 'bar', data: {labels: fi.map(k => at(FC.t[k])), datasets: [{label: 'Messages', data: fi.map(k => FC.n[k]),
      backgroundColor: fi.map(k => OUT.has(FC.t[k]) ? css('--signal') : css('--accent')), barPercentage: 1, categoryPercentage: 1}]},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {display: false}}, scales: {x: {ticks: {maxTicksLimit: 12}, grid: {display: false}}, y: {beginAtZero: true}}}});
  $('nBronze').textContent = bz.some(r => r.cb > 0)
    ? 'Checked on every Silver batch. A field absent from one small batch is normal, so absence counts only when no batch of the day had it (altitude only comes from aircraft).'
    : 'No batch in these days was checked yet: the check starts with this version of the pipeline.';
  table('tBronze', bz.filter(r => r.cb > 0).reverse(), [['', r => tag(r.issue)],
    ['Day', r => dayName(r.t / 24)], ['Batches checked', r => num(r.cb), 1],
    ['Records missing a Bronze column', r => num(r.bad), 1],
    ['New fields', r => el('div', Object.entries(r.unk).map(([k, n]) => k + ' (' + num(n) + ' rows)').join(', ') || 'none', 'why')],
    ['Expected fields absent all day', r => el('div', r.abs.map(a => a + (ESSENTIAL.includes(a) ? ' (essential)' : OPTIONAL.includes(a) ? ' (aircraft only)' : '')).join(', ') || 'none', 'why')]]);
  table('tRef', P.ref || [], [['', r => tag(r.missing.length > 0, r.columns == null)], ['File', r => r.file], ['Updated', r => r.updated ? when(r.updated) : 'no file'],
    ['Columns', r => r.columns == null ? '–' : num(r.columns), 1], ['Columns the pipeline reads', r => num(r.required), 1],
    ['Missing', r => el('div', r.missing.join(', ') || (r.columns == null ? '–' : 'none'), 'why')]]);

  // Silver checks: one row per day, reject reasons spelled out
  const reasonsOf = k => REASONS.filter(([r]) => (SV.rej[r] || [])[k]).map(([r, l]) => l + ' ' + num(SV.rej[r][k])).join(', ');
  $('nSilT').textContent = num(rej) + ' of ' + num(input) + ' rows rejected in these days (' + (input ? (100 * rej / input).toFixed(3) : 0)
    + '%). Rows in = accepted + rejected + duplicates removed. Corrected values (speed 102.3, heading 511 and similar "not available" codes, impossible speeds) are nulled but the row is kept.';
  table('tSil', ks.slice().reverse(), [['', k => tag((SV.in[k] ? (SV.rj || [])[k] / SV.in[k] : 0) > 0.01)], ['Day', k => dayName(SV.t[k] / 24)],
    ['Rows in', k => num(SV.in[k]), 1], ['Accepted', k => num(SV.ok[k]), 1], ['Rejected', k => num((SV.rj || [])[k]), 1],
    ['Duplicates', k => num(SV.dup[k]), 1], ['Values corrected', k => num((SV.fix || [])[k]), 1],
    ['Impossible speeds', k => num((SV.gl || [])[k]), 1], ['Aircraft rows', k => num((SV.air || [])[k]), 1],
    ['Reject reasons', k => el('div', reasonsOf(k) || 'none', 'why')]]);
  const dl = ks.map(k => dayName(SV.t[k] / 24));
  const pal = [css('--warn'), css('--signal'), css('--onshore'), css('--accent'), css('--muted'), css('--ink'), css('--line'), css('--warn')];
  const used = REASONS.map((r, j) => [r, j]).filter(([[r]]) => sum(SV.rej[r]) > 0);
  chart('cRej', {type: 'bar', data: {labels: dl, datasets: used.map(([[r, l], j]) => ({label: l, data: ks.map(k => SV.rej[r][k]), backgroundColor: pal[j]}))},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {position: 'bottom'}},
              scales: {x: {stacked: true, ticks: {maxTicksLimit: 10}, grid: {display: false}}, y: {stacked: true, beginAtZero: true, ticks: {precision: 0}}}}});
  const old = ks.filter(k => SV.p95[k] > 86400);
  $('nSil').textContent = 'Seconds. Message age: the typical live batch\'s 95th percentile when stored (backfill left out).'
    + (old.length ? ' Very old messages in some batch: ' + old.map(k => dayName(SV.t[k] / 24) + ' (' + dur(SV.p95[k] / 60) + ')').join(', ') + '.' : '');
  chart('cSil', {type: 'line', data: {labels: dl, datasets: [
      {label: 'Average batch', data: ks.map(k => SV.rt[k]), borderColor: css('--accent'), backgroundColor: css('--accent'), pointRadius: 2},
      {label: 'Slowest batch', data: ks.map(k => SV.rtx[k]), borderColor: css('--signal'), backgroundColor: css('--signal'), pointRadius: 2},
      {label: 'Message age p95', data: ks.map(k => (SV.p95m || SV.p95)[k]), borderColor: css('--onshore'), backgroundColor: css('--onshore'), pointRadius: 2}]},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {position: 'bottom'}}, scales: {x: {ticks: {maxTicksLimit: 10}, grid: {display: false}}, y: {beginAtZero: true}}}});

  // Gold checks
  $('nRuns').textContent = RU.length ? 'Runtime of each hourly run in seconds. Teal: published; orange: published full rebuild; red: blocked by the gate or crashed.'
    : 'No Gold runs logged in these days. The run log starts with this version of the pipeline.';
  const short = s => new Date(s).toLocaleString('en-GB', {day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC'});
  chart('cRuns', {type: 'bar', data: {labels: RU.map(r => short(r.at)), datasets: [{label: 'Runtime (s)', data: RU.map(r => r.rt),
      backgroundColor: RU.map(r => r.s !== 'READY' ? css('--warn') : r.m === 'full' ? css('--signal') : css('--accent'))}]},
    options: {maintainAspectRatio: false, animation: false, plugins: {legend: {display: false}}, scales: {x: {ticks: {maxTicksLimit: 12}, grid: {display: false}}, y: {beginAtZero: true}}}});
  table('tRuns', RU.slice().reverse(), [['Finished (UTC)', r => short(r.at)],
    ['Status', r => el('span', r.s === 'READY' ? 'published' : r.s === 'FAILED' ? 'blocked by gate' : 'crashed', r.s === 'READY' ? 'tag' : 'tag look')],
    ['Mode', r => r.m || '–'], ['Runtime', r => r.rt == null ? '–' : r.rt < 120 ? Math.round(r.rt) + ' s' : (r.rt / 60).toFixed(1) + ' min', 1],
    ['Silver rows read', r => num(r.rows), 1], ['Repeated keys', r => r.k == null ? '–' : num(r.k), 1],
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
