"""
main.py -- REVAP reservoir monitoring dashboard, served by a Cloud Function.

Entry point: dashboard  (HTTP, 2nd gen)

Routes
  /                          the dashboard: map + list of reservoirs + plot on click
  /data?name=NAME&span=1m&var=stage_m
                             JSON series for the bottom plot (span: 1m, 6m, 1y;
                             var: stage_m, area_m2, depth_m, volume_m3)
  /inspect                   the bucket inspection report from step 1

Data
  reservoirs.csv (bundled): RES_NAME, source, ..., OWEN_ID, LAT, LON
  Per-reservoir CSVs in the bucket, one per reservoir, full record, columns:
      date, stage_m, area_m2, volume_m3, depth_m, qflag  (others ignored)
  Read from the folder RES_PREFIX (default by_reservoir). Set RES_PREFIX to ''
  to pick the folder automatically (the folder with the most files named after
  the reservoirs; slower, it lists the whole bucket). The page shows which
  folder was used and every reservoir that wasn't found.

Definitions used on the page
  variable       = chosen on the page (one selector for the small plots, the map
                   colours, the stale flags and the bottom plot):
                   Elevation (stage_m, default), Area (area_m2), Depth (depth_m),
                   Storage (volume_m3). TWDB has no elevation (stage_m = -9999).
  last real data = last date with qflag 'O' and a value in the chosen variable
  stale          = last real data more than STALE_DAYS before today (Pacific)
  -9999          = no data (treated as blank)
  spans          = 1m = 30 days, 6m = 182 days, 1y = 365 days, ending today

No caching
  Every page load lists the folder and downloads every reservoir file; every
  /data request downloads that one reservoir's file. Responses are sent with
  Cache-Control: no-store, and the page does not keep plot data between clicks.
  The page carries the sparklines for all three spans; the bottom plot fetches
  one reservoir's series from /data on click.
"""
import html
import io
import json
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import functions_framework
import pandas as pd
from google.cloud import storage

# ---------------------------------------------------------------- settings --
BUCKET = os.environ.get('BUCKET', 'operevap-res-retrieval-elevation')
RES_PREFIX = os.environ.get('RES_PREFIX', 'by_reservoir').strip('/')   # '' = auto-detect
STALE_DAYS = int(os.environ.get('STALE_DAYS', '14'))
WORKERS = 32                                                 # parallel downloads
NO_STORE = {'Cache-Control': 'no-store'}
SPANS = {'1m': ('1 month', 30), '6m': ('6 months', 182), '1y': ('1 year', 365)}
DEFAULT_PLOT_SPAN = '1m'
DEFAULT_SPARK_SPAN = '1y'
MAX_DAYS = max(d for _, d in SPANS.values())
TZ = ZoneInfo('America/Los_Angeles')
NODATA = -9999
OBSERVED = 'O'
VALUE_COLS = ['stage_m', 'area_m2', 'volume_m3', 'depth_m']
# variable -> (label, unit, divide by); order = order in the selector
VARS = {'stage_m': ('Elevation', 'm', 1.0),
        'area_m2': ('Area', 'km²', 1e6),
        'depth_m': ('Depth', 'm', 1.0),
        'volume_m3': ('Storage', 'million m³', 1e6)}
DEFAULT_VAR = 'stage_m'

HERE = os.path.dirname(os.path.abspath(__file__))
RES_CSV = os.path.join(HERE, 'reservoirs.csv')


def esc(x):
    return html.escape(str(x))


def safe_name(name):
    """Reservoir name -> file name, as build_archive.py writes it: LAKE MEAD -> LAKE_MEAD."""
    return re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')


def today_local():
    return datetime.now(TZ).date()


# ------------------------------------------------------------ data loading --
def load_meta():
    meta = pd.read_csv(RES_CSV, dtype=str, keep_default_na=False)
    for c in ('RES_NAME', 'source', 'LAT', 'LON'):
        if c not in meta.columns:
            raise ValueError(f'reservoirs.csv has no {c} column')
    return meta


def list_csvs(client, prefix):
    """{folder: {FILE_STEM_UPPER: blob}} for every .csv under prefix."""
    by_folder = defaultdict(dict)
    for b in client.list_blobs(BUCKET, prefix=(prefix + '/') if prefix else None):
        if b.name.endswith('.csv'):
            folder, _, base = b.name.rpartition('/')
            by_folder[folder][base[:-4].upper()] = b
    return by_folder


def match_files(by_folder, names):
    """Pick the folder with the most reservoir-named files; map name -> blob."""
    keys = {n: [safe_name(n).upper(), n.upper(), n.upper().replace(' ', '_')] for n in names}

    def found_in(folder):
        return {n: next(by_folder[folder][k] for k in ks if k in by_folder[folder])
                for n, ks in keys.items() if any(k in by_folder[folder] for k in ks)}

    best_folder, best = None, {}
    for folder in by_folder:
        f = found_in(folder)
        if len(f) > len(best):
            best_folder, best = folder, f
    return best_folder, best


def parse_file(blob):
    """Download and reduce one per-reservoir file to what the page needs."""
    df = pd.read_csv(io.BytesIO(blob.download_as_bytes()),
                     dtype={'qflag': str, 'datum': str})
    missing = [c for c in ['date', 'qflag', *VALUE_COLS] if c not in df.columns]
    if missing:
        raise ValueError(f'missing columns {missing} (has {list(df.columns)})')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = (df.dropna(subset=['date']).sort_values('date')
            .drop_duplicates('date', keep='last').reset_index(drop=True))
    if df.empty:
        raise ValueError('file has no dated rows')
    for c in VALUE_COLS:
        v = pd.to_numeric(df[c], errors='coerce')
        df[c] = v.where(v != NODATA)
    df['qflag'] = df['qflag'].fillna('').str.strip()

    last_obs = {}
    for c in VALUE_COLS:                     # last observed date per variable
        o = df.loc[(df['qflag'] == OBSERVED) & df[c].notna(), 'date']
        last_obs[c] = o.iloc[-1].date() if len(o) else None
    last_row = df.iloc[-1]
    # Keep only the tail the page can show; windows are cut again at render time.
    keep_from = df['date'].iloc[-1] - timedelta(days=MAX_DAYS + 5)
    tail = df.loc[df['date'] >= keep_from, ['date', *VALUE_COLS, 'qflag']].copy()
    return {
        'rows': len(df),
        'first_date': df['date'].iloc[0].date(),
        'last_date': last_row['date'].date(),
        'last_obs': last_obs,
        'latest': {'date': last_row['date'].strftime('%Y-%m-%d'),
                   'qflag': last_row['qflag'],
                   **{c: (None if pd.isna(last_row[c]) else float(last_row[c]))
                      for c in VALUE_COLS}},
        'tail': tail,
    }


def load_all(client):
    """Read reservoirs.csv, list the folder, download and parse every reservoir
    file. Nothing is kept between requests."""
    t0 = time.time()
    meta = load_meta()
    names = meta['RES_NAME'].tolist()
    folder, files = match_files(list_csvs(client, RES_PREFIX), names)

    def work(n):
        try:
            return n, parse_file(files[n]), None
        except Exception as e:                           # shown on the page
            return n, None, f'{type(e).__name__}: {e}'

    parsed, errors = {}, {}
    with ThreadPoolExecutor(WORKERS) as ex:
        for n, p, err in ex.map(work, list(files)):
            if err:
                errors[n] = err
            else:
                parsed[n] = p
    return {'meta': meta, 'folder': folder, 'files': files, 'parsed': parsed,
            'errors': errors, 'load_s': round(time.time() - t0, 1), 'downloaded': len(files)}


def load_one(client, name):
    """Download and parse one reservoir's file. Returns (parsed, error)."""
    if RES_PREFIX:
        blob = client.bucket(BUCKET).blob(f'{RES_PREFIX}/{safe_name(name)}.csv')
        if not blob.exists():
            return None, f'no file {blob.name} in gs://{BUCKET}'
    else:
        _, files = match_files(list_csvs(client, ''), [name])
        blob = files.get(name)
        if blob is None:
            return None, f'no file for {name!r} in gs://{BUCKET}'
    try:
        return parse_file(blob), None
    except Exception as e:
        return None, f'{type(e).__name__}: {e}'


# ------------------------------------------------------- page data (JSON) --
def window(tail, var, days, scale):
    """Daily series for the last `days` days ending today (Pacific).
    Returns start date, values (None = no row), qflags (None = no row)."""
    t = pd.Timestamp(today_local())
    idx = pd.date_range(t - timedelta(days=days - 1), t, freq='D')
    s = tail.set_index('date').reindex(idx)
    vals = [None if pd.isna(v) else round(float(v) / scale, 3) for v in s[var]]
    flags = [None if pd.isna(q) else q for q in s['qflag']]
    return idx[0].strftime('%Y-%m-%d'), vals, flags


def scaled_0_100(vals):
    """Sparklines only need shape: rescale a window to 0..100 integers."""
    ok = [v for v in vals if v is not None]
    if not ok:
        return []
    lo, hi = min(ok), max(ok)
    rng = (hi - lo) or 1.0
    return [None if v is None else round(100 * (v - lo) / rng) for v in vals]


def build_payload(state):
    today = today_local()
    out = []
    for _, m in state['meta'].iterrows():
        n = m['RES_NAME']
        r = {'name': n, 'source': m['source'],
             'lat': float(m['LAT']) if m['LAT'] else None,
             'lon': float(m['LON']) if m['LON'] else None}
        p = state['parsed'].get(n)
        if p is None:
            r['error'] = state['errors'].get(n, 'no file found in the bucket')
            r['stale'] = True
            out.append(r)
            continue
        r.update({
            'last_obs': {c: (d.isoformat() if d else None) for c, d in p['last_obs'].items()},
            'age': {c: ((today - d).days if d else None) for c, d in p['last_obs'].items()},
            'latest': p['latest'],
            'record': f"{p['first_date']} to {p['last_date']} ({p['rows']} rows)",
            'file': state['files'][n].name,
            'spark': {c: {k: scaled_0_100(window(p['tail'], c, d, VARS[c][2])[1])
                          for k, (_, d) in SPANS.items()} for c in VARS},
        })
        out.append(r)
    return {
        'generated': datetime.now(TZ).strftime('%Y-%m-%d %H:%M %Z'),
        'today': today.isoformat(),
        'stale_days': STALE_DAYS,
        'spans': {k: {'label': lab, 'days': d} for k, (lab, d) in SPANS.items()},
        'plot_span': DEFAULT_PLOT_SPAN, 'spark_span': DEFAULT_SPARK_SPAN,
        'vars': {c: {'label': lab, 'unit': u} for c, (lab, u, _) in VARS.items()},
        'default_var': DEFAULT_VAR,
        'bucket': BUCKET, 'folder': state['folder'],
        'found': len(state['parsed']), 'total': len(out),
        'load_s': state['load_s'], 'downloaded': state['downloaded'],
        'reservoirs': out,
    }


def series_payload(client, name, span, var):
    """JSON for the bottom plot: one reservoir, one span, one variable (reads its file now)."""
    if span not in SPANS:
        return {'error': f'unknown span {span!r}; use one of {list(SPANS)}'}, 400
    if var not in VARS:
        return {'error': f'unknown var {var!r}; use one of {list(VARS)}'}, 400
    p, err = load_one(client, name)
    if p is None:
        return {'error': err}, 404
    label, unit, scale = VARS[var]
    start, vals, flags = window(p['tail'], var, SPANS[span][1], scale)
    lo = p['last_obs'][var]
    return {'name': name, 'span': span, 'var': var, 'start': start, 'vals': vals, 'flags': flags,
            'label': label, 'unit': unit, 'last_obs': lo.isoformat() if lo else None}, 200


# ------------------------------------------------------------- dashboard --
PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>REVAP Reservoir Monitor</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css">
<script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-basic-dist-min@2.35.2/plotly-basic.min.js"></script>
<style>
:root{--bg:#fff;--fg:#1d1d1f;--muted:#6b6b70;--line:#e3e3e6;--panel:#f7f7f8;
  --fresh:#2a78d6;--stale:#d6452a;--none:#9a9aa0;--sel:#eaf2fc}
*{box-sizing:border-box}
html,body{margin:0;height:100%;background:var(--bg);color:var(--fg);
  font:14px/1.35 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{display:flex;flex-wrap:wrap;gap:6px 18px;align-items:baseline;
  padding:10px 16px;border-bottom:1px solid var(--line)}
header h1{font-size:17px;margin:0}
header .meta{color:var(--muted);font-size:12px}
.warn{color:var(--stale);font-weight:600}
main{display:grid;grid-template-columns:380px 1fr;height:calc(100% - 46px)}
#side{display:flex;flex-direction:column;border-right:1px solid var(--line);min-height:0}
#tools{display:flex;gap:8px;flex-wrap:wrap;align-items:center;padding:8px 10px;
  border-bottom:1px solid var(--line);background:var(--panel)}
#tools input[type=search]{flex:1;min-width:120px;padding:5px 8px;border:1px solid var(--line);border-radius:6px}
select{padding:4px;border:1px solid var(--line);border-radius:6px;background:#fff;font:inherit;font-size:13px}
#count{font-size:12px;color:var(--muted);width:100%}
#list{overflow-y:auto;flex:1}
.row{display:grid;grid-template-columns:1fr 110px;gap:2px 8px;padding:7px 10px;
  border-bottom:1px solid var(--line);cursor:pointer}
.row:hover{background:var(--panel)} .row.sel{background:var(--sel)}
.row .nm{font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.row .src{color:var(--muted);font-size:11px;font-weight:400;margin-left:4px}
.row .dt{font-size:12px;color:var(--muted)}
.row svg{grid-row:1 / span 2;grid-column:2;align-self:center}
.flag{display:inline-block;font-size:11px;font-weight:700;color:#fff;background:var(--stale);
  border-radius:4px;padding:0 5px;margin-left:4px}
#right{display:grid;grid-template-rows:55% 45%;min-height:0}
#map{min-height:0;background:#222}
#detail{border-top:1px solid var(--line);display:grid;grid-template-columns:1fr 260px;min-height:0}
#plotwrap{display:flex;flex-direction:column;min-height:0}
#plotbar{display:flex;gap:8px;align-items:center;padding:6px 10px;font-size:12px;color:var(--muted)}
#plot{flex:1;min-height:0}
#info{border-left:1px solid var(--line);padding:10px 12px;overflow-y:auto;font-size:12px;background:var(--panel)}
#info h2{font-size:15px;margin:0 0 6px}
#info table{border-collapse:collapse;width:100%}
#info td{padding:2px 0;vertical-align:top} #info td:first-child{color:var(--muted);padding-right:8px;white-space:nowrap}
.hint{color:var(--muted);padding:16px;margin:0}
@media (max-width:820px){
  main{grid-template-columns:1fr;height:auto}
  #side{height:45vh;border-right:0;border-bottom:1px solid var(--line)}
  #right{grid-template-rows:50vh auto}
  #detail{grid-template-columns:1fr} #plot{height:320px;flex:none} #info{border-left:0}
}
</style></head><body>
<header>
  <h1>REVAP Reservoir Monitor</h1>
  <span class="meta" id="hdr"></span>
</header>
<main>
  <section id="side">
    <div id="tools">
      <input type="search" id="q" placeholder="Search reservoirs">
      <select id="sort" title="Sort the list">
        <option value="name">Sort: name</option>
        <option value="age">Sort: oldest data first</option>
        <option value="source">Sort: source</option>
      </select>
      <select id="var" title="Variable for the small plots, the map colours, the stale flags and the bottom plot"></select>
      <select id="sparkSpan" title="Time span of the small plots"></select>
      <label><input type="checkbox" id="staleOnly"> Stale only</label>
      <div id="count"></div>
    </div>
    <div id="list"></div>
  </section>
  <section id="right">
    <div id="map"></div>
    <div id="detail">
      <div id="plotwrap">
        <div id="plotbar">Show <select id="plotSpan" title="Time span of the plot"></select></div>
        <div id="plot"><p class="hint">Click a reservoir on the map or in the list to plot it.</p></div>
      </div>
      <div id="info"></div>
    </div>
  </section>
</main>
<script>
const DATA = __DATA__;
const R = DATA.reservoirs;
const C = {fresh:'#2a78d6', stale:'#d6452a', none:'#9a9aa0'};
const esc = s => String(s ?? '').replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const V = () => $('var').value;                       // chosen variable
const ageOf = r => r.age ? r.age[V()] : null;
const lastOf = r => r.last_obs ? r.last_obs[V()] : null;
const staleOf = r => !!r.error || ageOf(r) == null || ageOf(r) > DATA.stale_days;
const statusOf = r => r.error ? 'none' : (staleOf(r) ? 'stale' : 'fresh');
const fmt = (v, d=2) => v == null ? '–' : Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
const $ = id => document.getElementById(id);
// Works at the run.app root ("/") and under a path (".../revap-dashboard").
const BASE = location.pathname.endsWith('/') ? location.pathname : location.pathname + '/';

// ---- variable + span dropdowns
$('var').innerHTML = Object.entries(DATA.vars).map(([k, v]) =>
  `<option value="${k}"${k === DATA.default_var ? ' selected' : ''}>${esc(v.label)}</option>`).join('');
for (const [id, def] of [['sparkSpan', DATA.spark_span], ['plotSpan', DATA.plot_span]]) {
  $(id).innerHTML = Object.entries(DATA.spans).map(([k, s]) =>
    `<option value="${k}"${k === def ? ' selected' : ''}>${id === 'sparkSpan' ? 'Mini plots: ' : ''}${esc(s.label)}</option>`).join('');
}

// ---- header (stale count follows the chosen variable)
function renderHeader(){
const nStale = R.filter(staleOf).length;
$('hdr').innerHTML =
  `${DATA.found}/${DATA.total} reservoir files found` +
  (DATA.found < DATA.total ? ' <span class="warn">(see list for missing)</span>' : '') +
  ` · <span class="${nStale ? 'warn' : ''}">${nStale} stale (&gt;${DATA.stale_days} d)</span>` +
  ` · today ${esc(DATA.today)} · updated ${esc(DATA.generated)}` +
  ` · gs://${esc(DATA.bucket)}/${esc(DATA.folder ?? '?')}/` +
  ` · read ${DATA.downloaded ?? 0} files in ${DATA.load_s ?? '?'} s`;
}
renderHeader();

// ---- sparkline: values arrive scaled 0..100; one path segment per run of days with data
function spark(vals, color){
  const w = 110, h = 30, p = 2;
  if (!vals || !vals.some(v => v != null)) {
    const lab = DATA.spans[$('sparkSpan').value].label;
    return `<svg width="${w}" height="${h}"><text x="0" y="19" font-size="10" fill="#9a9aa0">no ${esc(DATA.vars[V()].label.toLowerCase())}, ${esc(lab)}</text></svg>`;
  }
  const x = i => p + i * (w - 2*p) / Math.max(vals.length - 1, 1);
  const y = v => h - p - v * (h - 2*p) / 100;
  let d = '', pen = false;
  vals.forEach((v, i) => {
    if (v == null) { pen = false; return; }
    d += `${pen ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`; pen = true;
  });
  return `<svg width="${w}" height="${h}" aria-hidden="true"><path d="${d}" fill="none" stroke="${color}" stroke-width="1.3"/></svg>`;
}

// ---- map: satellite imagery + boundaries/place names on top
const map = L.map('map', {preferCanvas:true});
L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
  {maxZoom:19, attribution:'Imagery &copy; Esri, Maxar, Earthstar Geographics, and the GIS User Community'}).addTo(map);
L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
  {maxZoom:19, attribution:'Labels &copy; Esri'}).addTo(map);
const markers = R.map((r, i) => {
  if (r.lat == null || r.lon == null) return null;
  const c = C[statusOf(r)];
  return L.circleMarker([r.lat, r.lon], {radius:6, color:'#fff', weight:1.5, fillColor:c, fillOpacity:.95})
    .bindTooltip(esc(r.name)).on('click', () => select(i, false)).addTo(map);
});
const pts = R.filter(r => r.lat != null).map(r => [r.lat, r.lon]);
if (pts.length) map.fitBounds(pts, {padding:[20,20]}); else map.setView([38,-105], 5);
const legend = L.control({position:'bottomleft'});
legend.onAdd = () => { const d = L.DomUtil.create('div');
  d.style.cssText = 'background:#fff;padding:5px 8px;border-radius:6px;font-size:12px;box-shadow:0 1px 3px #0003';
  d.innerHTML = [['fresh',`real data ≤ ${DATA.stale_days} d old`],['stale',`stale &gt; ${DATA.stale_days} d`],['none','no file / error']]
    .map(([k,t]) => `<div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${C[k]};margin-right:5px"></span>${t}</div>`).join('');
  return d; };
legend.addTo(map);
function recolor(){ R.forEach((r, i) => { if (markers[i]) markers[i].setStyle({fillColor: C[statusOf(r)]}); }); }

// ---- list
let selected = null;
function renderList(){
  const q = $('q').value.trim().toUpperCase();
  const staleOnly = $('staleOnly').checked;
  const sort = $('sort').value, sspan = $('sparkSpan').value;
  let idx = R.map((r, i) => i).filter(i =>
    (!q || R[i].name.toUpperCase().includes(q) || String(R[i].source).toUpperCase().includes(q)) &&
    (!staleOnly || staleOf(R[i])));
  const age = r => r.error ? 1e9 : (ageOf(r) ?? 1e8);
  idx.sort((a, b) => sort === 'age' ? age(R[b]) - age(R[a])
                   : sort === 'source' ? (R[a].source.localeCompare(R[b].source) || R[a].name.localeCompare(R[b].name))
                   : R[a].name.localeCompare(R[b].name));
  $('count').textContent = `${idx.length} of ${R.length} shown`;
  $('list').innerHTML = idx.map(i => {
    const r = R[i], st = statusOf(r);
    const when = r.error ? `<span class="warn">${esc(r.error)}</span>`
      : lastOf(r) ? `last real data ${esc(lastOf(r))} (${ageOf(r)} d)` : `no real (O) ${esc(DATA.vars[V()].label.toLowerCase())} in file`;
    const flag = staleOf(r) ? `<span class="flag">${r.error ? 'NO DATA' : ageOf(r) == null ? 'NO OBS' : 'STALE ' + ageOf(r) + ' d'}</span>` : '';
    return `<div class="row${i === selected ? ' sel' : ''}" data-i="${i}">
      <div class="nm">${esc(r.name)}<span class="src">${esc(r.source)}</span>${flag}</div>
      <div class="dt">${when}</div>
      ${r.spark ? spark(r.spark[V()][sspan], C[st]) : '<svg width="110" height="30"></svg>'}
    </div>`;
  }).join('');
}
$('list').addEventListener('click', e => {
  const row = e.target.closest('.row'); if (row) select(+row.dataset.i, true);
});
['q','sort','staleOnly','sparkSpan'].forEach(id => $(id).addEventListener('input', renderList));
$('var').addEventListener('input', () => {
  renderHeader(); renderList(); recolor();
  if (selected != null) select(selected, true);         // info panel + bottom plot
});
$('plotSpan').addEventListener('input', () => { if (selected != null) loadPlot(selected); });

// ---- selection: info panel now, plot once its data arrives
function select(i, fromList){
  selected = i; renderList();
  const r = R[i];
  const row = document.querySelector(`.row[data-i="${i}"]`);
  if (row && !fromList) row.scrollIntoView({block:'nearest'});
  if (markers[i]) { map.panTo(markers[i].getLatLng()); markers[i].openTooltip(); }
  if (r.error){
    Plotly.purge('plot');
    $('plot').innerHTML = `<p class="hint warn">${esc(r.error)}</p>`;
    $('info').innerHTML = `<h2>${esc(r.name)}</h2><p>${esc(r.source)}</p>`;
    return;
  }
  const L_ = r.latest;
  $('info').innerHTML = `<h2>${esc(r.name)}</h2>
   <table>
    <tr><td>Source</td><td>${esc(r.source)}</td></tr>
    <tr><td>Last real ${esc(DATA.vars[V()].label.toLowerCase())}</td><td>${lastOf(r) ? esc(lastOf(r)) + ' (' + ageOf(r) + ' d ago)' : '<span class="warn">none</span>'}
        ${staleOf(r) ? '<span class="flag">STALE</span>' : ''}</td></tr>
    <tr><td>Latest row</td><td>${esc(L_.date)} · qflag ${esc(L_.qflag || '–')}</td></tr>
    <tr><td>Elevation</td><td>${fmt(L_.stage_m)} m</td></tr>
    <tr><td>Area</td><td>${fmt(L_.area_m2 == null ? null : L_.area_m2 / 1e6, 3)} km²</td></tr>
    <tr><td>Storage</td><td>${fmt(L_.volume_m3 == null ? null : L_.volume_m3 / 1e6, 3)} million m³</td></tr>
    <tr><td>Depth</td><td>${fmt(L_.depth_m)} m</td></tr>
    <tr><td>Record</td><td>${esc(r.record)}</td></tr>
    <tr><td>Plotted</td><td>${esc(DATA.vars[V()].label)} (${esc(DATA.vars[V()].unit)})</td></tr>
    <tr><td>File</td><td style="word-break:break-all">${esc(r.file)}</td></tr>
   </table>`;
  loadPlot(i);
}

async function loadPlot(i){       // always fetched fresh from the bucket
  const r = R[i], span = $('plotSpan').value, v = V();
  let s;
  $('plot').innerHTML = '<p class="hint">Loading…</p>';
  try {
    const res = await fetch(`${BASE}data?name=${encodeURIComponent(r.name)}&span=${span}&var=${v}`, {cache:'no-store'});
    s = await res.json();
    if (!res.ok) throw new Error(s.error || res.status);
  } catch (e) {
    if (selected === i) $('plot').innerHTML = `<p class="hint warn">Could not load data: ${esc(e.message)}</p>`;
    return;
  }
  if (selected !== i || $('plotSpan').value !== span || V() !== v) return;   // user moved on
  drawPlot(r, s);
}

function drawPlot(r, s){
  const t0 = Date.parse(s.start + 'T00:00:00Z');
  const dates = s.vals.map((_, k) => new Date(t0 + k * 864e5).toISOString().slice(0, 10));
  const obsX = [], obsY = [], othX = [], othY = [], othT = [];
  dates.forEach((d, k) => {
    if (s.vals[k] == null) return;
    if (s.flags[k] === 'O') { obsX.push(d); obsY.push(s.vals[k]); }
    else { othX.push(d); othY.push(s.vals[k]); othT.push('qflag ' + (s.flags[k] || '(blank)')); }
  });
  const many = dates.length > 60;            // smaller markers for long spans
  $('plot').innerHTML = '';
  const traces = [
    {x:dates, y:s.vals, mode:'lines', name:'all rows', line:{color:'#b9c6d6', width:1.5},
     hoverinfo:'skip', connectgaps:false},
    {x:obsX, y:obsY, mode:'markers', name:'observed (O)', marker:{color:C.fresh, size:many ? 4 : 6}},
    {x:othX, y:othY, mode:'markers', name:'filled / flagged', text:othT,
     marker:{color:'#eb6834', size:many ? 5 : 7, symbol:'x'}, hovertemplate:'%{x}<br>%{y}<br>%{text}<extra></extra>'},
  ];
  const shapes = s.last_obs ? [{type:'line', xref:'x', yref:'paper', x0:s.last_obs, x1:s.last_obs, y0:0, y1:1,
                               line:{color:C.stale, width:1, dash:'dot'}}] : [];
  Plotly.newPlot('plot', traces, {
    title:{text:`${r.name} — ${s.label}, last ${DATA.spans[s.span].label}`, font:{size:14}},
    margin:{l:60, r:15, t:36, b:40},
    xaxis:{range:[dates[0], dates[dates.length - 1]], type:'date'},
    yaxis:{title:{text:`${s.label} (${s.unit})`}},
    legend:{orientation:'h', y:-0.18}, shapes,
    annotations: obsY.length + othY.length ? [] :
      [{text:`No data in the last ${DATA.spans[s.span].label}`, xref:'paper', yref:'paper', x:.5, y:.5, showarrow:false}],
  }, {responsive:true, displaylogo:false,
      modeBarButtons:[['zoom2d', 'autoScale2d', 'resetScale2d']]});
}
renderList();
</script>
</body></html>
"""


def render_dashboard(payload):
    data = json.dumps(payload, separators=(',', ':')).replace('</', '<\\/')
    return PAGE.replace('__DATA__', data)


# --------------------------------------------------------- inspect report --
CSS = """
body{font-family:system-ui,sans-serif;margin:24px;color:#222;background:#fff}
h1{font-size:20px} h2{font-size:16px;margin-top:28px;border-bottom:1px solid #ccc}
table{border-collapse:collapse;font-size:12px;margin:8px 0}
td,th{border:1px solid #ddd;padding:3px 6px;text-align:left}
th{background:#f3f3f3} .err{color:#b00020;font-weight:bold}
.mono{font-family:ui-monospace,monospace;font-size:12px}
"""


def df_html(df):
    return df.to_html(index=False, border=0, na_rep='NaN')


def inspect_page(client):
    t0 = time.time()
    parts = []
    try:
        groups = defaultdict(list)
        for b in client.list_blobs(BUCKET):
            groups[b.name.split('/')[0] if '/' in b.name else '(root)'].append(b)
        rows = []
        for top, blobs in sorted(groups.items()):
            names = sorted(b.name for b in blobs)
            newest = max(blobs, key=lambda b: b.updated)
            rows.append({'folder': top, 'objects': len(blobs),
                         'MB': round(sum(b.size for b in blobs) / 1e6, 1),
                         'newest upload (UTC)': f'{newest.updated:%Y-%m-%d %H:%M}',
                         'newest object': newest.name,
                         'first names': ', '.join(names[:3]),
                         'last names': ', '.join(names[-3:])})
        parts.append(f'<h2>1. Objects in gs://{esc(BUCKET)}</h2>' + df_html(pd.DataFrame(rows)))
        csvs = [b for bl in groups.values() for b in bl if b.name.endswith('.csv')]
        res_like = [b for b in csvs if 'LAKE_MEAD' in b.name.upper()]
        date_like = sorted((b for b in csvs if '/res_' in b.name), key=lambda b: b.name)
        parts.append('<h2>2. Sample file contents</h2>')
        for label, blob in [('Per-reservoir (LAKE_MEAD)', res_like[0] if res_like else None),
                            ('Newest per-date', date_like[-1] if date_like else None)]:
            parts.append(f'<h3>{esc(label)}</h3>')
            if blob is None:
                parts.append('<p class="err">Not found by name pattern.</p>')
                continue
            df = pd.read_csv(io.BytesIO(blob.download_as_bytes()), dtype={'qflag': str, 'datum': str})
            parts.append(f'<p class="mono">{esc(blob.name)} · rows={len(df)} · '
                         f'columns={esc(list(df.columns))}</p>'
                         + df_html(df.head(3)) + df_html(df.tail(3)))
    except Exception as e:
        parts.append(f'<p class="err">{esc(type(e).__name__)}: {esc(e)}</p>')
    body = (f'<!doctype html><html><head><meta charset="utf-8"><title>REVAP bucket inspection</title>'
            f'<style>{CSS}</style></head><body><h1>REVAP bucket inspection</h1>'
            f'<p>{time.time() - t0:.1f} s</p>' + ''.join(parts) + '</body></html>')
    return body


# ------------------------------------------------------------ entry point --
_client = None


def get_client():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client


@functions_framework.http
def dashboard(request):
    html_hdr = {'Content-Type': 'text/html; charset=utf-8', **NO_STORE}
    path = (request.path or '/').rstrip('/')
    client = get_client()
    if path.endswith('/inspect'):
        return inspect_page(client), 200, html_hdr
    try:
        if path.endswith('/data'):
            body, code = series_payload(client, request.args.get('name', ''),
                                        request.args.get('span', DEFAULT_PLOT_SPAN),
                                        request.args.get('var', DEFAULT_VAR))
            return json.dumps(body), code, {'Content-Type': 'application/json', **NO_STORE}
        return render_dashboard(build_payload(load_all(client))), 200, html_hdr
    except Exception as e:
        body = (f'<!doctype html><meta charset="utf-8"><h1>Dashboard error</h1>'
                f'<pre>{esc(type(e).__name__)}: {esc(e)}</pre>')
        return body, 500, html_hdr
