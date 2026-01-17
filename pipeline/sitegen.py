# pipeline/sitegen.py
from datetime import datetime, timezone
import json

KEYWORDS_MMA = [
    "ufc", "mma", "topuria", "makhachev", "pereira", "volkanovski",
    "fight night", "octagono", "octágono",
]
KEYWORDS_BOX = [
    "boxeo", "wbc", "wba", "ibf", "wbo",
    "canelo", "inoue", "tyson", "crawford", "usyk", "fury",
]


def classify_sport(title: str, url: str, text: str) -> str:
    s = f"{title} {url} {text}".lower()
    mma = any(k in s for k in KEYWORDS_MMA)
    box = any(k in s for k in KEYWORDS_BOX)
    if mma and not box:
        return "MMA"
    if box and not mma:
        return "Boxeo"
    if mma and box:
        return "Mixto"
    return "Otro"


def build_index(rows, out_path: str):
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    items = []
    for r in rows:
        title = r["title"] or ""
        url = r["url"] or ""
        txt = r["text"] or ""
        sport = classify_sport(title, url, txt)
        items.append({
            "title": title,
            "url": url,
            "source": r["source"] or "",
            "domain": r["domain"] or "",
            "published": r["published"] or "",
            "fetched_at": r["fetched_at"] or "",
            "sport": sport,
            "preview": (txt[:280].replace("\n", " ").strip() if txt else ""),
        })

    items_json = json.dumps(items, ensure_ascii=False)

    # Usamos tokens únicos para reemplazo controlado (sin .format / f-string)
    html = """<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Combat News</title>
<style>
  body { font-family: system-ui, Arial; margin: 18px; }
  header { display:flex; flex-wrap: wrap; gap: 12px; align-items: baseline; }
  .meta { color:#555; font-size: 13px; }
  .controls { display:flex; gap:8px; flex-wrap:wrap; margin:12px 0; }
  input, select { padding:8px 10px; border:1px solid #ddd; border-radius:10px; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(320px,1fr)); gap:12px; }
  .card { border:1px solid #ddd; border-radius:14px; padding:12px; background:#fff; }
  .pill { display:inline-block; font-size:12px; padding:3px 8px; border:1px solid #eee; border-radius:999px; margin-right:6px; }
  h3 { margin: 10px 0 6px; font-size: 16px; line-height: 1.25; }
  a { color: inherit; }
</style>

<header>
  <h1 style="margin:0">Combat News</h1>
  <div class="meta">Generado: __GENERATED__ · Items: __COUNT__</div>
</header>

<div class="controls">
  <input id="q" placeholder="Buscar (título / dominio / fuente)..." style="flex:1; min-width: 240px;">
  <select id="sport">
    <option value="">Todos</option>
    <option value="MMA">MMA</option>
    <option value="Boxeo">Boxeo</option>
    <option value="Mixto">Mixto</option>
  </select>
  <select id="source">
    <option value="">Todas las fuentes</option>
  </select>
</div>

<div id="count" class="meta"></div>
<div id="grid" class="grid"></div>

<script>
const ITEMS = __ITEMS_JSON__;

const qEl = document.getElementById('q');
const sportEl = document.getElementById('sport');
const sourceEl = document.getElementById('source');
const gridEl = document.getElementById('grid');
const countEl = document.getElementById('count');

function uniq(arr) {
  return Array.from(new Set(arr)).sort();
}

function initSources() {
  const sources = uniq(ITEMS.map(x => x.source).filter(Boolean));
  for (const s of sources) {
    const opt = document.createElement('option');
    opt.value = s;
    opt.textContent = s;
    sourceEl.appendChild(opt);
  }
}

function escapeHtml(s) {
  return (s || '')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;');
}

function render() {
  const q = (qEl.value || '').toLowerCase().trim();
  const sport = sportEl.value;
  const source = sourceEl.value;

  const filtered = ITEMS.filter(x => {
    if (sport && x.sport !== sport) return false;
    if (source && x.source !== source) return false;
    if (!q) return true;
    const hay = (x.title + ' ' + x.domain + ' ' + x.source).toLowerCase();
    return hay.includes(q);
  });

  countEl.textContent = `Mostrando ${filtered.length} de ${ITEMS.length}`;
  gridEl.innerHTML = '';

  for (const x of filtered) {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
      <div>
        <span class="pill">${escapeHtml(x.sport)}</span>
        <span class="pill">${escapeHtml(x.source)}</span>
        <span class="pill">${escapeHtml(x.domain)}</span>
      </div>
      <h3><a href="${x.url}" target="_blank" rel="noreferrer">${escapeHtml(x.title)}</a></h3>
      <div class="meta">Publicado: ${escapeHtml(x.published || '—')} · Capturado: ${escapeHtml(x.fetched_at || '—')}</div>
      <p class="meta" style="margin-top:10px">${escapeHtml(x.preview || '')}</p>
    `;
    gridEl.appendChild(div);
  }
}

qEl.addEventListener('input', render);
sportEl.addEventListener('change', render);
sourceEl.addEventListener('change', render);

initSources();
render();
</script>
"""

    html = html.replace("__GENERATED__", generated)
    html = html.replace("__COUNT__", str(len(items)))
    html = html.replace("__ITEMS_JSON__", items_json)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
