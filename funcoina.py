# pipeline/run.py
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import httpx
import trafilatura
from bs4 import BeautifulSoup

from sources import SOURCES

HEADERS = {
    "User-Agent": "combat-news-probe/0.1 (personal project)",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

MIN_CHARS = 900
PREVIEW_CHARS = 900

# Filtros para MMA/Boxeo (v1 simple)
KEYWORDS_MMA = [
    "ufc", "mma", "topuria", "makhachev", "pereira", "volkanovski",
    "fight night", "octagono", "octágono",
]
KEYWORDS_BOX = [
    "boxeo", "wbc", "wba", "ibf", "wbo",
    "peso pluma", "peso welter", "peso medio",
    "canelo", "inoue", "tyson", "crawford", "usyk", "fury",
]

# Excluir secciones/URLs típicas que no son MMA/Boxeo
STOPWORDS_URL = [
    "/futbol/", "/tenis/", "/baloncesto/", "/ciclismo/", "/juegos-olimpicos/",
    "/snooker/", "/motor/", "/formula-1/", "/motogp/", "/golf/", "/rugby/",
    "/calendario", "/resultados", "/medallero", "/equipo", "/deportes/",
    "/suscrib", "/registro", "/inicio", "/ver", "/para-ti",
]


def sha(text: str) -> str:
    t = " ".join((text or "").lower().split())
    return hashlib.sha256(t[:5000].encode("utf-8")).hexdigest()


def fetch(url: str):
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=25) as c:
        r = c.get(url)
        return r.status_code, str(r.url), r.text


def extract_text(html: str) -> str:
    return (trafilatura.extract(html, include_tables=False, include_comments=False) or "").strip()


def looks_like_combat_sports(title: str, url: str) -> bool:
    s = f"{title} {url}".lower()

    if any(x in s for x in STOPWORDS_URL):
        return False

    return any(k in s for k in KEYWORDS_MMA) or any(k in s for k in KEYWORDS_BOX)


def get_urls_from_rss(feed_url: str, limit: int = 30) -> list[dict]:
    feed = feedparser.parse(feed_url)
    out = []
    for e in feed.entries[:limit]:
        out.append({
            "title": getattr(e, "title", "") or "",
            "url": getattr(e, "link", "") or "",
            "published": getattr(e, "published", "") or "",
        })
    return out


def get_urls_from_html_list(list_url: str, limit: int = 60) -> list[dict]:
    # extrae links del listado (best effort)
    status, final_url, html = fetch(list_url)
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = (a.get_text(" ", strip=True) or "").strip()

        if not href or len(text) < 18:
            continue

        # normaliza URLs relativas
        if href.startswith("/"):
            href = f"{urlparse(final_url).scheme}://{urlparse(final_url).netloc}{href}"

        if not href.startswith("http"):
            continue

        href_l = href.lower()

        # descarta assets
        if any(href_l.endswith(ext) for ext in (
            ".jpg", ".png", ".svg", ".css", ".js", ".webp", ".mp4", ".woff", ".woff2"
        )):
            continue

        # descarta navegación típica
        if any(x in href_l for x in (
            "/inicio", "/ver", "/para-ti", "/suscrib", "/registro",
            "/deportes", "/resultados", "/calendario", "/medallero", "/equipo"
        )):
            continue

        # heurística: link “profundo” (rutas largas suelen ser artículos)
        if href_l.count("/") < 4:
            continue

        candidates.append({"title": text, "url": href, "published": ""})

    # dedupe por url manteniendo orden
    seen = set()
    out = []
    for c in candidates:
        if c["url"] in seen:
            continue
        seen.add(c["url"])
        out.append(c)
        if len(out) >= limit:
            break

    return out


def write_html(rows: list[dict], out_path: str):
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("<!doctype html><meta charset='utf-8'><title>Probe</title>")
        f.write("<style>body{font-family:system-ui;margin:20px}.card{border:1px solid #ddd;border-radius:10px;padding:12px;margin:12px 0}.meta{color:#555;font-size:13px}pre{white-space:pre-wrap}</style>")
        f.write(f"<h1>Probe MMA/Boxeo</h1><p class='meta'>Generated: {datetime.now(timezone.utc).isoformat()} · Items: {len(rows)}</p>")
        for r in rows:
            ok = "✅" if r.get("extract_ok") else "⚠️"
            f.write("<div class='card'>")
            f.write(f"<h3>{ok} {esc(r.get('title',''))}</h3>")
            f.write(f"<div class='meta'>Source: {esc(r.get('source',''))} · Domain: {esc(r.get('domain',''))} · HTTP: {esc(str(r.get('http_status','')))} · Chars: {esc(str(r.get('extracted_chars','')))}</div>")
            f.write(f"<div class='meta'><a href='{esc(r.get('url',''))}' target='_blank'>Open</a> · Final: {esc(r.get('final_url',''))}</div>")
            if r.get("error"):
                f.write(f"<pre><b>Error:</b> {esc(r['error'])}</pre>")
            f.write(f"<pre>{esc(r.get('text_preview','') or '')}</pre>")
            f.write("</div>")


def main(out_dir="out", per_source_limit=30):
    import os
    os.makedirs(out_dir, exist_ok=True)

    rows = []

    for src in SOURCES:
        if src["type"] == "rss":
            items = get_urls_from_rss(src["url"], limit=per_source_limit)
        elif src["type"] == "html_list":
            items = get_urls_from_html_list(src["url"], limit=per_source_limit * 2)
        else:
            continue

        for it in items:
            title = it["title"]
            url = it["url"]

            # En esta v1 filtramos SIEMPRE por MMA/Boxeo
            if not looks_like_combat_sports(title, url):
                continue

            row = {
                "source": src["name"],
                "source_type": src["type"],
                "rss_or_list_url": src["url"],
                "title": title,
                "url": url,
                "published": it.get("published", ""),
            }

            try:
                status, final_url, html = fetch(url)
                text = extract_text(html)

                row.update({
                    "http_status": status,
                    "final_url": final_url,
                    "domain": urlparse(final_url).netloc,
                    "extracted_chars": len(text),
                    "extract_ok": len(text) >= MIN_CHARS,
                    "content_hash": sha(text) if text else "",
                    "text_preview": (text[:PREVIEW_CHARS] if text else ""),
                })

                if not row["extract_ok"]:
                    row["error"] = f"Low extracted chars (<{MIN_CHARS})."

            except Exception as ex:
                row.update({
                    "http_status": None,
                    "final_url": None,
                    "domain": None,
                    "extracted_chars": 0,
                    "extract_ok": False,
                    "content_hash": "",
                    "text_preview": "",
                    "error": repr(ex),
                })

            rows.append(row)

    # guarda JSON
    json_path = os.path.join(out_dir, "probe.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    # guarda HTML
    html_path = os.path.join(out_dir, "probe.html")
    write_html(rows, html_path)

    ok = sum(1 for r in rows if r.get("extract_ok"))
    print(f"Total: {len(rows)} | Extract OK (>= {MIN_CHARS} chars): {ok} | Fail: {len(rows)-ok}")


if __name__ == "__main__":
    main()
