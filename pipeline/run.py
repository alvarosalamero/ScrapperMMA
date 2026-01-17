# pipeline/run.py
import json
import hashlib
from dataclasses import asdict
from urllib.parse import urlparse
from datetime import datetime, timezone

import feedparser
import httpx
import trafilatura
from bs4 import BeautifulSoup

from sources import SOURCES
from storage import SQLiteStore, ArticleRow
from sitegen import build_index

HEADERS = {
    "User-Agent": "combat-news-probe/0.1 (personal project)",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

MIN_CHARS = 900
PREVIEW_CHARS = 900

KEYWORDS_MMA = [
    "ufc", "mma", "topuria", "makhachev", "pereira", "volkanovski",
    "fight night", "octagono", "octágono",
]
KEYWORDS_BOX = [
    "boxeo", "wbc", "wba", "ibf", "wbo",
    "peso pluma", "peso welter", "peso medio",
    "canelo", "inoue", "tyson", "crawford", "usyk", "fury",
]
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
    status, final_url, html = fetch(list_url)
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = (a.get_text(" ", strip=True) or "").strip()

        if not href or len(text) < 18:
            continue

        if href.startswith("/"):
            href = f"{urlparse(final_url).scheme}://{urlparse(final_url).netloc}{href}"
        if not href.startswith("http"):
            continue

        href_l = href.lower()

        # assets
        if any(href_l.endswith(ext) for ext in (
            ".jpg", ".png", ".svg", ".css", ".js", ".webp", ".mp4", ".woff", ".woff2"
        )):
            continue

        # navegación
        if any(x in href_l for x in (
            "/inicio", "/ver", "/para-ti", "/suscrib", "/registro",
            "/deportes", "/resultados", "/calendario", "/medallero", "/equipo"
        )):
            continue

        if href_l.count("/") < 4:
            continue

        candidates.append({"title": text, "url": href, "published": ""})

    # dedupe
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


def main(db_path="data/news.db", out_dir="out", site_dir="site", per_source_limit=30):
    import os
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(site_dir, exist_ok=True)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    store = SQLiteStore(db_path=db_path)

    run_id = SQLiteStore.now_utc_iso()
    started_at = run_id

    rows_probe = []
    total_candidates = 0
    stored_new = 0
    stored_updated = 0
    skipped_existing = 0
    extract_ok = 0

    for src in SOURCES:
        if src["type"] == "rss":
            items = get_urls_from_rss(src["url"], limit=per_source_limit)
        elif src["type"] == "html_list":
            items = get_urls_from_html_list(src["url"], limit=per_source_limit * 2)
        else:
            continue

        for it in items:
            total_candidates += 1
            title = it["title"]
            url = it["url"]

            # Filtrado MMA/Boxeo
            if not looks_like_combat_sports(title, url):
                continue

            # Dedup rápido por URL antes de fetchear (ahorra requests)
            if store.has_url(url):
                skipped_existing += 1
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

                domain = urlparse(final_url).netloc
                fetched_at = SQLiteStore.now_utc_iso()

                row.update({
                    "http_status": status,
                    "final_url": final_url,
                    "domain": domain,
                    "extracted_chars": len(text),
                    "extract_ok": len(text) >= MIN_CHARS,
                    "content_hash": sha(text) if text else "",
                    "text_preview": (text[:PREVIEW_CHARS] if text else ""),
                })

                if row["extract_ok"]:
                    extract_ok += 1
                    art = ArticleRow(
                        url=url,
                        final_url=final_url,
                        title=title,
                        source=src["name"],
                        published=row["published"],
                        domain=domain,
                        fetched_at=fetched_at,
                        extracted_chars=len(text),
                        content_hash=row["content_hash"],
                        text=text,
                    )
                    res = store.upsert_article(art)
                    if res == "new":
                        stored_new += 1
                    elif res == "updated":
                        stored_updated += 1
                else:
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

            rows_probe.append(row)

    finished_at = SQLiteStore.now_utc_iso()
    store.record_run(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        total_candidates=total_candidates,
        stored_new=stored_new,
        stored_updated=stored_updated,
        skipped_existing=skipped_existing,
        extract_ok=extract_ok,
    )

    # Debug JSON
    with open(os.path.join(out_dir, "probe.json"), "w", encoding="utf-8") as f:
        json.dump(rows_probe, f, ensure_ascii=False, indent=2)

    # Genera site desde DB (recientes)
    recent = store.list_recent(days=14, limit=2000)
    build_index(recent, out_path=os.path.join(site_dir, "index.html"))

    print(
        f"Candidates: {total_candidates} | New: {stored_new} | Updated: {stored_updated} | "
        f"Skipped(existing URL): {skipped_existing} | Extract OK: {extract_ok}"
    )
    print(f"DB: {db_path} | Site: {site_dir}/index.html | Probe: {out_dir}/probe.json")


if __name__ == "__main__":
    main()
