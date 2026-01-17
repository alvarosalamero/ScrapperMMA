# pipeline/storage.py
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class ArticleRow:
    url: str
    final_url: str
    title: str
    source: str
    published: str
    domain: str
    fetched_at: str
    extracted_chars: int
    content_hash: str
    text: str


class SQLiteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                final_url TEXT,
                title TEXT,
                source TEXT,
                published TEXT,
                domain TEXT,
                fetched_at TEXT,
                extracted_chars INTEGER,
                content_hash TEXT,
                text TEXT
            )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_fetched_at ON articles(fetched_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(content_hash)")

            conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                finished_at TEXT,
                total_candidates INTEGER,
                stored_new INTEGER,
                stored_updated INTEGER,
                skipped_existing INTEGER,
                extract_ok INTEGER
            )
            """)

    def has_url(self, url: str) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT 1 FROM articles WHERE url = ? LIMIT 1", (url,)).fetchone()
            return row is not None

    def get_by_url(self, url: str) -> Optional[sqlite3.Row]:
        with self._conn() as conn:
            return conn.execute("SELECT * FROM articles WHERE url = ?", (url,)).fetchone()

    def upsert_article(self, a: ArticleRow) -> str:
        """
        Returns: "new" | "updated" | "unchanged"
        """
        existing = self.get_by_url(a.url)
        if existing is None:
            with self._conn() as conn:
                conn.execute("""
                INSERT INTO articles (url, final_url, title, source, published, domain, fetched_at,
                                      extracted_chars, content_hash, text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (a.url, a.final_url, a.title, a.source, a.published, a.domain, a.fetched_at,
                      a.extracted_chars, a.content_hash, a.text))
            return "new"

        # Si no cambió el contenido, no hacemos nada
        if (existing["content_hash"] or "") == (a.content_hash or "") and (existing["extracted_chars"] or 0) == a.extracted_chars:
            return "unchanged"

        with self._conn() as conn:
            conn.execute("""
            UPDATE articles
            SET final_url = ?, title = ?, source = ?, published = ?, domain = ?, fetched_at = ?,
                extracted_chars = ?, content_hash = ?, text = ?
            WHERE url = ?
            """, (a.final_url, a.title, a.source, a.published, a.domain, a.fetched_at,
                  a.extracted_chars, a.content_hash, a.text, a.url))
        return "updated"

    def record_run(self, run_id: str, started_at: str, finished_at: str,
                   total_candidates: int, stored_new: int, stored_updated: int,
                   skipped_existing: int, extract_ok: int):
        with self._conn() as conn:
            conn.execute("""
            INSERT OR REPLACE INTO runs (
                run_id, started_at, finished_at, total_candidates, stored_new,
                stored_updated, skipped_existing, extract_ok
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (run_id, started_at, finished_at, total_candidates, stored_new,
                  stored_updated, skipped_existing, extract_ok))

    def list_recent(self, days: int = 14, limit: int = 2000):
        """
        Devuelve artículos recientes ordenados por fetched_at desc.
        """
        with self._conn() as conn:
            # fetched_at es ISO8601 UTC, ordena bien en texto
            return conn.execute("""
            SELECT * FROM articles
            ORDER BY fetched_at DESC
            LIMIT ?
            """, (limit,)).fetchall()

    @staticmethod
    def now_utc_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
