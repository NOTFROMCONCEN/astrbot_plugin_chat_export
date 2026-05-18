from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from astrbot.api import logger

from config import float_conf, int_conf
from utils import norm


class ChatDatabase:
    def __init__(self, db_file: Path, config: dict[str, Any]):
        self._db_file = db_file
        self._config = config
        self._conn: sqlite3.Connection | None = None
        self._last_error = ""
        self._write_ok = 0
        self._write_fail = 0
        self._dedup_skip = 0
        self._pending: list[dict[str, Any]] = []
        self._last_flush_ts = time.time()
        self._error_count = 0
        self._last_error_ts = 0.0
        self._max_log_line_length = int_conf(config, "max_log_line_length", 512)

    @property
    def pending(self) -> list[dict[str, Any]]:
        return self._pending

    @property
    def write_ok(self) -> int:
        return self._write_ok

    @property
    def write_fail(self) -> int:
        return self._write_fail

    @property
    def dedup_skip(self) -> int:
        return self._dedup_skip

    @property
    def last_error(self) -> str:
        return self._last_error

    def set_last_error(self, value: str) -> None:
        self._last_error = value

    @property
    def error_count(self) -> int:
        return self._error_count

    def add_write_fail(self, count: int) -> None:
        self._write_fail += count

    def extend_pending(self, records: list[dict[str, Any]]) -> None:
        self._pending.extend(records)

    def truncate_pending(self, count: int) -> list[dict[str, Any]]:
        dropped = self._pending[:count]
        del self._pending[:count]
        return dropped

    def _get_conn(self, reset: bool = False) -> sqlite3.Connection:
        if reset and self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

        if self._conn is None:
            conn = sqlite3.connect(
                self._db_file, timeout=5, check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            if bool(self._config.get("sqlite_wal", True)):
                conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA busy_timeout=5000")
            self._conn = conn
        return self._conn

    def init_schema(self) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              group_id TEXT NOT NULL,
              user_id TEXT,
              sender_name TEXT,
              content TEXT,
              media_json TEXT,
              message_id TEXT,
              unique_key TEXT
            )
            """
        )
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(chat_messages)").fetchall()
        }
        if "message_id" not in cols:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN message_id TEXT")
        if "media_json" not in cols:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN media_json TEXT")
        if "unique_key" not in cols:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN unique_key TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_ts ON chat_messages(ts)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_group_ts ON chat_messages(group_id, ts)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_chat_unique_key ON chat_messages(unique_key)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS history_sync_state (
              group_id TEXT PRIMARY KEY,
              cursor_json TEXT,
              updated_at TEXT
            )
            """
        )
        conn.commit()

    @property
    def conn_ready(self) -> bool:
        return self._conn is not None

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def flush_if_needed(self, enqueue_qdrant_fn: Any) -> list[dict[str, Any]] | None:
        if not self._pending:
            return None
        batch_size = max(1, int_conf(self._config, "sqlite_batch_size", 20))
        interval = max(0.2, float_conf(self._config, "sqlite_flush_interval_sec", 1.0))
        now = time.time()
        if (
            len(self._pending) < batch_size
            and (now - self._last_flush_ts) < interval
        ):
            return None
        inserted = self.flush(force=False)
        if inserted and enqueue_qdrant_fn is not None:
            enqueue_qdrant_fn(inserted)
        return inserted

    def flush(self, force: bool) -> list[dict[str, Any]]:
        if not self._pending:
            return []

        max_consecutive_errors = int_conf(self._config, "max_consecutive_errors", 5)
        error_cooldown_sec = float_conf(self._config, "error_cooldown_sec", 60.0)
        max_pending_size = int_conf(self._config, "max_pending_size", 5000)

        if self._error_count >= max_consecutive_errors:
            now = time.time()
            if now - self._last_error_ts < error_cooldown_sec:
                dropped = self._pending[: len(self._pending) // 2 or 1]
                self._write_fail += len(dropped)
                del self._pending[: len(dropped)]
                self._last_flush_ts = now
                self._last_error = f"sqlite cooldown: dropped {len(dropped)}"
                logger.warning(f"[chat_export] sqlite cooldown: dropped {len(dropped)} records")
                return []
            self._error_count = 0

        batch_size = max(1, int_conf(self._config, "sqlite_batch_size", 20))
        take = len(self._pending) if force else min(len(self._pending), batch_size)
        batch = self._pending[:take]
        del self._pending[:take]

        sql = (
            "INSERT OR IGNORE INTO chat_messages(ts, group_id, user_id, sender_name, content, media_json, message_id, unique_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        inserted: list[dict[str, Any]] = []
        duplicate_count = 0

        for retry in range(2):
            conn = None
            try:
                conn = self._get_conn(reset=(retry == 1))
                conn.execute("BEGIN")
                for rec in batch:
                    cur = conn.execute(
                        sql,
                        (
                            rec["ts"],
                            rec["group_id"],
                            rec["user_id"],
                            rec["sender_name"],
                            rec["content"],
                            rec.get("media_json", ""),
                            rec["message_id"],
                            rec["unique_key"],
                        ),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        inserted.append(rec)
                    else:
                        duplicate_count += 1
                conn.commit()
                break
            except Exception as e:
                self._last_error = f"sqlite_insert_batch: {e}"
                try:
                    if conn is not None:
                        conn.rollback()
                except Exception:
                    pass
                inserted = []
                duplicate_count = 0
                if retry == 0:
                    continue
                self._write_fail += len(batch)
                self._error_count += 1
                self._last_error_ts = time.time()
                err_text = norm(e)[: self._max_log_line_length]
                logger.error(f"[chat_export] sqlite 批量写入失败: {err_text}")
                self._pending = batch + self._pending
                self._last_flush_ts = time.time()
                return []

        self._error_count = 0
        self._write_ok += len(inserted)
        self._dedup_skip += duplicate_count
        if inserted:
            self._last_error = ""
        self._last_flush_ts = time.time()
        return inserted

    def query_messages(
        self, start_dt: datetime, end_dt: datetime, group_id: str
    ) -> list[tuple[Any, ...]]:
        sql = "SELECT ts, group_id, user_id, sender_name, content, media_json FROM chat_messages WHERE ts >= ? AND ts <= ?"
        args: list[Any] = [
            start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        ]
        if group_id:
            sql += " AND group_id = ?"
            args.append(group_id)
        sql += " ORDER BY ts ASC"
        for retry in range(2):
            try:
                conn = self._get_conn(reset=(retry == 1))
                return conn.execute(sql, tuple(args)).fetchall()
            except Exception as e:
                self._last_error = f"sqlite_query: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite query 失败: {e}")
                return []
        return []

    def query_messages_for_analysis(
        self,
        group_id: str,
        user_id: str,
        since_dt: datetime | None,
        limit: int,
    ) -> list[tuple[str, str, str, str, str]]:
        sql = (
            "SELECT ts, group_id, user_id, sender_name, content "
            "FROM chat_messages WHERE group_id = ?"
        )
        args: list[Any] = [group_id]
        if user_id:
            sql += " AND user_id = ?"
            args.append(user_id)
        if since_dt is not None:
            sql += " AND ts >= ?"
            args.append(since_dt.strftime("%Y-%m-%d %H:%M:%S"))
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(max(1, limit))
        for retry in range(2):
            try:
                conn = self._get_conn(reset=(retry == 1))
                rows = conn.execute(sql, tuple(args)).fetchall()
                rows.reverse()
                return rows
            except Exception as e:
                self._last_error = f"sqlite_query_analysis: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite analysis query 失败: {e}")
                return []
        return []

    def count(self, group_id: str = "") -> int:
        sql = "SELECT COUNT(1) FROM chat_messages"
        args: list[Any] = []
        if group_id:
            sql += " WHERE group_id = ?"
            args.append(group_id)
        for retry in range(2):
            try:
                conn = self._get_conn(reset=(retry == 1))
                row = conn.execute(sql, tuple(args)).fetchone()
                return int(row[0]) if row and row[0] is not None else 0
            except Exception as e:
                self._last_error = f"sqlite_count: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite count 失败: {e}")
                return 0
        return 0

    def latest_group_message_id(self, group_id: str) -> str:
        if not group_id:
            return ""
        sql = (
            "SELECT message_id FROM chat_messages "
            "WHERE group_id = ? AND message_id IS NOT NULL AND message_id != '' "
            "ORDER BY ts DESC, id DESC LIMIT 1"
        )
        for retry in range(2):
            try:
                conn = self._get_conn(reset=(retry == 1))
                row = conn.execute(sql, (group_id,)).fetchone()
                return norm(row[0]) if row and row[0] is not None else ""
            except Exception as e:
                self._last_error = f"sqlite_latest_message_id: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite latest message_id 查询失败: {e}")
                return ""
        return ""

    def load_history_cursor(self, group_id: str) -> dict[str, Any] | None:
        sql = "SELECT cursor_json FROM history_sync_state WHERE group_id = ?"
        for retry in range(2):
            try:
                conn = self._get_conn(reset=(retry == 1))
                row = conn.execute(sql, (group_id,)).fetchone()
                if not row or not row[0]:
                    return None
                obj = json.loads(row[0])
                return obj if isinstance(obj, dict) else None
            except Exception as e:
                self._last_error = f"history_cursor_load: {e}"
                if retry == 0:
                    continue
                logger.warning(f"[chat_export] 历史游标读取失败: {e}")
                return None
        return None

    def save_history_cursor(self, group_id: str, cursor: dict[str, Any]) -> None:
        try:
            payload = json.dumps(cursor, ensure_ascii=False)
        except Exception:
            return
        sql = (
            "INSERT INTO history_sync_state(group_id, cursor_json, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(group_id) DO UPDATE SET cursor_json=excluded.cursor_json, updated_at=excluded.updated_at"
        )
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for retry in range(2):
            try:
                conn = self._get_conn(reset=(retry == 1))
                conn.execute(sql, (group_id, payload, now_text))
                conn.commit()
                return
            except Exception as e:
                self._last_error = f"history_cursor_save: {e}"
                if retry == 0:
                    continue
                logger.warning(f"[chat_export] 历史游标保存失败: {e}")
                return
