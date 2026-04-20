from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import request

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_chat_export",
    "NOTFROMCONCEN",
    "监听群消息并按时间范围导出聊天记录为 TXT，支持 Qdrant 语义检索",
    "1.3.7",
)
class ChatExportPlugin(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.config = config or {}

        self.plugin_dir = Path(__file__).resolve().parent
        self.data_dir = self._resolve_data_dir(self.config.get("data_dir", ""))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.db_file = self._resolve_path(self.config.get("db_path", "chat_export.db"), self.data_dir)
        self.export_dir = self._resolve_path(self.config.get("export_dir", "exports"), self.data_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

        self._startup_time = datetime.now()
        self._sqlite_conn: sqlite3.Connection | None = None
        self._listening_groups_cache: set[str] = set()
        self._listening_groups_sig = ""
        self._admin_ids_cache: set[str] = set()
        self._admin_ids_sig = ""
        self._init_db()
        self._qdrant_client = None
        self._qdrant_models = None
        self._sqlite_write_ok = 0
        self._sqlite_write_fail = 0
        self._sqlite_dedup_skip = 0
        self._qdrant_write_ok = 0
        self._qdrant_write_fail = 0
        self._received_group_events = 0
        self._last_sqlite_ok_ts = ""
        self._last_qdrant_ok_ts = ""
        self._last_error = ""
        self._sqlite_pending: list[dict[str, Any]] = []
        self._last_sqlite_flush_ts = time.time()
        self._qdrant_pending: list[dict[str, Any]] = []
        self._last_qdrant_flush_ts = time.time()
        self._init_qdrant_if_needed()
        self._log_startup_summary()

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        self._received_group_events += 1
        if not self._is_enabled():
            self._log_verbose("skip message: plugin disabled")
            return

        group_id = self._norm(event.get_group_id())
        if not group_id:
            self._log_verbose("skip message: empty group_id")
            return
        if not self._is_listening_group(group_id):
            self._log_verbose(f"skip message: group {group_id} not in listening_group_ids")
            return

        user_id = self._norm(event.get_sender_id())
        sender_name = self._norm(getattr(event, "get_sender_name", lambda: "")())
        content = self._extract_text(event)
        message_time = self._event_time(event)
        message_id = self._extract_message_id(event)
        unique_key = self._build_unique_key(group_id, user_id, message_time, content, message_id)

        record = {
            "ts": message_time.strftime("%Y-%m-%d %H:%M:%S"),
            "group_id": group_id,
            "user_id": user_id,
            "sender_name": sender_name,
            "content": content,
            "message_id": message_id,
            "unique_key": unique_key,
        }

        self._sqlite_pending.append(record)
        self._flush_sqlite_queue_if_needed()
        self._flush_qdrant_queue_if_needed()

        self._log_ingest_progress(group_id, user_id, content)

        # 可选：采集完成后终止传播，实现“只记录不回复”
        if self._stop_event_after_ingest():
            event.stop_event()
            self._log_verbose(f"stop_event_after_ingest: group={group_id}")

    @filter.command("聊天导出")
    async def export_chat_cn(self, event: AstrMessageEvent):
        async for result in self._handle_export(event):
            yield result

    @filter.command("chat_export")
    async def export_chat_en(self, event: AstrMessageEvent):
        async for result in self._handle_export(event):
            yield result

    @filter.command("聊天检索")
    async def semantic_search_cn(self, event: AstrMessageEvent):
        async for result in self._handle_semantic_search(event):
            yield result

    @filter.command("chat_search")
    async def semantic_search_en(self, event: AstrMessageEvent):
        async for result in self._handle_semantic_search(event):
            yield result

    @filter.command("聊天监听")
    async def manage_listen_cn(self, event: AstrMessageEvent):
        async for result in self._handle_listen_manage(event):
            yield result

    @filter.command("chat_listen")
    async def manage_listen_en(self, event: AstrMessageEvent):
        async for result in self._handle_listen_manage(event):
            yield result

    @filter.command("聊天统计")
    async def stats_cn(self, event: AstrMessageEvent):
        async for result in self._handle_stats(event):
            yield result

    @filter.command("chat_stats")
    async def stats_en(self, event: AstrMessageEvent):
        async for result in self._handle_stats(event):
            yield result

    @filter.command("聊天健康")
    async def health_cn(self, event: AstrMessageEvent):
        async for result in self._handle_health(event):
            yield result

    @filter.command("chat_health")
    async def health_en(self, event: AstrMessageEvent):
        async for result in self._handle_health(event):
            yield result

    async def _handle_export(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行导出")
            return

        self._flush_all_queues(force=True)

        tokens = [t for t in (event.message_str or "").strip().split() if t]
        if len(tokens) < 3:
            yield event.plain_result(
                "用法: /聊天导出 <开始时间> <结束时间> [群号]\n"
                "示例: /聊天导出 2026-04-17T00:00:00 2026-04-17T23:59:59 123456"
            )
            return

        start_s, end_s, group_id = self._parse_export_args(tokens, event)
        if not start_s or not end_s:
            yield event.plain_result(
                "参数格式错误。用法: /聊天导出 <开始时间> <结束时间> [群号]\n"
                "示例1: /聊天导出 2026-04-17T00:00:00 2026-04-17T23:59:59 123456\n"
                "示例2: /聊天导出 2026-04-17 00:00:00 2026-04-17 23:59:59 123456"
            )
            return

        start_dt = self._parse_dt(start_s)
        end_dt = self._parse_dt(end_s)
        if not start_dt or not end_dt:
            yield event.plain_result("时间格式错误，支持: YYYY-MM-DDTHH:MM:SS / YYYY-MM-DD_HH:MM:SS / YYYY-MM-DD HH:MM:SS")
            return

        if end_dt < start_dt:
            yield event.plain_result("结束时间不能早于开始时间")
            return

        rows = self._query_messages(start_dt, end_dt, group_id)
        if not rows:
            yield event.plain_result("该时间范围内没有聊天记录")
            return

        safe_group = group_id or "all"
        out_name = f"chat_{safe_group}_{start_dt.strftime('%Y%m%d%H%M%S')}_{end_dt.strftime('%Y%m%d%H%M%S')}.txt"
        out_file = self.export_dir / out_name

        with out_file.open("w", encoding="utf-8") as f:
            for ts, gid, uid, uname, text in rows:
                f.write(f"[{ts}] [群:{gid}] [{uname or uid}] {text}\n")

        yield event.plain_result(f"导出完成，共 {len(rows)} 条\n文件: {out_file}")

    async def _handle_semantic_search(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行检索")
            return

        if not self._qdrant_enabled():
            yield event.plain_result("Qdrant 未启用，请在插件配置里开启 qdrant_enabled")
            return

        if not self._qdrant_client or not self._qdrant_models:
            yield event.plain_result("Qdrant 初始化失败，请检查 qdrant_url / qdrant_api_key / qdrant_client 依赖")
            return

        self._flush_all_queues(force=True)

        tokens = [t for t in (event.message_str or "").strip().split() if t]
        if len(tokens) < 2:
            yield event.plain_result("用法: /聊天检索 [群号] [最近1小时|recent:2h] <问题>")
            return

        group_id, query_text, since_dt = self._parse_search_args(tokens, event)

        if not query_text:
            yield event.plain_result("检索内容不能为空")
            return

        vector = self._embedding(query_text)
        if not vector:
            yield event.plain_result("向量化失败，请检查 embedding_api_base / embedding_api_key / embedding_model")
            return

        limit = self._int_conf("search_top_k", 5)
        points = self._search_qdrant(vector, group_id, limit, since_dt)
        if not points:
            yield event.plain_result("未检索到相关聊天记录")
            return

        lines = [f"检索结果（Top {len(points)}）:"]
        for idx, p in enumerate(points, start=1):
            payload = self._point_payload(p)
            ts = self._norm(payload.get("ts"))
            gid = self._norm(payload.get("group_id"))
            uname = self._norm(payload.get("sender_name")) or self._norm(payload.get("user_id"))
            text = self._norm(payload.get("content"))
            lines.append(f"{idx}. [{ts}] [群:{gid}] [{uname}] {text}")

        yield event.plain_result("\n".join(lines))

    async def _handle_listen_manage(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行监听管理")
            return

        tokens = [t for t in (event.message_str or "").strip().split() if t]
        if len(tokens) < 2:
            yield event.plain_result(
                "用法: /聊天监听 <开始|停止|状态> [群号]\n"
                "示例: /聊天监听 开始 123456"
            )
            return

        action = self._norm(tokens[1]).lower()
        group_id = self._norm(tokens[2]) if len(tokens) >= 3 else self._norm(event.get_group_id())
        listening = set(self._listening_groups())

        if action in {"开始", "start", "on", "开启"}:
            if not group_id:
                yield event.plain_result("请提供群号：/聊天监听 开始 <群号>")
                return
            listening.add(group_id)
            self.config["listening_group_ids"] = sorted(listening)
            self._listening_groups_sig = ""
            self._save_config()
            yield event.plain_result(f"已开始监听群: {group_id}")
            return

        if action in {"停止", "stop", "off", "关闭"}:
            if not group_id:
                yield event.plain_result("请提供群号：/聊天监听 停止 <群号>")
                return
            listening.discard(group_id)
            self.config["listening_group_ids"] = sorted(listening)
            self._listening_groups_sig = ""
            self._save_config()
            yield event.plain_result(f"已停止监听群: {group_id}")
            return

        if action in {"状态", "status"}:
            if group_id:
                state = "监听中" if group_id in listening else "未监听"
                yield event.plain_result(f"群 {group_id} 当前状态: {state}")
                return
            groups = ", ".join(sorted(listening)) or "无"
            yield event.plain_result(f"当前监听群: {groups}")
            return

        yield event.plain_result("未知操作。用法: /聊天监听 <开始|停止|状态> [群号]")

    async def _handle_stats(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行统计")
            return

        self._flush_all_queues(force=True)

        tokens = [t for t in (event.message_str or "").strip().split() if t]
        group_id = self._norm(tokens[1]) if len(tokens) >= 2 else self._norm(event.get_group_id())

        sqlite_total = self._count_sqlite()
        sqlite_group = self._count_sqlite(group_id) if group_id else sqlite_total

        qdrant_total: int | None = None
        qdrant_group: int | None = None
        qdrant_error = ""
        if self._qdrant_enabled() and self._qdrant_client:
            try:
                qdrant_total = self._count_qdrant("")
                qdrant_group = self._count_qdrant(group_id) if group_id else qdrant_total
            except Exception as e:
                qdrant_error = self._norm(e)

        listening = ", ".join(sorted(self._listening_groups())) or "无"
        lines = [
            "[聊天统计]",
            f"- data_dir: {self.data_dir}",
            f"- sqlite_db: {self.db_file}",
            f"- listening_groups: {listening}",
            f"- sqlite_total: {sqlite_total}",
            f"- sqlite_group({group_id or 'all'}): {sqlite_group}",
            f"- runtime_received_group_events: {self._received_group_events}",
            f"- runtime_sqlite_ok/fail/dedup: {self._sqlite_write_ok}/{self._sqlite_write_fail}/{self._sqlite_dedup_skip}",
            f"- runtime_qdrant_ok/fail: {self._qdrant_write_ok}/{self._qdrant_write_fail}",
            f"- queue_sqlite: {len(self._sqlite_pending)}",
            f"- queue_qdrant: {len(self._qdrant_pending)}",
        ]

        if not self._qdrant_enabled():
            lines.append("- qdrant: 未启用")
        elif not self._qdrant_client:
            lines.append("- qdrant: 未初始化")
        else:
            lines.append(f"- qdrant_total: {qdrant_total if qdrant_total is not None else 'unknown'}")
            lines.append(f"- qdrant_group({group_id or 'all'}): {qdrant_group if qdrant_group is not None else 'unknown'}")
            if qdrant_error:
                lines.append(f"- qdrant_error: {qdrant_error}")

        if self._received_group_events == 0 and self._listening_groups():
            lines.append("- hint: 当前监听群非空但未收到事件，检查是否有其他插件提前 stop_event（如 force_silent 硬静默模式）")

        yield event.plain_result("\n".join(lines))

    async def _handle_health(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行健康检查")
            return

        self._flush_all_queues(force=True)
        mins = max((datetime.now() - self._startup_time).total_seconds() / 60.0, 1e-6)
        eps = self._received_group_events / mins
        lines = [
            "[聊天健康]",
            f"- uptime_minutes: {mins:.2f}",
            f"- events_per_min: {eps:.2f}",
            f"- sqlite_conn_ready: {self._sqlite_conn is not None}",
            f"- sqlite_batch_size: {self._int_conf('sqlite_batch_size', 20)}",
            f"- sqlite_flush_interval_sec: {self._float_conf('sqlite_flush_interval_sec', 1.0)}",
            f"- last_sqlite_ok: {self._last_sqlite_ok_ts or 'none'}",
            f"- last_qdrant_ok: {self._last_qdrant_ok_ts or 'none'}",
            f"- last_error: {self._last_error or 'none'}",
            f"- stop_event_after_ingest: {self._stop_event_after_ingest()}",
            f"- index_media_placeholders: {self._index_media_placeholders()}",
            f"- sqlite_pending: {len(self._sqlite_pending)}",
            f"- qdrant_pending: {len(self._qdrant_pending)}",
        ]
        yield event.plain_result("\n".join(lines))

    def _init_db(self):
        conn = self._get_db_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              group_id TEXT NOT NULL,
              user_id TEXT,
              sender_name TEXT,
              content TEXT,
              message_id TEXT,
              unique_key TEXT
            )
            """
        )
        cols = {row[1] for row in conn.execute("PRAGMA table_info(chat_messages)").fetchall()}
        if "message_id" not in cols:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN message_id TEXT")
        if "unique_key" not in cols:
            conn.execute("ALTER TABLE chat_messages ADD COLUMN unique_key TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_ts ON chat_messages(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_group_ts ON chat_messages(group_id, ts)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_chat_unique_key ON chat_messages(unique_key)")
        conn.commit()

    def _get_db_conn(self, reset: bool = False) -> sqlite3.Connection:
        if reset and self._sqlite_conn is not None:
            try:
                self._sqlite_conn.close()
            except Exception:
                pass
            self._sqlite_conn = None

        if self._sqlite_conn is None:
            conn = sqlite3.connect(self.db_file, timeout=5, check_same_thread=False)
            if bool(self.config.get("sqlite_wal", True)):
                conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA busy_timeout=5000")
            self._sqlite_conn = conn
        return self._sqlite_conn

    def _flush_all_queues(self, force: bool):
        inserted = self._flush_sqlite_queue(force=force)
        if inserted:
            self._enqueue_qdrant_records(inserted)
        if force:
            self._flush_qdrant_queue(force=True)

    def _enqueue_qdrant_records(self, records: list[dict[str, Any]]):
        if not records or not self._qdrant_enabled():
            return
        for rec in records:
            if self._should_index_to_qdrant(self._norm(rec.get("content"))):
                self._qdrant_pending.append(rec)

    def _flush_sqlite_queue_if_needed(self):
        if not self._sqlite_pending:
            return
        batch_size = max(1, self._int_conf("sqlite_batch_size", 20))
        interval = max(0.2, self._float_conf("sqlite_flush_interval_sec", 1.0))
        now = time.time()
        if len(self._sqlite_pending) < batch_size and (now - self._last_sqlite_flush_ts) < interval:
            return
        inserted = self._flush_sqlite_queue(force=False)
        if inserted:
            self._enqueue_qdrant_records(inserted)

    def _flush_sqlite_queue(self, force: bool) -> list[dict[str, Any]]:
        if not self._sqlite_pending:
            return []

        batch_size = max(1, self._int_conf("sqlite_batch_size", 20))
        take = len(self._sqlite_pending) if force else min(len(self._sqlite_pending), batch_size)
        batch = self._sqlite_pending[:take]
        del self._sqlite_pending[:take]

        sql = (
            "INSERT OR IGNORE INTO chat_messages(ts, group_id, user_id, sender_name, content, message_id, unique_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        inserted: list[dict[str, Any]] = []
        duplicate_count = 0

        for retry in range(2):
            conn = None
            try:
                conn = self._get_db_conn(reset=(retry == 1))
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
                self._sqlite_write_fail += len(batch)
                logger.error(f"[chat_export] sqlite 批量写入失败: {e}")
                self._sqlite_pending = batch + self._sqlite_pending
                self._last_sqlite_flush_ts = time.time()
                return []

        self._sqlite_write_ok += len(inserted)
        self._sqlite_dedup_skip += duplicate_count
        if inserted:
            self._last_sqlite_ok_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._last_sqlite_flush_ts = time.time()
        return inserted

    def _query_messages(self, start_dt: datetime, end_dt: datetime, group_id: str):
        sql = "SELECT ts, group_id, user_id, sender_name, content FROM chat_messages WHERE ts >= ? AND ts <= ?"
        args: list[Any] = [start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")]

        if group_id:
            sql += " AND group_id = ?"
            args.append(group_id)

        sql += " ORDER BY ts ASC"

        for retry in range(2):
            try:
                conn = self._get_db_conn(reset=(retry == 1))
                return conn.execute(sql, tuple(args)).fetchall()
            except Exception as e:
                self._last_error = f"sqlite_query: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite query 失败: {e}")
                return []
        return []

    def _count_sqlite(self, group_id: str = "") -> int:
        sql = "SELECT COUNT(1) FROM chat_messages"
        args: list[Any] = []
        if group_id:
            sql += " WHERE group_id = ?"
            args.append(group_id)
        for retry in range(2):
            try:
                conn = self._get_db_conn(reset=(retry == 1))
                row = conn.execute(sql, tuple(args)).fetchone()
                return int(row[0]) if row and row[0] is not None else 0
            except Exception as e:
                self._last_error = f"sqlite_count: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite count 失败: {e}")
                return 0
        return 0

    def _init_qdrant_if_needed(self):
        if not self._qdrant_enabled():
            return

        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http import models
        except Exception as e:
            logger.warning(f"[chat_export] qdrant_client 未安装或导入失败: {e}")
            return

        try:
            self._qdrant_client = QdrantClient(
                url=self._norm(self.config.get("qdrant_url", "http://127.0.0.1:6333")),
                api_key=self._norm(self.config.get("qdrant_api_key", "")) or None,
                timeout=self._int_conf("qdrant_timeout", 10),
            )
            self._qdrant_models = models
            self._ensure_collection()
            logger.info("[chat_export] qdrant initialized")
        except Exception as e:
            logger.error(f"[chat_export] qdrant 初始化失败: {e}")
            self._qdrant_client = None
            self._qdrant_models = None

    def _ensure_collection(self):
        if not self._qdrant_client or not self._qdrant_models:
            return

        collection = self._norm(self.config.get("qdrant_collection", "chat_export"))
        dim = self._int_conf("embedding_dimension", 1536)
        distance_name = self._norm(self.config.get("qdrant_distance", "Cosine")).upper()
        distance_map = {
            "COSINE": self._qdrant_models.Distance.COSINE,
            "DOT": self._qdrant_models.Distance.DOT,
            "EUCLID": self._qdrant_models.Distance.EUCLID,
        }
        distance = distance_map.get(distance_name, self._qdrant_models.Distance.COSINE)

        try:
            self._qdrant_client.get_collection(collection_name=collection)
            return
        except Exception:
            pass

        try:
            self._qdrant_client.create_collection(
                collection_name=collection,
                vectors_config=self._qdrant_models.VectorParams(size=dim, distance=distance),
            )
            return
        except Exception as e:
            logger.warning(f"[chat_export] qdrant sdk 建集合失败，尝试 HTTP 回退: {e}")

        self._create_collection_via_http(collection, dim, distance_name)

    def _create_collection_via_http(self, collection: str, dim: int, distance_name: str):
        base = self._norm(self.config.get("qdrant_url", "http://127.0.0.1:6333")).rstrip("/")
        api_key = self._norm(self.config.get("qdrant_api_key", ""))
        url = f"{base}/collections/{collection}"

        distance_http = {
            "COSINE": "Cosine",
            "DOT": "Dot",
            "EUCLID": "Euclid",
        }.get(distance_name, "Cosine")

        # 兼容不同 Qdrant 版本: 先尝试单向量格式，再尝试命名向量格式
        bodies = [
            {"vectors": {"size": int(dim), "distance": distance_http}},
            {"vectors": {"default": {"size": int(dim), "distance": distance_http}}},
        ]

        last_err = None
        for payload in bodies:
            body = json.dumps(payload).encode("utf-8")
            req = request.Request(url=url, data=body, method="PUT")
            req.add_header("Content-Type", "application/json")
            if api_key:
                req.add_header("api-key", api_key)
            try:
                with request.urlopen(req, timeout=self._int_conf("qdrant_timeout", 10)) as _:
                    return
            except Exception as e:
                last_err = e

        raise RuntimeError(f"qdrant HTTP 建集合失败: {last_err}")

    def _flush_qdrant_queue_if_needed(self):
        if not self._qdrant_pending:
            return
        if not self._qdrant_enabled() or not self._qdrant_client or not self._qdrant_models:
            return

        batch_size = max(1, self._int_conf("qdrant_batch_size", 20))
        interval = max(0.2, self._float_conf("qdrant_flush_interval_sec", 1.0))
        now = time.time()
        if len(self._qdrant_pending) < batch_size and (now - self._last_qdrant_flush_ts) < interval:
            return
        self._flush_qdrant_queue(force=False)

    def _flush_qdrant_queue(self, force: bool):
        if not self._qdrant_pending:
            return
        if not self._qdrant_enabled() or not self._qdrant_client or not self._qdrant_models:
            return

        batch_size = max(1, self._int_conf("qdrant_batch_size", 20))
        take = len(self._qdrant_pending) if force else min(len(self._qdrant_pending), batch_size)
        batch = self._qdrant_pending[:take]
        del self._qdrant_pending[:take]

        texts = [r["content"] for r in batch]
        vectors = self._embedding_batch(texts)
        if len(vectors) != len(batch):
            self._qdrant_write_fail += len(batch)
            self._last_error = "embedding_batch_size_mismatch"
            return

        collection = self._norm(self.config.get("qdrant_collection", "chat_export"))
        points = []
        for rec, vec in zip(batch, vectors):
            if not vec:
                self._qdrant_write_fail += 1
                continue
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, rec["unique_key"]))
            payload = {
                "ts": rec["ts"],
                "group_id": rec["group_id"],
                "user_id": rec["user_id"],
                "sender_name": rec["sender_name"],
                "content": rec["content"],
                "message_id": rec["message_id"],
                "unique_key": rec["unique_key"],
            }
            points.append(self._qdrant_models.PointStruct(id=point_id, vector=vec, payload=payload))

        if not points:
            self._last_qdrant_flush_ts = time.time()
            return

        try:
            self._qdrant_client.upsert(collection_name=collection, points=points, wait=False)
            self._qdrant_write_ok += len(points)
            self._last_qdrant_ok_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self._qdrant_write_fail += len(points)
            self._last_error = f"qdrant_upsert: {e}"
            logger.warning(f"[chat_export] qdrant upsert 失败: {e}")
            # 失败回队列头，避免丢数据
            self._qdrant_pending = batch + self._qdrant_pending
        finally:
            self._last_qdrant_flush_ts = time.time()

    def _search_qdrant(self, query_vector: list[float], group_id: str, limit: int, since_dt: datetime | None):
        if not self._qdrant_client or not self._qdrant_models:
            return []

        collection = self._norm(self.config.get("qdrant_collection", "chat_export"))
        q_filter = self._build_qdrant_filter(group_id, since_dt)

        top_k = max(1, min(limit, 20))

        # 兼容 qdrant-client 新旧 API：
        # - 旧版: client.search(...)
        # - 新版: client.query_points(...)
        try:
            if hasattr(self._qdrant_client, "query_points"):
                try:
                    result = self._qdrant_client.query_points(
                        collection_name=collection,
                        query=query_vector,
                        query_filter=q_filter,
                        limit=top_k,
                        with_payload=True,
                        with_vectors=False,
                    )
                except TypeError:
                    # 部分版本参数名为 filter
                    result = self._qdrant_client.query_points(
                        collection_name=collection,
                        query=query_vector,
                        filter=q_filter,
                        limit=top_k,
                        with_payload=True,
                        with_vectors=False,
                    )
                points = getattr(result, "points", None)
                if points is not None:
                    return points
                if isinstance(result, dict) and isinstance(result.get("points"), list):
                    return result["points"]
                if isinstance(result, list):
                    return result
                return []

            if hasattr(self._qdrant_client, "search"):
                return self._qdrant_client.search(
                    collection_name=collection,
                    query_vector=query_vector,
                    query_filter=q_filter,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False,
                )

            logger.error("[chat_export] qdrant search 失败: 当前 qdrant-client 不支持 search/query_points")
            return []
        except Exception as e:
            logger.error(f"[chat_export] qdrant search 失败: {e}")
            return []

    def _count_qdrant(self, group_id: str = "") -> int:
        if not self._qdrant_client or not self._qdrant_models:
            return 0

        collection = self._norm(self.config.get("qdrant_collection", "chat_export"))
        q_filter = self._build_qdrant_filter(group_id, None)

        # 兼容不同 qdrant-client 版本的 count 签名
        try:
            if hasattr(self._qdrant_client, "count"):
                try:
                    res = self._qdrant_client.count(
                        collection_name=collection,
                        count_filter=q_filter,
                        exact=True,
                    )
                except TypeError:
                    res = self._qdrant_client.count(
                        collection_name=collection,
                        filter=q_filter,
                        exact=True,
                    )
                if isinstance(res, dict):
                    return int(res.get("count", 0))
                return int(getattr(res, "count", 0))
        except Exception:
            pass

        # HTTP 回退：POST /collections/{collection}/points/count
        base = self._norm(self.config.get("qdrant_url", "http://127.0.0.1:6333")).rstrip("/")
        api_key = self._norm(self.config.get("qdrant_api_key", ""))
        url = f"{base}/collections/{collection}/points/count"
        body_obj: dict[str, Any] = {"exact": True}
        if group_id:
            body_obj["filter"] = {
                "must": [{"key": "group_id", "match": {"value": group_id}}]
            }
        body = json.dumps(body_obj).encode("utf-8")

        req = request.Request(url=url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if api_key:
            req.add_header("api-key", api_key)
        with request.urlopen(req, timeout=self._int_conf("qdrant_timeout", 10)) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        result = data.get("result", {}) if isinstance(data, dict) else {}
        return int(result.get("count", 0))

    def _embedding_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        api_base = self._norm(self.config.get("embedding_api_base", "https://api.openai.com/v1"))
        api_key = self._norm(self.config.get("embedding_api_key", ""))
        model = self._norm(self.config.get("embedding_model", "text-embedding-3-small"))

        if not api_key:
            logger.warning("[chat_export] embedding_api_key 为空，跳过向量化")
            return [[] for _ in texts]

        url = api_base.rstrip("/") + "/embeddings"
        body = json.dumps({"model": model, "input": texts}).encode("utf-8")
        req = request.Request(url=url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {api_key}")

        try:
            with request.urlopen(req, timeout=self._int_conf("embedding_timeout", 20)) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            vecs: list[list[float]] = [[] for _ in texts]
            for item in (data.get("data", []) if isinstance(data, dict) else []):
                idx = int(item.get("index", 0))
                emb = item.get("embedding", [])
                if 0 <= idx < len(vecs) and isinstance(emb, list):
                    vecs[idx] = [float(x) for x in emb]
            return vecs
        except Exception as e:
            self._last_error = f"embedding: {e}"
            logger.warning(f"[chat_export] embedding 调用失败: {e}")
            return [[] for _ in texts]

    def _build_qdrant_filter(self, group_id: str, since_dt: datetime | None):
        if not self._qdrant_models:
            return None
        must = []
        if group_id:
            must.append(
                self._qdrant_models.FieldCondition(
                    key="group_id",
                    match=self._qdrant_models.MatchValue(value=group_id),
                )
            )
        if since_dt:
            since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                must.append(
                    self._qdrant_models.FieldCondition(
                        key="ts",
                        range=self._qdrant_models.Range(gte=since_text),
                    )
                )
            except Exception:
                pass
        if not must:
            return None
        return self._qdrant_models.Filter(must=must)

    def _embedding(self, text: str) -> list[float]:
        arr = self._embedding_batch([text])
        return arr[0] if arr else []

    def _extract_text(self, event: AstrMessageEvent) -> str:
        # 1) 优先普通文本
        text = self._norm(event.message_str)
        if text and text not in {"[图片]", "[表情]", "[动画表情]"}:
            return text

        # 2) 尝试解析结构化消息段（图片/表情/回复等）
        structured = self._extract_structured_message(event)
        if structured:
            return structured

        # 3) 回退 raw_message
        raw = self._norm(getattr(event, "raw_message", ""))
        if raw:
            return raw

        # 4) 若 message_str 是图片占位符，也保留占位信息
        if text:
            return text
        return "<空消息>"

    def _extract_structured_message(self, event: AstrMessageEvent) -> str:
        candidates = [
            getattr(event, "message_obj", None),
            getattr(event, "message", None),
            getattr(event, "messages", None),
        ]
        for obj in candidates:
            content = self._format_message_obj(obj)
            if content:
                return content
        return ""

    def _format_message_obj(self, obj: Any) -> str:
        if obj is None:
            return ""

        if isinstance(obj, str):
            return obj.strip()

        if isinstance(obj, dict):
            # 常见格式: {"type":"image","data":{...}} 或 {"text":"..."}
            if "type" in obj:
                return self._format_segment(obj)
            # 也可能是 {"message":[...]} 这类容器
            for k in ("message", "messages", "segments"):
                if isinstance(obj.get(k), list):
                    return self._format_message_obj(obj.get(k))
            text = self._norm(obj.get("text"))
            return text

        if isinstance(obj, list):
            parts: list[str] = []
            for seg in obj:
                s = self._format_segment(seg)
                if s:
                    parts.append(s)
            return " ".join(parts).strip()

        # 兜底：尝试对象属性
        seg_type = self._norm(getattr(obj, "type", ""))
        if seg_type:
            data = getattr(obj, "data", None)
            seg = {"type": seg_type, "data": data if isinstance(data, dict) else {}}
            return self._format_segment(seg)
        return ""

    def _format_segment(self, seg: Any) -> str:
        if seg is None:
            return ""

        if isinstance(seg, str):
            return seg.strip()

        if not isinstance(seg, dict):
            return self._norm(seg)

        seg_type = self._norm(seg.get("type")).lower()
        data = seg.get("data") if isinstance(seg.get("data"), dict) else {}

        if seg_type in {"text", "plain"}:
            return self._norm(data.get("text") or seg.get("text"))

        if seg_type == "image":
            file = self._norm(data.get("file"))
            url = self._norm(data.get("url"))
            if url:
                return f"[图片][URL:{url}]"
            if file:
                return f"[图片][FILE:{file}]"
            return "[图片]"

        if seg_type in {"face", "emoji"}:
            face_id = self._norm(data.get("id") or data.get("face_id"))
            return f"[表情:id={face_id}]" if face_id else "[表情]"

        if seg_type in {"mface", "market_face"}:
            name = self._norm(data.get("summary") or data.get("name"))
            return f"[动画表情:{name}]" if name else "[动画表情]"

        if seg_type == "reply":
            msg_id = self._norm(data.get("id"))
            return f"[回复:id={msg_id}]" if msg_id else "[回复]"

        if seg_type == "at":
            qq = self._norm(data.get("qq") or data.get("user_id"))
            return f"@{qq}" if qq else "@"

        if seg_type == "file":
            name = self._norm(data.get("name") or data.get("file"))
            return f"[文件:{name}]" if name else "[文件]"

        if seg_type:
            return f"[{seg_type}]"
        return ""

    def _event_time(self, event: AstrMessageEvent) -> datetime:
        t = getattr(event, "time", None)
        if isinstance(t, (int, float)):
            try:
                return datetime.fromtimestamp(t)
            except Exception:
                pass
        return datetime.now()

    def _extract_message_id(self, event: AstrMessageEvent) -> str:
        for key in ("message_id", "msg_id", "id"):
            val = getattr(event, key, None)
            if val is not None:
                s = self._norm(val)
                if s:
                    return s
        obj = getattr(event, "message_obj", None)
        if isinstance(obj, dict):
            for key in ("message_id", "msg_id", "id"):
                s = self._norm(obj.get(key))
                if s:
                    return s
        return ""

    def _build_unique_key(
        self, group_id: str, user_id: str, message_time: datetime, content: str, message_id: str
    ) -> str:
        if message_id:
            return f"gid:{group_id}|mid:{message_id}"
        digest = hashlib.sha1(content.encode("utf-8", errors="ignore")).hexdigest()
        return f"gid:{group_id}|uid:{user_id}|ts:{message_time.strftime('%Y-%m-%d %H:%M:%S')}|sha1:{digest}"

    def _parse_search_args(self, tokens: list[str], event: AstrMessageEvent) -> tuple[str, str, datetime | None]:
        args = tokens[1:]
        group_id = self._norm(event.get_group_id())
        if args and args[0].isdigit():
            group_id = self._norm(args[0])
            args = args[1:]

        since_dt = None
        filtered: list[str] = []
        for token in args:
            parsed = self._parse_recent_time_token(token)
            if parsed is not None and since_dt is None:
                since_dt = parsed
                continue
            filtered.append(token)

        if since_dt is None:
            default_hours = self._int_conf("search_default_since_hours", 0)
            if default_hours > 0:
                since_dt = datetime.now() - timedelta(hours=default_hours)

        return group_id, " ".join(filtered).strip(), since_dt

    def _parse_recent_time_token(self, token: str) -> datetime | None:
        t = token.strip().lower()
        m = re.match(r"^最近(\d+)(小时|时|h|天|d)$", t)
        if m:
            num = int(m.group(1))
            unit = m.group(2)
            if unit in {"小时", "时", "h"}:
                return datetime.now() - timedelta(hours=num)
            return datetime.now() - timedelta(days=num)

        m2 = re.match(r"^recent:(\d+)(h|d)$", t)
        if m2:
            num = int(m2.group(1))
            if m2.group(2) == "h":
                return datetime.now() - timedelta(hours=num)
            return datetime.now() - timedelta(days=num)
        return None

    def _point_payload(self, point: Any) -> dict[str, Any]:
        if isinstance(point, dict):
            p = point.get("payload")
            return p if isinstance(p, dict) else {}
        p = getattr(point, "payload", None)
        return p if isinstance(p, dict) else {}

    def _index_media_placeholders(self) -> bool:
        return bool(self.config.get("index_media_placeholders", False))

    def _should_index_to_qdrant(self, content: str) -> bool:
        if self._index_media_placeholders():
            return True
        text = (content or "").strip()
        if not text:
            return False
        if text in {"[图片]", "[表情]", "[动画表情]", "[回复]", "[文件]"}:
            return False
        chunks = text.split()
        if chunks and all(c.startswith("[") and c.endswith("]") for c in chunks):
            return False
        return True

    def _is_enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    def _listening_groups(self) -> set[str]:
        raw = self.config.get("listening_group_ids", []) or []
        normalized = [self._norm(i) for i in raw if self._norm(i)]
        sig = "|".join(sorted(normalized))
        if sig != self._listening_groups_sig:
            self._listening_groups_sig = sig
            self._listening_groups_cache = set(normalized)
        return self._listening_groups_cache

    def _is_listening_group(self, group_id: str) -> bool:
        return group_id in self._listening_groups()

    def _qdrant_enabled(self) -> bool:
        return bool(self.config.get("qdrant_enabled", False))

    def _stop_event_after_ingest(self) -> bool:
        return bool(self.config.get("stop_event_after_ingest", False))

    def _is_manager(self, event: AstrMessageEvent) -> bool:
        sender_id = self._norm(event.get_sender_id())
        if sender_id in self._manager_ids():
            return True

        if bool(self.config.get("allow_astrbot_admin", True)):
            try:
                return bool(event.is_admin())
            except Exception:
                return False

        return False

    def _manager_ids(self) -> set[str]:
        raw = self.config.get("admin_user_ids", []) or []
        normalized = [self._norm(i) for i in raw if self._norm(i)]
        sig = "|".join(sorted(normalized))
        if sig != self._admin_ids_sig:
            self._admin_ids_sig = sig
            self._admin_ids_cache = set(normalized)
        return self._admin_ids_cache

    def _resolve_data_dir(self, p: Any) -> Path:
        raw = self._norm(p)
        if raw:
            path = Path(raw)
            if path.is_absolute():
                return path
            return self.plugin_dir / path

        astrbot_data = Path("/AstrBot/data")
        if astrbot_data.exists():
            return astrbot_data / "plugin_data" / "astrbot_plugin_chat_export"

        return self.plugin_dir / "data"

    def _resolve_path(self, p: Any, base_dir: Path) -> Path:
        raw = self._norm(p)
        if not raw:
            return base_dir / "chat_export.db"
        path = Path(raw)
        if path.is_absolute():
            return path
        return base_dir / path

    @staticmethod
    def _parse_dt(s: str) -> datetime | None:
        text = s.strip()
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d_%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _parse_export_args(self, tokens: list[str], event: AstrMessageEvent) -> tuple[str, str, str]:
        args = tokens[1:]
        default_group = self._norm(event.get_group_id())

        if len(args) >= 4 and ":" in args[1] and ":" in args[3]:
            start_s = f"{args[0]} {args[1]}"
            end_s = f"{args[2]} {args[3]}"
            group_id = self._norm(args[4]) if len(args) >= 5 else default_group
            return start_s, end_s, group_id

        if len(args) >= 2:
            start_s = args[0]
            end_s = args[1]
            group_id = self._norm(args[2]) if len(args) >= 3 else default_group
            return start_s, end_s, group_id

        return "", "", default_group

    def _int_conf(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(key, default))
        except Exception:
            return default

    def _float_conf(self, key: str, default: float) -> float:
        try:
            return float(self.config.get(key, default))
        except Exception:
            return default

    def _save_config(self):
        save_fn = getattr(self.config, "save_config", None)
        if callable(save_fn):
            save_fn()

    def _verbose_log_enabled(self) -> bool:
        return bool(self.config.get("verbose_log", True))

    def _log_verbose(self, text: str):
        if self._verbose_log_enabled():
            logger.info(f"[chat_export] {text}")

    def _log_startup_summary(self):
        self._log_verbose(
            "startup: "
            f"enabled={self._is_enabled()} "
            f"qdrant_enabled={self._qdrant_enabled()} "
            f"data_dir={self.data_dir} "
            f"db_file={self.db_file} "
            f"listening={sorted(self._listening_groups())} "
            f"qdrant_batch={self._int_conf('qdrant_batch_size', 20)} "
            f"qdrant_interval={self._float_conf('qdrant_flush_interval_sec', 1.0)}"
        )

    def _log_ingest_progress(self, group_id: str, user_id: str, content: str):
        every_n = self._int_conf("log_every_n", 1)
        if every_n <= 0:
            every_n = 1
        if self._received_group_events % every_n != 0:
            return
        preview_len = self._int_conf("log_preview_len", 24)
        preview = (content or "").replace("\n", " ").strip()
        if len(preview) > preview_len:
            preview = preview[:preview_len] + "..."
        self._log_verbose(
            "ingest: "
            f"group={group_id} user={user_id} "
            f"sqlite_ok/fail/dedup={self._sqlite_write_ok}/{self._sqlite_write_fail}/{self._sqlite_dedup_skip} "
            f"qdrant_ok/fail={self._qdrant_write_ok}/{self._qdrant_write_fail} "
            f"qdrant_pending={len(self._qdrant_pending)} "
            f"preview={preview}"
        )

    @staticmethod
    def _norm(v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    async def terminate(self):
        self._flush_all_queues(force=True)
        if self._sqlite_conn is not None:
            try:
                self._sqlite_conn.close()
            except Exception:
                pass
            self._sqlite_conn = None
        logger.info("[chat_export] terminated")



