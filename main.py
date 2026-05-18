from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_UTILS_PATH = Path(__file__).resolve().parent.parent / "utils"
if str(_UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(_UTILS_PATH))

try:
    from command_parser import parse_command
    from config_utils import env_override, mask_config_for_log
except Exception:
    parse_command = None  # type: ignore[misc]
    env_override = None  # type: ignore[misc]
    mask_config_for_log = None  # type: ignore[misc]

try:
    import httpx
except Exception:
    httpx = None  # type: ignore[misc]

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event.filter import EventMessageType
from astrbot.api.star import Context, Star, register

from config import float_conf, int_conf, load_secure_config, resolve_data_dir, resolve_path
from db import ChatDatabase
from export_core import (
    build_analysis_transcript,
    build_local_analysis,
    parse_analyze_args,
    parse_export_args,
    parse_history_sync_args,
    parse_search_args,
    post_filter_search_points,
)
from history_sync import HistorySync
from llm_client import LlmClient
from media_uploader import MediaUploader
from message_parser import (
    event_time,
    extract_image_refs,
    extract_message_id,
    extract_structured_message,
    extract_text,
    format_export_line,
)
from metrics import MetricsCollector
from qdrant_client import QdrantClientWrapper
from utils import build_unique_key, looks_like_model_refusal, norm, parse_dt


@register(
    "astrbot_plugin_chat_export",
    "NOTFROMCONCEN",
    "监听群消息并支持历史补录，按时间范围导出聊天记录为 TXT，支持 Qdrant 语义检索",
    "2.2.0",
)
class ChatExportPlugin(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.config = config or {}
        load_secure_config(self.config)

        self.plugin_dir = Path(__file__).resolve().parent
        self.data_dir = resolve_data_dir(self.plugin_dir, self.config)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.db_file = resolve_path(self.config.get("db_path", "chat_export.db"), self.data_dir, "chat_export.db")
        self.export_dir = resolve_path(self.config.get("export_dir", "exports"), self.data_dir, "exports")
        self.export_dir.mkdir(parents=True, exist_ok=True)

        self._startup_time = datetime.now()
        self._db = ChatDatabase(self.db_file, self.config)
        self._db.init_schema()

        self._qdrant = QdrantClientWrapper(self.config)
        self._llm = LlmClient(self.config, http_factory=self._get_async_http)
        self._media = MediaUploader(self.config, http_factory=self._get_async_http)
        self._history_sync = HistorySync(self.config, self._db)

        self._listening_groups_cache: set[str] = set()
        self._listening_groups_sig = ""
        self._admin_ids_cache: set[str] = set()
        self._admin_ids_sig = ""
        self._received_group_events = 0
        self._last_error = ""

        self._drop_reasons: dict[str, int] = {}
        self._max_pending_size = int_conf(self.config, "max_pending_size", 5000)
        self._error_cooldown_sec = float_conf(self.config, "error_cooldown_sec", 60.0)
        self._max_consecutive_errors = int_conf(self.config, "max_consecutive_errors", 5)
        self._max_log_line_length = int_conf(self.config, "max_log_line_length", 512)

        self._metrics = MetricsCollector()
        self._async_http: Any = None
        self._log_startup_summary()

    def _get_async_http(self) -> Any:
        if self._async_http is None and httpx is not None:
            timeout = httpx.Timeout(
                connect=10.0,
                read=int_conf(self.config, "embedding_timeout", 20),
                write=10.0,
                pool=5.0,
            )
            self._async_http = httpx.AsyncClient(timeout=timeout, http2=False)
        return self._async_http

    @filter.event_message_type(EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        self._received_group_events += 1
        self._metrics.inc("events_received_total")
        if not self._is_enabled():
            self._metrics.inc("events_dropped_total", labels={"reason": "disabled"})
            self._log_verbose("skip message: plugin disabled")
            return

        group_id = norm(event.get_group_id())
        if not group_id:
            self._metrics.inc("events_dropped_total", labels={"reason": "empty_group_id"})
            self._log_verbose("skip message: empty group_id")
            return
        if not self._is_listening_group(group_id):
            self._metrics.inc("events_dropped_total", labels={"reason": "not_listening"})
            self._log_verbose(f"skip message: group {group_id} not in listening_group_ids")
            return

        try:
            user_id = norm(event.get_sender_id())
            sender_name = norm(getattr(event, "get_sender_name", lambda: "")())
            content = extract_text(event)
            message_time = event_time(event)
            message_id = extract_message_id(event)
            unique_key = build_unique_key(group_id, user_id, message_time, content, message_id)
            refs = extract_image_refs(event)
        except Exception as e:
            self._metrics.inc("events_dropped_total", labels={"reason": "parse_error"})
            self._log_verbose(f"message parse error: {e}")
            return

        media_items = await self._media.upload_refs(refs)
        import json
        try:
            media_json = json.dumps(media_items, ensure_ascii=False) if media_items else ""
        except Exception:
            media_json = ""

        record = {
            "ts": message_time.strftime("%Y-%m-%d %H:%M:%S"),
            "group_id": group_id,
            "user_id": user_id,
            "sender_name": sender_name,
            "content": content,
            "message_id": message_id,
            "unique_key": unique_key,
            "media_json": media_json,
        }

        if len(self._db.pending) >= self._max_pending_size:
            dropped = self._db.truncate_pending(self._max_pending_size // 10)
            self._db.add_write_fail(len(dropped))
            self._drop_reasons["sqlite_pending_overflow"] = (
                self._drop_reasons.get("sqlite_pending_overflow", 0) + len(dropped)
            )
            self._log_verbose(f"pending overflow: dropped {len(dropped)} sqlite records")
        self._db.pending.append(record)

        self._metrics.gauge("queue_size", len(self._db.pending), labels={"target": "sqlite"})
        self._metrics.gauge("queue_size", len(self._qdrant.pending), labels={"target": "qdrant"})

        inserted = self._db.flush_if_needed(self._qdrant.enqueue)
        self._qdrant.flush_if_needed(self._llm.embedding_batch_sync)

        if inserted:
            self._metrics.inc("messages_ingested_total", len(inserted))

        self._log_ingest_progress(group_id, user_id, content)

        if self._stop_event_after_ingest():
            event.stop_event()
            self._log_verbose(f"stop_event_after_ingest: group={group_id}")

    @filter.command("聊天导出")
    async def export_chat_cn(self, event: AstrMessageEvent):
        async for result in self._handle_export(event):
            yield result

    @filter.command("聊天匯出")
    async def export_chat_tw(self, event: AstrMessageEvent):
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

    @filter.command("聊天檢索")
    async def semantic_search_tw(self, event: AstrMessageEvent):
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

    @filter.command("聊天監聽")
    async def manage_listen_tw(self, event: AstrMessageEvent):
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

    @filter.command("聊天統計")
    async def stats_tw(self, event: AstrMessageEvent):
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

    @filter.command("聊天历史同步")
    async def sync_history_cn(self, event: AstrMessageEvent):
        async for result in self._handle_history_sync(event):
            yield result

    @filter.command("聊天歷史同步")
    async def sync_history_tw(self, event: AstrMessageEvent):
        async for result in self._handle_history_sync(event):
            yield result

    @filter.command("chat_history_sync")
    async def sync_history_en(self, event: AstrMessageEvent):
        async for result in self._handle_history_sync(event):
            yield result

    @filter.command("聊天分析")
    async def analyze_chat_cn(self, event: AstrMessageEvent):
        async for result in self._handle_chat_analyze(event):
            yield result

    @filter.command("chat_analyze")
    async def analyze_chat_en(self, event: AstrMessageEvent):
        async for result in self._handle_chat_analyze(event):
            yield result

    async def _handle_export(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行导出")
            return

        self._flush_all_queues(force=True)

        tokens = self._parse_tokens(event)
        if len(tokens) < 3:
            yield event.plain_result(
                "用法: /聊天导出 <开始时间> <结束时间> [群号]\n"
                "示例: /聊天导出 2026-04-17T00:00:00 2026-04-17T23:59:59 123456"
            )
            return

        start_s, end_s, group_id = parse_export_args(tokens, norm(event.get_group_id()))
        if not start_s or not end_s:
            yield event.plain_result(
                "参数格式错误。用法: /聊天导出 <开始时间> <结束时间> [群号]\n"
                "示例1: /聊天导出 2026-04-17T00:00:00 2026-04-17T23:59:59 123456\n"
                "示例2: /聊天导出 2026-04-17 00:00:00 2026-04-17 23:59:59 123456"
            )
            return

        start_dt = parse_dt(start_s)
        end_dt = parse_dt(end_s)
        if not start_dt or not end_dt:
            yield event.plain_result(
                "时间格式错误，支持: YYYY-MM-DDTHH:MM:SS / YYYY-MM-DD_HH:MM:SS / YYYY-MM-DD HH:MM:SS"
            )
            return

        if end_dt < start_dt:
            yield event.plain_result("结束时间不能早于开始时间")
            return

        t0 = time.time()
        rows = self._db.query_messages(start_dt, end_dt, group_id)
        self._metrics.record("sqlite_flush_latency_ms", (time.time() - t0) * 1000)

        if not rows:
            yield event.plain_result("该时间范围内没有聊天记录")
            return

        safe_group = group_id or "all"
        out_name = f"chat_{safe_group}_{start_dt.strftime('%Y%m%d%H%M%S')}_{end_dt.strftime('%Y%m%d%H%M%S')}.txt"
        out_file = self.export_dir / out_name

        with out_file.open("w", encoding="utf-8") as f:
            for ts, gid, uid, uname, text, media_json in rows:
                line = format_export_line(text, media_json)
                f.write(f"[{ts}] [群:{gid}] [{uname or uid}] {line}\n")

        yield event.plain_result(f"导出完成，共 {len(rows)} 条\n文件: {out_file}")

    async def _handle_semantic_search(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行检索")
            return

        if not self._qdrant.enabled():
            yield event.plain_result("Qdrant 未启用，请在插件配置里开启 qdrant_enabled")
            return

        if not self._qdrant.client or not self._qdrant.models:
            yield event.plain_result(
                "Qdrant 初始化失败，请检查 qdrant_url / qdrant_api_key / qdrant_client 依赖"
            )
            return

        self._flush_all_queues(force=True)

        tokens = self._parse_tokens(event)
        if len(tokens) < 2:
            yield event.plain_result(
                "用法: /聊天检索 [群号] [最近1小时|recent:2h] <问题>"
            )
            return

        group_id, query_text, since_dt = parse_search_args(tokens, norm(event.get_group_id()))

        if not query_text:
            yield event.plain_result("检索内容不能为空")
            return

        t0 = time.time()
        vector = await self._llm.embedding_async(query_text)
        self._metrics.record("embedding_latency_ms", (time.time() - t0) * 1000)

        if not vector:
            yield event.plain_result(
                "向量化失败，请检查 embedding_api_base / embedding_api_key / embedding_model"
            )
            return

        limit = int_conf(self.config, "search_top_k", 5)
        fetch_k = max(limit, int_conf(self.config, "search_fetch_k", 60))

        t0 = time.time()
        candidates = self._qdrant.search(vector, group_id, fetch_k, since_dt)
        self._metrics.record("qdrant_flush_latency_ms", (time.time() - t0) * 1000)

        points = post_filter_search_points(
            candidates, group_id, since_dt, query_text, limit, self.config
        )
        if not points:
            yield event.plain_result("未检索到相关聊天记录")
            return

        since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else "不限"
        lines = [f"检索结果（Top {len(points)}，时间下限: {since_text}）："]
        for idx, p in enumerate(points, start=1):
            from .export_core import _point_payload
            payload = _point_payload(p)
            ts = norm(payload.get("ts"))
            gid = norm(payload.get("group_id"))
            uname = norm(payload.get("sender_name")) or norm(payload.get("user_id"))
            text = norm(payload.get("content"))
            lines.append(f"{idx}. [{ts}] [群:{gid}] [{uname}] {text}")

        yield event.plain_result("\n".join(lines))

    async def _handle_listen_manage(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行监听管理")
            return

        tokens = self._parse_tokens(event)
        if len(tokens) < 2:
            yield event.plain_result(
                "用法: /聊天监听 <开始|停止|状态> [群号]\n"
                "示例: /聊天监听 开始 123456"
            )
            return

        action = norm(tokens[1]).lower()
        group_id = norm(tokens[2]) if len(tokens) >= 3 else norm(event.get_group_id())
        listening = set(self._listening_groups())

        if action in {"开始", "開始", "start", "on", "开启", "開啟"}:
            if not group_id:
                yield event.plain_result("请提供群号：/聊天监听 开始 <群号>")
                return
            listening.add(group_id)
            self.config["listening_group_ids"] = sorted(listening)
            self._listening_groups_sig = ""
            self._save_config()
            yield event.plain_result(f"已开始监听群: {group_id}")
            return

        if action in {"停止", "stop", "off", "关闭", "關閉"}:
            if not group_id:
                yield event.plain_result("请提供群号：/聊天监听 停止 <群号>")
                return
            listening.discard(group_id)
            self.config["listening_group_ids"] = sorted(listening)
            self._listening_groups_sig = ""
            self._save_config()
            yield event.plain_result(f"已停止监听群: {group_id}")
            return

        if action in {"状态", "狀態", "status"}:
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

        tokens = self._parse_tokens(event)
        group_id = norm(tokens[1]) if len(tokens) >= 2 else norm(event.get_group_id())

        sqlite_total = self._db.count()
        sqlite_group = self._db.count(group_id) if group_id else sqlite_total

        qdrant_total: int | None = None
        qdrant_group: int | None = None
        qdrant_error = ""
        if self._qdrant.enabled() and self._qdrant.client:
            try:
                qdrant_total = self._qdrant.count("")
                qdrant_group = self._qdrant.count(group_id) if group_id else qdrant_total
            except Exception as e:
                qdrant_error = norm(e)

        listening = ", ".join(sorted(self._listening_groups())) or "无"
        lines = [
            "[聊天统计]",
            f"- data_dir: {self.data_dir}",
            f"- sqlite_db: {self.db_file}",
            f"- listening_groups: {listening}",
            f"- sqlite_total: {sqlite_total}",
            f"- sqlite_group({group_id or 'all'}): {sqlite_group}",
            f"- runtime_received_group_events: {self._received_group_events}",
            f"- runtime_sqlite_ok/fail/dedup: {self._db.write_ok}/{self._db.write_fail}/{self._db.dedup_skip}",
            f"- runtime_qdrant_ok/fail: {self._qdrant.write_ok}/{self._qdrant.write_fail}",
            f"- runtime_lsky_ok/fail: {self._media.upload_ok}/{self._media.upload_fail}",
            f"- queue_sqlite: {len(self._db.pending)}",
            f"- queue_qdrant: {len(self._qdrant.pending)}",
        ]

        if self._drop_reasons:
            drop_parts = [f"{k}={v}" for k, v in self._drop_reasons.items()]
            lines.append(f"- drop_reasons: {', '.join(drop_parts)}")

        if not self._qdrant.enabled():
            lines.append("- qdrant: 未启用")
        elif not self._qdrant.client:
            lines.append("- qdrant: 未初始化")
        else:
            lines.append(
                f"- qdrant_total: {qdrant_total if qdrant_total is not None else 'unknown'}"
            )
            lines.append(
                f"- qdrant_group({group_id or 'all'}): {qdrant_group if qdrant_group is not None else 'unknown'}"
            )
            if qdrant_error:
                lines.append(f"- qdrant_error: {qdrant_error}")

        if self._received_group_events == 0 and self._listening_groups():
            lines.append(
                "- hint: 当前监听群非空但未收到事件，检查是否有其他插件提前 stop_event（如 force_silent 硬静默模式）"
            )

        yield event.plain_result("\n".join(lines))

    async def _handle_health(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行健康检查")
            return

        self._flush_all_queues(force=True)
        mins = max((datetime.now() - self._startup_time).total_seconds() / 60.0, 1e-6)
        eps = self._received_group_events / mins
        snap = self._metrics.snapshot()

        lines = [
            "[聊天健康]",
            f"- uptime_minutes: {mins:.2f}",
            f"- events_per_min: {eps:.2f}",
            f"- sqlite_conn_ready: {self._db.conn_ready}",
            f"- sqlite_batch_size: {int_conf(self.config, 'sqlite_batch_size', 20)}",
            f"- sqlite_flush_interval_sec: {float_conf(self.config, 'sqlite_flush_interval_sec', 1.0)}",
            f"- last_sqlite_ok: {self._db.last_error or 'ok'}",
            f"- last_qdrant_ok: {self._qdrant.last_error or 'ok'}",
            f"- last_error: {self._last_error or 'none'}",
            f"- stop_event_after_ingest: {self._stop_event_after_ingest()}",
            f"- index_media_placeholders: {self._index_media_placeholders()}",
            f"- lsky_enabled: {self._media.enabled()}",
            f"- sqlite_pending: {len(self._db.pending)}",
            f"- qdrant_pending: {len(self._qdrant.pending)}",
        ]
        if self._drop_reasons:
            drop_parts = [f"{k}={v}" for k, v in self._drop_reasons.items()]
            lines.append(f"- drop_reasons: {', '.join(drop_parts)}")

        # metrics snapshot
        lines.append("[运行时指标]")
        lines.append(f"- events_received_total: {self._metrics.counter_value('events_received_total')}")
        lines.append(f"- events_dropped_total: {self._metrics.counter_value('events_dropped_total')}")
        lines.append(f"- messages_ingested_total: {self._metrics.counter_value('messages_ingested_total')}")
        lines.append(f"- queue_sqlite: {self._metrics.gauge_value('queue_size', labels={'target': 'sqlite'})}")
        lines.append(f"- queue_qdrant: {self._metrics.gauge_value('queue_size', labels={'target': 'qdrant'})}")
        lines.append(f"- circuit_breaker_sqlite: {self._db.error_count}")
        lines.append(f"- circuit_breaker_qdrant: {self._qdrant.error_count}")

        hist = snap.get("histogram_recent", {})
        for name, vals in hist.items():
            lines.append(
                f"- {name}: count={vals['count']} avg={vals['avg']}ms min={vals['min']}ms max={vals['max']}ms"
            )

        yield event.plain_result("\n".join(lines))

    async def _handle_history_sync(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行历史同步")
            return

        if norm(event.get_platform_name()).lower() != "aiocqhttp":
            yield event.plain_result(
                "当前仅支持 aiocqhttp 平台历史同步（NapCat / Lagrange / OneBot V11）"
            )
            return

        tokens = self._parse_tokens(event)
        group_id, limit = parse_history_sync_args(
            tokens, norm(event.get_group_id()),
            max(1, int_conf(self.config, "history_sync_default_limit", 100)),
            max(1, int_conf(self.config, "history_sync_max_limit", 1000)),
        )
        if not group_id:
            yield event.plain_result(
                "用法: /聊天历史同步 [条数] [群号]\n"
                "示例1: /聊天历史同步 200\n"
                "示例2: /聊天历史同步 200 123456"
            )
            return

        self._flush_all_queues(force=True)
        yield event.plain_result(
            f"开始同步群 {group_id} 的历史消息，目标 {limit} 条，请稍候..."
        )

        bot = getattr(event, "bot", None)
        if bot is None:
            message_obj = getattr(event, "message_obj", None)
            bot = getattr(message_obj, "bot", None)

        if bot is None:
            yield event.plain_result("未找到 aiocqhttp 客户端")
            return

        t0 = time.time()
        try:
            stats = await self._history_sync.sync_group_history(event, bot, group_id, limit)
        except Exception as e:
            err = norm(e) or "unknown_error"
            self._last_error = f"history_sync: {err}"
            logger.warning(f"[chat_export] 历史同步失败: {err}")
            yield event.plain_result(f"历史同步失败: {err}")
            return
        self._metrics.record("sqlite_flush_latency_ms", (time.time() - t0) * 1000)

        if stats["fetched"] <= 0:
            yield event.plain_result(
                "未获取到历史消息，请检查群号、协议端权限和历史 API 是否受支持"
            )
            return

        lines = [
            "历史同步完成",
            f"群号: {group_id}",
            f"轮次: {stats.get('rounds', 1)}",
            f"分页请求: {stats['pages']} 次",
            f"拉取消息: {stats['fetched']} 条",
            f"有效记录: {stats['normalized']} 条",
            f"新增入库: {stats['inserted']} 条",
            f"重复跳过: {stats['duplicates']} 条",
        ]
        if stats["invalid"] > 0:
            lines.append(f"解析跳过: {stats['invalid']} 条")
        stop_reason = norm(stats.get("stop_reason"))
        if stop_reason:
            lines.append(f"终止原因: {stop_reason}")
        yield event.plain_result("\n".join(lines))

    async def _handle_chat_analyze(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行聊天分析")
            return

        self._flush_all_queues(force=True)

        tokens = self._parse_tokens(event)
        group_id, user_id, since_dt = parse_analyze_args(tokens, norm(event.get_group_id()))
        if not group_id:
            yield event.plain_result(
                "用法: /聊天分析 <群号> [用户ID] [最近24小时|recent:2d]\n"
                "示例1: /聊天分析 1058402699 最近24小时\n"
                "示例2: /聊天分析 1058402699 1097681347 recent:7d"
            )
            return

        limit = max(20, int_conf(self.config, "analysis_max_messages", 400))
        t0 = time.time()
        rows = self._db.query_messages_for_analysis(group_id, user_id, since_dt, limit)
        self._metrics.record("sqlite_flush_latency_ms", (time.time() - t0) * 1000)

        if not rows:
            yield event.plain_result("未找到可分析的聊天记录")
            return

        transcript = build_analysis_transcript(
            rows, max(2000, int_conf(self.config, "analysis_max_chars", 18000))
        )

        t0 = time.time()
        summary = await self._llm.analysis_async(
            transcript=transcript,
            group_id=group_id,
            user_id=user_id,
            since_dt=since_dt,
            sample_size=len(rows),
        )
        self._metrics.record("embedding_latency_ms", (time.time() - t0) * 1000)

        if not summary:
            yield event.plain_result("分析失败：请检查 analysis_api_base / analysis_api_key / analysis_model")
            return
        if looks_like_model_refusal(summary):
            summary = (
                "模型触发风控，已切换为本地统计分析：\n\n"
                + build_local_analysis(rows, group_id, user_id, since_dt)
            )

        who = f"用户 {user_id}" if user_id else "全群"
        since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else "不限"
        yield event.plain_result(
            f"[聊天分析]\n群号: {group_id}\n对象: {who}\n时间下限: {since_text}\n样本数: {len(rows)}\n\n{summary}"
        )

    def _flush_all_queues(self, force: bool):
        t0 = time.time()
        inserted = self._db.flush(force=force)
        self._metrics.record("sqlite_flush_latency_ms", (time.time() - t0) * 1000)
        if inserted:
            self._metrics.inc("messages_ingested_total", len(inserted))
            t0 = time.time()
            self._qdrant.enqueue(inserted, self._should_index_to_qdrant)
            self._metrics.record("qdrant_flush_latency_ms", (time.time() - t0) * 1000)
        if force:
            t0 = time.time()
            self._qdrant.flush(force=True, embedding_fn=self._llm.embedding_batch_sync)
            self._metrics.record("qdrant_flush_latency_ms", (time.time() - t0) * 1000)
        else:
            t0 = time.time()
            self._qdrant.flush_if_needed(self._llm.embedding_batch_sync)
            self._metrics.record("qdrant_flush_latency_ms", (time.time() - t0) * 1000)

    def _parse_tokens(self, event: AstrMessageEvent) -> list[str]:
        if parse_command is not None:
            parsed = parse_command(event.message_str or "")
            return [parsed.command] + parsed.args
        return [t for t in (event.message_str or "").strip().split() if t]

    def _is_enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    def _listening_groups(self) -> set[str]:
        raw = self.config.get("listening_group_ids", []) or []
        normalized = [norm(i) for i in raw if norm(i)]
        sig = "|".join(sorted(normalized))
        if sig != self._listening_groups_sig:
            self._listening_groups_sig = sig
            self._listening_groups_cache = set(normalized)
        return self._listening_groups_cache

    def _is_listening_group(self, group_id: str) -> bool:
        return group_id in self._listening_groups()

    def _stop_event_after_ingest(self) -> bool:
        return bool(self.config.get("stop_event_after_ingest", False))

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

    def _is_manager(self, event: AstrMessageEvent) -> bool:
        sender_id = norm(event.get_sender_id())
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
        normalized = [norm(i) for i in raw if norm(i)]
        sig = "|".join(sorted(normalized))
        if sig != self._admin_ids_sig:
            self._admin_ids_sig = sig
            self._admin_ids_cache = set(normalized)
        return self._admin_ids_cache

    def _save_config(self):
        save_fn = getattr(self.config, "save_config", None)
        if callable(save_fn):
            save_fn()

    def _verbose_log_enabled(self) -> bool:
        return bool(self.config.get("verbose_log", False))

    def _log_verbose(self, text: str):
        if not self._verbose_log_enabled():
            return
        msg = f"[chat_export] {text}"
        if len(msg) > self._max_log_line_length:
            msg = msg[: self._max_log_line_length] + "...[truncated]"
        logger.info(msg)

    def _log_startup_summary(self):
        safe_config = mask_config_for_log(self.config) if mask_config_for_log else self.config
        self._log_verbose(
            "startup: "
            f"enabled={self._is_enabled()} "
            f"qdrant_enabled={self._qdrant.enabled()} "
            f"data_dir={self.data_dir} "
            f"db_file={self.db_file} "
            f"listening={sorted(self._listening_groups())} "
            f"lsky_enabled={self._media.enabled()} "
            f"qdrant_batch={int_conf(self.config, 'qdrant_batch_size', 20)} "
            f"qdrant_interval={float_conf(self.config, 'qdrant_flush_interval_sec', 1.0)}"
        )
        self._log_verbose(f"config_safe_keys={list(safe_config.keys())}")

    def _log_ingest_progress(self, group_id: str, user_id: str, content: str):
        every_n = int_conf(self.config, "log_every_n", 500)
        if every_n <= 0:
            every_n = 500
        if self._received_group_events % every_n != 0:
            return
        preview_len = int_conf(self.config, "log_preview_len", 0)
        preview = (content or "").replace("\n", " ").strip()
        if preview_len > 0 and len(preview) > preview_len:
            preview = preview[:preview_len] + "..."
        self._log_verbose(
            "ingest: "
            f"group={group_id} user={user_id} "
            f"sqlite_ok/fail/dedup={self._db.write_ok}/{self._db.write_fail}/{self._db.dedup_skip} "
            f"qdrant_ok/fail={self._qdrant.write_ok}/{self._qdrant.write_fail} "
            f"qdrant_pending={len(self._qdrant.pending)} "
            f"preview={preview}"
        )

    async def terminate(self):
        try:
            self._flush_all_queues(force=True)
        except Exception as e:
            logger.warning(f"[chat_export] terminate flush error: {e}")
        self._db.close()
        if self._async_http is not None:
            try:
                await self._async_http.aclose()
            except Exception:
                pass
            self._async_http = None
        logger.info("[chat_export] terminated")
