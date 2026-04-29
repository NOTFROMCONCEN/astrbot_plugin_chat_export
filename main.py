from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import re
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import request
from urllib.parse import urlparse

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_chat_export",
    "NOTFROMCONCEN",
    "监听群消息并支持历史补录，按时间范围导出聊天记录为 TXT，支持 Qdrant 语义检索",
    "2.0.4",
)
class ChatExportPlugin(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.config = config or {}

        self.plugin_dir = Path(__file__).resolve().parent
        self.data_dir = self._resolve_data_dir(self.config.get("data_dir", ""))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.db_file = self._resolve_path(
            self.config.get("db_path", "chat_export.db"), self.data_dir
        )
        self.export_dir = self._resolve_path(
            self.config.get("export_dir", "exports"), self.data_dir
        )
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
        self._lsky_upload_ok = 0
        self._lsky_upload_fail = 0
        self._sqlite_pending: list[dict[str, Any]] = []
        self._last_sqlite_flush_ts = time.time()
        self._qdrant_pending: list[dict[str, Any]] = []
        self._last_qdrant_flush_ts = time.time()
        self._history_action_hints: dict[str, tuple[str, tuple[str, ...]]] = {}

        # 错误冷却与限流保护（防止异常时日志/重试风暴）
        self._sqlite_error_count = 0
        self._sqlite_last_error_ts = 0.0
        self._qdrant_error_count = 0
        self._qdrant_last_error_ts = 0.0
        self._max_pending_size = self._int_conf("max_pending_size", 5000)
        self._error_cooldown_sec = self._float_conf("error_cooldown_sec", 60.0)
        self._max_consecutive_errors = self._int_conf("max_consecutive_errors", 5)
        self._max_log_line_length = self._int_conf("max_log_line_length", 512)

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
            self._log_verbose(
                f"skip message: group {group_id} not in listening_group_ids"
            )
            return

        try:
            user_id = self._norm(event.get_sender_id())
            sender_name = self._norm(getattr(event, "get_sender_name", lambda: "")())
            content = self._extract_text(event)
            message_time = self._event_time(event)
            message_id = self._extract_message_id(event)
            unique_key = self._build_unique_key(
                group_id, user_id, message_time, content, message_id
            )
            media_json = self._build_media_json(event)
        except Exception as e:
            self._log_verbose(f"message parse error: {e}")
            return

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

        if len(self._sqlite_pending) >= self._max_pending_size:
            dropped = self._sqlite_pending[: self._max_pending_size // 10]
            del self._sqlite_pending[: len(dropped)]
            self._sqlite_write_fail += len(dropped)
            self._log_verbose(f"pending overflow: dropped {len(dropped)} sqlite records")
        self._sqlite_pending.append(record)

        self._flush_sqlite_queue_if_needed()
        self._flush_qdrant_queue_if_needed()
        self._log_ingest_progress(group_id, user_id, content)

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

    @filter.command("聊天历史同步")
    async def sync_history_cn(self, event: AstrMessageEvent):
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
            yield event.plain_result(
                "时间格式错误，支持: YYYY-MM-DDTHH:MM:SS / YYYY-MM-DD_HH:MM:SS / YYYY-MM-DD HH:MM:SS"
            )
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
            for ts, gid, uid, uname, text, media_json in rows:
                line = self._format_export_line(text, media_json)
                f.write(f"[{ts}] [群:{gid}] [{uname or uid}] {line}\n")

        yield event.plain_result(f"导出完成，共 {len(rows)} 条\n文件: {out_file}")

    async def _handle_semantic_search(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行检索")
            return

        if not self._qdrant_enabled():
            yield event.plain_result("Qdrant 未启用，请在插件配置里开启 qdrant_enabled")
            return

        if not self._qdrant_client or not self._qdrant_models:
            yield event.plain_result(
                "Qdrant 初始化失败，请检查 qdrant_url / qdrant_api_key / qdrant_client 依赖"
            )
            return

        self._flush_all_queues(force=True)

        tokens = [t for t in (event.message_str or "").strip().split() if t]
        if len(tokens) < 2:
            yield event.plain_result(
                "用法: /聊天检索 [群号] [最近1小时|recent:2h] <问题>"
            )
            return

        group_id, query_text, since_dt = self._parse_search_args(tokens, event)

        if not query_text:
            yield event.plain_result("检索内容不能为空")
            return

        vector = self._embedding(query_text)
        if not vector:
            yield event.plain_result(
                "向量化失败，请检查 embedding_api_base / embedding_api_key / embedding_model"
            )
            return

        limit = self._int_conf("search_top_k", 5)
        fetch_k = max(limit, self._int_conf("search_fetch_k", 60))
        candidates = self._search_qdrant(vector, group_id, fetch_k, since_dt)
        points = self._post_filter_search_points(
            candidates, group_id, since_dt, query_text, limit
        )
        if not points:
            yield event.plain_result("未检索到相关聊天记录")
            return

        since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else "不限"
        lines = [f"检索结果（Top {len(points)}，时间下限: {since_text}）:"]
        for idx, p in enumerate(points, start=1):
            payload = self._point_payload(p)
            ts = self._norm(payload.get("ts"))
            gid = self._norm(payload.get("group_id"))
            uname = self._norm(payload.get("sender_name")) or self._norm(
                payload.get("user_id")
            )
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
        group_id = (
            self._norm(tokens[2])
            if len(tokens) >= 3
            else self._norm(event.get_group_id())
        )
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
        group_id = (
            self._norm(tokens[1])
            if len(tokens) >= 2
            else self._norm(event.get_group_id())
        )

        sqlite_total = self._count_sqlite()
        sqlite_group = self._count_sqlite(group_id) if group_id else sqlite_total

        qdrant_total: int | None = None
        qdrant_group: int | None = None
        qdrant_error = ""
        if self._qdrant_enabled() and self._qdrant_client:
            try:
                qdrant_total = self._count_qdrant("")
                qdrant_group = (
                    self._count_qdrant(group_id) if group_id else qdrant_total
                )
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
            f"- runtime_lsky_ok/fail: {self._lsky_upload_ok}/{self._lsky_upload_fail}",
            f"- queue_sqlite: {len(self._sqlite_pending)}",
            f"- queue_qdrant: {len(self._qdrant_pending)}",
        ]

        if not self._qdrant_enabled():
            lines.append("- qdrant: 未启用")
        elif not self._qdrant_client:
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
            f"- lsky_enabled: {self._lsky_enabled()}",
            f"- sqlite_pending: {len(self._sqlite_pending)}",
            f"- qdrant_pending: {len(self._qdrant_pending)}",
        ]
        yield event.plain_result("\n".join(lines))

    async def _handle_history_sync(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行历史同步")
            return

        if self._norm(event.get_platform_name()).lower() != "aiocqhttp":
            yield event.plain_result(
                "当前仅支持 aiocqhttp 平台历史同步（NapCat / Lagrange / OneBot V11）"
            )
            return

        tokens = [t for t in (event.message_str or "").strip().split() if t]
        group_id, limit = self._parse_history_sync_args(tokens, event)
        if not group_id:
            yield event.plain_result(
                "用法: /聊天历史同步 [条数] [群号]\n"
                "示例1: /聊天历史同步 200\n"
                "示例2: /聊天历史同步 200 123456"
            )
            return

        self._flush_all_queues(force=True)
        yield event.plain_result(
            f"开始同步群 {group_id} 的历史消息，目标 {self._clamp_history_limit(limit)} 条，请稍候..."
        )

        try:
            stats = await self._sync_group_history(event, group_id, limit)
        except Exception as e:
            err = self._norm(e) or "unknown_error"
            self._last_error = f"history_sync: {err}"
            logger.warning(f"[chat_export] 历史同步失败: {err}")
            yield event.plain_result(f"历史同步失败: {err}")
            return

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
        stop_reason = self._norm(stats.get("stop_reason"))
        if stop_reason:
            lines.append(f"终止原因: {stop_reason}")
        yield event.plain_result("\n".join(lines))

    async def _handle_chat_analyze(self, event: AstrMessageEvent):
        if not self._is_manager(event):
            yield event.plain_result("无权限执行聊天分析")
            return

        self._flush_all_queues(force=True)
        tokens = [t for t in (event.message_str or "").strip().split() if t]
        group_id, user_id, since_dt = self._parse_analyze_args(tokens, event)
        if not group_id:
            yield event.plain_result(
                "用法: /聊天分析 <群号> [用户ID] [最近24小时|recent:2d]\n"
                "示例1: /聊天分析 1058402699 最近24小时\n"
                "示例2: /聊天分析 1058402699 1097681347 recent:7d"
            )
            return

        limit = max(20, self._int_conf("analysis_max_messages", 400))
        rows = self._query_messages_for_analysis(group_id, user_id, since_dt, limit)
        if not rows:
            yield event.plain_result("未找到可分析的聊天记录")
            return

        transcript = self._build_analysis_transcript(rows)
        summary = self._call_analysis_llm(
            transcript=transcript,
            group_id=group_id,
            user_id=user_id,
            since_dt=since_dt,
            sample_size=len(rows),
        )
        if not summary:
            yield event.plain_result("分析失败：请检查 analysis_api_base / analysis_api_key / analysis_model")
            return

        who = f"用户 {user_id}" if user_id else "全群"
        since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else "不限"
        yield event.plain_result(
            f"[聊天分析]\n群号: {group_id}\n对象: {who}\n时间下限: {since_text}\n样本数: {len(rows)}\n\n{summary}"
        )

    async def _sync_group_history(
        self, event: AstrMessageEvent, group_id: str, limit: int
    ) -> dict[str, int]:
        bot = self._get_onebot_client(event)
        if bot is None:
            raise RuntimeError("未找到 aiocqhttp 客户端")

        limit = self._clamp_history_limit(limit)
        max_rounds = max(1, self._int_conf("history_sync_rounds", 3))
        no_new_stop_rounds = max(1, self._int_conf("history_sync_stop_no_new_rounds", 2))
        use_saved_cursor = bool(self.config.get("history_sync_resume_from_saved_cursor", True))
        overall_seen_keys: set[str] = set()
        total_pages = 0
        total_fetched = 0
        total_invalid = 0
        total_records: list[dict[str, Any]] = []
        rounds = 0
        consecutive_no_new = 0
        stop_reason = ""

        saved_cursor = self._load_history_cursor(group_id) if use_saved_cursor else None
        cursor = saved_cursor
        anchor_message_id = self._resolve_history_anchor_message_id(event, group_id)

        while rounds < max_rounds and len(total_records) < limit:
            rounds += 1
            pass_stats = await self._sync_group_history_pass(
                bot=bot,
                group_id=group_id,
                limit=(limit - len(total_records)),
                cursor=cursor,
                anchor_message_id=anchor_message_id,
                seen_keys=overall_seen_keys,
            )
            total_pages += pass_stats["pages"]
            total_fetched += pass_stats["fetched"]
            total_invalid += pass_stats["invalid"]
            total_records.extend(pass_stats["records"])
            cursor = pass_stats.get("cursor")

            if cursor:
                self._save_history_cursor(group_id, cursor)

            if pass_stats["insertable"] <= 0:
                consecutive_no_new += 1
            else:
                consecutive_no_new = 0

            if pass_stats["stop_reason"] == "no_items":
                stop_reason = "接口返回空页"
                break
            if pass_stats["stop_reason"] == "cursor_stable":
                stop_reason = "游标不再推进"
                break
            if len(total_records) >= limit:
                stop_reason = "达到目标条数"
                break
            if consecutive_no_new >= no_new_stop_rounds:
                stop_reason = f"连续{consecutive_no_new}轮无新增候选"
                break

        if not stop_reason:
            stop_reason = "达到最大轮次"

        total_records.sort(key=lambda rec: (rec["ts"], rec["message_id"], rec["user_id"]))
        inserted = self._ingest_history_records(total_records)
        return {
            "rounds": rounds,
            "pages": total_pages,
            "fetched": total_fetched,
            "normalized": len(total_records),
            "inserted": len(inserted),
            "duplicates": max(0, len(total_records) - len(inserted)),
            "invalid": total_invalid,
            "stop_reason": stop_reason,
        }

    async def _sync_group_history_pass(
        self,
        bot: Any,
        group_id: str,
        limit: int,
        cursor: dict[str, Any] | None,
        anchor_message_id: str,
        seen_keys: set[str],
    ) -> dict[str, Any]:
        page_size = min(limit, max(1, self._int_conf("history_sync_page_size", 20)))
        max_pages = max(1, self._int_conf("history_sync_max_pages", 50))
        pages = 0
        fetched = 0
        invalid = 0
        records: list[dict[str, Any]] = []
        stop_reason = ""
        local_cursor = cursor

        while len(records) < limit and pages < max_pages:
            batch_size = min(page_size, limit - len(records))
            response, action_name = await self._fetch_group_history_page(
                bot, group_id, batch_size, local_cursor, anchor_message_id
            )
            items = self._extract_history_items(response)
            if not items:
                stop_reason = "no_items"
                break

            pages += 1
            fetched += len(items)
            self._log_verbose(
                f"history_sync page={pages} action={action_name} items={len(items)} cursor={local_cursor or {}}"
            )

            for item in items:
                record = self._normalize_history_message(group_id, item)
                if record is None:
                    invalid += 1
                    continue
                if record["unique_key"] in seen_keys:
                    continue
                seen_keys.add(record["unique_key"])
                records.append(record)
                if len(records) >= limit:
                    break

            next_cursor = self._next_history_cursor(items, local_cursor)
            if next_cursor is None:
                stop_reason = "cursor_none"
                break
            if local_cursor is not None and next_cursor == local_cursor:
                stop_reason = "cursor_stable"
                break
            local_cursor = next_cursor

        if not stop_reason and pages >= max_pages:
            stop_reason = "max_pages"

        return {
            "pages": pages,
            "fetched": fetched,
            "invalid": invalid,
            "records": records,
            "insertable": len(records),
            "cursor": local_cursor,
            "stop_reason": stop_reason,
        }

    def _ingest_history_records(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not records:
            return []

        self._sqlite_pending.extend(records)
        inserted = self._flush_sqlite_queue(force=True)
        if inserted:
            self._enqueue_qdrant_records(inserted)
            self._flush_qdrant_queue(force=True)
        return inserted

    def _get_onebot_client(self, event: AstrMessageEvent) -> Any:
        bot = getattr(event, "bot", None)
        if bot is not None:
            return bot
        message_obj = getattr(event, "message_obj", None)
        return getattr(message_obj, "bot", None)

    async def _fetch_group_history_page(
        self,
        bot: Any,
        group_id: str,
        limit: int,
        cursor: dict[str, Any] | None,
        anchor_message_id: str,
    ) -> tuple[Any, str]:
        errors: list[str] = []
        bucket = self._history_strategy_bucket(cursor)
        for action, params in self._build_history_call_candidates(
            group_id, limit, cursor, anchor_message_id
        ):
            try:
                response = await self._call_onebot_action(bot, action, **params)
            except Exception as e:
                errors.append(f"{action}: {self._norm(e)}")
                continue

            if self._is_onebot_action_failed(response):
                err = self._extract_onebot_action_error(response) or "action_failed"
                errors.append(f"{action}: {err}")
                continue

            self._remember_history_action_hint(bucket, action, params)
            return response, action

        err_text = (
            "; ".join(err for err in errors[-4:] if err) or "协议端不支持群历史消息 API"
        )
        raise RuntimeError(err_text)

    async def _call_onebot_action(self, bot: Any, action: str, **params) -> Any:
        errors: list[str] = []
        timeout_sec = max(1.0, self._float_conf("history_sync_action_timeout_sec", 8.0))

        call_action = getattr(bot, "call_action", None)
        if callable(call_action):
            try:
                awaitable = call_action(action=action, **params)
            except TypeError:
                awaitable = call_action(action, **params)
            try:
                return await asyncio.wait_for(awaitable, timeout=timeout_sec)
            except asyncio.TimeoutError:
                errors.append(f"timeout>{timeout_sec:.0f}s")
            except Exception as e:
                errors.append(self._norm(e))

        api = getattr(bot, "api", None)
        api_call_action = getattr(api, "call_action", None)
        if callable(api_call_action):
            try:
                return await asyncio.wait_for(
                    api_call_action(action, **params), timeout=timeout_sec
                )
            except asyncio.TimeoutError:
                errors.append(f"api_timeout>{timeout_sec:.0f}s")
            except Exception as e:
                errors.append(self._norm(e))

        err_text = (
            " | ".join(err for err in errors if err) or "call_action_not_available"
        )
        raise RuntimeError(err_text)

    def _build_history_call_candidates(
        self,
        group_id: str,
        limit: int,
        cursor: dict[str, Any] | None,
        anchor_message_id: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        group_value: Any = int(group_id) if group_id.isdigit() else group_id
        count = max(1, limit)
        initial_message_id = anchor_message_id or "0"
        if cursor is None:
            param_candidates = [
                {
                    "group_id": group_value,
                    "message_seq": 0,
                    "count": count,
                    "reverseOrder": False,
                },
                {
                    "group_id": group_value,
                    "message_seq": 0,
                    "count": count,
                    "reverse_order": False,
                },
                {
                    "group_id": group_value,
                    "message_seq": 0,
                    "count": count,
                    "reverse_order": False,
                    "disable_get_url": False,
                    "parse_mult_msg": True,
                    "quick_reply": False,
                },
                {"group_id": group_value, "message_seq": 0, "count": count},
                {
                    "group_id": group_value,
                    "message_id": initial_message_id,
                    "count": count,
                },
                {"group_id": group_value, "message_id": 0, "count": count},
                {"group_id": group_value, "count": count},
            ]
        else:
            message_seq = cursor.get("message_seq")
            message_id = self._norm(cursor.get("message_id"))
            param_candidates = []
            if message_seq is not None:
                param_candidates.extend(
                    [
                        {
                            "group_id": group_value,
                            "message_seq": message_seq,
                            "count": count,
                            "reverseOrder": False,
                        },
                        {
                            "group_id": group_value,
                            "message_seq": message_seq,
                            "count": count,
                            "reverse_order": False,
                        },
                        {
                            "group_id": group_value,
                            "message_seq": message_seq,
                            "count": count,
                        },
                        {"group_id": group_value, "seq": message_seq, "count": count},
                        {
                            "group_id": group_value,
                            "last_seq": message_seq,
                            "count": count,
                        },
                        {
                            "group_id": group_value,
                            "start_seq": message_seq,
                            "count": count,
                        },
                    ]
                )
            if message_id:
                param_candidates.extend(
                    [
                        {
                            "group_id": group_value,
                            "message_id": message_id,
                            "count": count,
                        },
                        {"group_id": group_value, "message_id": message_id},
                    ]
                )

        candidates: list[tuple[str, dict[str, Any]]] = []
        seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
        for action in ("get_group_msg_history", "get_group_history_msg"):
            for params in param_candidates:
                key = (
                    action,
                    tuple(sorted((k, self._norm(v)) for k, v in params.items())),
                )
                if key in seen:
                    continue
                seen.add(key)
                candidates.append((action, params))

        preferred = self._history_action_hints.get(
            self._history_strategy_bucket(cursor)
        )
        if preferred is None:
            return candidates

        preferred_candidates: list[tuple[str, dict[str, Any]]] = []
        other_candidates: list[tuple[str, dict[str, Any]]] = []
        for action, params in candidates:
            if self._history_action_signature(action, params) == preferred:
                preferred_candidates.append((action, params))
            else:
                other_candidates.append((action, params))
        return preferred_candidates + other_candidates

    def _history_strategy_bucket(self, cursor: dict[str, Any] | None) -> str:
        if not cursor:
            return "initial"

        has_seq = cursor.get("message_seq") is not None
        has_message_id = bool(self._norm(cursor.get("message_id")))
        if has_seq and has_message_id:
            return "cursor_seq_message_id"
        if has_seq:
            return "cursor_seq"
        if has_message_id:
            return "cursor_message_id"
        return "cursor"

    @staticmethod
    def _history_action_signature(
        action: str, params: dict[str, Any]
    ) -> tuple[str, tuple[str, ...]]:
        return action, tuple(sorted(params.keys()))

    def _remember_history_action_hint(
        self, bucket: str, action: str, params: dict[str, Any]
    ):
        signature = self._history_action_signature(action, params)
        if self._history_action_hints.get(bucket) == signature:
            return
        self._history_action_hints[bucket] = signature
        self._log_verbose(
            f"history_sync strategy bucket={bucket} action={action} keys={sorted(params.keys())}"
        )

    def _extract_history_items(self, response: Any) -> list[dict[str, Any]]:
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        if not isinstance(response, dict):
            return []

        for key in ("messages", "msg_list", "records", "list"):
            value = response.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        data = response.get("data")
        if isinstance(data, (dict, list)):
            items = self._extract_history_items(data)
            if items or self._has_history_container(data):
                return items

        if self._looks_like_history_item(response):
            return [response]
        return []

    @staticmethod
    def _has_history_container(obj: Any) -> bool:
        if isinstance(obj, list):
            return True
        if not isinstance(obj, dict):
            return False
        return any(
            key in obj for key in ("messages", "msg_list", "records", "list", "data")
        )

    @staticmethod
    def _looks_like_history_item(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        keys = {
            "message_id",
            "msg_id",
            "message",
            "raw_message",
            "sender",
            "time",
            "user_id",
        }
        return any(key in obj for key in keys)

    def _normalize_history_message(
        self, default_group_id: str, item: dict[str, Any]
    ) -> dict[str, Any] | None:
        group_id = self._norm(item.get("group_id")) or default_group_id
        if not group_id:
            return None

        message_time = self._extract_history_time(item)
        if message_time is None:
            return None

        user_id = self._extract_history_user_id(item)
        sender_name = self._extract_history_sender_name(item)
        content = self._extract_text_from_history_message(item)
        message_id = self._extract_message_id_from_obj(item)
        unique_key = self._build_unique_key(
            group_id, user_id, message_time, content, message_id
        )
        media_json = self._build_media_json_from_obj(
            item.get("message") if "message" in item else item
        )

        return {
            "ts": message_time.strftime("%Y-%m-%d %H:%M:%S"),
            "group_id": group_id,
            "user_id": user_id,
            "sender_name": sender_name,
            "content": content,
            "message_id": message_id,
            "unique_key": unique_key,
            "media_json": media_json,
        }

    def _extract_history_time(self, item: dict[str, Any]) -> datetime | None:
        for key in (
            "time",
            "timestamp",
            "message_time",
            "msg_time",
            "send_time",
            "date",
        ):
            dt = self._coerce_datetime(item.get(key))
            if dt is not None:
                return dt
        return None

    def _extract_history_user_id(self, item: dict[str, Any]) -> str:
        sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}
        for value in (
            item.get("user_id"),
            item.get("sender_id"),
            sender.get("user_id"),
            sender.get("uin"),
            sender.get("id"),
        ):
            text = self._norm(value)
            if text:
                return text
        return ""

    def _extract_history_sender_name(self, item: dict[str, Any]) -> str:
        sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}
        for value in (
            item.get("sender_name"),
            item.get("nickname"),
            sender.get("card"),
            sender.get("nickname"),
            sender.get("name"),
        ):
            text = self._norm(value)
            if text:
                return text
        return ""

    def _extract_text_from_history_message(self, item: dict[str, Any]) -> str:
        raw_candidates = [
            item.get("message_str"),
            item.get("raw_message"),
            item.get("text"),
            item.get("content"),
        ]
        for value in raw_candidates:
            text = self._norm(value)
            if text and text not in {"[图片]", "[表情]", "[动画表情]"}:
                return text

        for key in ("message", "messages", "segments", "content"):
            structured = self._format_message_obj(item.get(key))
            if structured:
                return structured

        for value in raw_candidates:
            text = self._norm(value)
            if text:
                return text
        return "<空消息>"

    def _extract_history_seq(self, item: dict[str, Any]) -> int | None:
        for key in ("message_seq", "messageSeq", "seq", "msg_seq"):
            value = item.get(key)
            if value is None:
                continue
            text = self._norm(value)
            if text.lstrip("-").isdigit():
                return int(text)
        return None

    def _extract_history_message_id(self, item: dict[str, Any]) -> str:
        message_id = self._extract_message_id_from_obj(item)
        if message_id:
            return message_id
        return self._norm(item.get("message_id"))

    def _next_history_cursor(
        self, items: list[dict[str, Any]], prev_cursor: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if not items:
            return None

        oldest_seq: int | None = None
        last_message_id = ""
        oldest_message_id = ""
        for item in items:
            message_id = self._extract_history_message_id(item)
            if message_id:
                last_message_id = message_id

            seq = self._extract_history_seq(item)
            if seq is not None and (oldest_seq is None or seq < oldest_seq):
                oldest_seq = seq
                oldest_message_id = message_id

        next_cursor: dict[str, Any] = {}
        if oldest_seq is not None:
            next_seq = oldest_seq - 1 if oldest_seq > 0 else oldest_seq
            prev_seq = None if prev_cursor is None else prev_cursor.get("message_seq")
            if prev_seq != next_seq:
                next_cursor["message_seq"] = next_seq

        message_id_cursor = oldest_message_id or last_message_id
        prev_message_id = (
            "" if prev_cursor is None else self._norm(prev_cursor.get("message_id"))
        )
        if message_id_cursor and prev_message_id != message_id_cursor:
            next_cursor["message_id"] = message_id_cursor

        return next_cursor or None

    def _resolve_history_anchor_message_id(
        self, event: AstrMessageEvent, group_id: str
    ) -> str:
        if self._norm(event.get_group_id()) == group_id:
            message_id = self._extract_message_id(event)
            if message_id:
                return message_id
        return self._latest_group_message_id(group_id)

    def _load_history_cursor(self, group_id: str) -> dict[str, Any] | None:
        sql = "SELECT cursor_json FROM history_sync_state WHERE group_id = ?"
        for retry in range(2):
            try:
                conn = self._get_db_conn(reset=(retry == 1))
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

    def _save_history_cursor(self, group_id: str, cursor: dict[str, Any]):
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
                conn = self._get_db_conn(reset=(retry == 1))
                conn.execute(sql, (group_id, payload, now_text))
                conn.commit()
                return
            except Exception as e:
                self._last_error = f"history_cursor_save: {e}"
                if retry == 0:
                    continue
                logger.warning(f"[chat_export] 历史游标保存失败: {e}")
                return

    def _is_onebot_action_failed(self, response: Any) -> bool:
        if not isinstance(response, dict):
            return False
        status = self._norm(response.get("status")).lower()
        retcode = response.get("retcode")
        if status == "failed":
            return True
        return retcode not in (None, 0, "0")

    def _extract_onebot_action_error(self, response: Any) -> str:
        if not isinstance(response, dict):
            return ""
        for key in ("wording", "msg", "message"):
            text = self._norm(response.get(key))
            if text:
                return text
        return ""

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

        # 冷却期检查：Qdrant 连续失败时直接丢弃，避免队列无限膨胀
        if self._qdrant_error_count >= self._max_consecutive_errors:
            now = time.time()
            if now - self._qdrant_last_error_ts < self._error_cooldown_sec:
                dropped = len([
                    r for r in records
                    if self._should_index_to_qdrant(self._norm(r.get("content")))
                ])
                self._qdrant_write_fail += dropped
                self._log_verbose(f"qdrant enqueue skipped: in cooldown, dropped {dropped} records")
                return

        # 队列溢出保护：超出上限时丢弃最旧 10%
        if len(self._qdrant_pending) >= self._max_pending_size:
            drop_count = min(len(self._qdrant_pending) // 10 or 1, len(self._qdrant_pending))
            dropped = self._qdrant_pending[:drop_count]
            del self._qdrant_pending[:drop_count]
            self._qdrant_write_fail += drop_count
            self._log_verbose(f"qdrant pending overflow: dropped {drop_count} records")

        for rec in records:
            if self._should_index_to_qdrant(self._norm(rec.get("content"))):
                self._qdrant_pending.append(rec)

    def _flush_sqlite_queue_if_needed(self):
        if not self._sqlite_pending:
            return
        batch_size = max(1, self._int_conf("sqlite_batch_size", 20))
        interval = max(0.2, self._float_conf("sqlite_flush_interval_sec", 1.0))
        now = time.time()
        if (
            len(self._sqlite_pending) < batch_size
            and (now - self._last_sqlite_flush_ts) < interval
        ):
            return
        inserted = self._flush_sqlite_queue(force=False)
        if inserted:
            self._enqueue_qdrant_records(inserted)

    def _flush_sqlite_queue(self, force: bool) -> list[dict[str, Any]]:
        if not self._sqlite_pending:
            return []

        # 错误冷却：连续错误超限后丢弃数据，防止无限重试风暴
        if self._sqlite_error_count >= self._max_consecutive_errors:
            now = time.time()
            if now - self._sqlite_last_error_ts < self._error_cooldown_sec:
                dropped = self._sqlite_pending[: len(self._sqlite_pending) // 2 or 1]
                self._sqlite_write_fail += len(dropped)
                del self._sqlite_pending[: len(dropped)]
                self._last_sqlite_flush_ts = now
                self._log_verbose(f"sqlite cooldown: dropped {len(dropped)} records")
                return []
            # 冷却结束，重置计数
            self._sqlite_error_count = 0

        batch_size = max(1, self._int_conf("sqlite_batch_size", 20))
        take = (
            len(self._sqlite_pending)
            if force
            else min(len(self._sqlite_pending), batch_size)
        )
        batch = self._sqlite_pending[:take]
        del self._sqlite_pending[:take]

        sql = (
            "INSERT OR IGNORE INTO chat_messages(ts, group_id, user_id, sender_name, content, media_json, message_id, unique_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
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
                self._sqlite_write_fail += len(batch)
                self._sqlite_error_count += 1
                self._sqlite_last_error_ts = time.time()
                # 使用截断日志，避免超长异常信息
                err_text = self._norm(e)[: self._max_log_line_length]
                logger.error(f"[chat_export] sqlite 批量写入失败: {err_text}")
                self._sqlite_pending = batch + self._sqlite_pending
                self._last_sqlite_flush_ts = time.time()
                return []

        # 成功则重置错误计数
        self._sqlite_error_count = 0
        self._sqlite_write_ok += len(inserted)
        self._sqlite_dedup_skip += duplicate_count
        if inserted:
            self._last_sqlite_ok_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._last_sqlite_flush_ts = time.time()
        return inserted

    def _query_messages(self, start_dt: datetime, end_dt: datetime, group_id: str):
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
                conn = self._get_db_conn(reset=(retry == 1))
                return conn.execute(sql, tuple(args)).fetchall()
            except Exception as e:
                self._last_error = f"sqlite_query: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite query 失败: {e}")
                return []
        return []

    def _query_messages_for_analysis(
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
                conn = self._get_db_conn(reset=(retry == 1))
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

    def _latest_group_message_id(self, group_id: str) -> str:
        if not group_id:
            return ""

        sql = (
            "SELECT message_id FROM chat_messages "
            "WHERE group_id = ? AND message_id IS NOT NULL AND message_id != '' "
            "ORDER BY ts DESC, id DESC LIMIT 1"
        )
        for retry in range(2):
            try:
                conn = self._get_db_conn(reset=(retry == 1))
                row = conn.execute(sql, (group_id,)).fetchone()
                return self._norm(row[0]) if row and row[0] is not None else ""
            except Exception as e:
                self._last_error = f"sqlite_latest_message_id: {e}"
                if retry == 0:
                    continue
                logger.error(f"[chat_export] sqlite latest message_id 查询失败: {e}")
                return ""
        return ""

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
                vectors_config=self._qdrant_models.VectorParams(
                    size=dim, distance=distance
                ),
            )
            return
        except Exception as e:
            logger.warning(f"[chat_export] qdrant sdk 建集合失败，尝试 HTTP 回退: {e}")

        self._create_collection_via_http(collection, dim, distance_name)

    def _create_collection_via_http(
        self, collection: str, dim: int, distance_name: str
    ):
        base = self._norm(
            self.config.get("qdrant_url", "http://127.0.0.1:6333")
        ).rstrip("/")
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
                with request.urlopen(
                    req, timeout=self._int_conf("qdrant_timeout", 10)
                ) as _:
                    return
            except Exception as e:
                last_err = e

        raise RuntimeError(f"qdrant HTTP 建集合失败: {last_err}")

    def _flush_qdrant_queue_if_needed(self):
        if not self._qdrant_pending:
            return
        if (
            not self._qdrant_enabled()
            or not self._qdrant_client
            or not self._qdrant_models
        ):
            return

        batch_size = max(1, self._int_conf("qdrant_batch_size", 20))
        interval = max(0.2, self._float_conf("qdrant_flush_interval_sec", 1.0))
        now = time.time()
        if (
            len(self._qdrant_pending) < batch_size
            and (now - self._last_qdrant_flush_ts) < interval
        ):
            return
        self._flush_qdrant_queue(force=False)

    def _flush_qdrant_queue(self, force: bool) -> list[dict[str, Any]]:
        if not self._qdrant_pending:
            return []
        if (
            not self._qdrant_enabled()
            or not self._qdrant_client
            or not self._qdrant_models
        ):
            return []

        # 错误冷却保护：冷却期内直接丢弃数据，不调用 Embedding API
        if self._qdrant_error_count >= self._max_consecutive_errors:
            now = time.time()
            if now - self._qdrant_last_error_ts < self._error_cooldown_sec:
                dropped = self._qdrant_pending[: len(self._qdrant_pending) // 2 or 1]
                self._qdrant_write_fail += len(dropped)
                del self._qdrant_pending[: len(dropped)]
                self._last_qdrant_flush_ts = now
                self._log_verbose(f"qdrant cooldown: dropped {len(dropped)} records")
                return []
            self._qdrant_error_count = 0

        batch_size = max(1, self._int_conf("qdrant_batch_size", 20))
        take = (
            len(self._qdrant_pending)
            if force
            else min(len(self._qdrant_pending), batch_size)
        )
        batch = self._qdrant_pending[:take]
        del self._qdrant_pending[:take]

        texts = [r["content"] for r in batch]
        vectors = self._embedding_batch(texts)
        if len(vectors) != len(batch):
            self._qdrant_write_fail += len(batch)
            self._qdrant_error_count += 1
            self._qdrant_last_error_ts = time.time()
            self._last_error = "embedding_batch_size_mismatch"
            # 回退到队列头部，避免丢数据
            self._qdrant_pending = batch + self._qdrant_pending
            return []

        collection = self._norm(self.config.get("qdrant_collection", "chat_export"))
        points: list[Any] = []
        retry_records: list[dict[str, Any]] = []
        for rec, vec in zip(batch, vectors):
            if not vec:
                # 空向量也回退到队列尾部重试，避免永久丢失
                retry_records.append(rec)
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
            points.append(
                self._qdrant_models.PointStruct(
                    id=point_id, vector=vec, payload=payload
                )
            )

        # 空向量记录回退
        if retry_records:
            self._qdrant_pending.extend(retry_records)
            self._log_verbose(f"qdrant empty vectors: retry {len(retry_records)} records")

        if not points:
            self._last_qdrant_flush_ts = time.time()
            return []

        try:
            self._qdrant_client.upsert(
                collection_name=collection, points=points, wait=False
            )
            self._qdrant_write_ok += len(points)
            self._qdrant_error_count = 0
            self._last_qdrant_ok_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self._qdrant_write_fail += len(points)
            self._qdrant_error_count += 1
            self._qdrant_last_error_ts = time.time()
            self._last_error = f"qdrant_upsert: {e}"
            err_text = self._norm(e)[: self._max_log_line_length]
            logger.warning(f"[chat_export] qdrant upsert 失败: {err_text}")
            # upsert 失败时，将对应原始记录回退到队列头部
            failed_records = [rec for rec, vec in zip(batch, vectors) if vec]
            self._qdrant_pending = failed_records[:len(points)] + self._qdrant_pending
        finally:
            self._last_qdrant_flush_ts = time.time()
        return points

    def _search_qdrant(
        self,
        query_vector: list[float],
        group_id: str,
        limit: int,
        since_dt: datetime | None,
    ):
        if not self._qdrant_client or not self._qdrant_models:
            return []

        collection = self._norm(self.config.get("qdrant_collection", "chat_export"))
        q_filter = self._build_qdrant_filter(group_id, since_dt)

        # 先拉取更大的候选集，后续在插件层做硬过滤与重排
        top_k = max(1, min(limit, 100))

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

            logger.error(
                "[chat_export] qdrant search 失败: 当前 qdrant-client 不支持 search/query_points"
            )
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
        base = self._norm(
            self.config.get("qdrant_url", "http://127.0.0.1:6333")
        ).rstrip("/")
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

        api_base = self._norm(
            self.config.get("embedding_api_base", "https://api.openai.com/v1")
        )
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
            with request.urlopen(
                req, timeout=self._int_conf("embedding_timeout", 20)
            ) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            vecs: list[list[float]] = [[] for _ in texts]
            for item in data.get("data", []) if isinstance(data, dict) else []:
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

    def _build_media_json(self, event: AstrMessageEvent) -> str:
        refs = self._extract_image_refs(event)
        return self._build_media_json_from_refs(refs)

    def _build_media_json_from_obj(self, obj: Any) -> str:
        refs = self._extract_image_refs_from_obj(obj)
        return self._build_media_json_from_refs(refs)

    def _build_media_json_from_refs(self, refs: list[dict[str, str]]) -> str:
        if not refs:
            return ""

        items: list[dict[str, Any]] = []
        for ref in refs:
            item: dict[str, Any] = {
                "type": "image",
                "source_url": self._norm(ref.get("url")),
                "source_file": self._norm(ref.get("file")),
            }
            if self._lsky_enabled():
                ok, lsky_url, lsky_key, err = self._upload_ref_to_lsky(ref)
                if ok:
                    item["lsky_url"] = lsky_url
                    item["lsky_key"] = lsky_key
                    item["status"] = "uploaded"
                    self._lsky_upload_ok += 1
                else:
                    item["status"] = "failed"
                    item["error"] = err
                    self._lsky_upload_fail += 1
            else:
                item["status"] = "skipped"
            items.append(item)

        try:
            return json.dumps(items, ensure_ascii=False)
        except Exception:
            return ""

    def _format_export_line(self, text: str, media_json: str) -> str:
        line = text or ""
        if not media_json:
            return line
        try:
            items = json.loads(media_json)
        except Exception:
            return line
        if not isinstance(items, list):
            return line
        lsky_urls = []
        for it in items:
            if not isinstance(it, dict):
                continue
            url = self._norm(it.get("lsky_url"))
            if url:
                lsky_urls.append(url)
        if not lsky_urls:
            return line
        return f"{line} {' '.join(f'[图床:{u}]' for u in lsky_urls)}".strip()

    def _extract_image_refs(self, event: AstrMessageEvent) -> list[dict[str, str]]:
        refs: list[dict[str, str]] = []
        seen: set[str] = set()
        candidates = [
            getattr(event, "message_obj", None),
            getattr(event, "message", None),
            getattr(event, "messages", None),
        ]
        for obj in candidates:
            self._collect_image_refs(obj, refs, seen)
        return refs

    def _extract_image_refs_from_obj(self, obj: Any) -> list[dict[str, str]]:
        refs: list[dict[str, str]] = []
        seen: set[str] = set()
        self._collect_image_refs(obj, refs, seen)
        return refs

    def _collect_image_refs(self, obj: Any, refs: list[dict[str, str]], seen: set[str]):
        if obj is None:
            return
        if isinstance(obj, list):
            for seg in obj:
                self._collect_image_refs(seg, refs, seen)
            return
        if isinstance(obj, dict):
            seg_type = self._norm(obj.get("type")).lower()
            data = obj.get("data") if isinstance(obj.get("data"), dict) else {}
            if seg_type == "image":
                url = self._norm(data.get("url"))
                file = self._norm(data.get("file"))
                key = f"{url}|{file}"
                if key and key not in seen:
                    seen.add(key)
                    refs.append({"url": url, "file": file})
            for k in ("message", "messages", "segments"):
                if isinstance(obj.get(k), list):
                    self._collect_image_refs(obj.get(k), refs, seen)
            return

        seg_type = self._norm(getattr(obj, "type", "")).lower()
        if seg_type == "image":
            data = getattr(obj, "data", None)
            if isinstance(data, dict):
                url = self._norm(data.get("url"))
                file = self._norm(data.get("file"))
                key = f"{url}|{file}"
                if key and key not in seen:
                    seen.add(key)
                    refs.append({"url": url, "file": file})

    def _lsky_enabled(self) -> bool:
        return bool(self.config.get("lsky_enabled", False))

    def _upload_ref_to_lsky(self, ref: dict[str, str]) -> tuple[bool, str, str, str]:
        source_url = self._norm(ref.get("url"))
        source_file = self._norm(ref.get("file"))

        data = b""
        filename = ""
        if source_url.startswith("http://") or source_url.startswith("https://"):
            data, filename, err = self._download_bytes(source_url)
            if err:
                self._last_error = f"lsky_download: {err}"
                return False, "", "", err
        elif source_file and (
            source_file.startswith("http://") or source_file.startswith("https://")
        ):
            data, filename, err = self._download_bytes(source_file)
            if err:
                self._last_error = f"lsky_download: {err}"
                return False, "", "", err
        else:
            return False, "", "", "no_http_image_source"

        ok, lsky_url, lsky_key, err = self._lsky_upload_bytes(data, filename)
        if not ok:
            self._last_error = f"lsky_upload: {err}"
            return False, "", "", err
        return True, lsky_url, lsky_key, ""

    def _download_bytes(self, url: str) -> tuple[bytes, str, str]:
        try:
            req = request.Request(url=url, method="GET")
            with request.urlopen(
                req, timeout=self._int_conf("lsky_timeout", 20)
            ) as resp:
                body = resp.read()
            parsed = urlparse(url)
            filename = Path(parsed.path).name or f"img_{int(time.time() * 1000)}.jpg"
            return body, filename, ""
        except Exception as e:
            return b"", "", self._norm(e)

    def _lsky_upload_bytes(
        self, data: bytes, filename: str
    ) -> tuple[bool, str, str, str]:
        api_base = self._norm(self.config.get("lsky_api_base", "")).rstrip("/")
        token = self._norm(self.config.get("lsky_token", ""))
        album_id = self._norm(self.config.get("lsky_album_id", ""))
        if not api_base or not token:
            return False, "", "", "missing_lsky_api_or_token"

        endpoint = f"{api_base}/api/v1/upload"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        boundary = f"----AstrBotBoundary{int(time.time()*1000)}"
        body = self._build_multipart_body(
            boundary, filename, content_type, data, album_id
        )

        req = request.Request(url=endpoint, data=body, method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
        req.add_header("Accept", "application/json")
        try:
            with request.urlopen(
                req, timeout=self._int_conf("lsky_timeout", 20)
            ) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            obj = json.loads(raw) if raw else {}
            payload = obj.get("data", {}) if isinstance(obj, dict) else {}
            links = payload.get("links", {}) if isinstance(payload, dict) else {}
            lsky_url = self._norm(links.get("url")) or self._norm(payload.get("url"))
            lsky_key = self._norm(payload.get("key"))
            if not lsky_url:
                return False, "", "", "lsky_response_no_url"
            return True, lsky_url, lsky_key, ""
        except Exception as e:
            return False, "", "", self._norm(e)

    def _build_multipart_body(
        self,
        boundary: str,
        filename: str,
        content_type: str,
        file_bytes: bytes,
        album_id: str,
    ) -> bytes:
        sep = f"--{boundary}\r\n".encode("utf-8")
        end = f"--{boundary}--\r\n".encode("utf-8")
        chunks: list[bytes] = []
        if album_id:
            chunks.append(sep)
            chunks.append(b'Content-Disposition: form-data; name="album_id"\r\n\r\n')
            chunks.append(album_id.encode("utf-8"))
            chunks.append(b"\r\n")

        safe_name = filename.replace('"', "_")
        chunks.append(sep)
        chunks.append(
            (
                'Content-Disposition: form-data; name="file"; filename="'
                + safe_name
                + '"\r\n'
            ).encode("utf-8")
        )
        chunks.append((f"Content-Type: {content_type}\r\n\r\n").encode("utf-8"))
        chunks.append(file_bytes)
        chunks.append(b"\r\n")
        chunks.append(end)
        return b"".join(chunks)

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
        s = self._extract_message_id_from_obj(obj)
        if s:
            return s
        return ""

    def _extract_message_id_from_obj(self, obj: Any) -> str:
        if not isinstance(obj, dict):
            return ""
        for key in ("message_id", "msg_id", "id"):
            s = self._norm(obj.get(key))
            if s:
                return s
        data = obj.get("data")
        if isinstance(data, dict):
            for key in ("message_id", "msg_id", "id"):
                s = self._norm(data.get(key))
                if s:
                    return s
        return ""

    def _build_unique_key(
        self,
        group_id: str,
        user_id: str,
        message_time: datetime,
        content: str,
        message_id: str,
    ) -> str:
        if message_id:
            return f"gid:{group_id}|mid:{message_id}"
        digest = hashlib.sha1(content.encode("utf-8", errors="ignore")).hexdigest()
        return f"gid:{group_id}|uid:{user_id}|ts:{message_time.strftime('%Y-%m-%d %H:%M:%S')}|sha1:{digest}"

    def _parse_search_args(
        self, tokens: list[str], event: AstrMessageEvent
    ) -> tuple[str, str, datetime | None]:
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

    def _parse_analyze_args(
        self, tokens: list[str], event: AstrMessageEvent
    ) -> tuple[str, str, datetime | None]:
        args = tokens[1:]
        default_group = self._norm(event.get_group_id())
        group_id = default_group
        user_id = ""
        since_dt = None

        if args and args[0].isdigit():
            group_id = self._norm(args[0])
            args = args[1:]
        if args and args[0].isdigit():
            user_id = self._norm(args[0])
            args = args[1:]

        for token in args:
            parsed = self._parse_recent_time_token(token)
            if parsed is not None and since_dt is None:
                since_dt = parsed

        if since_dt is None:
            default_hours = self._int_conf("analysis_default_since_hours", 72)
            if default_hours > 0:
                since_dt = datetime.now() - timedelta(hours=default_hours)

        return group_id, user_id, since_dt

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

    def _post_filter_search_points(
        self,
        points: list[Any],
        group_id: str,
        since_dt: datetime | None,
        query_text: str,
        limit: int,
    ) -> list[Any]:
        if not points:
            return []

        strict_group = bool(self.config.get("search_hard_group_filter", True))
        strict_time = bool(self.config.get("search_hard_time_filter", True))
        keyword_mode = self._norm(
            self.config.get("search_keyword_mode", "auto")
        ).lower()
        query = (query_text or "").strip().lower()

        filtered: list[Any] = []
        for p in points:
            payload = self._point_payload(p)
            if not payload:
                continue

            if (
                strict_group
                and group_id
                and self._norm(payload.get("group_id")) != group_id
            ):
                continue

            if strict_time and since_dt:
                ts = self._parse_dt(self._norm(payload.get("ts")))
                if not ts or ts < since_dt:
                    continue

            filtered.append(p)

        if not filtered:
            return []

        # 提升“包含关键词”的结果，减少纯语义误召回。
        if query and keyword_mode != "off":
            scored: list[tuple[int, Any]] = []
            for p in filtered:
                payload = self._point_payload(p)
                text = self._norm(payload.get("content")).lower()
                hit = 1 if (query and query in text) else 0
                scored.append((hit, p))

            scored.sort(key=lambda x: x[0], reverse=True)
            filtered = [x[1] for x in scored]

            # auto 模式：query 很短时，只保留包含关键词的结果，避免偏题
            if keyword_mode == "auto" and query and len(query) <= 8:
                hard_hits = [
                    p
                    for p in filtered
                    if query
                    in self._norm(self._point_payload(p).get("content")).lower()
                ]
                if hard_hits:
                    filtered = hard_hits

        return filtered[: max(1, limit)]

    def _build_analysis_transcript(
        self, rows: list[tuple[str, str, str, str, str]]
    ) -> str:
        max_chars = max(2000, self._int_conf("analysis_max_chars", 18000))
        lines: list[str] = []
        total = 0
        for ts, gid, uid, uname, content in rows:
            speaker = self._norm(uname) or self._norm(uid)
            text = self._norm(content).replace("\n", " ").strip()
            line = f"[{ts}] [{speaker}] {text}"
            total += len(line) + 1
            if total > max_chars:
                break
            lines.append(line)
        return "\n".join(lines)

    def _call_analysis_llm(
        self,
        transcript: str,
        group_id: str,
        user_id: str,
        since_dt: datetime | None,
        sample_size: int,
    ) -> str:
        if not transcript:
            return ""
        api_base = self._norm(self.config.get("analysis_api_base", "")) or self._norm(
            self.config.get("embedding_api_base", "https://api.openai.com/v1")
        )
        api_key = self._norm(self.config.get("analysis_api_key", "")) or self._norm(
            self.config.get("embedding_api_key", "")
        )
        model = self._norm(self.config.get("analysis_model", "gpt-4o-mini"))
        timeout = max(10, self._int_conf("analysis_timeout_sec", 60))
        temperature = max(0.0, min(1.5, self._float_conf("analysis_temperature", 0.4)))
        max_tokens = max(256, self._int_conf("analysis_max_output_tokens", 900))
        if not api_key:
            return ""

        target_text = f"用户 {user_id}" if user_id else "全群"
        since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else "不限"
        system_prompt = (
            "你是聊天行为分析助手。请基于提供的真实聊天记录做客观分析。"
            "输出结构：1) 主要话题 2) 语言风格 3) 情绪倾向 4) 关系互动特征 "
            "5) 可用于人格学习的稳定特征 6) 风险与偏差提醒。"
            "不要编造未出现的信息。"
        )
        user_prompt = (
            f"分析对象: {target_text}\n"
            f"群号: {group_id}\n"
            f"时间下限: {since_text}\n"
            f"样本条数: {sample_size}\n\n"
            f"聊天记录:\n{transcript}"
        )
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        url = api_base.rstrip("/") + "/chat/completions"
        req = request.Request(
            url=url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {api_key}")
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            obj = json.loads(raw) if raw else {}
            choices = obj.get("choices", []) if isinstance(obj, dict) else []
            if not choices:
                return ""
            msg = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
            return self._norm(msg.get("content"))
        except Exception as e:
            self._last_error = f"analysis_llm: {e}"
            logger.warning(f"[chat_export] 聊天分析调用失败: {e}")
            return ""

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
        if not text:
            return None

        iso_text = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso_text)
            if dt.tzinfo is not None:
                return dt.astimezone().replace(tzinfo=None)
            return dt
        except ValueError:
            pass

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

    def _parse_export_args(
        self, tokens: list[str], event: AstrMessageEvent
    ) -> tuple[str, str, str]:
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

    def _parse_history_sync_args(
        self, tokens: list[str], event: AstrMessageEvent
    ) -> tuple[str, int]:
        args = tokens[1:]
        group_id = self._norm(event.get_group_id())
        limit = self._history_default_limit()

        if not args:
            return group_id, limit

        if len(args) == 1:
            arg = self._norm(args[0])
            if group_id and arg.isdigit():
                return group_id, self._clamp_history_limit(int(arg))
            return arg, limit

        first = self._norm(args[0])
        second = self._norm(args[1])
        if first.isdigit():
            return second or group_id, self._clamp_history_limit(int(first))
        if second.isdigit():
            return first or group_id, self._clamp_history_limit(int(second))
        return first or group_id, limit

    def _history_default_limit(self) -> int:
        return max(1, self._int_conf("history_sync_default_limit", 100))

    def _history_max_limit(self) -> int:
        return max(1, self._int_conf("history_sync_max_limit", 1000))

    def _clamp_history_limit(self, value: int) -> int:
        return max(1, min(value, self._history_max_limit()))

    def _coerce_datetime(self, value: Any) -> datetime | None:
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            try:
                return datetime.fromtimestamp(ts)
            except Exception:
                return None

        text = self._norm(value)
        if not text:
            return None
        if text.isdigit():
            return self._coerce_datetime(int(text))
        try:
            return self._coerce_datetime(float(text))
        except ValueError:
            pass
        return self._parse_dt(text)

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
        return bool(self.config.get("verbose_log", False))

    def _log_verbose(self, text: str):
        if not self._verbose_log_enabled():
            return
        msg = f"[chat_export] {text}"
        if len(msg) > self._max_log_line_length:
            msg = msg[: self._max_log_line_length] + "...[truncated]"
        logger.info(msg)

    def _log_startup_summary(self):
        self._log_verbose(
            "startup: "
            f"enabled={self._is_enabled()} "
            f"qdrant_enabled={self._qdrant_enabled()} "
            f"data_dir={self.data_dir} "
            f"db_file={self.db_file} "
            f"listening={sorted(self._listening_groups())} "
            f"lsky_enabled={self._lsky_enabled()} "
            f"qdrant_batch={self._int_conf('qdrant_batch_size', 20)} "
            f"qdrant_interval={self._float_conf('qdrant_flush_interval_sec', 1.0)}"
        )

    def _log_ingest_progress(self, group_id: str, user_id: str, content: str):
        every_n = self._int_conf("log_every_n", 500)
        if every_n <= 0:
            every_n = 500
        if self._received_group_events % every_n != 0:
            return
        preview_len = self._int_conf("log_preview_len", 0)
        preview = (content or "").replace("\n", " ").strip()
        if preview_len > 0 and len(preview) > preview_len:
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
        try:
            self._flush_all_queues(force=True)
        except Exception as e:
            logger.warning(f"[chat_export] terminate flush error: {e}")
        if self._sqlite_conn is not None:
            try:
                self._sqlite_conn.close()
            except Exception:
                pass
            self._sqlite_conn = None
        logger.info("[chat_export] terminated")




