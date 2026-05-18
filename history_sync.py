from __future__ import annotations

import asyncio
from typing import Any

from astrbot.api import logger

from config import int_conf
from utils import build_unique_key, coerce_datetime, norm, parse_dt


class HistorySync:
    def __init__(self, config: dict[str, Any], db: Any):
        self._config = config
        self._db = db
        self._action_hints: dict[str, tuple[str, tuple[str, ...]]] = {}

    async def sync_group_history(
        self, event: Any, bot: Any, group_id: str, limit: int
    ) -> dict[str, int]:
        limit = self._clamp_limit(limit)
        max_rounds = max(1, int_conf(self._config, "history_sync_rounds", 3))
        no_new_stop_rounds = max(1, int_conf(self._config, "history_sync_stop_no_new_rounds", 2))
        use_saved_cursor = bool(self._config.get("history_sync_resume_from_saved_cursor", True))
        overall_seen_keys: set[str] = set()
        total_pages = 0
        total_fetched = 0
        total_invalid = 0
        total_records: list[dict[str, Any]] = []
        rounds = 0
        consecutive_no_new = 0
        stop_reason = ""

        saved_cursor = self._db.load_history_cursor(group_id) if use_saved_cursor else None
        cursor = saved_cursor
        anchor_message_id = self._resolve_anchor_message_id(event, group_id)

        while rounds < max_rounds and len(total_records) < limit:
            rounds += 1
            pass_stats = await self._sync_pass(
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
                self._db.save_history_cursor(group_id, cursor)

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
        inserted = self._ingest(total_records)
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

    async def _sync_pass(
        self,
        bot: Any,
        group_id: str,
        limit: int,
        cursor: dict[str, Any] | None,
        anchor_message_id: str,
        seen_keys: set[str],
    ) -> dict[str, Any]:
        page_size = min(limit, max(1, int_conf(self._config, "history_sync_page_size", 20)))
        max_pages = max(1, int_conf(self._config, "history_sync_max_pages", 50))
        pages = 0
        fetched = 0
        invalid = 0
        records: list[dict[str, Any]] = []
        stop_reason = ""
        local_cursor = cursor

        while len(records) < limit and pages < max_pages:
            batch_size = min(page_size, limit - len(records))
            response, action_name = await self._fetch_page(
                bot, group_id, batch_size, local_cursor, anchor_message_id
            )
            items = self._extract_items(response)
            if not items:
                stop_reason = "no_items"
                break

            pages += 1
            fetched += len(items)
            logger.info(
                f"[chat_export] history_sync page={pages} action={action_name} items={len(items)}"
            )

            for item in items:
                record = self._normalize_message(group_id, item)
                if record is None:
                    invalid += 1
                    continue
                if record["unique_key"] in seen_keys:
                    continue
                seen_keys.add(record["unique_key"])
                records.append(record)
                if len(records) >= limit:
                    break

            next_cursor = self._next_cursor(items, local_cursor)
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

    async def _fetch_page(
        self,
        bot: Any,
        group_id: str,
        limit: int,
        cursor: dict[str, Any] | None,
        anchor_message_id: str,
    ) -> tuple[Any, str]:
        errors: list[str] = []
        bucket = self._strategy_bucket(cursor)
        for action, params in self._build_candidates(
            group_id, limit, cursor, anchor_message_id
        ):
            try:
                response = await self._call_action(bot, action, **params)
            except Exception as e:
                errors.append(f"{action}: {norm(e)}")
                continue

            if self._is_failed(response):
                err = self._extract_error(response) or "action_failed"
                errors.append(f"{action}: {err}")
                continue

            self._remember_hint(bucket, action, params)
            return response, action

        err_text = (
            "; ".join(err for err in errors[-4:] if err) or "协议端不支持群历史消息 API"
        )
        raise RuntimeError(err_text)

    async def _call_action(self, bot: Any, action: str, **params) -> Any:
        errors: list[str] = []
        timeout_sec = max(1.0, float(self._config.get("history_sync_action_timeout_sec", 8.0)))

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
                errors.append(norm(e))

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
                errors.append(norm(e))

        err_text = (
            " | ".join(err for err in errors if err) or "call_action_not_available"
        )
        raise RuntimeError(err_text)

    def _build_candidates(
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
                {"group_id": group_value, "message_seq": 0, "count": count, "reverseOrder": False},
                {"group_id": group_value, "message_seq": 0, "count": count, "reverse_order": False},
                {"group_id": group_value, "message_seq": 0, "count": count, "reverse_order": False, "disable_get_url": False, "parse_mult_msg": True, "quick_reply": False},
                {"group_id": group_value, "message_seq": 0, "count": count},
                {"group_id": group_value, "message_id": initial_message_id, "count": count},
                {"group_id": group_value, "message_id": 0, "count": count},
                {"group_id": group_value, "count": count},
            ]
        else:
            message_seq = cursor.get("message_seq")
            message_id = norm(cursor.get("message_id"))
            param_candidates = []
            if message_seq is not None:
                param_candidates.extend([
                    {"group_id": group_value, "message_seq": message_seq, "count": count, "reverseOrder": False},
                    {"group_id": group_value, "message_seq": message_seq, "count": count, "reverse_order": False},
                    {"group_id": group_value, "message_seq": message_seq, "count": count},
                    {"group_id": group_value, "seq": message_seq, "count": count},
                    {"group_id": group_value, "last_seq": message_seq, "count": count},
                    {"group_id": group_value, "start_seq": message_seq, "count": count},
                ])
            if message_id:
                param_candidates.extend([
                    {"group_id": group_value, "message_id": message_id, "count": count},
                    {"group_id": group_value, "message_id": message_id},
                ])

        candidates: list[tuple[str, dict[str, Any]]] = []
        seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
        for action in ("get_group_msg_history", "get_group_history_msg"):
            for params in param_candidates:
                key = (
                    action,
                    tuple(sorted((k, norm(v)) for k, v in params.items())),
                )
                if key in seen:
                    continue
                seen.add(key)
                candidates.append((action, params))

        preferred = self._action_hints.get(self._strategy_bucket(cursor))
        if preferred is None:
            return candidates

        preferred_candidates: list[tuple[str, dict[str, Any]]] = []
        other_candidates: list[tuple[str, dict[str, Any]]] = []
        for action, params in candidates:
            if self._action_signature(action, params) == preferred:
                preferred_candidates.append((action, params))
            else:
                other_candidates.append((action, params))
        return preferred_candidates + other_candidates

    @staticmethod
    def _action_signature(action: str, params: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
        return action, tuple(sorted(params.keys()))

    def _remember_hint(self, bucket: str, action: str, params: dict[str, Any]):
        signature = self._action_signature(action, params)
        if self._action_hints.get(bucket) == signature:
            return
        self._action_hints[bucket] = signature
        logger.info(
            f"[chat_export] history_sync strategy bucket={bucket} action={action} keys={sorted(params.keys())}"
        )

    def _strategy_bucket(self, cursor: dict[str, Any] | None) -> str:
        if not cursor:
            return "initial"
        has_seq = cursor.get("message_seq") is not None
        has_message_id = bool(norm(cursor.get("message_id")))
        if has_seq and has_message_id:
            return "cursor_seq_message_id"
        if has_seq:
            return "cursor_seq"
        if has_message_id:
            return "cursor_message_id"
        return "cursor"

    def _extract_items(self, response: Any) -> list[dict[str, Any]]:
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
            items = self._extract_items(data)
            if items or self._has_container(data):
                return items
        if self._looks_like_item(response):
            return [response]
        return []

    @staticmethod
    def _has_container(obj: Any) -> bool:
        if isinstance(obj, list):
            return True
        if not isinstance(obj, dict):
            return False
        return any(key in obj for key in ("messages", "msg_list", "records", "list", "data"))

    @staticmethod
    def _looks_like_item(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        keys = {"message_id", "msg_id", "message", "raw_message", "sender", "time", "user_id"}
        return any(key in obj for key in keys)

    def _normalize_message(
        self, default_group_id: str, item: dict[str, Any]
    ) -> dict[str, Any] | None:
        from .message_parser import extract_image_refs_from_obj, extract_message_id_from_obj
        group_id = norm(item.get("group_id")) or default_group_id
        if not group_id:
            return None

        message_time = self._extract_time(item)
        if message_time is None:
            return None

        from .message_parser import extract_history_sender_name, extract_history_user_id, extract_text_from_history_message
        user_id = extract_history_user_id(item)
        sender_name = extract_history_sender_name(item)
        content = extract_text_from_history_message(item)
        message_id = extract_message_id_from_obj(item)
        unique_key = build_unique_key(
            group_id, user_id, message_time, content, message_id
        )
        media_json = ""
        refs = extract_image_refs_from_obj(
            item.get("message") if "message" in item else item
        )
        if refs:
            try:
                import json
                media_json = json.dumps([{"type": "image", "source_url": r.get("url"), "source_file": r.get("file")} for r in refs], ensure_ascii=False)
            except Exception:
                pass

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

    @staticmethod
    def _extract_time(item: dict[str, Any]) -> Any:
        return coerce_datetime(item.get("time") or item.get("timestamp") or item.get("message_time") or item.get("msg_time") or item.get("send_time") or item.get("date"))

    def _next_cursor(
        self, items: list[dict[str, Any]], prev_cursor: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if not items:
            return None
        oldest_seq: int | None = None
        last_message_id = ""
        oldest_message_id = ""
        for item in items:
            message_id = norm(item.get("message_id")) or norm(item.get("msg_id")) or norm(item.get("id"))
            if message_id:
                last_message_id = message_id
            seq = None
            for key in ("message_seq", "messageSeq", "seq", "msg_seq"):
                val = item.get(key)
                if val is not None:
                    text = norm(val)
                    if text.lstrip("-").isdigit():
                        seq = int(text)
                        break
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
        prev_message_id = "" if prev_cursor is None else norm(prev_cursor.get("message_id"))
        if message_id_cursor and prev_message_id != message_id_cursor:
            next_cursor["message_id"] = message_id_cursor

        return next_cursor or None

    def _resolve_anchor_message_id(self, event: Any, group_id: str) -> str:
        from .message_parser import extract_message_id
        if norm(getattr(event, "get_group_id", lambda: "")()) == group_id:
            message_id = extract_message_id(event)
            if message_id:
                return message_id
        return self._db.latest_group_message_id(group_id)

    def _ingest(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not records:
            return []
        self._db.extend_pending(records)
        return self._db.flush(force=True)

    def _clamp_limit(self, value: int) -> int:
        return max(1, min(value, max(1, int_conf(self._config, "history_sync_max_limit", 1000))))

    @staticmethod
    def _is_failed(response: Any) -> bool:
        if not isinstance(response, dict):
            return False
        status = norm(response.get("status")).lower()
        retcode = response.get("retcode")
        if status == "failed":
            return True
        return retcode not in (None, 0, "0")

    @staticmethod
    def _extract_error(response: Any) -> str:
        if not isinstance(response, dict):
            return ""
        for key in ("wording", "msg", "message"):
            text = norm(response.get(key))
            if text:
                return text
        return ""
