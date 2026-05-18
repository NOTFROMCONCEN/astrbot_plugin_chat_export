from __future__ import annotations

import json
from typing import Any

from utils import norm


def extract_text(event: Any) -> str:
    text = norm(getattr(event, "message_str", None))
    if text and text not in {"[图片]", "[表情]", "[动画表情]"}:
        return text

    structured = extract_structured_message(event)
    if structured:
        return structured

    raw = norm(getattr(event, "raw_message", None))
    if raw:
        return raw

    if text:
        return text
    return "<空消息>"


def extract_structured_message(event: Any) -> str:
    candidates = [
        getattr(event, "message_obj", None),
        getattr(event, "message", None),
        getattr(event, "messages", None),
    ]
    for obj in candidates:
        content = format_message_obj(obj)
        if content:
            return content
    return ""


def format_message_obj(obj: Any) -> str:
    if obj is None:
        return ""

    if isinstance(obj, str):
        return obj.strip()

    if isinstance(obj, dict):
        if "type" in obj:
            return format_segment(obj)
        for k in ("message", "messages", "segments"):
            if isinstance(obj.get(k), list):
                return format_message_obj(obj.get(k))
        text = norm(obj.get("text"))
        return text

    if isinstance(obj, list):
        parts: list[str] = []
        for seg in obj:
            s = format_segment(seg)
            if s:
                parts.append(s)
        return " ".join(parts).strip()

    seg_type = norm(getattr(obj, "type", None))
    if seg_type:
        data = getattr(obj, "data", None)
        if isinstance(data, dict):
            seg = {"type": seg_type, "data": data}
        else:
            seg = {"type": seg_type, "data": {}}
        return format_segment(seg)
    return ""


def format_segment(seg: Any) -> str:
    if seg is None:
        return ""

    if isinstance(seg, str):
        return seg.strip()

    if not isinstance(seg, dict):
        return norm(seg)

    seg_type = norm(seg.get("type")).lower()
    data = seg.get("data") if isinstance(seg.get("data"), dict) else {}

    if seg_type in {"text", "plain"}:
        return norm(data.get("text") or seg.get("text"))

    if seg_type == "image":
        file = norm(data.get("file"))
        url = norm(data.get("url"))
        if url:
            return f"[图片][URL:{url}]"
        if file:
            return f"[图片][FILE:{file}]"
        return "[图片]"

    if seg_type in {"face", "emoji"}:
        face_id = norm(data.get("id") or data.get("face_id"))
        return f"[表情:id={face_id}]" if face_id else "[表情]"

    if seg_type in {"mface", "market_face"}:
        name = norm(data.get("summary") or data.get("name"))
        return f"[动画表情:{name}]" if name else "[动画表情]"

    if seg_type == "reply":
        msg_id = norm(data.get("id"))
        return f"[回复:id={msg_id}]" if msg_id else "[回复]"

    if seg_type == "at":
        qq = norm(data.get("qq") or data.get("user_id"))
        return f"@{qq}" if qq else "@"

    if seg_type == "file":
        name = norm(data.get("name") or data.get("file"))
        return f"[文件:{name}]" if name else "[文件]"

    if seg_type:
        return f"[{seg_type}]"
    return ""


def extract_message_id(event: Any) -> str:
    for key in ("message_id", "msg_id", "id"):
        val = getattr(event, key, None)
        if val is not None:
            s = norm(val)
            if s:
                return s
    obj = getattr(event, "message_obj", None)
    s = extract_message_id_from_obj(obj)
    if s:
        return s
    return ""


def extract_message_id_from_obj(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in ("message_id", "msg_id", "id"):
        s = norm(obj.get(key))
        if s:
            return s
    data = obj.get("data")
    if isinstance(data, dict):
        for key in ("message_id", "msg_id", "id"):
            s = norm(data.get(key))
            if s:
                return s
    return ""


def event_time(event: Any) -> datetime:
    from datetime import datetime
    t = getattr(event, "time", None)
    if isinstance(t, (int, float)):
        try:
            return datetime.fromtimestamp(t)
        except Exception:
            pass
    return datetime.now()


def extract_image_refs(event: Any) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[str] = set()
    candidates = [
        getattr(event, "message_obj", None),
        getattr(event, "message", None),
        getattr(event, "messages", None),
    ]
    for obj in candidates:
        collect_image_refs(obj, refs, seen)
    return refs


def extract_image_refs_from_obj(obj: Any) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[str] = set()
    collect_image_refs(obj, refs, seen)
    return refs


def collect_image_refs(obj: Any, refs: list[dict[str, str]], seen: set[str]) -> None:
    if obj is None:
        return
    if isinstance(obj, list):
        for seg in obj:
            collect_image_refs(seg, refs, seen)
        return
    if isinstance(obj, dict):
        seg_type = norm(obj.get("type")).lower()
        data = obj.get("data") if isinstance(obj.get("data"), dict) else {}
        if seg_type == "image":
            url = norm(data.get("url"))
            file = norm(data.get("file"))
            key = f"{url}|{file}"
            if key and key not in seen:
                seen.add(key)
                refs.append({"url": url, "file": file})
        for k in ("message", "messages", "segments"):
            if isinstance(obj.get(k), list):
                collect_image_refs(obj.get(k), refs, seen)
        return

    seg_type = norm(getattr(obj, "type", "")).lower()
    if seg_type == "image":
        data = getattr(obj, "data", None)
        if isinstance(data, dict):
            url = norm(data.get("url"))
            file = norm(data.get("file"))
            key = f"{url}|{file}"
            if key and key not in seen:
                seen.add(key)
                refs.append({"url": url, "file": file})


def build_media_json_from_refs(refs: list[dict[str, str]], lsky_items: list[dict[str, Any]] | None = None) -> str:
    if not refs:
        return ""
    items: list[dict[str, Any]] = []
    for idx, ref in enumerate(refs):
        item: dict[str, Any] = {
            "type": "image",
            "source_url": norm(ref.get("url")),
            "source_file": norm(ref.get("file")),
        }
        if lsky_items and idx < len(lsky_items):
            item.update(lsky_items[idx])
        items.append(item)
    try:
        return json.dumps(items, ensure_ascii=False)
    except Exception:
        return ""


def format_export_line(text: str, media_json: str) -> str:
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
        url = norm(it.get("lsky_url"))
        if url:
            lsky_urls.append(url)
    if not lsky_urls:
        return line
    return f"{line} {' '.join(f'[图床:{u}]' for u in lsky_urls)}".strip()


def extract_history_time(item: dict[str, Any]) -> datetime | None:
    from datetime import datetime
    for key in (
        "time",
        "timestamp",
        "message_time",
        "msg_time",
        "send_time",
        "date",
    ):
        dt = coerce_datetime(item.get(key))
        if dt is not None:
            return dt
    return None


def extract_history_user_id(item: dict[str, Any]) -> str:
    sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}
    for value in (
        item.get("user_id"),
        item.get("sender_id"),
        sender.get("user_id"),
        sender.get("uin"),
        sender.get("id"),
    ):
        text = norm(value)
        if text:
            return text
    return ""


def extract_history_sender_name(item: dict[str, Any]) -> str:
    sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}
    for value in (
        item.get("sender_name"),
        item.get("nickname"),
        sender.get("card"),
        sender.get("nickname"),
        sender.get("name"),
    ):
        text = norm(value)
        if text:
            return text
    return ""


def extract_text_from_history_message(item: dict[str, Any]) -> str:
    raw_candidates = [
        item.get("message_str"),
        item.get("raw_message"),
        item.get("text"),
        item.get("content"),
    ]
    for value in raw_candidates:
        text = norm(value)
        if text and text not in {"[图片]", "[表情]", "[动画表情]"}:
            return text

    for key in ("message", "messages", "segments", "content"):
        structured = format_message_obj(item.get(key))
        if structured:
            return structured

    for value in raw_candidates:
        text = norm(value)
        if text:
            return text
    return "<空消息>"


def extract_history_seq(item: dict[str, Any]) -> int | None:
    for key in ("message_seq", "messageSeq", "seq", "msg_seq"):
        value = item.get(key)
        if value is None:
            continue
        text = norm(value)
        if text.lstrip("-").isdigit():
            return int(text)
    return None


def extract_history_message_id(item: dict[str, Any]) -> str:
    message_id = extract_message_id_from_obj(item)
    if message_id:
        return message_id
    return norm(item.get("message_id"))
