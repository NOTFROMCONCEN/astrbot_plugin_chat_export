from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any

_ANALYSIS_SENSITIVE_RE = re.compile(
    r"(自杀|殺人|杀人|強姦|强奸|毒品|爆炸|未成年|嫖娼)",
    re.IGNORECASE,
)


def norm(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def parse_dt(s: str) -> datetime | None:
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


def coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts)
        except Exception:
            return None

    text = norm(value)
    if not text:
        return None
    if text.isdigit():
        return coerce_datetime(int(text))
    try:
        return coerce_datetime(float(text))
    except ValueError:
        pass
    return parse_dt(text)


def build_unique_key(
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


def parse_recent_time_token(token: str) -> datetime | None:
    t = token.strip().lower()
    m = re.match(r"^最近(\d+)(小时|时|h|天|d)$", t)
    if m:
        num = int(m.group(1))
        unit = m.group(2)
        if unit in {"小时", "时", "h"}:
            return datetime.now() - __import__("datetime").timedelta(hours=num)
        return datetime.now() - __import__("datetime").timedelta(days=num)

    m2 = re.match(r"^recent:(\d+)(h|d)$", t)
    if m2:
        num = int(m2.group(1))
        if m2.group(2) == "h":
            return datetime.now() - __import__("datetime").timedelta(hours=num)
        return datetime.now() - __import__("datetime").timedelta(days=num)
    return None


def looks_like_model_refusal(text: str) -> bool:
    t = norm(text).lower()
    if not t:
        return True
    flags = [
        "rejected because it was considered high risk",
        "considered high risk",
        "content policy",
        "cannot help with",
        "i can't help with",
        "i cannot comply",
        "请求被拒绝",
        "高风险",
        "无法协助",
    ]
    return any(flag in t for flag in flags)


def mask_sensitive_text(text: str) -> str:
    return _ANALYSIS_SENSITIVE_RE.sub("[敏感词]", text)
