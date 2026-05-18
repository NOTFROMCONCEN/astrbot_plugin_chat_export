from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from utils import mask_sensitive_text, norm, parse_dt, parse_recent_time_token


def parse_export_args(
    tokens: list[str], default_group: str
) -> tuple[str, str, str]:
    args = tokens[1:]
    if len(args) >= 4 and ":" in args[1] and ":" in args[3]:
        start_s = f"{args[0]} {args[1]}"
        end_s = f"{args[2]} {args[3]}"
        group_id = norm(args[4]) if len(args) >= 5 else default_group
        return start_s, end_s, group_id

    if len(args) >= 2:
        start_s = args[0]
        end_s = args[1]
        group_id = norm(args[2]) if len(args) >= 3 else default_group
        return start_s, end_s, group_id

    return "", "", default_group


def parse_search_args(
    tokens: list[str], default_group: str
) -> tuple[str, str, datetime | None]:
    args = tokens[1:]
    group_id = default_group
    if args and args[0].isdigit():
        group_id = norm(args[0])
        args = args[1:]

    since_dt = None
    filtered: list[str] = []
    for token in args:
        parsed = parse_recent_time_token(token)
        if parsed is not None and since_dt is None:
            since_dt = parsed
            continue
        filtered.append(token)

    if since_dt is None:
        return group_id, " ".join(filtered).strip(), since_dt

    return group_id, " ".join(filtered).strip(), since_dt


def parse_analyze_args(
    tokens: list[str], default_group: str
) -> tuple[str, str, datetime | None]:
    args = tokens[1:]
    group_id = default_group
    user_id = ""
    since_dt = None

    if args and args[0].isdigit():
        group_id = norm(args[0])
        args = args[1:]
    if args and args[0].isdigit():
        user_id = norm(args[0])
        args = args[1:]

    for token in args:
        parsed = parse_recent_time_token(token)
        if parsed is not None and since_dt is None:
            since_dt = parsed

    return group_id, user_id, since_dt


def parse_history_sync_args(
    tokens: list[str], default_group: str, default_limit: int, max_limit: int
) -> tuple[str, int]:
    args = tokens[1:]
    group_id = default_group
    limit = default_limit

    if not args:
        return group_id, limit

    if len(args) == 1:
        arg = norm(args[0])
        if group_id and arg.isdigit():
            return group_id, min(int(arg), max_limit)
        return arg, limit

    first = norm(args[0])
    second = norm(args[1])
    if first.isdigit():
        return second or group_id, min(int(first), max_limit)
    if second.isdigit():
        return first or group_id, min(int(second), max_limit)
    return first or group_id, limit


def post_filter_search_points(
    points: list[Any],
    group_id: str,
    since_dt: datetime | None,
    query_text: str,
    limit: int,
    config: dict[str, Any],
) -> list[Any]:
    if not points:
        return []

    strict_group = bool(config.get("search_hard_group_filter", True))
    strict_time = bool(config.get("search_hard_time_filter", True))
    keyword_mode = norm(config.get("search_keyword_mode", "auto")).lower()
    query = (query_text or "").strip().lower()

    filtered: list[Any] = []
    for p in points:
        payload = _point_payload(p)
        if not payload:
            continue

        if strict_group and group_id and norm(payload.get("group_id")) != group_id:
            continue

        if strict_time and since_dt:
            ts = parse_dt(norm(payload.get("ts")))
            if not ts or ts < since_dt:
                continue

        filtered.append(p)

    if not filtered:
        return []

    if query and keyword_mode != "off":
        scored: list[tuple[int, Any]] = []
        for p in filtered:
            payload = _point_payload(p)
            text = norm(payload.get("content")).lower()
            hit = 1 if (query and query in text) else 0
            scored.append((hit, p))

        scored.sort(key=lambda x: x[0], reverse=True)
        filtered = [x[1] for x in scored]

        if keyword_mode == "auto" and query and len(query) <= 8:
            hard_hits = [
                p
                for p in filtered
                if query in norm(_point_payload(p).get("content", "")).lower()
            ]
            if hard_hits:
                filtered = hard_hits

    return filtered[: max(1, limit)]


def _point_payload(point: Any) -> dict[str, Any]:
    if isinstance(point, dict):
        p = point.get("payload")
        return p if isinstance(p, dict) else {}
    p = getattr(point, "payload", None)
    return p if isinstance(p, dict) else {}


def build_analysis_transcript(
    rows: list[tuple[str, str, str, str, str]], max_chars: int
) -> str:
    lines: list[str] = []
    total = 0
    for ts, gid, uid, uname, content in rows:
        speaker = norm(uname) or norm(uid)
        text = mask_sensitive_text(norm(content).replace("\n", " ").strip())
        line = f"[{ts}] [{speaker}] {text}"
        total += len(line) + 1
        if total > max_chars:
            break
        lines.append(line)
    return "\n".join(lines)


def build_local_analysis(
    rows: list[tuple[str, str, str, str, str]],
    group_id: str,
    user_id: str,
    since_dt: datetime | None,
) -> str:
    msg_count = len(rows)
    speakers: dict[str, int] = {}
    total_chars = 0
    short_msgs = 0
    for _ts, _gid, uid, uname, content in rows:
        speaker = norm(uname) or norm(uid)
        speakers[speaker] = speakers.get(speaker, 0) + 1
        c = len(norm(content))
        total_chars += c
        if c <= 8:
            short_msgs += 1

    avg_chars = (total_chars / msg_count) if msg_count else 0.0
    top_speakers = sorted(speakers.items(), key=lambda x: x[1], reverse=True)[:5]
    top_text = "、".join(f"{name}:{cnt}" for name, cnt in top_speakers) or "无"
    since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else "不限"
    target = f"用户 {user_id}" if user_id else "全群"
    return (
        f"1) 范围: 群 {group_id}，对象 {target}，时间下限 {since_text}\n"
        f"2) 消息量: {msg_count} 条\n"
        f"3) 发言者分布(Top5): {top_text}\n"
        f"4) 平均消息长度: {avg_chars:.1f} 字\n"
        f"5) 短句占比(<=8字): {short_msgs}/{msg_count}\n"
        "6) 说明: 该结果为本地统计摘要（未调用模型语义推断）"
    )
