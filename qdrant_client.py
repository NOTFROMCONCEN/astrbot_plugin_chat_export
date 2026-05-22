from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any

from astrbot.api import logger

from config import float_conf, int_conf
from utils import norm


class QdrantClientWrapper:
    def __init__(self, config: dict[str, Any]):
        self._config = config
        self._client = None
        self._models = None
        self._pending: list[dict[str, Any]] = []
        self._last_flush_ts = time.time()
        self._write_ok = 0
        self._write_fail = 0
        self._error_count = 0
        self._last_error_ts = 0.0
        self._last_error = ""
        self._last_ok_ts = ""
        self._max_log_line_length = int_conf(config, "max_log_line_length", 512)
        self._init()

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
    def last_error(self) -> str:
        return self._last_error

    @property
    def client(self) -> Any:
        return self._client

    @property
    def models(self) -> Any:
        return self._models

    @property
    def error_count(self) -> int:
        return self._error_count

    def enabled(self) -> bool:
        return bool(self._config.get("qdrant_enabled", False))

    def _init(self) -> None:
        if not self.enabled():
            return
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http import models
        except Exception as e:
            logger.warning(f"[chat_export] qdrant_client 未安装或导入失败: {e}")
            return

        try:
            self._client = QdrantClient(
                url=norm(self._config.get("qdrant_url", "http://127.0.0.1:6333")),
                api_key=norm(self._config.get("qdrant_api_key", "")) or None,
                timeout=int_conf(self._config, "qdrant_timeout", 10),
            )
            self._models = models
            self._ensure_collection()
            logger.info("[chat_export] qdrant initialized")
        except Exception as e:
            logger.error(f"[chat_export] qdrant 初始化失败: {e}")
            self._client = None
            self._models = None

    def _ensure_collection(self) -> None:
        if not self._client or not self._models:
            return
        collection = norm(self._config.get("qdrant_collection", "chat_export"))
        dim = int_conf(self._config, "embedding_dimension", 1536)
        distance_name = norm(self._config.get("qdrant_distance", "Cosine")).upper()
        distance_map = {
            "COSINE": self._models.Distance.COSINE,
            "DOT": self._models.Distance.DOT,
            "EUCLID": self._models.Distance.EUCLID,
        }
        distance = distance_map.get(distance_name, self._models.Distance.COSINE)

        try:
            self._client.get_collection(collection_name=collection)
            return
        except Exception:
            pass

        try:
            self._client.create_collection(
                collection_name=collection,
                vectors_config=self._models.VectorParams(size=dim, distance=distance),
            )
        except Exception as e:
            logger.warning(f"[chat_export] qdrant 建集合失败: {e}")

    def enqueue(self, records: list[dict[str, Any]], should_index_fn: Any) -> None:
        if not records or not self.enabled():
            return

        max_consecutive_errors = int_conf(self._config, "max_consecutive_errors", 5)
        error_cooldown_sec = float_conf(self._config, "error_cooldown_sec", 60.0)
        max_pending_size = int_conf(self._config, "max_pending_size", 5000)

        if self._error_count >= max_consecutive_errors:
            now = time.time()
            if now - self._last_error_ts < error_cooldown_sec:
                dropped = len([r for r in records if should_index_fn(norm(r.get("content")))])
                self._write_fail += dropped
                self._last_error = f"qdrant cooldown: dropped {dropped}"
                logger.warning(f"[chat_export] qdrant enqueue skipped: in cooldown, dropped {dropped}")
                return

        if len(self._pending) >= max_pending_size:
            drop_count = min(len(self._pending) // 10 or 1, len(self._pending))
            del self._pending[:drop_count]
            self._write_fail += drop_count
            self._last_error = f"qdrant pending overflow: dropped {drop_count}"
            logger.warning(f"[chat_export] qdrant pending overflow: dropped {drop_count}")

        for rec in records:
            if should_index_fn(norm(rec.get("content"))):
                self._pending.append(rec)

    def flush_if_needed(self, embedding_fn: Any) -> list[dict[str, Any]] | None:
        if not self._pending:
            return None
        if not self.enabled() or not self._client or not self._models:
            return None
        batch_size = max(1, int_conf(self._config, "qdrant_batch_size", 20))
        interval = max(0.2, float_conf(self._config, "qdrant_flush_interval_sec", 1.0))
        now = time.time()
        if len(self._pending) < batch_size and (now - self._last_flush_ts) < interval:
            return None
        return self.flush(force=False, embedding_fn=embedding_fn)

    def flush(self, force: bool, embedding_fn: Any) -> list[dict[str, Any]]:
        if not self._pending:
            return []
        if not self.enabled() or not self._client or not self._models:
            return []

        max_consecutive_errors = int_conf(self._config, "max_consecutive_errors", 5)
        error_cooldown_sec = float_conf(self._config, "error_cooldown_sec", 60.0)

        if self._error_count >= max_consecutive_errors:
            now = time.time()
            if now - self._last_error_ts < error_cooldown_sec:
                dropped = self._pending[: len(self._pending) // 2 or 1]
                self._write_fail += len(dropped)
                del self._pending[: len(dropped)]
                self._last_flush_ts = now
                self._last_error = f"qdrant cooldown: dropped {len(dropped)}"
                logger.warning(f"[chat_export] qdrant cooldown: dropped {len(dropped)}")
                return []
            self._error_count = 0

        batch_size = max(1, int_conf(self._config, "qdrant_batch_size", 20))
        take = len(self._pending) if force else min(len(self._pending), batch_size)
        batch = self._pending[:take]
        del self._pending[:take]

        texts = [r["content"] for r in batch]
        vectors = embedding_fn(texts)
        if len(vectors) != len(batch):
            self._write_fail += len(batch)
            self._error_count += 1
            self._last_error_ts = time.time()
            self._last_error = "embedding_batch_size_mismatch"
            self._pending = batch + self._pending
            return []

        collection = norm(self._config.get("qdrant_collection", "chat_export"))
        points: list[Any] = []
        retry_records: list[dict[str, Any]] = []
        for rec, vec in zip(batch, vectors):
            if not vec:
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
                self._models.PointStruct(id=point_id, vector=vec, payload=payload)
            )

        if retry_records:
            self._pending.extend(retry_records)
            logger.warning(f"[chat_export] qdrant empty vectors: retry {len(retry_records)} records")

        if not points:
            self._last_flush_ts = time.time()
            return []

        try:
            self._client.upsert(collection_name=collection, points=points, wait=False)
            self._write_ok += len(points)
            self._error_count = 0
            self._last_ok_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self._write_fail += len(points)
            self._error_count += 1
            self._last_error_ts = time.time()
            self._last_error = f"qdrant_upsert: {e}"
            err_text = norm(e)[: self._max_log_line_length]
            logger.warning(f"[chat_export] qdrant upsert 失败: {err_text}")
            failed = [rec for rec, vec in zip(batch, vectors) if vec]
            self._pending = failed[:len(points)] + self._pending
        finally:
            self._last_flush_ts = time.time()
        return points

    def search(
        self,
        query_vector: list[float],
        group_id: str,
        limit: int,
        since_dt: datetime | None,
    ) -> list[Any]:
        if not self._client or not self._models:
            return []

        collection = norm(self._config.get("qdrant_collection", "chat_export"))
        q_filter = self._build_filter(group_id, since_dt)
        top_k = max(1, min(limit, 100))
        # 时间过滤场景下适当过采样，减少 ANN 召回被旧消息“挤占”导致的漏检。
        if since_dt and bool(self._config.get("search_hard_time_filter", True)):
            overfetch = max(1, int_conf(self._config, "search_overfetch_multiplier", 4))
            max_top_k = max(top_k, int_conf(self._config, "search_overfetch_max_top_k", 300))
            top_k = min(max_top_k, max(top_k, limit * overfetch))

        try:
            if hasattr(self._client, "query_points"):
                try:
                    result = self._client.query_points(
                        collection_name=collection,
                        query=query_vector,
                        query_filter=q_filter,
                        limit=top_k,
                        with_payload=True,
                        with_vectors=False,
                    )
                except TypeError:
                    result = self._client.query_points(
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

            if hasattr(self._client, "search"):
                return self._client.search(
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

    def count(self, group_id: str = "") -> int:
        if not self._client or not self._models:
            return 0

        collection = norm(self._config.get("qdrant_collection", "chat_export"))
        q_filter = self._build_filter(group_id, None)

        try:
            if hasattr(self._client, "count"):
                try:
                    res = self._client.count(
                        collection_name=collection,
                        count_filter=q_filter,
                        exact=True,
                    )
                except TypeError:
                    res = self._client.count(
                        collection_name=collection,
                        filter=q_filter,
                        exact=True,
                    )
                if isinstance(res, dict):
                    return int(res.get("count", 0))
                return int(getattr(res, "count", 0))
        except Exception:
            pass
        return 0

    def _build_filter(self, group_id: str, since_dt: datetime | None) -> Any:
        if not self._models:
            return None
        must = []
        if group_id:
            must.append(
                self._models.FieldCondition(
                    key="group_id",
                    match=self._models.MatchValue(value=group_id),
                )
            )
        if since_dt:
            since_text = since_dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                must.append(
                    self._models.FieldCondition(
                        key="ts",
                        range=self._models.Range(gte=since_text),
                    )
                )
            except Exception:
                pass
        if not must:
            return None
        return self._models.Filter(must=must)
