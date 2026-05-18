from __future__ import annotations

import json
from typing import Any

from astrbot.api import logger

from config import int_conf
from utils import norm

try:
    import httpx
except Exception:
    httpx = None  # type: ignore[misc]


class LlmClient:
    def __init__(self, config: dict[str, Any], http_factory: Any = None):
        self._config = config
        self._http_factory = http_factory
        self._last_error = ""

    @property
    def last_error(self) -> str:
        return self._last_error

    def _http(self) -> Any:
        if self._http_factory is not None:
            return self._http_factory()
        return None

    def _embedding_url(self) -> str:
        base = norm(self._config.get("embedding_api_base", "https://api.openai.com/v1"))
        return base.rstrip("/") + "/embeddings"

    def _analysis_url(self) -> str:
        base = norm(self._config.get("analysis_api_base", "")) or norm(
            self._config.get("embedding_api_base", "https://api.openai.com/v1")
        )
        return base.rstrip("/") + "/chat/completions"

    def _embedding_headers(self) -> dict[str, str]:
        api_key = norm(self._config.get("embedding_api_key", ""))
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

    def _analysis_headers(self) -> dict[str, str]:
        api_key = norm(self._config.get("analysis_api_key", "")) or norm(
            self._config.get("embedding_api_key", "")
        )
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

    async def embedding_async(self, text: str) -> list[float]:
        arr = await self.embedding_batch_async([text])
        return arr[0] if arr else []

    async def embedding_batch_async(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        api_key = norm(self._config.get("embedding_api_key", ""))
        model = norm(self._config.get("embedding_model", "text-embedding-3-small"))
        if not api_key:
            logger.warning("[chat_export] embedding_api_key 为空，跳过向量化")
            return [[] for _ in texts]

        client = self._http()
        if client is None or httpx is None:
            return self.embedding_batch_sync(texts)

        body = {"model": model, "input": texts}
        try:
            resp = await client.post(
                self._embedding_url(),
                headers=self._embedding_headers(),
                json=body,
                timeout=int_conf(self._config, "embedding_timeout", 20),
            )
            resp.raise_for_status()
            data = resp.json()
            vecs: list[list[float]] = [[] for _ in texts]
            for item in data.get("data", []) if isinstance(data, dict) else []:
                idx = int(item.get("index", 0))
                emb = item.get("embedding", [])
                if 0 <= idx < len(vecs) and isinstance(emb, list):
                    vecs[idx] = [float(x) for x in emb]
            return vecs
        except Exception as e:
            self._last_error = f"embedding: {e}"
            logger.warning(f"[chat_export] embedding 异步调用失败: {e}")
            return [[] for _ in texts]

    def embedding_batch_sync(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        api_key = norm(self._config.get("embedding_api_key", ""))
        model = norm(self._config.get("embedding_model", "text-embedding-3-small"))
        if not api_key:
            logger.warning("[chat_export] embedding_api_key 为空，跳过向量化")
            return [[] for _ in texts]

        try:
            import urllib.request
            payload = json.dumps({"model": model, "input": texts}).encode("utf-8")
            req = urllib.request.Request(url=self._embedding_url(), data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Authorization", f"Bearer {api_key}")
            with urllib.request.urlopen(req, timeout=int_conf(self._config, "embedding_timeout", 20)) as resp:
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
            logger.warning(f"[chat_export] embedding 同步调用失败: {e}")
            return [[] for _ in texts]

    async def analysis_async(
        self,
        transcript: str,
        group_id: str,
        user_id: str,
        since_dt: Any,
        sample_size: int,
    ) -> str:
        if not transcript:
            return ""

        api_key = norm(self._config.get("analysis_api_key", "")) or norm(
            self._config.get("embedding_api_key", "")
        )
        if not api_key:
            return ""

        client = self._http()
        if client is None or httpx is None:
            return self.analysis_sync(transcript, group_id, user_id, since_dt, sample_size)

        body = self._build_analysis_body(transcript, group_id, user_id, since_dt, sample_size)
        url = self._analysis_url()
        timeout = max(10, int_conf(self._config, "analysis_timeout_sec", 60))
        try:
            resp = await client.post(url, json=body, timeout=timeout)
            resp.raise_for_status()
            obj = resp.json()
            choices = obj.get("choices", []) if isinstance(obj, dict) else []
            if not choices:
                return ""
            msg = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
            return norm(msg.get("content"))
        except Exception as e:
            self._last_error = f"analysis_llm: {e}"
            logger.warning(f"[chat_export] 聊天分析异步调用失败: {e}")
            return ""

    def analysis_sync(
        self,
        transcript: str,
        group_id: str,
        user_id: str,
        since_dt: Any,
        sample_size: int,
    ) -> str:
        if not transcript:
            return ""

        api_key = norm(self._config.get("analysis_api_key", "")) or norm(
            self._config.get("embedding_api_key", "")
        )
        if not api_key:
            return ""

        body = self._build_analysis_body(transcript, group_id, user_id, since_dt, sample_size)
        url = self._analysis_url()
        timeout = max(10, int_conf(self._config, "analysis_timeout_sec", 60))
        try:
            import urllib.request
            req = urllib.request.Request(
                url=url,
                data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                method="POST",
            )
            req.add_header("Content-Type", "application/json")
            req.add_header("Authorization", f"Bearer {api_key}")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            obj = json.loads(raw) if raw else {}
            choices = obj.get("choices", []) if isinstance(obj, dict) else []
            if not choices:
                return ""
            msg = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
            return norm(msg.get("content"))
        except Exception as e:
            self._last_error = f"analysis_llm: {e}"
            logger.warning(f"[chat_export] 聊天分析同步调用失败: {e}")
            return ""

    def _build_analysis_body(
        self,
        transcript: str,
        group_id: str,
        user_id: str,
        since_dt: Any,
        sample_size: int,
    ) -> dict[str, Any]:
        from datetime import datetime
        model = norm(self._config.get("analysis_model", "gpt-4o-mini"))
        temperature = max(0.0, min(1.5, float(self._config.get("analysis_temperature", 0.4))))
        max_tokens = max(256, int_conf(self._config, "analysis_max_output_tokens", 900))
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
        return {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
