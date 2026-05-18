from __future__ import annotations

import json
import mimetypes
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from astrbot.api import logger

from config import int_conf
from utils import norm

try:
    import httpx
except Exception:
    httpx = None  # type: ignore[misc]


class MediaUploader:
    def __init__(self, config: dict[str, Any], http_factory: Any = None):
        self._config = config
        self._http_factory = http_factory
        self._upload_ok = 0
        self._upload_fail = 0
        self._last_error = ""

    @property
    def upload_ok(self) -> int:
        return self._upload_ok

    @property
    def upload_fail(self) -> int:
        return self._upload_fail

    @property
    def last_error(self) -> str:
        return self._last_error

    def _http(self) -> Any:
        if self._http_factory is not None:
            return self._http_factory()
        return None

    def enabled(self) -> bool:
        return bool(self._config.get("lsky_enabled", False))

    async def upload_refs(
        self, refs: list[dict[str, str]]
    ) -> list[dict[str, Any]]:
        if not refs or not self.enabled():
            return []

        items: list[dict[str, Any]] = []
        for ref in refs:
            item = await self._upload_single(ref)
            items.append(item)
        return items

    async def _upload_single(self, ref: dict[str, str]) -> dict[str, Any]:
        source_url = norm(ref.get("url"))
        source_file = norm(ref.get("file"))

        item: dict[str, Any] = {
            "type": "image",
            "source_url": source_url,
            "source_file": source_file,
        }

        data = b""
        filename = ""
        if source_url.startswith("http://") or source_url.startswith("https://"):
            data, filename, err = await self._download_bytes_async(source_url)
            if err:
                self._last_error = f"lsky_download: {err}"
                item["status"] = "failed"
                item["error"] = err
                self._upload_fail += 1
                return item
        elif source_file and (
            source_file.startswith("http://") or source_file.startswith("https://")
        ):
            data, filename, err = await self._download_bytes_async(source_file)
            if err:
                self._last_error = f"lsky_download: {err}"
                item["status"] = "failed"
                item["error"] = err
                self._upload_fail += 1
                return item
        else:
            item["status"] = "skipped"
            item["error"] = "no_http_image_source"
            return item

        ok, lsky_url, lsky_key, err = await self._upload_bytes_async(data, filename)
        if ok:
            item["lsky_url"] = lsky_url
            item["lsky_key"] = lsky_key
            item["status"] = "uploaded"
            self._upload_ok += 1
        else:
            item["status"] = "failed"
            item["error"] = err
            self._last_error = f"lsky_upload: {err}"
            self._upload_fail += 1
        return item

    async def _download_bytes_async(self, url: str) -> tuple[bytes, str, str]:
        client = self._http()
        if client is None or httpx is None:
            return self._download_bytes_sync(url)
        try:
            resp = await client.get(url, timeout=int_conf(self._config, "lsky_timeout", 20))
            resp.raise_for_status()
            body = resp.content
            parsed = urlparse(url)
            filename = Path(parsed.path).name or f"img_{int(time.time() * 1000)}.jpg"
            return body, filename, ""
        except Exception as e:
            return b"", "", norm(e)

    def _download_bytes_sync(self, url: str) -> tuple[bytes, str, str]:
        try:
            import urllib.request
            req = urllib.request.Request(url=url, method="GET")
            with urllib.request.urlopen(
                req, timeout=int_conf(self._config, "lsky_timeout", 20)
            ) as resp:
                body = resp.read()
            parsed = urlparse(url)
            filename = Path(parsed.path).name or f"img_{int(time.time() * 1000)}.jpg"
            return body, filename, ""
        except Exception as e:
            return b"", "", norm(e)

    async def _upload_bytes_async(
        self, data: bytes, filename: str
    ) -> tuple[bool, str, str, str]:
        api_base = norm(self._config.get("lsky_api_base", "")).rstrip("/")
        token = norm(self._config.get("lsky_token", ""))
        album_id = norm(self._config.get("lsky_album_id", ""))
        if not api_base or not token:
            return False, "", "", "missing_lsky_api_or_token"

        endpoint = f"{api_base}/api/v1/upload"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        boundary = f"----AstrBotBoundary{int(time.time()*1000)}"
        body = self._build_multipart_body(boundary, filename, content_type, data, album_id)

        client = self._http()
        if client is None or httpx is None:
            return self._upload_bytes_sync(data, filename)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
        }
        try:
            resp = await client.post(
                endpoint, headers=headers, content=body,
                timeout=int_conf(self._config, "lsky_timeout", 20)
            )
            resp.raise_for_status()
            obj = resp.json()
            payload = obj.get("data", {}) if isinstance(obj, dict) else {}
            links = payload.get("links", {}) if isinstance(payload, dict) else {}
            lsky_url = norm(links.get("url")) or norm(payload.get("url"))
            lsky_key = norm(payload.get("key"))
            if not lsky_url:
                return False, "", "", "lsky_response_no_url"
            return True, lsky_url, lsky_key, ""
        except Exception as e:
            return False, "", "", norm(e)

    def _upload_bytes_sync(self, data: bytes, filename: str) -> tuple[bool, str, str, str]:
        api_base = norm(self._config.get("lsky_api_base", "")).rstrip("/")
        token = norm(self._config.get("lsky_token", ""))
        album_id = norm(self._config.get("lsky_album_id", ""))
        if not api_base or not token:
            return False, "", "", "missing_lsky_api_or_token"

        endpoint = f"{api_base}/api/v1/upload"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        boundary = f"----AstrBotBoundary{int(time.time()*1000)}"
        body = self._build_multipart_body(boundary, filename, content_type, data, album_id)

        try:
            import urllib.request
            req = urllib.request.Request(url=endpoint, data=body, method="POST")
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(
                req, timeout=int_conf(self._config, "lsky_timeout", 20)
            ) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            obj = json.loads(raw) if raw else {}
            payload = obj.get("data", {}) if isinstance(obj, dict) else {}
            links = payload.get("links", {}) if isinstance(payload, dict) else {}
            lsky_url = norm(links.get("url")) or norm(payload.get("url"))
            lsky_key = norm(payload.get("key"))
            if not lsky_url:
                return False, "", "", "lsky_response_no_url"
            return True, lsky_url, lsky_key, ""
        except Exception as e:
            return False, "", "", norm(e)

    @staticmethod
    def _build_multipart_body(
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
