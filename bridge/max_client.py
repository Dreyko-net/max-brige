"""
Обёртка над pymax.Client для одного пользователя.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Callable, Optional

from pymax import Client, Message

from bridge.queue import BridgeEvent, max_to_tg_queue
from config import SESSIONS_DIR

log = logging.getLogger(__name__)


class MaxUserClient:
    def __init__(
        self,
        tg_user_id:        int,
        max_phone:         str,
        session_path:      str,
        on_ready:          Optional[Callable] = None,
        sms_code_provider = None,
    ):
        self.tg_user_id          = tg_user_id
        self.max_phone           = max_phone
        self.session_path        = session_path
        self.on_ready            = on_ready
        self._sms_provider       = sms_code_provider
        self._client: Optional[Client] = None
        self._task:   Optional[asyncio.Task] = None
        self.me                  = None
        self._ready              = asyncio.Event()
        self._on_session_revoked = None

    def _build_client(self) -> Client:
        Path(self.session_path).mkdir(parents=True, exist_ok=True)
        return Client(
            phone             = self.max_phone,
            work_dir          = self.session_path,
            session_name      = "session.db",
            sms_code_provider = self._sms_provider,
        )

    async def start(self) -> None:
        """
        Запускает клиент.

        Стратегия: запускаем _app.start() напрямую (авторизация + логин),
        ждём его завершения — это и есть сигнал готовности.
        После этого запускаем бесконечный reconnect-цикл как Task.
        """
        self._client = self._build_client()
        self._register_handlers()

        # Шаг 1: авторизация + первый логин.
        # _app.start() завершается после успешного логина.
        # Таймаут большой (5 мин) — пользователь может долго вводить SMS.
        log.info("[user=%s] running _app.start() (auth + login)", self.tg_user_id)
        try:
            await asyncio.wait_for(
                self._client._app.start(),
                timeout=60,  # чуть больше таймаута SMS-провайдера (300 сек)
            )
        except asyncio.TimeoutError:
            log.error("[user=%s] _app.start() timeout", self.tg_user_id)
            raise TimeoutError("MAX auth timeout")
        except asyncio.CancelledError:
            raise

        # Авторизация успешна
        self.me = getattr(self._client, "me", None)
        log.info("[user=%s] MAX auth done, me=%s", self.tg_user_id,
                 getattr(self.me, "id", "?") if self.me else "?")
        self._ready.set()

        if self.on_ready:
            await self.on_ready(self)

        # Шаг 2: запускаем reconnect-цикл в фоне
        # (он будет переподключаться при обрывах)
        self._task = asyncio.create_task(
            self._reconnect_loop(),
            name=f"max_client_{self.tg_user_id}",
        )

    async def _reconnect_loop(self):
        """
        Поддерживает соединение живым после первого логина.
        Повторяет цикл: wait_closed → reconnect при обрыве.
        """
        while True:
            try:
                await self._client._connection.wait_closed()
            except asyncio.CancelledError:
                return
            except Exception:
                pass

            # Соединение оборвалось — переподключаемся
            log.info("[user=%s] connection lost, reconnecting…", self.tg_user_id)
            try:
                self._client._reset_runtime()
                await self._client._app.start()
                log.info("[user=%s] reconnected", self.tg_user_id)
            except asyncio.CancelledError:
                return
            except Exception as e:
                err = str(e).lower()
                if "fail_logout_all" in err or "login.token" in err:
                    log.error("[user=%s] session revoked on reconnect", self.tg_user_id)
                    if self._on_session_revoked:
                        asyncio.create_task(self._on_session_revoked(self.tg_user_id))
                    return
                log.error("[user=%s] reconnect error: %s", self.tg_user_id, e)
                await asyncio.sleep(5)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        try:
            await self._client.close()
        except Exception:
            pass

    def _register_handlers(self):
        client = self._client

        @client.on_message()
        async def handle_message(msg: Message, _client: Client) -> None:
            try:
                text      = getattr(msg, "text",      "") or ""
                msg_id    = str(getattr(msg, "id",    "") or "")
                chat_id   = str(getattr(msg, "chat_id", "") or "")
                timestamp = getattr(msg, "timestamp", None) or int(time.time() * 1000)
                has_media, media_type = _detect_media(msg)
                event = BridgeEvent(
                    direction   = "max_to_tg",
                    tg_user_id  = self.tg_user_id,
                    max_chat_id = chat_id,
                    text        = text,
                    timestamp   = timestamp,
                    max_msg_id  = msg_id,
                    has_media   = has_media,
                    media_type  = media_type,
                )
                await max_to_tg_queue.put(event)
            except Exception as e:
                log.error("[user=%s] handle_message error: %s", self.tg_user_id, e)

    # ── API методы ────────────────────────────────────────────────────────────

    async def get_chats(self) -> list:
        all_chats = []
        marker = None
        try:
            while True:
                page = await self._client.fetch_chats(marker) if marker else \
                       await self._client.fetch_chats()
                if not page:
                    break
                all_chats.extend(page)
                if len(page) < 20:
                    break
                last   = page[-1]
                marker = getattr(last, "id", None) or getattr(last, "chat_id", None)
                if not marker:
                    break
            log.info("[user=%s] get_chats: found %d", self.tg_user_id, len(all_chats))
            return all_chats
        except Exception as e:
            log.error("[user=%s] get_chats error: %s", self.tg_user_id, e)
            return []

    async def get_history(self, max_chat_id: str, from_ts: int,
                          to_ts: int, limit: int = 100) -> list:
        try:
            result = await self._client.fetch_history(
                chat_id   = int(max_chat_id),
                from_time = to_ts,
                backward  = limit,
            )
            if not result:
                return []
            filtered = [m for m in result if getattr(m, "timestamp", 0) >= from_ts]
            log.info("[user=%s] get_history chat=%s: got %d, filtered %d",
                     self.tg_user_id, max_chat_id, len(result), len(filtered))
            return filtered
        except Exception as e:
            log.error("[user=%s] get_history error: %s", self.tg_user_id, e)
            return []

    async def send_message(self, max_chat_id: str, text: str) -> Optional[str]:
        try:
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=text)
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_message error: %s", self.tg_user_id, e)
            return None

    async def send_file(self, max_chat_id: str, data: bytes,
                        filename: str, caption: str = "") -> Optional[str]:
        try:
            from pymax.files.file import File
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=caption,
                attachments=[File(data=data, filename=filename)])
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_file error: %s", self.tg_user_id, e)
            return None

    async def send_photo(self, max_chat_id: str, data: bytes,
                         caption: str = "") -> Optional[str]:
        try:
            from pymax.files.photo import Photo
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=caption,
                attachments=[Photo(data=data)])
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_photo error: %s", self.tg_user_id, e)
            return None

    async def download_file(self, chat_id: str, message_id: str,
                            file_id: int) -> Optional[bytes]:
        try:
            req = await self._client.get_file_by_id(
                chat_id=int(chat_id), message_id=message_id, file_id=file_id)
            return getattr(req, "data", None) if req else None
        except Exception as e:
            log.error("[user=%s] download_file error: %s", self.tg_user_id, e)
            return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_media(msg) -> tuple[bool, Optional[str]]:
    attaches = getattr(msg, "attaches", None) or getattr(msg, "attachments", None) or []
    if attaches:
        t = type(attaches[0]).__name__.lower()
        if "photo"  in t: return True, "photo"
        if "video"  in t: return True, "video"
        if "voice"  in t: return True, "voice"
        if "audio"  in t: return True, "audio"
        if "file"   in t: return True, "document"
        return True, "document"
    for attr, kind in [("photo","photo"),("video","video"),("document","document"),
                       ("voice","voice"),("audio","audio"),("sticker","sticker")]:
        if getattr(msg, attr, None):
            return True, kind
    return False, None


def session_path_for(tg_user_id: int) -> str:
    return str(SESSIONS_DIR / f"user_{tg_user_id}")