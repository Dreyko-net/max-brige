"""
Обёртка над pymax.Client для одного пользователя.
Методы приведены к реальному API pymax 2.1.x
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
        self.tg_user_id    = tg_user_id
        self.max_phone     = max_phone
        self.session_path  = session_path
        self.on_ready      = on_ready
        self._sms_provider = sms_code_provider
        self._client: Optional[Client] = None
        self._task:   Optional[asyncio.Task] = None
        self.me            = None
        self._ready        = asyncio.Event()

    def _build_client(self) -> Client:
        Path(self.session_path).mkdir(parents=True, exist_ok=True)
        return Client(
            phone             = self.max_phone,
            work_dir          = self.session_path,
            session_name      = "session.db",
            sms_code_provider = self._sms_provider,
        )

    async def start(self) -> None:
        """Запускает клиент как Task, ждёт готовности через Event."""
        self._client = self._build_client()
        self._register_handlers()

        # Патчим _app.start чтобы поймать момент готовности
        original_app_start = self._client._app.start

        async def patched_app_start():
            await original_app_start()
            self.me = getattr(self._client, "me", None) or \
                      getattr(self._client._app, "profile", None)
            log.info("[user=%s] MAX ready, me=%s", self.tg_user_id, self.me)
            self._ready.set()
            if self.on_ready:
                await self.on_ready(self)

        self._client._app.start = patched_app_start

        self._task = asyncio.create_task(
            self._run_forever(),
            name=f"max_client_{self.tg_user_id}",
        )

        try:
            await asyncio.wait_for(self._ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            log.error("[user=%s] MAX client ready timeout", self.tg_user_id)
            self._task.cancel()
            raise

    async def _run_forever(self):
        try:
            await self._client.start()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("[user=%s] MAX client error: %s", self.tg_user_id, e, exc_info=True)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
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

    # ── Получение чатов ───────────────────────────────────────────────────────

    async def get_chats(self) -> list:
        """
        Возвращает все чаты пользователя через fetch_chats с пагинацией.
        fetch_chats(marker) → list[Chat], marker=None для первой страницы.
        """
        all_chats = []
        marker = None
        try:
            while True:
                if marker is not None:
                    page = await self._client.fetch_chats(marker=marker)
                else:
                    page = await self._client.fetch_chats()

                if not page:
                    break
                all_chats.extend(page)

                # Если вернули меньше стандартного размера страницы — конец
                if len(page) < 20:
                    break

                # marker — id последнего чата для следующей страницы
                last = page[-1]
                marker = getattr(last, "id", None) or getattr(last, "chat_id", None)
                if not marker:
                    break

            log.info("[user=%s] get_chats: found %d chats", self.tg_user_id, len(all_chats))
            return all_chats
        except Exception as e:
            log.error("[user=%s] get_chats error: %s", self.tg_user_id, e)
            return []

    # ── История сообщений ─────────────────────────────────────────────────────

    async def get_history(
        self,
        max_chat_id: str,
        from_ts:     int,
        to_ts:       int,
        limit:       int = 100,
    ) -> list:
        """
        Возвращает историю сообщений чата.
        fetch_history(chat_id, from_time=<конец периода>, backward=<кол-во>)
        from_time — точка отсчёта, берём сообщения ДО неё.
        Фильтрация по from_ts выполняется на нашей стороне.
        """
        try:
            result = await self._client.fetch_history(
                chat_id   = int(max_chat_id),
                from_time = to_ts,   # точка отсчёта — конец периода
                backward  = limit,   # сколько сообщений назад
            )
            log.info("[DEBUG user=%s] get_history chat=%s: result_messangers: %d", self.tg_user_id, max_chat_id, result)
            if not result:
                return []
            # Фильтруем по нижней границе from_ts
            filtered = [m for m in result
                        if getattr(m, "timestamp", 0) >= from_ts]
            log.info("[user=%s] get_history chat=%s: got %d, filtered to %d",
                     self.tg_user_id, max_chat_id, len(result), len(filtered))
            return filtered
        except Exception as e:
            log.error("[user=%s] get_history(%s) error: %s",
                      self.tg_user_id, max_chat_id, e)
            return []

    # ── Отправка сообщений ────────────────────────────────────────────────────

    async def send_message(self, max_chat_id: str, text: str) -> Optional[str]:
        """Отправляет текстовое сообщение."""
        try:
            result = await self._client.send_message(
                chat_id = int(max_chat_id),
                text    = text,
            )
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_message error: %s", self.tg_user_id, e)
            return None

    async def send_file(
        self,
        max_chat_id: str,
        data:        bytes,
        filename:    str,
        caption:     str = "",
    ) -> Optional[str]:
        """Отправляет файл с подписью."""
        try:
            from pymax.files.file import File
            attachment = File(data=data, filename=filename)
            result = await self._client.send_message(
                chat_id     = int(max_chat_id),
                text        = caption,
                attachments = [attachment],
            )
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_file error: %s", self.tg_user_id, e)
            return None

    async def send_photo(
        self,
        max_chat_id: str,
        data:        bytes,
        caption:     str = "",
    ) -> Optional[str]:
        """Отправляет фото."""
        try:
            from pymax.files.photo import Photo
            attachment = Photo(data=data)
            result = await self._client.send_message(
                chat_id     = int(max_chat_id),
                text        = caption,
                attachments = [attachment],
            )
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_photo error: %s", self.tg_user_id, e)
            return None

    async def download_file(self, chat_id: str, message_id: str, file_id: int) -> Optional[bytes]:
        """Скачивает файл из MAX."""
        try:
            file_req = await self._client.get_file_by_id(
                chat_id    = int(chat_id),
                message_id = message_id,
                file_id    = file_id,
            )
            if file_req:
                return getattr(file_req, "data", None)
            return None
        except Exception as e:
            log.error("[user=%s] download_file error: %s", self.tg_user_id, e)
            return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_media(msg) -> tuple[bool, Optional[str]]:
    """Определяет тип медиа в сообщении."""
    # pymax хранит вложения в attaches или attachments
    attaches = getattr(msg, "attaches", None) or getattr(msg, "attachments", None) or []
    if attaches:
        first = attaches[0] if attaches else None
        if first:
            t = type(first).__name__.lower()
            if "photo" in t:   return True, "photo"
            if "video" in t:   return True, "video"
            if "file"  in t:   return True, "document"
            if "voice" in t:   return True, "voice"
            if "audio" in t:   return True, "audio"
            return True, "document"
    # Старый API — прямые поля
    for attr, kind in [("photo","photo"), ("video","video"), ("document","document"),
                       ("voice","voice"), ("audio","audio"), ("sticker","sticker")]:
        if getattr(msg, attr, None):
            return True, kind
    return False, None


def session_path_for(tg_user_id: int) -> str:
    return str(SESSIONS_DIR / f"user_{tg_user_id}")