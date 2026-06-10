"""
Хэндлеры входящих сообщений из Telegram → MAX.
Также обрабатывает пересланное сообщение из группы (регистрация group_id).
"""

from __future__ import annotations

import logging
import time

from aiogram import Router, F, Bot
from aiogram.types import Message, ContentType

from bridge.manager import manager
from bridge.queue import BridgeEvent, tg_to_max_queue
from bridge.sync_worker import SyncWorker
from database import db
from telegram.handlers.auth import AuthStates

log = logging.getLogger(__name__)
router = Router()


# ── Регистрация группы (пересланное сообщение) ────────────────────────────────

@router.message(AuthStates.CONNECTED, F.forward_origin )
@router.message(F.forward_origin )
async def handle_forwarded_group(msg: Message, bot: Bot):
    """
    Пользователь переслал сообщение из группы.
    Регистрируем group_id и запускаем синхронизацию.
    """
    tg_user_id = msg.from_user.id
    user = await db.get_user(tg_user_id)
    if not user:
        return

    group = msg.forward_origin 
    if not group or group.type not in ("supergroup", "group"):
        log.info("[DEBUG] Данные группы=%s", group)
        log.info("[DEBUG] Данные типа группы=%s", group.type)
        await msg.answer("❌ Перешлите сообщение из <b>супергруппы</b>.",
                         parse_mode="HTML")
        return

    group_id = group.id

    # Проверяем что бот — администратор
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(group_id, me.id)
        if member.status not in ("administrator", "creator"):
            await msg.answer(
                "❌ Бот не является администратором группы.\n"
                "Добавьте его с правами управления темами и попробуйте снова."
            )
            return
    except Exception as e:
        await msg.answer(f"❌ Не могу получить доступ к группе: {e}")
        return

    await db.set_user_group(tg_user_id, group_id)
    user = await db.get_user(tg_user_id)

    await msg.answer("✅ Группа зарегистрирована! Начинаю синхронизацию…")

    client = manager.get_client(tg_user_id)
    if client:
        sync = SyncWorker(bot=bot, manager=manager)
        import asyncio
        asyncio.create_task(sync.full_sync(user=user, client=client))
    else:
        await msg.answer("⚠️ Клиент MAX не найден. Попробуйте /start")


# ── Текстовые сообщения из темы → MAX ────────────────────────────────────────

@router.message(F.chat.type.in_({"supergroup", "group"}), F.text)
async def handle_group_text(msg: Message):
    if not msg.message_thread_id:
        return  # сообщение не в теме — игнорируем

    tg_user_id = await _find_owner(msg.chat.id)
    if not tg_user_id:
        return

    user = await db.get_user(tg_user_id)
    if not user:
        return

    chat = await db.get_chat_by_topic(user.id, msg.message_thread_id)
    if not chat:
        log.warning("No MAX chat for topic %s", msg.message_thread_id)
        return

    event = BridgeEvent(
        direction   = "tg_to_max",
        tg_user_id  = tg_user_id,
        max_chat_id = chat.max_chat_id,
        text        = msg.text or "",
        timestamp   = int(time.time() * 1000),
        tg_msg_id   = msg.message_id,
    )
    await tg_to_max_queue.put(event)


# ── Медиафайлы из темы → MAX ──────────────────────────────────────────────────

@router.message(
    F.chat.type.in_({"supergroup", "group"}),
    F.content_type.in_({
        ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT,
        ContentType.VOICE, ContentType.AUDIO,
    }),
)
async def handle_group_media(msg: Message, bot: Bot):
    if not msg.message_thread_id:
        return

    tg_user_id = await _find_owner(msg.chat.id)
    if not tg_user_id:
        return

    user = await db.get_user(tg_user_id)
    if not user:
        return

    chat = await db.get_chat_by_topic(user.id, msg.message_thread_id)
    if not chat:
        return

    # Скачиваем файл
    try:
        media_type, file_id, filename = _extract_media(msg)
        file = await bot.get_file(file_id)
        data = await bot.download_file(file.file_path)
        media_bytes = data.read() if hasattr(data, "read") else bytes(data)
    except Exception as e:
        log.error("Download TG media error: %s", e)
        return

    event = BridgeEvent(
        direction   = "tg_to_max",
        tg_user_id  = tg_user_id,
        max_chat_id = chat.max_chat_id,
        text        = msg.caption or "",
        timestamp   = int(time.time() * 1000),
        tg_msg_id   = msg.message_id,
        has_media   = True,
        media_type  = media_type,
        media_bytes = media_bytes,
        media_name  = filename,
    )
    await tg_to_max_queue.put(event)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _find_owner(tg_group_id: int) -> int | None:
    """Находит tg_user_id владельца по group_id."""
    import aiosqlite
    from config import DB_PATH
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT tg_user_id FROM users WHERE tg_group_id=?", (tg_group_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


def _extract_media(msg: Message) -> tuple[str, str, str]:
    """Возвращает (media_type, file_id, filename)."""
    if msg.photo:
        return "photo", msg.photo[-1].file_id, "photo.jpg"
    if msg.video:
        return "video", msg.video.file_id, msg.video.file_name or "video.mp4"
    if msg.document:
        return "document", msg.document.file_id, msg.document.file_name or "file"
    if msg.voice:
        return "voice", msg.voice.file_id, "voice.ogg"
    if msg.audio:
        return "audio", msg.audio.file_id, msg.audio.file_name or "audio.mp3"
    return "file", "", "file"

from telegram.handlers.auth import AuthStates

@router.message(AuthStates.WAIT_GROUP)
async def debug_wait_group(msg: Message):
    log.info("[DEBUG] WAIT_GROUP got message: content_type=%s forward_origin =%s forward_origin=%s",
             msg.content_type,
             msg.forward_origin ,
             getattr(msg, 'forward_origin', None))
    await msg.answer(f"DEBUG: content_type={msg.content_type}, forward_origin ={msg.forward_origin }")