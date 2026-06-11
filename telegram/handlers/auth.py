"""
Хэндлеры авторизации пользователя.
FSM: /start → WAIT_PHONE → WAIT_SMS → WAIT_GROUP → CONNECTED
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F, Bot
from aiogram.filters import CommandStart
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, IS_NOT_MEMBER, IS_MEMBER, ADMINISTRATOR
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatMemberUpdated,
)

from bridge.manager import manager
from bridge.max_client import session_path_for
from database import db
from telegram.sms_provider import TelegramSmsCodeProvider
from bridge.sync_worker import SyncWorker

log = logging.getLogger(__name__)
router = Router()

# tg_user_id → TelegramSmsCodeProvider
_pending_auth: dict[int, TelegramSmsCodeProvider] = {}

# tg_user_id → asyncio.Task авторизации (чтобы можно было отменить)
_auth_tasks: dict[int, asyncio.Task] = {}


class AuthStates(StatesGroup):
    WAIT_PHONE = State()
    WAIT_SMS   = State()
    WAIT_GROUP = State()
    CONNECTED  = State()


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def _step1_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Группа создана", callback_data="group_created"),
    ]])


def _step2_kb(bot_username: str) -> InlineKeyboardMarkup:
    add_url = (
        f"https://t.me/{bot_username}?startgroup=setup"
        f"&admin=manage_topics+post_messages+delete_messages"
    )
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="➕ Добавить бота в группу", url=add_url),
    ]])


def _cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отменить авторизацию", callback_data="cancel_auth"),
    ]])


# ── Тексты ────────────────────────────────────────────────────────────────────

STEP1_TEXT = (
    "🏗 <b>Шаг 1 из 2 — создайте группу-зеркало</b>\n\n"
    "1. Откройте Telegram\n"
    "2. Создайте новую супергруппу\n"
    "3. Зайдите в <b>Настройки группы → Темы</b> и включите их\n\n"
    "Нажмите кнопку когда группа готова."
)


async def _send_step1(bot: Bot, chat_id: int):
    await bot.send_message(chat_id, STEP1_TEXT, parse_mode="HTML",
                           reply_markup=_step1_kb())


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, bot: Bot):
    user_id = msg.from_user.id
    log.info("[/start] user_id=%s", user_id)

    # Если уже идёт авторизация — предлагаем отменить
    if user_id in _auth_tasks and not _auth_tasks[user_id].done():
        await msg.answer(
            "⏳ Авторизация уже выполняется.\n"
            "Если хотите начать заново — отмените текущую.",
            reply_markup=_cancel_kb(),
        )
        return

    user = await db.get_user(user_id)
    log.info("[/start] db_user=%s status=%s tg_group_id=%s",
             user.id if user else None,
             user.status if user else None,
             user.tg_group_id if user else None)

    if user and user.status == "active":
        client = manager.get_client(user_id)
        log.info("[/start] client_in_pool=%s", client is not None)

        if client and user.tg_group_id:
            await msg.answer("✅ Вы уже подключены к MAX.\nСообщения зеркалируются в вашу группу.")
            return

        if client and not user.tg_group_id:
            await _send_step1(bot, msg.chat.id)
            await state.set_state(AuthStates.WAIT_GROUP)
            return

        log.info("[/start] client not in pool, reconnecting")
        await msg.answer("🔄 Переподключаюсь к MAX…")
        asyncio.create_task(_reconnect(user_id, user.max_phone, bot, msg.chat.id, state))
        return

    log.info("[/start] fresh auth flow")
    await msg.answer(
        "👋 Добро пожаловать в <b>MAX Bridge</b>!\n\n"
        "Введите ваш номер телефона, зарегистрированный в MAX\n"
        "(формат: <code>+79001234567</code>):",
        parse_mode="HTML",
    )
    await state.set_state(AuthStates.WAIT_PHONE)


# ── Телефон ───────────────────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_PHONE)
async def handle_phone(msg: Message, state: FSMContext, bot: Bot):
    phone = msg.text.strip() if msg.text else ""
    if not phone.startswith("+") or len(phone) < 10:
        await msg.answer("❌ Неверный формат. Введите номер в формате +79001234567:")
        return

    tg_user_id = msg.from_user.id
    log.info("[auth] phone entered user=%s", tg_user_id)
    await state.update_data(phone=phone)

    provider = TelegramSmsCodeProvider(tg_user_id=tg_user_id, bot=bot, chat_id=msg.chat.id)
    _pending_auth[tg_user_id] = provider

    await msg.answer(
        f"📲 Подключаюсь к MAX с номером <code>{phone}</code>…",
        parse_mode="HTML",
        reply_markup=_cancel_kb(),
    )
    await db.create_user(
        tg_user_id   = tg_user_id,
        tg_username  = msg.from_user.username,
        max_phone    = phone,
        session_path = session_path_for(tg_user_id),
    )
    await state.set_state(AuthStates.WAIT_SMS)

    task = asyncio.create_task(
        _run_auth(tg_user_id=tg_user_id, phone=phone, provider=provider,
                  bot=bot, chat_id=msg.chat.id, state=state)
    )
    _auth_tasks[tg_user_id] = task


# ── SMS-код ───────────────────────────────────────────────────────────────────

@router.message(AuthStates.WAIT_SMS)
async def handle_sms_code(msg: Message, state: FSMContext):
    code = msg.text.strip() if msg.text else ""
    if not code.isdigit():
        await msg.answer("❌ Код должен состоять из цифр. Попробуйте ещё раз:")
        return

    tg_user_id = msg.from_user.id
    log.info("[auth] sms code entered user=%s", tg_user_id)
    provider = _pending_auth.get(tg_user_id)
    if not provider:
        await msg.answer("⚠️ Сессия устарела. Начните заново: /start")
        await state.clear()
        return

    provider.set_code(code)
    await msg.answer("🔐 Проверяю код…")


# ── Отмена авторизации ────────────────────────────────────────────────────────

@router.callback_query(F.data == "cancel_auth")
async def cb_cancel_auth(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    tg_user_id = callback.from_user.id

    # Отменяем задачу авторизации
    task = _auth_tasks.pop(tg_user_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Отменяем провайдер SMS
    provider = _pending_auth.pop(tg_user_id, None)
    if provider:
        provider.cancelled = True
        provider._event.set()  # разблокируем ожидание если зависло

    await state.clear()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "❌ Авторизация отменена.\n\n"
        "Для повторной попытки напишите /start\n"
        "⚠️ Если MAX заблокировал запросы — подождите несколько минут перед повторной попыткой."
    )
    log.info("[auth] cancelled by user=%s", tg_user_id)


# ── Callbacks шагов ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "group_created")
async def cb_group_created(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    me = await bot.get_me()
    await callback.message.answer(
        "✅ Отлично!\n\n"
        "🏗 <b>Шаг 2 из 2 — добавьте бота в группу</b>\n\n"
        "Нажмите кнопку ниже — Telegram предложит выбрать вашу группу "
        "и автоматически назначит бота администратором с нужными правами.\n\n"
        "После добавления бот <b>автоматически</b> начнёт синхронизацию.",
        parse_mode="HTML",
        reply_markup=_step2_kb(me.username),
    )
    await state.set_state(AuthStates.WAIT_GROUP)


# ── Добавление бота в группу (my_chat_member) ─────────────────────────────────
# Это событие приходит когда бота добавляют в группу или назначают админом.
# Именно здесь мы узнаём ID группы — без пересылки сообщений.

@router.my_chat_member(
    ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER >> ADMINISTRATOR)
)
async def bot_added_as_admin(event: ChatMemberUpdated, bot: Bot):
    """Бот добавлен в группу с правами администратора."""
    group = event.chat
    if group.type not in ("supergroup", "group"):
        return

    group_id = group.id
    tg_user_id = event.from_user.id
    log.info("[group] bot added as admin to group=%s by user=%s", group_id, tg_user_id)

    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        log.info("[group] user not found or not active, skipping")
        return

    if user.tg_group_id:
        log.info("[group] user already has group, skipping")
        return

    # Проверяем Topics
    try:
        chat_info = await bot.get_chat(group_id)
        if not getattr(chat_info, "is_forum", False):
            await bot.send_message(
                tg_user_id,
                f"⚠️ Бот добавлен в группу <b>{group.title}</b>, "
                f"но <b>Темы (Topics)</b> не включены.\n\n"
                f"Включите: Настройки группы → Темы → Включить\n"
                f"Затем удалите бота из группы и добавьте снова.",
                parse_mode="HTML",
            )
            return
    except Exception as e:
        log.error("[group] get_chat error: %s", e)

    # Сохраняем group_id
    await db.set_user_group(tg_user_id, group_id)
    user = await db.get_user(tg_user_id)

    await bot.send_message(
        tg_user_id,
        f"✅ Группа <b>{group.title}</b> подключена!\n"
        f"🔄 Начинаю синхронизацию чатов MAX…\n\n"
        f"Прогресс буду отправлять в группу.",
        parse_mode="HTML",
    )

    client = manager.get_client(tg_user_id)
    if client:
        sync = SyncWorker(bot=bot, manager=manager)
        asyncio.create_task(sync.full_sync(user=user, client=client))
    else:
        await bot.send_message(tg_user_id, "⚠️ Клиент MAX не найден. Попробуйте /start")


@router.my_chat_member(
    ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER >> IS_MEMBER)
)
async def bot_added_as_member(event: ChatMemberUpdated, bot: Bot):
    """Бот добавлен как обычный участник (без прав админа)."""
    group = event.chat
    if group.type not in ("supergroup", "group"):
        return
    tg_user_id = event.from_user.id
    await bot.send_message(
        tg_user_id,
        f"⚠️ Бот добавлен в группу <b>{group.title}</b> без прав администратора.\n\n"
        f"Назначьте бота администратором с правами:\n"
        f"• Управление темами\n"
        f"• Отправка сообщений",
        parse_mode="HTML",
    )


# ── Фоновые функции ───────────────────────────────────────────────────────────

async def _run_auth(tg_user_id, phone, provider, bot, chat_id, state):
    log.info("[auth] _run_auth started user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id=tg_user_id, max_phone=phone, sms_code_provider=provider)
        log.info("[auth] connect_user done user=%s me=%s", tg_user_id, client.me)

        _pending_auth.pop(tg_user_id, None)
        _auth_tasks.pop(tg_user_id, None)
        await db.set_user_active(tg_user_id)
        await state.set_state(AuthStates.WAIT_GROUP)

        me_name = getattr(client.me, "name", phone) or phone
        await bot.send_message(
            chat_id,
            f"✅ Авторизован в MAX как <b>{me_name}</b>",
            parse_mode="HTML",
        )
        await _send_step1(bot, chat_id)

    except asyncio.CancelledError:
        log.info("[auth] task cancelled user=%s", tg_user_id)
        _pending_auth.pop(tg_user_id, None)
        _auth_tasks.pop(tg_user_id, None)
        await state.clear()
    except Exception as e:
        log.error("[auth] error user=%s: %s", tg_user_id, e, exc_info=True)
        _pending_auth.pop(tg_user_id, None)
        _auth_tasks.pop(tg_user_id, None)
        await state.clear()
        await bot.send_message(
            chat_id,
            f"❌ Ошибка авторизации: <code>{e}</code>\n\n"
            f"Попробуйте снова: /start\n"
            f"⚠️ Если ошибка повторяется — подождите несколько минут.",
            parse_mode="HTML",
        )


async def _reconnect(tg_user_id, phone, bot, chat_id, state):
    log.info("[reconnect] starting user=%s", tg_user_id)
    try:
        client = await manager.connect_user(
            tg_user_id=tg_user_id, max_phone=phone, sms_code_provider=None)
        log.info("[reconnect] done user=%s", tg_user_id)
        user = await db.get_user(tg_user_id)
        if user and not user.tg_group_id:
            await _send_step1(bot, chat_id)
            await state.set_state(AuthStates.WAIT_GROUP)
        else:
            await bot.send_message(chat_id, "✅ Переподключено к MAX.")
    except Exception as e:
        log.error("[reconnect] error user=%s: %s", tg_user_id, e, exc_info=True)
        await bot.send_message(
            chat_id,
            f"❌ Ошибка переподключения: <code>{e}</code>",
            parse_mode="HTML",
        )


# ── Ручной запуск синхронизации ───────────────────────────────────────────────

from aiogram.filters import Command

@router.message(Command("sync"))
async def cmd_sync(msg: Message, bot: Bot):
    """Принудительно запускает синхронизацию чатов MAX."""
    tg_user_id = msg.from_user.id
    user = await db.get_user(tg_user_id)
    if not user or user.status != "active":
        await msg.answer("❌ Сначала авторизуйтесь: /start")
        return
    if not user.tg_group_id:
        await msg.answer("❌ Группа не подключена. Пройдите /start")
        return
    client = manager.get_client(tg_user_id)
    if not client:
        await msg.answer("❌ Клиент MAX не найден. Перезапустите: /start")
        return

    await msg.answer("🔄 Запускаю синхронизацию…")
    sync = SyncWorker(bot=bot, manager=manager)
    asyncio.create_task(sync.full_sync(user=user, client=client))


@router.message(Command("debug_chats"))
async def cmd_debug_chats(msg: Message):
    """Отладка: показывает что возвращает fetch_chats."""
    tg_user_id = msg.from_user.id
    client = manager.get_client(tg_user_id)
    if not client:
        await msg.answer("❌ Клиент не найден")
        return

    await msg.answer("🔍 Запрашиваю чаты из MAX…")
    try:
        raw = await client._client.fetch_chats()
        if not raw:
            await msg.answer("⚠️ fetch_chats() вернул пустой список")
            return
        lines = [f"Всего чатов: {len(raw)}"]
        for c in raw[:5]:
            cid    = getattr(c, "id",    None) or getattr(c, "chat_id", "?")
            ctitle = getattr(c, "title", None) or getattr(c, "name",    "?")
            ctype  = type(c).__name__
            lines.append(f"• [{ctype}] id={cid} title={ctitle}")
        await msg.answer("\n".join(lines))
    except Exception as e:
        await msg.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")