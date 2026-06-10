#!/usr/bin/env python3
"""
MAX ↔ Telegram Bridge
Точка входа: запускает Telegram бота и BridgeManager в одном event loop.
"""

import asyncio
import logging
import sys

from aiogram import Bot

from config import TG_BOT_TOKEN
from database import db
from bridge.manager import manager
from telegram import create_bot

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bridge.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Отключаем лишние логи pymax
logging.getLogger("pymax").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.INFO)


async def main():
    if not TG_BOT_TOKEN:
        print("❌ Укажите TG_BOT_TOKEN в файле .env или переменной окружения")
        sys.exit(1)

    print("\n" + "═" * 55)
    print("  MAX ↔ Telegram Bridge")
    print("═" * 55 + "\n")

    # Инициализация БД
    await db.connect()
    log.info("Database connected")

    # Создание бота и диспетчера
    bot, dp = create_bot(TG_BOT_TOKEN)

    # Запуск BridgeManager (восстанавливает сессии из БД)
    await manager.start(bot)

    log.info("Starting Telegram bot polling…")
    print("🚀  Бот запущен. Нажмите Ctrl+C для остановки.\n")

    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_member"])
    finally:
        await manager.stop()
        await db.close()
        await bot.session.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка…")