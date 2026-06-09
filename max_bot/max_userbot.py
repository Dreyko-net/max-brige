#!/usr/bin/env python3
import asyncio
import logging
import sys
from datetime import datetime

from pymax import Client, Message, ConsoleSmsCodeProvider

from database import Database
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("max_userbot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

cfg = Config()
db  = Database(cfg.db_path)


async def main():
    print("\n" + "═" * 55)
    print("  MAX Messenger Userbot — сборщик сообщений")
    print("═" * 55 + "\n")

    phone = cfg.phone
    if not phone:
        phone = input("Введите номер телефона (например, +79001234567): ").strip()
    if not phone:
        print("Номер не введён. Выход.")
        sys.exit(1)

    db.init()

    client = Client(
        phone=phone,
        session_name="session.db",
        work_dir="cache",
        sms_code_provider=ConsoleSmsCodeProvider(),
    )



    @client.on_message()
    async def handle_message(msg: Message, client: Client) -> None
        try:
            sender_id   = str(getattr(msg.sender,   "id",       "") or "")
            sender_name = str(getattr(msg.sender,   "name",     "") or
                              getattr(msg.sender,   "username", "") or "")
            chat_id     = str(getattr(msg,          "chat_id",  "") or "")
            text        = getattr(msg, "text", "") or ""
            msg_id      = str(getattr(msg, "id",    "") or "")
            timestamp   = getattr(msg, "timestamp", None)

            if timestamp is None:
                timestamp = int(datetime.now().timestamp() * 1000)

            db.save_message(
                message_id=msg_id, sender_id=sender_id,
                sender_name=sender_name, chat_id=chat_id,
                text=text, timestamp=timestamp, raw=repr(msg),
            )

            dt = datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M:%S")
            log.info("[%s] %s → chat:%s | %s",
                     dt, sender_name or sender_id, chat_id,
                     text[:100] if text else "(без текста)")

        except Exception as e:
            log.error("Ошибка обработки сообщения: %s | %s", e, msg)

    print(f"\nПодключение для номера {phone}…")
    print("(При первом запуске придёт SMS с кодом)\n")

    try:
        me = await client.start()
        me_id   = getattr(me, "id",   "?")
        me_name = getattr(me, "name", "?") or getattr(me, "username", "?")
        print(f"✅  Авторизован как: {me_name} (id={me_id})")
        print(f"💾  База данных: {cfg.db_path}")
        print("🚀  Ожидание сообщений. Нажмите Ctrl+C для остановки.\n")
        db.set_meta("me_id",   str(me_id))
        db.set_meta("me_name", str(me_name))
        await client.run_forever()
    except KeyboardInterrupt:
        print("\n\nОстановка…")
    except Exception as e:
        log.error("Критическая ошибка: %s", e)
        raise
    finally:
        db.close()
        print("До свидания!")


if __name__ == "__main__":
    asyncio.run(main())