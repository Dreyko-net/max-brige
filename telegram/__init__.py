import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import TelegramObject

from telegram.handlers import auth, messages, callbacks

log = logging.getLogger(__name__)


class LogAllUpdatesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        log.info("[MIDDLEWARE] update type=%s data_keys=%s",
                 type(event).__name__, list(data.keys()))
        for key, value in data.items():
            log.info(f"[DEBUG]  {key}: {value}")
        result = await handler(event, data)
        return result


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    bot = Bot(token=token)
    dp  = Dispatcher(storage=MemoryStorage())

    dp.update.middleware(LogAllUpdatesMiddleware())

    dp.include_router(auth.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)

    return bot, dp