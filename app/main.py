from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode

from .config import get_settings
from .storage import Storage
from .wb_client import WBClient
from .bot import setup_handlers

async def main() -> None:
    settings = get_settings()

    storage = Storage(settings.sqlite_path)
    await storage.connect()

    wb = WBClient(dest=settings.wb_dest, locale=settings.wb_locale)

    bot = Bot(token=settings.bot_token, parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    setup_handlers(dp, settings, storage, wb)

    try:
        await dp.start_polling(bot)
    finally:
        await wb.aclose()
        await storage.close()

if __name__ == "__main__":
    asyncio.run(main())
