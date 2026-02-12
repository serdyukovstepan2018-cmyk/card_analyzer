from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _get_int(name: str, default: int) -> int:
    v = os.getenv(name, str(default)).strip()
    try:
        return int(v)
    except ValueError:
        return default

@dataclass(frozen=True)
class Settings:
    bot_token: str

    wb_locale: str = os.getenv("WB_LOCALE", "ru").strip()
    wb_dest: str = os.getenv("WB_DEST", "-1216601,-115136,-421732,123585595").strip()

    reviews_limit: int = _get_int("REVIEWS_LIMIT", 120)
    card_ttl_seconds: int = _get_int("CARD_TTL_SECONDS", 600)
    reviews_ttl_seconds: int = _get_int("REVIEWS_TTL_SECONDS", 3600)

    rate_limit_window_seconds: int = _get_int("RATE_LIMIT_WINDOW_SECONDS", 60)
    rate_limit_max_requests: int = _get_int("RATE_LIMIT_MAX_REQUESTS", 6)

    sqlite_path: str = os.getenv("SQLITE_PATH", "/data/bot.sqlite3").strip()

def get_settings() -> Settings:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is empty. Put it into .env (see .env.example).")
    return Settings(bot_token=token)
