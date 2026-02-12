from __future__ import annotations

import json
import time
from typing import Any, Optional, List, Dict

import aiosqlite

CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS cache (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  ttl_seconds INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rate_limit (
  user_id INTEGER PRIMARY KEY,
  window_start INTEGER NOT NULL,
  count INTEGER NOT NULL
);

-- price snapshots: we build history ourselves (from the moment the bot runs)
CREATE TABLE IF NOT EXISTS price_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nmid INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  basic_u INTEGER,
  product_u INTEGER
);

CREATE INDEX IF NOT EXISTS idx_price_history_nmid_ts ON price_history(nmid, ts DESC);
'''

class Storage:
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self.sqlite_path)
        await self._db.executescript(CREATE_SQL)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("Storage not connected")
        return self._db

    # --- cache ---
    async def cache_get(self, key: str) -> Optional[Any]:
        now = int(time.time())
        cur = await self.db.execute("SELECT value_json, updated_at, ttl_seconds FROM cache WHERE key=?", (key,))
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        value_json, updated_at, ttl_seconds = row
        if now - int(updated_at) > int(ttl_seconds):
            await self.db.execute("DELETE FROM cache WHERE key=?", (key,))
            await self.db.commit()
            return None
        try:
            return json.loads(value_json)
        except json.JSONDecodeError:
            return None

    async def cache_set(self, key: str, value: Any, ttl_seconds: int) -> None:
        now = int(time.time())
        value_json = json.dumps(value, ensure_ascii=False)
        await self.db.execute(
            "INSERT INTO cache(key, value_json, updated_at, ttl_seconds) VALUES(?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at, ttl_seconds=excluded.ttl_seconds",
            (key, value_json, now, int(ttl_seconds)),
        )
        await self.db.commit()

    # --- rate limit ---
    async def rate_limit_allow(self, user_id: int, window_seconds: int, max_requests: int) -> bool:
        now = int(time.time())
        cur = await self.db.execute("SELECT window_start, count FROM rate_limit WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        await cur.close()

        if not row:
            await self.db.execute("INSERT INTO rate_limit(user_id, window_start, count) VALUES(?,?,?)", (user_id, now, 1))
            await self.db.commit()
            return True

        window_start, count = int(row[0]), int(row[1])
        if now - window_start >= window_seconds:
            await self.db.execute("UPDATE rate_limit SET window_start=?, count=? WHERE user_id=?", (now, 1, user_id))
            await self.db.commit()
            return True

        if count >= max_requests:
            return False

        await self.db.execute("UPDATE rate_limit SET count=count+1 WHERE user_id=?", (user_id,))
        await self.db.commit()
        return True

    # --- price history ---
    async def price_add_snapshot(self, nmid: int, basic_u: Optional[int], product_u: Optional[int], ts: Optional[int] = None) -> None:
        ts_i = int(ts or time.time())

        cur = await self.db.execute(
            "SELECT basic_u, product_u FROM price_history WHERE nmid=? ORDER BY ts DESC LIMIT 1",
            (int(nmid),),
        )
        row = await cur.fetchone()
        await cur.close()
        if row:
            last_basic, last_product = row
            if (last_basic == basic_u) and (last_product == product_u):
                return

        await self.db.execute(
            "INSERT INTO price_history(nmid, ts, basic_u, product_u) VALUES(?,?,?,?)",
            (int(nmid), ts_i, basic_u, product_u),
        )
        await self.db.commit()

    async def price_get_history(self, nmid: int, limit: int = 12) -> List[Dict[str, Optional[int]]]:
        cur = await self.db.execute(
            "SELECT ts, basic_u, product_u FROM price_history WHERE nmid=? ORDER BY ts DESC LIMIT ?",
            (int(nmid), int(limit)),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [{"ts": int(ts), "basic_u": basic_u, "product_u": product_u} for (ts, basic_u, product_u) in rows]
