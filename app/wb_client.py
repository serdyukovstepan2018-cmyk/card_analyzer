from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

NMID_RE = re.compile(r"(?:/catalog/|nm=)(\d{6,12})")

def extract_nmid(text: str) -> Optional[int]:
    text = text.strip()
    if text.isdigit():
        return int(text)
    m = NMID_RE.search(text)
    if not m:
        return None
    return int(m.group(1))

class WBClient:
    # Неофициальный публичный эндпойнт карточки товара (может меняться)
    CARD_URL = "https://card.wb.ru/cards/v4/detail"

    # Неофициальный публичный эндпойнт отзывов (может меняться)
    FEEDBACK_HOSTS = [
        "https://feedbacks1.wb.ru",
        "https://feedbacks2.wb.ru",
    ]

    def __init__(self, dest: str, locale: str = "ru", timeout_s: float = 12.0):
        self.dest = dest
        self.locale = locale
        self.client = httpx.AsyncClient(
            timeout=timeout_s,
            headers={
                "User-Agent": "Mozilla/5.0 (AntiFakeBot/1.0)",
                "Accept": "application/json, text/plain, */*",
            },
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def get_product(self, nmid: int) -> Dict[str, Any]:
        params = {"dest": self.dest, "locale": self.locale, "nm": str(nmid)}
        r = await self.client.get(self.CARD_URL, params=params)
        r.raise_for_status()
        data = r.json()
        products = data.get("products") or data.get("data", {}).get("products") or []
        if not products:
            raise ValueError("WB: product not found")
        return products[0]

    async def get_feedbacks(self, root_id: int, limit: int = 120) -> Dict[str, Any]:
        candidates: List[Tuple[str, Dict[str, str]]] = [
            (f"/feedbacks/v1/{root_id}", {"take": str(limit), "skip": "0"}),
            (f"/feedbacks/v1/{root_id}", {"limit": str(limit), "offset": "0"}),
            (f"/feedbacks/v1/{root_id}", {}),
        ]

        last_exc: Optional[Exception] = None
        for host in self.FEEDBACK_HOSTS:
            for path, params in candidates:
                url = host + path
                try:
                    r = await self.client.get(url, params=params)
                    if r.status_code != 200:
                        continue
                    return r.json()
                except Exception as e:
                    last_exc = e
                    continue

        raise RuntimeError(f"WB: cannot fetch feedbacks for root={root_id}: {last_exc}")

    @staticmethod
    def parse_price(product: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
        sizes = product.get("sizes") or []
        for s in sizes:
            price = (s.get("price") or {})
            basic = price.get("basic")
            product_price = price.get("product")
            if basic is not None and product_price is not None:
                return int(basic), int(product_price)
        return None, None

    @staticmethod
    def total_stock(product: Dict[str, Any]) -> Optional[int]:
        tq = product.get("totalQuantity")
        if isinstance(tq, int):
            return tq
        total = 0
        found = False
        for s in product.get("sizes") or []:
            for st in s.get("stocks") or []:
                qty = st.get("qty")
                if isinstance(qty, int):
                    total += qty
                    found = True
        return total if found else None
