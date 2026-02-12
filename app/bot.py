from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple, List

from aiogram import Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from .analyzer import (
    extract_reviews,
    trust_score_details,
    detect_suspicious_reviews,
    clean_rating,
    summarize_stub,
)
from .config import Settings
from .storage import Storage
from .wb_client import WBClient, extract_nmid


def _fmt_money(value_u: Optional[int]) -> str:
    if value_u is None:
        return "—"
    rub = value_u / 100
    if rub.is_integer():
        return f"{int(rub)} ₽"
    return f"{rub:.2f} ₽"


def _traffic_light(score: int) -> str:
    if score < 50:
        return "🔴"
    if score < 75:
        return "🟡"
    return "🟢"


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


async def analyze_one(nmid: int, settings: Settings, storage: Storage, wb: WBClient) -> Dict[str, Any]:
    # card
    card_key = f"card:{nmid}:{settings.wb_dest}:{settings.wb_locale}"
    product = await storage.cache_get(card_key)
    if not product:
        product = await wb.get_product(nmid)
        await storage.cache_set(card_key, product, ttl_seconds=settings.card_ttl_seconds)

    root_id = int(product.get("root") or nmid)

    # price snapshot (we build history ourselves)
    basic_u, product_u = WBClient.parse_price(product)
    await storage.price_add_snapshot(nmid=nmid, basic_u=basic_u, product_u=product_u)
    price_hist = await storage.price_get_history(nmid=nmid, limit=12)

    # feedbacks
    fb_key = f"fb:{root_id}:limit={settings.reviews_limit}"
    feedback_json = await storage.cache_get(fb_key)
    if not feedback_json:
        feedback_json = await wb.get_feedbacks(root_id=root_id, limit=settings.reviews_limit)
        await storage.cache_set(fb_key, feedback_json, ttl_seconds=settings.reviews_ttl_seconds)

    reviews = extract_reviews(feedback_json)

    score, reasons, signals, penalties = trust_score_details(reviews)

    drop_idx, drop_counts = detect_suspicious_reviews(reviews)
    clean = clean_rating(reviews, drop_idx=drop_idx)

    summ = summarize_stub(reviews)

    return {
        "nmid": nmid,
        "root_id": root_id,
        "product": product,
        "reviews_count": len(reviews),
        "trust_score": score,
        "reasons": reasons,
        "signals": signals,
        "penalties": penalties,
        "clean_rating": clean,
        "drop_counts": drop_counts,
        "summary": summ,
        "price": {"basic_u": basic_u, "product_u": product_u},
        "price_history": price_hist,
    }


def build_message(result: Dict[str, Any], original_url: Optional[str] = None) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    product = result["product"]
    nmid = result["nmid"]
    name = product.get("name") or "Товар"
    brand = product.get("brand") or ""
    rating_raw = product.get("rating") or product.get("reviewRating")
    try:
        rating = f"{float(rating_raw):.1f}"
    except Exception:
        rating = str(rating_raw) if rating_raw is not None else "—"

    fb_cnt = product.get("feedbacks") or product.get("nmFeedbacks") or "—"

    score = int(result["trust_score"])
    tl = _traffic_light(score)
    penalties = result.get("penalties") or {}
    signals = result.get("signals") or {}

    basic_u = (result.get("price") or {}).get("basic_u")
    product_u = (result.get("price") or {}).get("product_u")

    lines: List[str] = []
    lines.append(f"<b>{name}</b>")
    if brand:
        lines.append(f"Бренд: <b>{brand}</b>")
    lines.append(f"Артикул (nmId): <code>{nmid}</code>")
    lines.append(f"Рейтинг WB: <b>{rating}</b> • отзывов: <b>{fb_cnt}</b> • текстовых взято: <b>{result['reviews_count']}</b>")

    # show price numbers but not "discount verdict"
    if product_u is not None:
        if basic_u is not None and basic_u != product_u:
            lines.append(f"Цена сейчас (WB): <b>{_fmt_money(product_u)}</b> • basic: {_fmt_money(basic_u)}")
        else:
            lines.append(f"Цена сейчас (WB): <b>{_fmt_money(product_u)}</b>")

    # history we collected
    hist = result.get("price_history") or []
    if hist:
        lines.append("")
        lines.append("<b>История цены (бот собирает сам):</b>")
        for row in reversed(hist):  # oldest -> newest
            ts = _fmt_ts(int(row["ts"]))
            bu = row.get("basic_u")
            pu = row.get("product_u")
            if pu is None:
                continue
            if bu is not None and bu != pu:
                lines.append(f"• {ts}: {_fmt_money(pu)} (basic {_fmt_money(bu)})")
            else:
                lines.append(f"• {ts}: {_fmt_money(pu)}")

    # trust score + breakdown
    lines.append("")
    lines.append(f"{tl} <b>Trust Score:</b> <b>{score}/100</b>")
    lines.append("<b>Снятые очки (эвристики):</b>")
    lines.append(f"• Дубли/однотипность: -{int(penalties.get('duplicates', 0))} (near={signals.get('near_dup_ratio', 0):.3f}, exact={signals.get('exact_dup_ratio', 0):.3f})")
    lines.append(f"• Всплеск по времени: -{int(penalties.get('time_spike', 0))} (spike_share={signals.get('spike_share', 0):.3f})")
    lines.append(f"• Несостыковка тональности: -{int(penalties.get('mismatch', 0))} (mismatch_ratio={signals.get('mismatch_ratio', 0):.3f})")
    lines.append(f"• Слишком короткие: -{int(penalties.get('too_short', 0))} (short_ratio={signals.get('short_ratio', 0):.3f})")

    lines.append("")
    for r in result["reasons"][:6]:
        lines.append(f"• {r}")

    # clean rating (from text reviews)
    clean = result.get("clean_rating") or {}
    drop_counts = result.get("drop_counts") or {}
    lines.append("")
    lines.append("<b>Итоговый рейтинг товара (по очищенным текстовым отзывам):</b>")
    if clean.get("avg") is not None:
        lines.append(f"• Средняя оценка: <b>{clean['avg']}/5</b> (n={clean.get('count', 0)})")
    else:
        lines.append("• Не смог посчитать (нет оценок в оставшихся текстовых отзывах).")
    lines.append(
        "• Отброшено как 'подозрительное': "
        f"short={drop_counts.get('too_short', 0)}, mismatch={drop_counts.get('mismatch', 0)}, "
        f"exact_dup={drop_counts.get('exact_duplicate', 0)}, near_dup={drop_counts.get('near_duplicate', 0)}"
    )

    # keep useful age failures
    summ = result.get("summary") or {}
    if summ.get("age_failures"):
        lines.append("")
        lines.append("<b>Жалобы по сроку службы:</b>")
        for x in summ["age_failures"]:
            lines.append(f"• {x}")

    kb = None
    if original_url and "wildberries.ru" in original_url:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Открыть товар на WB", url=original_url)]])
    return "\n".join(lines), kb


def setup_handlers(dp: Dispatcher, settings: Settings, storage: Storage, wb: WBClient) -> None:
    @dp.message(CommandStart())
    async def start(m: Message) -> None:
        txt = (
            "Пришли ссылку на товар Wildberries или артикул (nmId).\n\n"
            "Пример: https://www.wildberries.ru/catalog/98892471/detail.aspx\n"
            "Или просто: 98892471"
        )
        await m.answer(txt)

    @dp.message(F.text)
    async def on_text(m: Message) -> None:
        user_id = m.from_user.id if m.from_user else 0
        ok = await storage.rate_limit_allow(
            user_id=user_id,
            window_seconds=settings.rate_limit_window_seconds,
            max_requests=settings.rate_limit_max_requests,
        )
        if not ok:
            await m.answer("Слишком часто 🙂 Попробуй ещё раз чуть позже.")
            return

        text = (m.text or "").strip()
        nmid = extract_nmid(text)
        if not nmid:
            await m.answer("Не вижу артикул WB. Пришли ссылку на товар или nmId цифрами.")
            return

        await m.answer("Секунду… анализирую отзывы и обновляю историю цены 👀")

        try:
            res = await analyze_one(nmid=nmid, settings=settings, storage=storage, wb=wb)
        except Exception as e:
            await m.answer(f"Не получилось получить данные WB: {e}")
            return

        msg, kb = build_message(res, original_url=text)
        await m.answer(msg, parse_mode=ParseMode.HTML, reply_markup=kb)
