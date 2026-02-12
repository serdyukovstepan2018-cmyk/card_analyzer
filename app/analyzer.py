from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import Counter

RU_STOP = set(
    "и в во не что он на я с со как а то все она так его но да ты к у же вы за бы по "
    "только ее мне было вот от меня еще нет о из ему теперь когда даже ну вдруг ли если "
    "уже или ни быть был него до вас нибудь опять уж вам ведь там потом себя ничего ей "
    "может они тут где есть надо ней для мы тебя их чем была сам чтоб без будто чего раз "
    "тоже себе под будет ж тогда кто этот того потому этого какой совсем ним здесь этом "
    "один почти мой тем чтобы нее сейчас были куда зачем всех никогда можно при наконец два "
    "об другой"
    .split()
)

NEG_WORDS = set("плох ужас отврат не работает слом сломал сломалась брак возврат не советую разочар не подошел дешев хлипк воняет запах".split())
POS_WORDS = set("отлич супер класс понравилось рекомендую качеств хороший красив удобн".split())

AGE_PATTERN = re.compile(r"через\s+(\d+)\s*(дн\w*|недел\w*|мес\w*|месяц\w*)", re.I)

@dataclass
class Review:
    rating: Optional[int]
    text: str
    created: Optional[datetime] = None


def _to_datetime(x: Any) -> Optional[datetime]:
    if not x:
        return None
    if isinstance(x, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(x[:len(fmt)], fmt)
            except Exception:
                continue
    return None


def extract_reviews(feedback_json: Dict[str, Any]) -> List[Review]:
    candidates = []
    if isinstance(feedback_json.get("feedbacks"), list):
        candidates = feedback_json["feedbacks"]
    elif isinstance(feedback_json.get("data", {}).get("feedbacks"), list):
        candidates = feedback_json["data"]["feedbacks"]
    elif isinstance(feedback_json.get("feedbacksWithText"), list):
        candidates = feedback_json["feedbacksWithText"]
    elif isinstance(feedback_json.get("data", {}).get("feedbacksWithText"), list):
        candidates = feedback_json["data"]["feedbacksWithText"]

    reviews: List[Review] = []
    for it in candidates[:4000]:
        if not isinstance(it, dict):
            continue
        rating = it.get("productValuation") or it.get("valuation") or it.get("rating") or it.get("stars")
        try:
            rating_i = int(rating) if rating is not None else None
        except Exception:
            rating_i = None

        text_parts = []
        for k in ("text", "review", "comment", "pros", "cons"):
            v = it.get(k)
            if isinstance(v, str) and v.strip():
                text_parts.append(v.strip())
        text = "\n".join(text_parts).strip()
        if not text:
            continue

        created = _to_datetime(it.get("createdDate") or it.get("created") or it.get("date"))
        reviews.append(Review(rating=rating_i, text=text, created=created))
    return reviews


def tokenize(text: str) -> List[str]:
    text = text.lower()
    text = re.sub(r"[^a-zа-я0-9\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    out = []
    for w in text.split():
        w = w.strip("-")
        if len(w) <= 2:
            continue
        if w in RU_STOP:
            continue
        out.append(w)
    return out


def shingles(tokens: List[str], k: int = 3) -> Set[str]:
    if len(tokens) < k:
        return set(tokens)
    return {" ".join(tokens[i:i+k]) for i in range(len(tokens)-k+1)}


def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _has_neg(text: str) -> bool:
    toks = set(tokenize(text))
    return any(w in toks for w in NEG_WORDS)


def _has_pos(text: str) -> bool:
    toks = set(tokenize(text))
    return any(w in toks for w in POS_WORDS)


def trust_score_details(reviews: List[Review]) -> Tuple[int, List[str], Dict[str, float], Dict[str, int]]:
    """Return: (score 0..100, reasons, signals, penalties_by_factor)."""
    reasons: List[str] = []
    penalties: Dict[str, int] = {}
    if not reviews:
        return 50, ["Нет отзывов с текстом — оценивать нечего."], {}, {"no_reviews": 0}

    n = len(reviews)
    texts = [r.text for r in reviews]
    tokens_list = [tokenize(t) for t in texts]
    sh_list = [shingles(toks, 3) for toks in tokens_list]

    # exact duplicates
    exact = Counter(re.sub(r"\s+", " ", t.lower()).strip() for t in texts)
    exact_dup_ratio = sum(1 for v in exact.values() if v >= 2) / max(1, len(exact))

    # near duplicates (bounded sample)
    near_pairs = 0
    total_pairs = 0
    max_n = min(n, 450)
    for i in range(max_n):
        for j in range(i + 1, max_n):
            total_pairs += 1
            if jaccard(sh_list[i], sh_list[j]) >= 0.8:
                near_pairs += 1
    near_dup_ratio = near_pairs / max(1, total_pairs)

    dup_pen = int(min(40, 40 * (0.7 * near_dup_ratio + 0.3 * exact_dup_ratio)))
    penalties["duplicates"] = dup_pen

    # time spike
    dates = [r.created.date() for r in reviews if r.created]
    spike_share = 0.0
    if dates:
        c = Counter(dates)
        spike_share = max(c.values()) / len(dates)
    spike_pen = int(min(20, 20 * spike_share))
    penalties["time_spike"] = spike_pen

    # mismatch
    mismatch = 0
    rated = 0
    for r in reviews:
        if r.rating is None:
            continue
        rated += 1
        has_neg = _has_neg(r.text)
        has_pos = _has_pos(r.text)
        if r.rating >= 4 and has_neg:
            mismatch += 1
        elif r.rating <= 2 and has_pos:
            mismatch += 1
    mismatch_ratio = mismatch / max(1, rated)
    mismatch_pen = int(min(20, 20 * mismatch_ratio))
    penalties["mismatch"] = mismatch_pen

    # short
    short = sum(1 for toks in tokens_list if len(toks) <= 3)
    short_ratio = short / n
    short_pen = int(min(20, 20 * short_ratio))
    penalties["too_short"] = short_pen

    score = 100 - (dup_pen + spike_pen + mismatch_pen + short_pen)
    score = max(0, min(100, score))

    if near_dup_ratio > 0.08 or exact_dup_ratio > 0.12:
        reasons.append("Много однотипных/похожих отзывов (шаблоны/дубли).")
    if spike_share > 0.35:
        reasons.append("Есть заметный всплеск отзывов в один день (аномалия по времени).")
    if mismatch_ratio > 0.10:
        reasons.append("Есть отзывы вида '5★, но текст ругается' (несостыковка тональности).")
    if short_ratio > 0.35:
        reasons.append("Много очень коротких отзывов без деталей.")
    if not reasons:
        reasons.append("Явных красных флагов по текстам не видно (по простым эвристикам).")

    signals = {
        "near_dup_ratio": float(near_dup_ratio),
        "exact_dup_ratio": float(exact_dup_ratio),
        "spike_share": float(spike_share),
        "mismatch_ratio": float(mismatch_ratio),
        "short_ratio": float(short_ratio),
        "sampled_reviews_for_similarity": float(max_n),
        "rated_text_reviews": float(rated),
    }
    return score, reasons, signals, penalties


def detect_suspicious_reviews(reviews: List[Review]) -> Tuple[Set[int], Dict[str, int]]:
    """Indexes to drop + counts by reason.

    This is a heuristic filter used ONLY to compute a 'clean rating' on text reviews.
    """
    drop: Set[int] = set()
    counts = {"exact_duplicate": 0, "near_duplicate": 0, "too_short": 0, "mismatch": 0}

    # short / mismatch first
    for i, r in enumerate(reviews):
        toks = tokenize(r.text)
        if len(toks) <= 3:
            drop.add(i)
            counts["too_short"] += 1
            continue
        if r.rating is not None:
            if r.rating >= 4 and _has_neg(r.text):
                drop.add(i)
                counts["mismatch"] += 1
            elif r.rating <= 2 and _has_pos(r.text):
                drop.add(i)
                counts["mismatch"] += 1

    # exact duplicates: keep first, drop rest
    norm_map: Dict[str, List[int]] = {}
    for i, r in enumerate(reviews):
        norm = re.sub(r"\s+", " ", r.text.lower()).strip()
        norm_map.setdefault(norm, []).append(i)
    for idxs in norm_map.values():
        if len(idxs) >= 2:
            kept = None
            for i in idxs:
                if i not in drop:
                    kept = i
                    break
            for i in idxs:
                if i != kept:
                    if i not in drop:
                        counts["exact_duplicate"] += 1
                    drop.add(i)

    # near duplicates (bounded). cluster by similarity and keep one representative
    n = len(reviews)
    max_n = min(n, 450)
    tokens_list = [tokenize(reviews[i].text) for i in range(max_n)]
    sh_list = [shingles(toks, 3) for toks in tokens_list]

    rep = list(range(max_n))

    def find(x: int) -> int:
        while rep[x] != x:
            rep[x] = rep[rep[x]]
            x = rep[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            rep[rb] = ra

    for i in range(max_n):
        for j in range(i + 1, max_n):
            if jaccard(sh_list[i], sh_list[j]) >= 0.8:
                union(i, j)

    groups: Dict[int, List[int]] = {}
    for i in range(max_n):
        groups.setdefault(find(i), []).append(i)

    for g in groups.values():
        if len(g) < 3:
            continue
        kept = None
        for i in g:
            if i not in drop:
                kept = i
                break
        for i in g:
            if i != kept:
                if i not in drop:
                    counts["near_duplicate"] += 1
                drop.add(i)

    return drop, counts


def clean_rating(reviews: List[Review], drop_idx: Set[int]) -> Dict[str, Any]:
    kept = [r for i, r in enumerate(reviews) if i not in drop_idx and r.rating is not None]
    if not kept:
        return {"count": 0, "avg": None}
    avg = sum(r.rating for r in kept if r.rating is not None) / len(kept)
    return {"count": len(kept), "avg": round(avg, 2)}


def summarize_stub(reviews: List[Review]) -> Dict[str, Any]:
    """We intentionally disable pros/cons until we add AI."""
    age_hits: List[str] = []
    neg = [r for r in reviews if (r.rating or 0) <= 2]
    for r in neg:
        m = AGE_PATTERN.search(r.text)
        if m:
            frag = r.text.strip().replace("\n", " ")
            age_hits.append(f"{m.group(0)} — «{frag[:120]}…»" if len(frag) > 120 else f"{m.group(0)} — «{frag}»")
        if len(age_hits) >= 3:
            break
    return {"age_failures": age_hits}
