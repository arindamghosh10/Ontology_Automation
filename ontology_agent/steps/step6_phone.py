"""
Step 6 — Extract HQ Phone Number.
FIX: Strict US phone validation rejects product codes, prices, and non-US numbers.
"""

import asyncio
import logging
import re

from ..search_engine import SearchEngine
from ..validators import is_toll_free, score_phone

logger = logging.getLogger(__name__)

HQ_KEYWORDS = [
    "headquarters", "hq", "corporate office", "corporate headquarters",
    "main office", "investor relations", "corporate", "head office",
]

# Strict US phone pattern — requires standard US area codes and format
# Rejects things like (170) xxx-xxxx which are not valid US area codes
_STRICT_US_PHONE = re.compile(
    r'(?<!\d)'                           # Not preceded by digit
    r'(?:\+?1[-.\s]?)?'                  # Optional +1
    r'\(([2-9]\d{2})\)'                  # Area code in parens: must start 2-9
    r'[-.\s]?'
    r'([2-9]\d{2})'                      # Exchange: must start 2-9
    r'[-.\s]?'
    r'(\d{4})'                           # Subscriber
    r'(?!\d)'                            # Not followed by digit
)

# Also match without parens: 800-555-1234
_STRICT_US_PHONE2 = re.compile(
    r'(?<!\d)'
    r'(?:\+?1[-.\s]?)?'
    r'([2-9]\d{2})'
    r'[-.\s]'
    r'([2-9]\d{2})'
    r'[-.\s]'
    r'(\d{4})'
    r'(?!\d)'
)

# Known bad area codes (not assigned to US geographic areas)
_INVALID_AREA_CODES = {
    "000", "100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
    "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
    "120", "130", "140", "150", "160", "170", "180", "190",
}


def _extract_strict_phones(text: str) -> list:
    """Extract only valid-format US phone numbers, rejecting fake/invalid ones."""
    if not text:
        return []

    phones = []
    seen = set()

    for pattern in (_STRICT_US_PHONE, _STRICT_US_PHONE2):
        for m in pattern.finditer(text):
            area = m.group(1)
            exchange = m.group(2)
            subscriber = m.group(3)

            # Reject invalid area codes
            if area in _INVALID_AREA_CODES:
                continue
            # Reject obviously fake patterns (all same digit)
            if len(set(area + exchange + subscriber)) <= 2:
                continue
            # Reject sequential numbers like 123-456-7890
            digits = area + exchange + subscriber
            if digits in ("1234567890", "0987654321"):
                continue

            formatted = f"({area}) {exchange}-{subscriber}"
            if formatted not in seen:
                seen.add(formatted)
                phones.append(formatted)

    return phones


def _extract_phones_near_keywords(text: str, keywords: list, window: int = 300) -> list:
    """Extract phones appearing near HQ-related keywords."""
    if not text:
        return []
    results = []
    text_lower = text.lower()

    for keyword in keywords:
        start = 0
        while True:
            idx = text_lower.find(keyword, start)
            if idx == -1:
                break
            window_start = max(0, idx - window)
            window_end = min(len(text), idx + len(keyword) + window)
            text_window = text[window_start:window_end]
            for phone in _extract_strict_phones(text_window):
                if phone not in [r["phone"] for r in results]:
                    results.append({"phone": phone, "keyword": keyword})
            start = idx + len(keyword)

    return results


async def execute(merchant: dict, context: dict, search: SearchEngine) -> dict:
    store_name = merchant.get("store_name", "")

    result = {
        "other_phone_numbers": "",
        "phone_score": 0,
    }

    logger.info(f"[{store_name}] Step 6: Extracting phone numbers")

    all_phones = {}  # phone -> {sources, is_hq, is_toll_free}

    def _add_phone(phone, source, is_hq=False):
        if phone not in all_phones:
            all_phones[phone] = {"sources": [], "is_hq": False, "is_toll_free": is_toll_free(phone)}
        if source not in all_phones[phone]["sources"]:
            all_phones[phone]["sources"].append(source)
        if is_hq:
            all_phones[phone]["is_hq"] = True

    # Source 1: Website — HQ keyword search
    website_text = context.get("corrected_website_text", "")
    for p in _extract_phones_near_keywords(website_text, HQ_KEYWORDS):
        _add_phone(p["phone"], "Website", is_hq=True)
    for phone in _extract_strict_phones(website_text):
        _add_phone(phone, "Website")

    # Source 2: D&B text — generally reliable HQ info
    dnb_text = context.get("corrected_dnb_page_text", "")
    for phone in _extract_strict_phones(dnb_text):
        _add_phone(phone, "DNB", is_hq=True)

    # Source 3: ZoomInfo text
    zi_text = context.get("corrected_zoominfo_page_text", "")
    for phone in _extract_strict_phones(zi_text):
        _add_phone(phone, "ZoomInfo", is_hq=True)

    # Source 4: Web search
    try:
        query = f'"{store_name}" corporate headquarters phone number'
        search_results = await search.google_search(query, num_results=3)
        for sr in search_results:
            for phone in _extract_strict_phones(sr.get("snippet", "")):
                _add_phone(phone, "Web", is_hq=True)
    except Exception as e:
        logger.warning(f"[{store_name}] Phone search error: {e}")

    if not all_phones:
        result["phone_score"] = score_phone(hq_found=False, toll_free_only=False)
        logger.info(f"[{store_name}] Step 6 done: No valid phone numbers found")
        return result

    # Format — HQ non-toll-free first, then toll-free, then others
    # Limit to top 5 to avoid noise
    sorted_phones = sorted(
        all_phones.items(),
        key=lambda x: (-x[1]["is_hq"], x[1]["is_toll_free"], x[0])
    )[:5]

    formatted = []
    for phone, info in sorted_phones:
        source_str = "/".join(info["sources"])
        if info["is_hq"] and not info["is_toll_free"]:
            label = " [HQ]"
        elif info["is_toll_free"]:
            label = " [Toll-Free]"
        else:
            label = ""
        formatted.append(f"{phone}{label} [Source: {source_str}]")

    result["other_phone_numbers"] = ", ".join(formatted)

    has_hq = any(info["is_hq"] and not info["is_toll_free"] for info in all_phones.values())
    has_toll_free_only = not has_hq and any(info["is_toll_free"] for info in all_phones.values())
    result["phone_score"] = score_phone(hq_found=has_hq, toll_free_only=has_toll_free_only)

    logger.info(f"[{store_name}] Step 6 done: {len(all_phones)} valid numbers, score={result['phone_score']}")
    return result
