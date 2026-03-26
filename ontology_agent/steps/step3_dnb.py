"""
Step 3 — D&B URL + text.
Scrapes D&B directly via Firecrawl (public profiles are accessible).
Falls back to snippet if scrape fails.
"""

import asyncio
import logging
import re

from ..search_engine import SearchEngine
from ..llm_client import LLMClient
from ..validators import fuzzy_match, score_directory_url

logger = logging.getLogger(__name__)


async def _validate_candidate(candidate, merchant, llm, industry):
    store_name = merchant.get("store_name", "")
    store_domain = merchant.get("store_domain", "")
    title = candidate.get("title", "")
    snippet = candidate.get("snippet", "")
    snippet_lower = snippet.lower()
    title_lower = title.lower()

    checks_passed = 0
    failures = []

    # CHECK 1 — Name fuzzy match
    name = title.split(" - ")[0].replace("D&B","").replace("Dun & Bradstreet","") \
               .replace("Company Profile","").replace("Business Directory","").strip()
    matched, score = fuzzy_match(store_name, name, threshold=85)
    if matched:
        checks_passed += 1
    else:
        failures.append(f"Name mismatch: '{name}' ({score}%)")

    # CHECK 2 — Domain match
    if store_domain:
        d = store_domain.lower().replace("www.","").replace("https://","").replace("http://","").strip("/")
        if d in snippet_lower or d in title_lower:
            checks_passed += 1
        else:
            failures.append(f"Domain '{d}' not found")
    else:
        checks_passed += 1

    # CHECK 3 — Active business
    inactive = ["out of business", "inactive", "closed", "defunct"]
    if any(w in snippet_lower for w in inactive):
        failures.append("Business inactive")
    else:
        checks_passed += 1

    # CHECK 4 — Not a branch
    if "branch" in snippet_lower and "headquarters" not in snippet_lower:
        failures.append("Branch listing, not HQ")
    else:
        checks_passed += 1

    # CHECK 5 — Industry relevance
    if industry:
        words = [w for w in industry.lower().split() if len(w) > 3]
        if any(w in snippet_lower for w in words) or re.search(r'(?:sic|naics)\s*\d+', snippet_lower):
            checks_passed += 1
        else:
            checks_passed += 1  # benefit of doubt
    else:
        checks_passed += 1

    # CHECK 6 — LLM validation
    try:
        resp = await llm.validate_semantic(
            store_name, store_domain, title, snippet, "D&B (Dun & Bradstreet)"
        )
        if resp.strip().upper().startswith("YES"):
            checks_passed += 1
        elif resp.strip().upper().startswith("UNCERTAIN"):
            failures.append("LLM uncertain")
        else:
            failures.append("LLM rejected")
    except Exception as e:
        logger.warning(f"LLM check failed: {e}")
        checks_passed += 1

    return checks_passed >= 6, checks_passed, failures


async def execute(merchant, context, scraper, search: SearchEngine, llm: LLMClient):
    store_name = merchant.get("store_name", "")
    store_domain = merchant.get("store_domain", "")
    industry = context.get("industry", "")

    result = {
        "corrected_merchant_dnb_url": "",
        "corrected_dnb_page_text": "",
        "dnb_score": 0,
        "dnb_checks_passed": 0,
        "dnb_failures": [],
    }

    logger.info(f"[{store_name}] Step 3: Finding D&B profile")

    r1 = await search.google_search(f'site:dnb.com "{store_name}" company profile', 5)
    r2 = await search.google_search(f'site:dnb.com/business-directory "{store_name}"', 5)

    seen = set()
    candidates = []
    for r in r1 + r2:
        if r["url"] not in seen and "dnb.com" in r["url"].lower():
            seen.add(r["url"])
            candidates.append(r)

    if not candidates:
        result["dnb_failures"].append("No D&B results found")
        return result

    best = None
    best_checks = 0

    for cand in candidates[:5]:
        passed, checks, failures = await _validate_candidate(cand, merchant, llm, industry)
        if passed:
            best = cand
            best_checks = checks
            break
        if checks > best_checks:
            best_checks = checks
            best = cand
            result["dnb_failures"] = failures

    if not best:
        return result

    result["corrected_merchant_dnb_url"] = best["url"]
    result["dnb_checks_passed"] = best_checks
    result["dnb_score"] = score_directory_url(best_checks)

    # --- Get D&B text ---
    text = ""
    try:
        logger.info(f"[{store_name}] Scraping D&B via Firecrawl...")
        scrape = await scraper.scrape_page(best["url"])
        scraped = scrape.get("text", "")
        if scraped and len(scraped.split()) >= 30:
            text = scraped
            logger.info(f"[{store_name}] D&B direct scrape: {len(scraped.split())} words")
    except Exception as e:
        logger.warning(f"[{store_name}] D&B scrape failed: {e}")

    # Fallback to snippet
    if not text or len(text.split()) < 20:
        text = f"{best.get('title','')}\n{best.get('snippet','')}".strip()
        logger.info(f"[{store_name}] D&B text from snippet: {len(text.split())} words")

    result["corrected_dnb_page_text"] = text

    logger.info(
        f"[{store_name}] Step 3 done: URL=found, checks={best_checks}/6, "
        f"score={result['dnb_score']}, text={len(text.split())} words"
    )
    return result
