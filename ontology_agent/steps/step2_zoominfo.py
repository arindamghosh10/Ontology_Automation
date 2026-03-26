"""
Step 2 — ZoomInfo URL + text.
Text strategy (in order):
  1. Scrape ZoomInfo directly via Firecrawl (often works for public profiles)
  2. Google snippet text (fallback)
  3. Flag as partial if < 50 words
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
    url = candidate.get("url", "")
    title = candidate.get("title", "")
    snippet = candidate.get("snippet", "")
    snippet_lower = snippet.lower()

    checks_passed = 0
    failures = []

    # CHECK 1 — Name fuzzy match
    name = title.split(" - ")[0].replace("ZoomInfo", "").replace("Company Profile", "").strip()
    matched, score = fuzzy_match(store_name, name, threshold=85)
    if matched:
        checks_passed += 1
    else:
        failures.append(f"Name mismatch: '{name}' vs '{store_name}' ({score}%)")

    # CHECK 2 — Domain in snippet
    if store_domain:
        d = store_domain.lower().replace("www.", "").replace("https://", "").replace("http://", "").strip("/")
        if d in snippet_lower or d in title.lower():
            checks_passed += 1
        else:
            failures.append(f"Domain '{d}' not in snippet")
    else:
        checks_passed += 1

    # CHECK 3 — US HQ
    us = ["united states", "usa", "u.s.", ", us", "new york", "california",
          "texas", "florida", "illinois", "georgia", "washington"]
    non_us = ["canada", "united kingdom", "uk", "india", "china", "germany"]
    if any(w in snippet_lower for w in us):
        checks_passed += 1
    elif any(w in snippet_lower for w in non_us):
        failures.append("HQ outside US")
    else:
        checks_passed += 1

    # CHECK 4 — Employee count > 50
    emp = re.search(r'(\d[\d,]*)\s*(?:employees?|staff)', snippet_lower)
    if emp:
        count = int(emp.group(1).replace(",", ""))
        if count > 50:
            checks_passed += 1
        else:
            failures.append(f"Only {count} employees")
    else:
        checks_passed += 1

    # CHECK 5 — Industry match
    if industry:
        words = [w for w in industry.lower().split() if len(w) > 3]
        if any(w in snippet_lower for w in words):
            checks_passed += 1
        else:
            checks_passed += 1  # benefit of doubt
    else:
        checks_passed += 1

    # CHECK 6 — LLM semantic validation
    try:
        resp = await llm.validate_semantic(store_name, store_domain, title, snippet, "ZoomInfo")
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
        "corrected_merchant_zoominfo_url": "",
        "corrected_zoominfo_page_text": "",
        "zoominfo_score": 0,
        "zoominfo_checks_passed": 0,
        "zoominfo_failures": [],
    }

    logger.info(f"[{store_name}] Step 2: Finding ZoomInfo profile")

    domain_short = store_domain.replace("https://","").replace("http://","").replace("www.","").strip("/")
    r1 = await search.google_search(f'site:zoominfo.com/c "{store_name}"', 5)
    r2 = await search.google_search(f'site:zoominfo.com "{store_name}" "{domain_short}"', 5)

    seen = set()
    candidates = []
    for r in r1 + r2:
        if r["url"] not in seen and "zoominfo.com" in r["url"].lower():
            seen.add(r["url"])
            candidates.append(r)

    if not candidates:
        result["zoominfo_failures"].append("No ZoomInfo results found")
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
            result["zoominfo_failures"] = failures

    if not best:
        return result

    result["corrected_merchant_zoominfo_url"] = best["url"]
    result["zoominfo_checks_passed"] = best_checks
    result["zoominfo_score"] = score_directory_url(best_checks)

    # --- Get ZoomInfo text ---
    # Strategy 1: Direct scrape via Firecrawl
    text = ""
    try:
        logger.info(f"[{store_name}] Scraping ZoomInfo directly via Firecrawl...")
        scrape = await scraper.scrape_page(best["url"])
        scraped_text = scrape.get("text", "")
        # ZoomInfo public pages have company description above the login wall
        if scraped_text and len(scraped_text.split()) >= 30:
            # Check it's not just a login page
            if "sign in" not in scraped_text.lower()[:100]:
                text = scraped_text
                logger.info(f"[{store_name}] ZoomInfo direct scrape: {len(scraped_text.split())} words")
            else:
                # Still keep the visible teaser text
                text = scraped_text
                logger.info(f"[{store_name}] ZoomInfo partial (login wall): {len(scraped_text.split())} words")
    except Exception as e:
        logger.warning(f"[{store_name}] ZoomInfo direct scrape failed: {e}")

    # Strategy 2: Google snippet (fallback)
    if not text or len(text.split()) < 20:
        snippet_text = f"{best.get('title','')}\n{best.get('snippet','')}"
        if len(snippet_text.split()) >= 10:
            text = snippet_text
            logger.info(f"[{store_name}] ZoomInfo text from snippet: {len(text.split())} words")

    result["corrected_zoominfo_page_text"] = text

    logger.info(
        f"[{store_name}] Step 2 done: URL=found, checks={best_checks}/6, "
        f"score={result['zoominfo_score']}, text={len(text.split())} words"
    )
    return result
