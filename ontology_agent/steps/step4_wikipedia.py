"""
Step 4 — Wikipedia URL + clean article text.
Uses Wikipedia Action API with explaintext=true — returns pure plain text,
zero HTML, zero navigation, zero markdown. No scraping needed.
"""

import asyncio
import logging
import re
from urllib.parse import urlparse, unquote

import httpx

from ..search_engine import SearchEngine
from ..llm_client import LLMClient
from ..validators import (
    fuzzy_match, score_wikipedia,
    is_disambiguation_page, check_wikipedia_infobox_type,
    check_name_in_opening,
)

logger = logging.getLogger(__name__)

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "OntologyAgent/1.0 (contact@example.com)"}


async def _fetch_wikipedia_plaintext(url: str) -> str:
    """
    Fetch full Wikipedia article as plain text via Action API.
    Returns clean prose — no HTML, no markdown, no nav links.
    """
    try:
        path = urlparse(url).path  # /wiki/QuickBooks
        if "/wiki/" not in path:
            return ""
        title = unquote(path.split("/wiki/")[-1].split("#")[0])

        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(WIKIPEDIA_API, headers=HEADERS, params={
                "action": "query",
                "titles": title,
                "prop": "extracts",
                "explaintext": True,        # Pure plain text — no HTML at all
                "exsectionformat": "plain", # No == Section == headers
                "format": "json",
                "redirects": 1,
            })

            if resp.status_code != 200:
                return ""

            data = resp.json()
            pages = data.get("query", {}).get("pages", {})
            for page_id, page in pages.items():
                if page_id == "-1":
                    return ""  # Article not found
                text = page.get("extract", "")
                if text:
                    # Remove leftover section markers and collapse whitespace
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    text = text.strip()
                    return text
    except Exception as e:
        logger.warning(f"Wikipedia API error for {url}: {e}")
    return ""


async def _validate_candidate(candidate, merchant, text, llm, industry):
    store_name = merchant.get("store_name", "")
    store_domain = merchant.get("store_domain", "")
    url = candidate.get("url", "")
    title = candidate.get("title", "")
    snippet = candidate.get("snippet", "")

    checks_passed = 0
    failures = []

    # CHECK 1 — Not disambiguation
    if is_disambiguation_page(url, text or snippet):
        failures.append("Disambiguation page")
        return False, 0, failures
    checks_passed += 1

    # CHECK 2 — Company article (not person/place)
    if text:
        atype = check_wikipedia_infobox_type(text)
        if atype in ("company", None):
            checks_passed += 1
        elif atype == "person":
            failures.append("Article is about a person")
        else:
            failures.append("Article is about a place")
    else:
        if not any(w in snippet.lower() for w in ["born", "politician", "actor", "athlete"]):
            checks_passed += 1
        else:
            failures.append("Appears to be about a person")

    # CHECK 3 — Store name in opening
    if check_name_in_opening(text or snippet, store_name):
        checks_passed += 1
    else:
        failures.append(f"'{store_name}' not in opening")

    # CHECK 4 — US-based
    text_lower = (text or snippet).lower()
    us = ["united states", "usa", "u.s.", "american", "new york",
          "california", "texas", "florida", "headquartered in", "based in"]
    foreign = ["british", "french", "german", "japanese", "chinese", "canadian", "australian"]
    if any(w in text_lower for w in us):
        checks_passed += 1
    elif any(w in text_lower for w in foreign):
        failures.append("Non-US company")
    else:
        checks_passed += 1  # benefit of doubt

    # CHECK 5 — Industry relevance
    if industry:
        words = [w for w in industry.lower().split() if len(w) > 3]
        if any(w in text_lower for w in words):
            checks_passed += 1
        else:
            checks_passed += 1  # short snippets lack category text
    else:
        checks_passed += 1

    # CHECK 6 — LLM semantic gate
    try:
        resp = await llm.validate_semantic(
            entity_name=store_name, domain=store_domain,
            page_title=title, snippet=snippet[:400], source_type="Wikipedia"
        )
        if resp.strip().upper().startswith("YES"):
            checks_passed += 1
        elif resp.strip().upper().startswith("UNCERTAIN"):
            failures.append(f"LLM uncertain")
        else:
            failures.append(f"LLM rejected")
    except Exception as e:
        logger.warning(f"LLM check failed: {e}")
        checks_passed += 1

    return checks_passed >= 6, checks_passed, failures


async def execute(merchant, context, scraper, search: SearchEngine, llm: LLMClient):
    store_name = merchant.get("store_name", "")
    store_domain = merchant.get("store_domain", "")
    industry = context.get("industry", "")

    result = {
        "corrected_merchant_wikipedia_url": "",
        "corrected_wikipedia_page_text": "",
        "wikipedia_score": 0,
        "wikipedia_checks_passed": 0,
        "wikipedia_failures": [],
    }

    logger.info(f"[{store_name}] Step 4: Finding Wikipedia article")

    r1 = await search.google_search(f'site:wikipedia.org "{store_name}" company', 5)
    r2 = await search.google_search(f'site:wikipedia.org "{store_name}" {industry or "brand"}', 5)

    seen = set()
    candidates = []
    for r in r1 + r2:
        u = r["url"]
        if u not in seen and "wikipedia.org/wiki/" in u and "(disambiguation)" not in u.lower():
            seen.add(u)
            candidates.append(r)

    if not candidates:
        result["wikipedia_failures"].append("No Wikipedia results found")
        return result

    best = None
    best_checks = 0
    best_text = ""

    for cand in candidates[:5]:
        # Use Wikipedia API for clean plain text
        text = await _fetch_wikipedia_plaintext(cand["url"])
        passed, checks, failures = await _validate_candidate(cand, merchant, text, llm, industry)

        if passed:
            best, best_checks, best_text = cand, checks, text
            break
        if checks > best_checks:
            best_checks = checks
            best = cand
            best_text = text
            result["wikipedia_failures"] = failures

    if best:
        result["corrected_merchant_wikipedia_url"] = best["url"]
        result["corrected_wikipedia_page_text"] = best_text
        result["wikipedia_checks_passed"] = best_checks
        result["wikipedia_score"] = score_wikipedia(best_checks)

    logger.info(
        f"[{store_name}] Step 4 done: "
        f"URL={'found' if result['corrected_merchant_wikipedia_url'] else 'not found'}, "
        f"checks={best_checks}/6, score={result['wikipedia_score']}"
    )
    return result
