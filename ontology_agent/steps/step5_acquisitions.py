"""
Step 5 — Extract Acquisitions.
Find all brands/companies acquired by this merchant using scraped texts and web search.
"""

import asyncio
import logging
import re

from ..search_engine import SearchEngine
from ..llm_client import LLMClient
from ..validators import score_acquisitions

logger = logging.getLogger(__name__)

# Keywords that indicate acquisition activity
ACQUISITION_KEYWORDS = [
    "acquired", "acquisition", "purchased", "merger",
    "merged with", "bought", "takeover", "take over",
    "acquiring", "acquires",
]


def _find_acquisition_mentions(text: str) -> list[str]:
    """Extract sentences containing acquisition keywords from text."""
    if not text:
        return []

    sentences = re.split(r'[.!?\n]', text)
    mentions = []
    for sentence in sentences:
        sentence_lower = sentence.lower()
        if any(kw in sentence_lower for kw in ACQUISITION_KEYWORDS):
            sentence = sentence.strip()
            if len(sentence) > 20:  # Skip very short fragments
                mentions.append(sentence)
    return mentions


async def execute(merchant: dict, context: dict,
                  search: SearchEngine, llm: LLMClient) -> dict:
    """
    Extract acquisitions from scraped texts and web search.

    Returns:
        {
            "acquisitions": str (comma-separated list or NONE),
            "acquisitions_score": int (0-15),
            "acquisitions_sources": int (number of sources confirming),
        }
    """
    store_name = merchant.get("store_name", "")

    result = {
        "acquisitions": "",
        "acquisitions_score": 0,
        "acquisitions_sources": 0,
    }

    logger.info(f"[{store_name}] Step 5: Extracting acquisitions")

    # Collect acquisition mentions from all scraped sources
    all_mentions = []
    source_mentions = {}

    # Source 1: Website text
    website_text = context.get("corrected_website_text", "")
    website_mentions = _find_acquisition_mentions(website_text)
    if website_mentions:
        all_mentions.extend(website_mentions)
        source_mentions["website"] = website_mentions

    # Source 2: ZoomInfo text
    zoominfo_text = context.get("corrected_zoominfo_page_text", "")
    zoominfo_mentions = _find_acquisition_mentions(zoominfo_text)
    if zoominfo_mentions:
        all_mentions.extend(zoominfo_mentions)
        source_mentions["zoominfo"] = zoominfo_mentions

    # Source 3: Wikipedia text
    wikipedia_text = context.get("corrected_wikipedia_page_text", "")
    wiki_mentions = _find_acquisition_mentions(wikipedia_text)
    if wiki_mentions:
        all_mentions.extend(wiki_mentions)
        source_mentions["wikipedia"] = wiki_mentions

    # Source 4: D&B text
    dnb_text = context.get("corrected_dnb_page_text", "")
    dnb_mentions = _find_acquisition_mentions(dnb_text)
    if dnb_mentions:
        all_mentions.extend(dnb_mentions)
        source_mentions["dnb"] = dnb_mentions

    # Source 5: Web search
    try:
        query1 = f'"{store_name}" acquisitions history'
        search_results1 = await search.google_search(query1, num_results=3)
        for sr in search_results1:
            snippet_mentions = _find_acquisition_mentions(sr.get("snippet", ""))
            if snippet_mentions:
                all_mentions.extend(snippet_mentions)
                source_mentions["web_search"] = source_mentions.get("web_search", []) + snippet_mentions

        query2 = f'"{store_name}" acquired company'
        search_results2 = await search.google_search(query2, num_results=3)
        for sr in search_results2:
            snippet_mentions = _find_acquisition_mentions(sr.get("snippet", ""))
            if snippet_mentions:
                all_mentions.extend(snippet_mentions)
                source_mentions["web_search"] = source_mentions.get("web_search", []) + snippet_mentions
    except Exception as e:
        logger.warning(f"[{store_name}] Acquisition web search error: {e}")

    if not all_mentions:
        result["acquisitions"] = "NONE"
        result["acquisitions_score"] = score_acquisitions(found=False, verified=False, searched=True)
        logger.info(f"[{store_name}] Step 5 complete: No acquisitions found")
        return result

    # Use LLM to extract clean acquisition list from all mentions
    combined_text = "\n".join(all_mentions[:50])  # Limit to avoid token overflow
    try:
        llm_result = await llm.extract_acquisitions(combined_text, store_name)
        llm_result = llm_result.strip()

        if llm_result.upper() == "NONE" or not llm_result:
            result["acquisitions"] = "NONE"
            result["acquisitions_score"] = score_acquisitions(found=False, verified=False, searched=True)
        else:
            # Cross-reference: check which acquisitions appear in multiple sources
            acquisitions = [a.strip() for a in llm_result.split(",") if a.strip()]
            verified_acquisitions = []
            num_sources = len(source_mentions)

            for acq in acquisitions:
                acq_lower = acq.lower().replace("(unverified)", "").strip()
                source_count = 0
                for source, mentions in source_mentions.items():
                    combined_source = " ".join(mentions).lower()
                    if acq_lower in combined_source:
                        source_count += 1

                if source_count >= 2:
                    verified_acquisitions.append(acq_lower.title())
                else:
                    verified_acquisitions.append(f"{acq_lower.title()} (Unverified)")

            result["acquisitions"] = ", ".join(verified_acquisitions)
            has_verified = any("(Unverified)" not in a for a in verified_acquisitions)
            result["acquisitions_sources"] = num_sources
            result["acquisitions_score"] = score_acquisitions(
                found=True,
                verified=has_verified,
                searched=True
            )

    except Exception as e:
        logger.error(f"[{store_name}] LLM acquisition extraction failed: {e}")
        # Fall back to raw mentions
        result["acquisitions"] = "NONE"
        result["acquisitions_score"] = score_acquisitions(found=False, verified=False, searched=True)

    logger.info(
        f"[{store_name}] Step 5 complete: {result['acquisitions']}, "
        f"score={result['acquisitions_score']}"
    )

    return result
