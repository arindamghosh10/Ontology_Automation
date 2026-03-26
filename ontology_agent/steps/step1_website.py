"""
Step 1 — Scrape the Official Website.
Goal: Extract ALL readable text from store_domain.
The scraper now uses onlyMainContent=true (Firecrawl) + _clean_text(),
so what gets stored is pure prose — no nav, no markdown, no base64.
"""

import asyncio
import logging
from ..validators import validate_scrape_quality, score_website_scrape

logger = logging.getLogger(__name__)


async def execute(merchant: dict, context: dict, scraper) -> dict:
    store_domain = merchant.get("store_domain", "")
    store_name = merchant.get("store_name", "")

    result = {
        "corrected_website_text": "",
        "website_score": 0,
        "website_links": [],
        "verification_notes": "",
    }

    if not store_domain:
        result["verification_notes"] = "No store_domain provided"
        return result

    url = str(store_domain).strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    notes = []
    logger.info(f"[{store_name}] Step 1: Scraping {url}")

    scrape = await scraper.scrape_page(url, extract_links=True)

    # Retry if blocked
    if scrape.get("blocked") or scrape.get("captcha_detected"):
        notes.append("Initial scrape blocked, retrying")
        await asyncio.sleep(2)
        scrape = await scraper.scrape_page(url, use_residential_proxy=True, extract_links=True)

    # One more attempt on hard error
    if scrape.get("error") and not scrape.get("text"):
        notes.append(f"Error: {scrape.get('error','')[:80]}")
        await asyncio.sleep(3)
        scrape = await scraper.scrape_page(url, extract_links=True)

    text = scrape.get("text", "")  # Already cleaned by scraper._clean_text()
    status = scrape.get("status", -1)

    if scrape.get("domain_changed"):
        notes.append(f"Redirected to: {scrape.get('final_url', '')}")
    if status not in (200, -1):
        notes.append(f"HTTP {status}")

    quality = validate_scrape_quality(text)
    if quality["issues"]:
        notes.extend(quality["issues"])

    if quality["valid"]:
        result["corrected_website_text"] = text
        result["website_score"] = score_website_scrape(text)
    elif text and len(text.split()) > 50:
        result["corrected_website_text"] = text
        result["website_score"] = 5
        notes.append("Partial scrape")
    else:
        result["website_score"] = 0
        if scrape.get("error"):
            notes.append(f"Failed: {scrape['error'][:80]}")

    result["website_links"] = scrape.get("links", [])
    if notes:
        result["verification_notes"] = "; ".join(notes)

    logger.info(f"[{store_name}] Step 1 done: {quality['word_count']} words, score={result['website_score']}")
    return result
