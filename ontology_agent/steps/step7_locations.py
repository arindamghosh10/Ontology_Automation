"""
Step 7 — Store Locations.
FIX: Uses Google Places API for accurate verified locations.
Falls back to store locator scraping + LLM only if Places API key not set.
"""

import asyncio
import logging
import re
from urllib.parse import urlparse
from typing import Optional

import httpx

from ..search_engine import SearchEngine
from ..llm_client import LLMClient
from ..validators import fuzzy_match, is_us_phone, score_locations

logger = logging.getLogger(__name__)

LOCATOR_PATTERNS = [
    "store-locator", "find-a-store", "locations", "find-us",
    "our-locations", "store-finder", "find-store", "storelocator",
    "find-location", "find-a-location", "our-stores", "store-list",
    "restaurant-locator", "find-a-restaurant", "hotel-locator",
]

US_STATE_ABBREVS = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

import os
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")


def _validate_us_address(address: str) -> bool:
    if not address:
        return False
    address_upper = address.upper()
    has_state = any(
        f" {s} " in f" {address_upper} " or
        f", {s} " in f", {address_upper} " or
        address_upper.endswith(f" {s}")
        for s in US_STATE_ABBREVS
    )
    has_zip = bool(re.search(r'\b\d{5}(?:-\d{4})?\b', address))
    return has_state or has_zip


def _deduplicate(locations: list) -> list:
    unique = []
    seen = set()
    for loc in locations:
        key = re.sub(r'[^a-z0-9]', '', loc.get("address", "").lower())
        if key and key not in seen:
            seen.add(key)
            unique.append(loc)
    return unique


async def _google_places_search(store_name: str, max_results: int = 10) -> list:
    """
    Use Google Places Text Search API to find real verified locations.
    Returns list of {address, phone} dicts.
    """
    if not GOOGLE_PLACES_API_KEY:
        return []

    locations = []
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            # Text search for the store
            search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
            params = {
                "query": f"{store_name} United States",
                "key": GOOGLE_PLACES_API_KEY,
                "region": "us",
                "type": "establishment",
            }
            resp = await client.get(search_url, params=params)
            if resp.status_code != 200:
                logger.warning(f"Places API HTTP {resp.status_code}")
                return []

            data = resp.json()
            if data.get("status") not in ("OK", "ZERO_RESULTS"):
                logger.warning(f"Places API status: {data.get('status')}")
                return []

            results = data.get("results", [])[:max_results]

            # Get details for each result
            details_url = "https://maps.googleapis.com/maps/api/place/details/json"
            for place in results:
                place_id = place.get("place_id")
                if not place_id:
                    continue

                # Filter to US addresses only
                address = place.get("formatted_address", "")
                if not _validate_us_address(address):
                    continue

                detail_resp = await client.get(details_url, params={
                    "place_id": place_id,
                    "fields": "formatted_address,formatted_phone_number,name",
                    "key": GOOGLE_PLACES_API_KEY,
                })

                phone = ""
                if detail_resp.status_code == 200:
                    detail_data = detail_resp.json()
                    result = detail_data.get("result", {})
                    address = result.get("formatted_address", address)
                    phone = result.get("formatted_phone_number", "")

                locations.append({"address": address, "phone": phone})
                await asyncio.sleep(0.1)  # Respect rate limits

        logger.info(f"Google Places returned {len(locations)} locations")
    except Exception as e:
        logger.warning(f"Google Places API error: {e}")

    return locations


def _find_locator_url(links: list, store_domain: str) -> str:
    if not links:
        return ""
    domain = store_domain.lower().replace("www.", "").replace("https://", "").replace("http://", "").strip("/")
    for link in links:
        link_lower = link.lower()
        if domain and domain not in link_lower:
            continue
        for pattern in LOCATOR_PATTERNS:
            if pattern in link_lower:
                return link
    return ""


async def _scrape_locator(scraper, locator_url: str, store_name: str) -> list:
    """Scrape a store locator page for addresses."""
    locations = []
    try:
        scrape_result = await scraper.scrape_page(locator_url)
        text = scrape_result.get("text", "")
        if not text or len(text.split()) < 20:
            return locations

        # FIX: Only accept lines that look like real addresses
        # Reject navigation/UI text like "Items in cart", "Toggle"
        lines = text.split("\n")
        current_address = ""
        current_phone = ""

        for line in lines:
            line = line.strip()
            if not line or len(line) > 200:
                continue
            # Skip obvious UI noise
            if any(skip in line.lower() for skip in [
                "cart", "toggle", "filter", "search", "navigation",
                "cookie", "javascript", "loading", "tab to access",
                "enter to proceed", "following text field"
            ]):
                continue

            if _validate_us_address(line):
                if current_address:
                    locations.append({"address": current_address, "phone": current_phone})
                    current_phone = ""
                current_address = line
            elif current_address:
                # Check for phone after address
                from ..validators import extract_phone_numbers as _ep
                phones = _ep(line)
                if phones:
                    current_phone = phones[0]

        if current_address:
            locations.append({"address": current_address, "phone": current_phone})

    except Exception as e:
        logger.warning(f"[{store_name}] Locator scrape error: {e}")
    return locations


async def execute(merchant: dict, context: dict,
                  scraper, search: SearchEngine, llm: LLMClient) -> dict:
    store_name = merchant.get("store_name", "")
    store_domain = merchant.get("store_domain", "")

    result = {"locations": [], "location_score": 0}

    logger.info(f"[{store_name}] Step 7: Finding store locations")
    locations = []

    # Method A — Google Places API (most accurate)
    if GOOGLE_PLACES_API_KEY:
        logger.info(f"[{store_name}] Using Google Places API")
        locations = await _google_places_search(store_name, max_results=10)
        locations = _deduplicate(locations)

    # Method B — Store locator scraping (if Places API not available or < 5 results)
    if len(locations) < 5:
        website_links = context.get("website_links", [])
        locator_url = _find_locator_url(website_links, store_domain)
        if locator_url:
            logger.info(f"[{store_name}] Scraping locator: {locator_url}")
            scraped = await _scrape_locator(scraper, locator_url, store_name)
            scraped = [s for s in scraped if _validate_us_address(s.get("address", ""))]
            for loc in scraped:
                locations.append(loc)
            locations = _deduplicate(locations)

    # Method C — LLM fallback (last resort, limited reliability)
    if len(locations) < 3:
        logger.info(f"[{store_name}] LLM fallback for locations")
        try:
            extra = ""
            if context.get("corrected_website_text"):
                extra += f"Website: {context['corrected_website_text'][:500]}\n"
            if context.get("corrected_wikipedia_page_text"):
                extra += f"Wikipedia: {context['corrected_wikipedia_page_text'][:500]}\n"
            llm_locs = await llm.find_locations(store_name, store_domain, extra)
            for loc in llm_locs:
                if _validate_us_address(loc.get("address", "")):
                    locations.append(loc)
            locations = _deduplicate(locations)
        except Exception as e:
            logger.warning(f"[{store_name}] LLM location error: {e}")

    # Cap and validate
    locations = locations[:10]
    validated = []
    for loc in locations:
        address = loc.get("address", "").strip()
        phone = loc.get("phone", "").strip()
        if not address:
            continue
        if phone and not is_us_phone(phone):
            phone = ""
        validated.append({"address": address, "phone": phone})

    result["locations"] = validated
    result["location_score"] = score_locations(len(validated))

    logger.info(f"[{store_name}] Step 7 done: {len(validated)} locations, score={result['location_score']}")
    return result
