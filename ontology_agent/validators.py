"""
Validation utilities for the Ontology Automation Agent.
Fuzzy matching, domain comparison, phone validation, confidence scoring.
"""

import re
import logging
from urllib.parse import urlparse
from typing import Optional

from thefuzz import fuzz

logger = logging.getLogger(__name__)

# ─── Fuzzy Matching ──────────────────────────────────────────────

def fuzzy_match(a: str, b: str, threshold: int = 85) -> tuple[bool, int]:
    """
    Compare two strings using fuzzy matching.
    Returns (passed, score) where score is 0-100.
    """
    if not a or not b:
        return False, 0
    a_clean = a.strip().lower()
    b_clean = b.strip().lower()
    # Use token_sort_ratio for best results with reordered words
    score = max(
        fuzz.ratio(a_clean, b_clean),
        fuzz.token_sort_ratio(a_clean, b_clean),
        fuzz.partial_ratio(a_clean, b_clean),
    )
    return score >= threshold, score


# ─── Domain Matching ─────────────────────────────────────────────

def normalize_domain(url: str) -> str:
    """Extract and normalize domain from URL — strips www., trailing slashes."""
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = domain.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        # Remove port if present
        domain = domain.split(":")[0]
        return domain
    except Exception:
        return url.lower().strip()


def domain_match(url1: str, url2: str) -> bool:
    """Check if two URLs point to the same domain."""
    d1 = normalize_domain(url1)
    d2 = normalize_domain(url2)
    if not d1 or not d2:
        return False
    return d1 == d2


# ─── Phone Validation ────────────────────────────────────────────

US_PHONE_PATTERN = re.compile(
    r'(?:\+?1[-.\s]?)?'           # optional +1 prefix
    r'(?:\((\d{3})\)|(\d{3}))'    # area code with or without parens
    r'[-.\s]?'
    r'(\d{3})'                     # exchange
    r'[-.\s]?'
    r'(\d{4})'                     # subscriber number
)

TOLL_FREE_PREFIXES = {"800", "888", "877", "866", "855", "844", "833"}


def extract_phone_numbers(text: str) -> list[str]:
    """Extract all US-format phone numbers from text."""
    if not text:
        return []
    matches = US_PHONE_PATTERN.findall(text)
    phones = []
    for match in matches:
        area = match[0] or match[1]
        exchange = match[2]
        subscriber = match[3]
        if area and exchange and subscriber:
            formatted = f"({area}) {exchange}-{subscriber}"
            if formatted not in phones:
                phones.append(formatted)
    return phones


def is_us_phone(number: str) -> bool:
    """Validate that a string looks like a US phone number."""
    if not number:
        return False
    digits = re.sub(r'\D', '', number)
    # US numbers are 10 digits (or 11 with leading 1)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return len(digits) == 10


def is_toll_free(number: str) -> bool:
    """Check if a phone number has a toll-free prefix."""
    if not number:
        return False
    digits = re.sub(r'\D', '', number)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return digits[:3] in TOLL_FREE_PREFIXES
    return False


# ─── Scrape Quality Validation ────────────────────────────────────

BLOCK_INDICATORS = [
    "access denied",
    "403 forbidden",
    "enable javascript",
    "please verify you are human",
    "ray id",
    "captcha",
    "just a moment",
    "checking your browser",
    "this site can't be reached",
]


def validate_scrape_quality(text: str, min_words: int = 200) -> dict:
    """
    Validate scraped text quality.
    Returns dict with: valid (bool), word_count, issues (list of strings).
    """
    if not text:
        return {"valid": False, "word_count": 0, "issues": ["No text extracted"]}

    issues = []
    words = text.split()
    word_count = len(words)

    if word_count < min_words:
        issues.append(f"Text too short: {word_count} words (minimum {min_words})")

    text_lower = text.lower()
    for indicator in BLOCK_INDICATORS:
        if indicator in text_lower:
            issues.append(f"Block indicator detected: '{indicator}'")

    return {
        "valid": len(issues) == 0,
        "word_count": word_count,
        "issues": issues,
    }


# ─── Wikipedia Validation Helpers ─────────────────────────────────

def is_disambiguation_page(url: str, text: str) -> bool:
    """Check if a Wikipedia page is a disambiguation page."""
    if "(disambiguation)" in (url or "").lower():
        return True
    if text:
        first_lines = text[:500].lower()
        if "may refer to:" in first_lines or "can refer to:" in first_lines:
            return True
    return False


def check_wikipedia_infobox_type(text: str) -> Optional[str]:
    """
    Try to determine if a Wikipedia article is about a company, person, or place.
    Returns: 'company', 'person', 'place', or None if unclear.
    """
    text_lower = text[:3000].lower() if text else ""

    company_signals = ["founded", "headquarters", "industry", "parent company",
                       "revenue", "number of employees", "subsidiaries", "products"]
    person_signals = ["born", "nationality", "occupation", "spouse",
                      "children", "alma mater", "years active"]
    place_signals = ["population", "area", "country", "elevation",
                     "time zone", "postal code", "coordinates"]

    company_score = sum(1 for s in company_signals if s in text_lower)
    person_score = sum(1 for s in person_signals if s in text_lower)
    place_score = sum(1 for s in place_signals if s in text_lower)

    if company_score > person_score and company_score > place_score:
        return "company"
    if person_score > company_score and person_score > place_score:
        return "person"
    if place_score > company_score and place_score > person_score:
        return "place"
    return None


def check_name_in_opening(text: str, store_name: str) -> bool:
    """Check if store_name appears in the first 2 sentences of text."""
    if not text or not store_name:
        return False
    # Approximate first 2 sentences
    sentences = re.split(r'[.!?]\s+', text[:1000])
    opening = " ".join(sentences[:2]).lower()
    return store_name.lower() in opening


# ─── Confidence Score Calculation ─────────────────────────────────

def calculate_confidence(results: dict) -> int:
    """
    Calculate overall confidence score (0-100) based on the scoring rubric.

    results dict should have keys:
      - website_score: 0-15
      - zoominfo_score: 0-20
      - dnb_score: 0-20
      - wikipedia_score: 0-15
      - acquisitions_score: 0-15
      - phone_score: 0-5
      - location_score: 0-10
    """
    total = 0
    total += min(results.get("website_score", 0), 15)
    total += min(results.get("zoominfo_score", 0), 20)
    total += min(results.get("dnb_score", 0), 20)
    total += min(results.get("wikipedia_score", 0), 15)
    total += min(results.get("acquisitions_score", 0), 15)
    total += min(results.get("phone_score", 0), 5)
    total += min(results.get("location_score", 0), 10)
    return min(total, 100)


def score_website_scrape(text: str) -> int:
    """Score website scrape quality: 0, 5, 10, or 15 points."""
    quality = validate_scrape_quality(text, min_words=200)
    if not quality["valid"]:
        if quality["word_count"] > 50:
            return 5  # Partial scrape
        return 0  # Failed

    if quality["word_count"] >= 500:
        return 15  # Clean scrape, 500+ words
    return 10  # Scrape worked but < 500 words


def score_directory_url(checks_passed: int, max_checks: int = 6) -> int:
    """Score a directory URL (ZoomInfo/D&B): 0, 10, 15, or 20 points."""
    if checks_passed >= 6:
        return 20
    if checks_passed >= 5:
        return 15
    if checks_passed >= 4:
        return 10
    return 0


def score_wikipedia(checks_passed: int) -> int:
    """Score Wikipedia article: 0, 5, 10, or 15 points."""
    if checks_passed >= 6:
        return 15
    if checks_passed >= 4:
        return 10
    if checks_passed >= 3:
        return 5
    return 0


def score_acquisitions(found: bool, verified: bool, searched: bool) -> int:
    """Score acquisitions data: 0, 5, 8, or 15 points."""
    if found and verified:
        return 15
    if found:
        return 8  # Found in 1 source (unverified)
    if searched:
        return 5  # NONE found but searched thoroughly
    return 0


def score_phone(hq_found: bool, toll_free_only: bool) -> int:
    """Score phone number: 0, 3, or 5 points."""
    if hq_found:
        return 5
    if toll_free_only:
        return 3
    return 0


def score_locations(count: int) -> int:
    """Score location data: 0, 4, 7, or 10 points."""
    if count >= 8:
        return 10
    if count >= 5:
        return 7
    if count >= 1:
        return 4
    return 0
