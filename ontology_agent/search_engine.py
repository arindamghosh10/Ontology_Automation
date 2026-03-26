"""
Google search engine for the Ontology Automation Agent.
PRIMARY: SerpAPI (reliable JSON results, no bot-blocking)
FALLBACK: Direct Playwright scraping (unreliable, kept for reference)

Sign up for SerpAPI at: https://serpapi.com (100 free searches/month on free tier)
"""

import asyncio
import logging
import random
import time
from typing import Optional
from urllib.parse import quote_plus

import httpx

from .config import SearchConfig, ScraperConfig, ProxyConfig, SerpApiConfig

logger = logging.getLogger(__name__)


class SearchEngine:
    """
    Google search using SerpAPI.
    Returns clean, structured results without any bot-blocking issues.
    """

    def __init__(self, search_config: SearchConfig,
                 scraper_config: ScraperConfig,
                 proxy_config: Optional[ProxyConfig] = None,
                 serpapi_config: Optional[SerpApiConfig] = None):
        self.config = search_config
        self.scraper_config = scraper_config
        self.proxy_config = proxy_config
        self.serpapi_config = serpapi_config
        self._client: Optional[httpx.AsyncClient] = None
        self._last_search_time: float = 0

        if serpapi_config and serpapi_config.api_key:
            logger.info("SearchEngine: using SerpAPI")
        else:
            logger.warning("SearchEngine: SERPAPI_KEY not set — Google searches will return empty results")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        return self._client

    async def _rate_limit(self):
        """Enforce minimum delay between searches."""
        now = time.time()
        elapsed = now - self._last_search_time
        if elapsed < self.config.delay_between_searches:
            wait = self.config.delay_between_searches - elapsed + random.uniform(0.1, 0.5)
            await asyncio.sleep(wait)
        self._last_search_time = time.time()

    async def google_search(self, query: str, num_results: int = 5) -> list[dict]:
        """
        Search Google via SerpAPI and return organic results.
        Returns list of {url, title, snippet} dicts.
        """
        if not self.serpapi_config or not self.serpapi_config.api_key:
            logger.warning(f"SerpAPI key missing — skipping search: {query}")
            return []

        await self._rate_limit()
        num_results = min(num_results, self.config.max_results_per_query)

        params = {
            "engine": "google",
            "q": query,
            "num": num_results + 3,
            "hl": "en",
            "gl": "us",
            "api_key": self.serpapi_config.api_key,
        }

        try:
            client = await self._get_client()
            response = await client.get(
                self.serpapi_config.base_url,
                params=params,
            )

            if response.status_code == 200:
                data = response.json()

                # Check for SerpAPI errors
                if "error" in data:
                    logger.error(f"SerpAPI error for '{query}': {data['error']}")
                    return []

                organic = data.get("organic_results", [])
                results = []
                for item in organic[:num_results]:
                    url = item.get("link", "")
                    if not url:
                        continue
                    results.append({
                        "url": url,
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                    })

                logger.info(f"SerpAPI search '{query}' → {len(results)} results")
                return results

            elif response.status_code == 401:
                logger.error("SerpAPI: invalid API key — check SERPAPI_KEY in .env")
                return []

            elif response.status_code == 429:
                logger.warning("SerpAPI: rate limited — waiting 5s")
                await asyncio.sleep(5)
                return []

            else:
                logger.error(f"SerpAPI HTTP {response.status_code}: {response.text[:200]}")
                return []

        except httpx.RequestError as e:
            logger.error(f"SerpAPI request error for '{query}': {e}")
            return []

    async def search_and_get_snippets(self, query: str,
                                      target_domain: str = "",
                                      num_results: int = 5) -> list[dict]:
        """
        Search Google and optionally filter results by target domain.
        """
        results = await self.google_search(query, num_results)

        if target_domain:
            target_domain = target_domain.lower().replace("www.", "")
            filtered = [r for r in results if target_domain in r["url"].lower()]
            if filtered:
                return filtered

        return results

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        # Legacy compatibility — no Playwright browser to close
