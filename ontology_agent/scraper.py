"""
Web scraper — Firecrawl API primary, Playwright fallback.

KEY FIX: Firecrawl is called with onlyMainContent=true which strips
navigation, headers, footers, cookie banners, and ads — returning
only the actual page body content as clean text.
"""

import asyncio
import logging
import random
import re
from typing import Optional
from urllib.parse import urlparse

import httpx

from .config import ScraperConfig, ProxyConfig, CaptchaConfig, FirecrawlConfig

logger = logging.getLogger(__name__)

BLOCK_INDICATORS = [
    "access denied", "403 forbidden", "enable javascript",
    "please verify you are human", "ray id", "just a moment",
    "checking your browser",
]
CAPTCHA_INDICATORS = ["captcha", "recaptcha", "hcaptcha", "i'm not a robot"]


def _clean_text(text: str) -> str:
    """
    Strip markdown syntax and noise from Firecrawl output.
    Keeps readable prose, removes link URLs, image tags, nav boilerplate.
    """
    if not text:
        return ""
    # Remove markdown images: ![alt](url)
    text = re.sub(r'!\[[^\]]*\]\([^)]*\)', '', text)
    # Remove markdown links but keep text: [text](url) -> text
    text = re.sub(r'\[([^\]]+)\]\([^)]*\)', r'\1', text)
    # Remove bare URLs
    text = re.sub(r'https?://\S+', '', text)
    # Remove markdown headers (## Header -> Header)
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    # Remove bold/italic markers
    text = re.sub(r'\*{1,3}([^*\n]+)\*{1,3}', r'\1', text)
    text = re.sub(r'_{1,2}([^_\n]+)_{1,2}', r'\1', text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove base64 data
    text = re.sub(r'data:[^;]+;base64,[A-Za-z0-9+/=]+', '', text)
    # Remove lines that are just special characters / noise
    lines = []
    for line in text.split('\n'):
        stripped = line.strip()
        # Skip very short lines that are likely nav items (< 3 words)
        if stripped and len(stripped.split()) >= 3:
            lines.append(stripped)
        elif stripped and any(c.isalpha() for c in stripped):
            lines.append(stripped)
    text = '\n'.join(lines)
    # Collapse multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Remove non-breaking spaces
    text = text.replace('\xa0', ' ')
    return text.strip()


class FirecrawlScraper:
    """Firecrawl API scraper — handles JS, bot-bypass, proxy rotation."""

    def __init__(self, config: FirecrawlConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self):
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def scrape_page(self, url: str, extract_links: bool = False,
                          use_residential_proxy: bool = False) -> dict:
        result = {
            "status": -1, "text": "", "final_url": url, "title": "",
            "links": [], "redirect_chain": [], "domain_changed": False,
            "captcha_detected": False, "blocked": False, "error": None,
        }

        if not self.config.api_key:
            result["error"] = "FIRECRAWL_API_KEY not set"
            return result

        payload = {
            "url": url,
            # onlyMainContent strips nav/footer/ads — KEY for clean text
            "onlyMainContent": True,
            "formats": ["markdown"],
            "waitFor": 2000,
            "timeout": self.config.timeout * 1000,
        }
        if extract_links:
            payload["formats"] = ["markdown", "links"]

        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                client = await self._get_client()
                resp = await client.post(f"{self.config.base_url}/scrape", json=payload)

                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        fc = data.get("data", {})
                        raw = fc.get("markdown", "") or fc.get("content", "")
                        result["text"] = _clean_text(raw)
                        result["status"] = 200
                        result["final_url"] = fc.get("metadata", {}).get("sourceURL", url)
                        result["title"] = fc.get("metadata", {}).get("title", "")
                        if extract_links:
                            result["links"] = fc.get("links", [])
                        orig = urlparse(url).netloc.lower().replace("www.", "")
                        final = urlparse(result["final_url"]).netloc.lower().replace("www.", "")
                        if orig and final and orig != final:
                            result["domain_changed"] = True
                        tl = result["text"].lower()
                        result["blocked"] = any(i in tl for i in BLOCK_INDICATORS)
                        result["captcha_detected"] = any(i in tl for i in CAPTCHA_INDICATORS)
                        return result
                    result["error"] = data.get("error", "success=false")
                    return result

                elif resp.status_code == 429:
                    await asyncio.sleep(2 ** attempt)
                    continue
                elif resp.status_code == 402:
                    result["error"] = "Firecrawl quota exceeded"
                    return result
                else:
                    result["error"] = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    if attempt < self.config.retry_attempts:
                        await asyncio.sleep(2)

            except httpx.RequestError as e:
                result["error"] = str(e)
                if attempt < self.config.retry_attempts:
                    await asyncio.sleep(2)

        return result

    async def scrape_simple(self, url: str) -> dict:
        return await self.scrape_page(url, extract_links=False)

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


class StealthScraper:
    """Playwright fallback — used only if Firecrawl key not set."""

    def __init__(self, scraper_config, proxy_config=None, captcha_config=None):
        self.config = scraper_config
        self.proxy_config = proxy_config
        self._playwright = None
        self._browser = None

    async def _ensure_browser(self):
        if self._browser is None or not self._browser.is_connected():
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            args = {
                "headless": self.config.headless,
                "args": ["--disable-blink-features=AutomationControlled",
                         "--disable-dev-shm-usage", "--no-first-run"],
            }
            if self.proxy_config and self.proxy_config.enabled and self.proxy_config.url:
                args["proxy"] = {"server": self.proxy_config.url}
            self._browser = await self._playwright.chromium.launch(**args)
        return self._browser

    async def scrape_page(self, url: str, use_residential_proxy: bool = False,
                          extract_links: bool = False) -> dict:
        result = {"status": -1, "text": "", "final_url": url, "title": "",
                  "links": [], "redirect_chain": [], "domain_changed": False,
                  "captcha_detected": False, "blocked": False, "error": None}
        ctx = None
        try:
            browser = await self._ensure_browser()
            ua = random.choice(self.config.user_agents)
            ctx = await browser.new_context(
                user_agent=ua,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )
            page = await ctx.new_page()
            resp = await page.goto(url, wait_until="domcontentloaded",
                                   timeout=self.config.page_load_timeout)
            if resp:
                result["status"] = resp.status
            await asyncio.sleep(2)
            result["final_url"] = page.url
            result["title"] = await page.title()
            raw = await page.evaluate("""
                () => {
                    document.querySelectorAll('script,style,noscript,nav,header,footer,svg').forEach(e=>e.remove());
                    return document.body ? document.body.innerText : '';
                }
            """)
            result["text"] = _clean_text(raw or "")
            if extract_links:
                links = await page.evaluate(
                    "()=>Array.from(document.querySelectorAll('a[href]')).map(a=>a.href).filter(h=>h.startsWith('http'))"
                )
                result["links"] = links or []
        except Exception as e:
            result["error"] = str(e)
        finally:
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
        return result

    async def scrape_simple(self, url: str) -> dict:
        return await self.scrape_page(url)

    async def close(self):
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass


class SmartScraper:
    """Unified interface — Firecrawl if key set, Playwright otherwise."""

    def __init__(self, scraper_config, firecrawl_config,
                 proxy_config=None, captcha_config=None):
        self._fc = FirecrawlScraper(firecrawl_config)
        self._pw = StealthScraper(scraper_config, proxy_config, captcha_config)
        self._use_fc = bool(firecrawl_config.api_key)
        if self._use_fc:
            logger.info("Scraper: Firecrawl API (onlyMainContent=true)")
        else:
            logger.warning("Scraper: Playwright fallback (set FIRECRAWL_API_KEY for better results)")

    async def scrape_page(self, url: str, use_residential_proxy: bool = False,
                          extract_links: bool = False) -> dict:
        if self._use_fc:
            return await self._fc.scrape_page(url, extract_links=extract_links)
        return await self._pw.scrape_page(url, use_residential_proxy, extract_links)

    async def scrape_simple(self, url: str) -> dict:
        if self._use_fc:
            return await self._fc.scrape_simple(url)
        return await self._pw.scrape_simple(url)

    async def close(self):
        await self._fc.close()
        await self._pw.close()
