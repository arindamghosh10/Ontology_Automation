"""
LLM client — OpenRouter DeepSeek wrapper with retry logic.
Key fix: analyze_industry now returns a clean short label, no markdown.
"""

import asyncio
import json
import logging
import re
from typing import Optional

import httpx

from .config import LLMConfig

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self, config: LLMConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://ontology-agent.local",
                    "X-Title": "Ontology Automation Agent",
                },
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def ask(self, prompt: str, system_prompt: Optional[str] = None,
                  max_tokens: Optional[int] = None) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.config.model,
            "messages": messages,
            "max_tokens": max_tokens or self.config.max_tokens,
            "temperature": self.config.temperature,
        }

        last_error = None
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                client = await self._get_client()
                response = await client.post(
                    f"{self.config.base_url}/chat/completions",
                    json=payload,
                )
                if response.status_code == 200:
                    data = response.json()
                    return data["choices"][0]["message"]["content"].strip()
                if response.status_code in (429, 500, 502, 503, 504):
                    wait = self.config.retry_delay * (2 ** (attempt - 1))
                    logger.warning(f"LLM {response.status_code}, retry {attempt} in {wait}s")
                    last_error = f"HTTP {response.status_code}"
                    await asyncio.sleep(wait)
                    continue
                raise Exception(f"LLM error {response.status_code}: {response.text[:300]}")
            except httpx.RequestError as e:
                wait = self.config.retry_delay * (2 ** (attempt - 1))
                last_error = str(e)
                await asyncio.sleep(wait)

        raise Exception(f"LLM failed after {self.config.retry_attempts} retries: {last_error}")

    async def validate_semantic(self, entity_name: str, domain: str,
                                page_title: str, snippet: str,
                                source_type: str = "ZoomInfo") -> str:
        prompt = (
            f"Does this {source_type} page represent the corporate brand entity for "
            f"'{entity_name}' whose website is '{domain}'? "
            f"Page title: '{page_title}'. Snippet: '{snippet[:500]}'. "
            f"Answer only YES, NO, or UNCERTAIN and give one sentence reason."
        )
        system_prompt = (
            "You are a data validation assistant. Only answer YES if confident it is "
            "the correct entity. Be strict."
        )
        return await self.ask(prompt, system_prompt=system_prompt, max_tokens=150)

    async def extract_acquisitions(self, text: str, store_name: str) -> str:
        prompt = (
            f"Based on this text, list all companies or brands that '{store_name}' has acquired. "
            f"Return ONLY a comma-separated list of acquired brand names. "
            f"If none found, return NONE. Do not include companies that acquired '{store_name}'.\n\n"
            f"Text:\n{text[:6000]}"
        )
        return await self.ask(prompt, max_tokens=500)

    async def find_locations(self, store_name: str, store_domain: str,
                             context: str = "") -> list:
        prompt = (
            f"Find up to 10 real US store/hotel/restaurant locations for '{store_name}' "
            f"(website: {store_domain}). For each location provide the full street address "
            f"(including city, state, ZIP) and phone number if available.\n\n"
            f"Context:\n{context[:2000]}\n\n"
            f"Return ONLY a JSON array. Example: "
            f'[{{"address": "123 Main St, New York, NY 10001", "phone": "(212) 555-1234"}}]\n'
            f"If no locations found, return []"
        )
        response = await self.ask(prompt, max_tokens=2000)
        try:
            clean = response.strip()
            if clean.startswith("```"):
                lines = [l for l in clean.split("\n") if not l.strip().startswith("```")]
                clean = "\n".join(lines)
            start = clean.find("[")
            end = clean.rfind("]") + 1
            if start >= 0 and end > start:
                return json.loads(clean[start:end])
        except Exception as e:
            logger.warning(f"Failed to parse LLM location response: {e}")
        return []

    async def analyze_industry(self, store_name: str, store_domain: str,
                               website_text: str = "") -> str:
        """
        FIX: Returns a clean 2-3 word label only — no markdown, no asterisks, no parentheticals.
        This is used directly in SerpAPI search queries so must be plain text.
        """
        prompt = (
            f"What industry does '{store_name}' (website: {store_domain}) belong to? "
            f"Reply with ONLY a 2-4 word industry label. No markdown, no asterisks, "
            f"no explanation, no punctuation. Examples: Hotels and Hospitality, "
            f"Fast Food Restaurant, Retail Clothing, Accounting Software, Airline.\n\n"
            f"Reply with the label only:"
        )
        response = await self.ask(prompt, max_tokens=20)
        # Strip any remaining markdown/punctuation defensively
        clean = re.sub(r'[*_`#\[\]\(\)]', '', response).strip().strip('"').strip("'")
        # Take only the first line in case of multi-line response
        clean = clean.split('\n')[0].strip()
        # Cap at 40 chars to prevent runaway responses being used in searches
        clean = clean[:40]
        return clean
