"""
Centralized configuration for the Ontology Automation Agent.
All API keys, proxy settings, and model config live here.
Load from environment variables or .env file.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class FirecrawlConfig:
    """Firecrawl API settings for website scraping."""
    api_key: Optional[str] = None
    base_url: str = "https://api.firecrawl.dev/v1"
    timeout: int = 30
    retry_attempts: int = 3

    def __post_init__(self):
        self.api_key = os.getenv("FIRECRAWL_API_KEY", self.api_key)


@dataclass
class SerpApiConfig:
    """SerpAPI settings for Google search."""
    api_key: Optional[str] = None
    base_url: str = "https://serpapi.com/search"
    timeout: int = 20

    def __post_init__(self):
        self.api_key = os.getenv("SERPAPI_KEY", self.api_key)


@dataclass
class ScraperConfig:
    """Browser/scraper settings (fallback only — Firecrawl is primary)."""
    headless: bool = True
    user_agents: list = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ])
    viewport_width: int = 1920
    viewport_height: int = 1080
    page_load_timeout: int = 30000
    scroll_delay: float = 0.5
    max_redirects: int = 3
    min_text_words: int = 200


@dataclass
class ProxyConfig:
    """Proxy settings."""
    enabled: bool = False
    url: Optional[str] = None
    residential_url: Optional[str] = None
    rotate: bool = True

    def __post_init__(self):
        self.url = os.getenv("PROXY_URL", self.url)
        self.residential_url = os.getenv("RESIDENTIAL_PROXY_URL", self.residential_url)
        if self.url or self.residential_url:
            self.enabled = True


@dataclass
class CaptchaConfig:
    """CAPTCHA solver settings."""
    enabled: bool = False
    api_key: Optional[str] = None
    service: str = "2captcha"
    timeout: int = 120

    def __post_init__(self):
        self.api_key = os.getenv("CAPTCHA_API_KEY", self.api_key)
        if self.api_key:
            self.enabled = True


@dataclass
class LLMConfig:
    """LLM provider settings."""
    provider: str = "openrouter"
    api_key: Optional[str] = None
    model: str = "deepseek/deepseek-chat"
    base_url: str = "https://openrouter.ai/api/v1"
    max_tokens: int = 4096
    temperature: float = 0.3
    retry_attempts: int = 3
    retry_delay: float = 2.0

    def __post_init__(self):
        self.api_key = os.getenv("OPENROUTER_API_KEY", self.api_key)
        model_env = os.getenv("OPENROUTER_MODEL")
        if model_env:
            self.model = model_env


@dataclass
class SearchConfig:
    """Search settings."""
    delay_between_searches: float = 1.0
    max_results_per_query: int = 5
    retry_on_captcha: bool = True


@dataclass
class PipelineConfig:
    """Pipeline processing settings."""
    batch_size: int = 20
    retry_attempts: int = 3
    retry_delay: float = 5.0
    confidence_auto_write: int = 85
    confidence_review_write: int = 60


@dataclass
class Config:
    """Master configuration."""
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    captcha: CaptchaConfig = field(default_factory=CaptchaConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    firecrawl: FirecrawlConfig = field(default_factory=FirecrawlConfig)
    serpapi: SerpApiConfig = field(default_factory=SerpApiConfig)

    @classmethod
    def load(cls) -> "Config":
        return cls()

    def validate(self) -> list[str]:
        warnings = []
        if not self.llm.api_key:
            warnings.append("OPENROUTER_API_KEY not set — LLM calls will fail")
        if not self.firecrawl.api_key:
            warnings.append("FIRECRAWL_API_KEY not set — website scraping will fall back to Playwright")
        if not self.serpapi.api_key:
            warnings.append("SERPAPI_KEY not set — Google search will fail; ZoomInfo/D&B/Wikipedia steps will be skipped")
        return warnings
