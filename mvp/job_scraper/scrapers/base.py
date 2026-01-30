"""Base scraper class for job platforms."""

from abc import ABC, abstractmethod
from typing import Optional
import time
import random
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.models import Job, SearchQuery, ScrapeResult


logger = logging.getLogger(__name__)


# Rotating user agents for better anti-detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

# Common referers to appear more natural
REFERERS = [
    "https://www.google.com/",
    "https://www.google.com/search?q=jobs",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    None,  # Sometimes no referer is natural
]


class BaseScraper(ABC):
    """Abstract base class for platform-specific scrapers."""

    platform_name: str = "base"
    base_url: str = ""
    max_results_per_search: int = 100
    rate_limit_seconds: float = 2.0
    max_retries: int = 3
    backoff_factor: float = 1.0

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self._last_request_time = 0
        self._request_count = 0

        # Proxy configuration
        self.proxies = config.get("proxies", []) if config else []
        self._current_proxy_index = 0

        # Create session with retry logic
        self.session = self._create_session()

    @abstractmethod
    def search(self, query: SearchQuery) -> ScrapeResult:
        """
        Execute a search and return results.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        """
        Parse raw job data into a Job object.
        Must be implemented by subclasses.
        """
        pass

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic and connection pooling."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _rate_limit(self):
        """Enforce rate limiting between requests with human-like randomness."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            sleep_time = self.rate_limit_seconds - elapsed
            # Add randomness to appear more human
            sleep_time += random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

        # Occasionally add longer pauses to seem more natural
        self._request_count += 1
        if self._request_count % random.randint(8, 15) == 0:
            pause = random.uniform(3.0, 8.0)
            logger.debug(f"Taking a natural pause of {pause:.1f}s")
            time.sleep(pause)

        self._last_request_time = time.time()

    def _build_search_url(self, query: SearchQuery, page: int = 0) -> str:
        """Build the search URL for a query. Override in subclasses."""
        raise NotImplementedError

    def _get_headers(self) -> dict:
        """Get HTTP headers with rotating user agent for anti-detection."""
        user_agent = random.choice(USER_AGENTS)
        referer = random.choice(REFERERS)

        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none" if not referer else "cross-site",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

        if referer:
            headers["Referer"] = referer

        return headers

    def _get_proxy(self) -> Optional[dict]:
        """Get the next proxy from the rotation, if available."""
        if not self.proxies:
            return None

        proxy = self.proxies[self._current_proxy_index]
        self._current_proxy_index = (self._current_proxy_index + 1) % len(self.proxies)

        # Support both string format and dict format
        if isinstance(proxy, str):
            return {"http": proxy, "https": proxy}
        return proxy

    def _make_request(
        self,
        url: str,
        method: str = "GET",
        **kwargs
    ) -> requests.Response:
        """Make an HTTP request with anti-detection measures."""
        headers = kwargs.pop("headers", None) or self._get_headers()
        proxies = kwargs.pop("proxies", None) or self._get_proxy()
        timeout = kwargs.pop("timeout", 30)

        self._rate_limit()

        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            proxies=proxies,
            timeout=timeout,
            **kwargs
        )

        # Log response status
        if response.status_code != 200:
            logger.warning(f"{self.platform_name}: HTTP {response.status_code} for {url}")

        return response

    def test_connection(self) -> bool:
        """Test if the platform is accessible."""
        try:
            response = self._make_request(self.base_url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Connection test failed for {self.platform_name}: {e}")
            return False


class ScraperRegistry:
    """Registry for managing available scrapers."""

    _scrapers: dict[str, type[BaseScraper]] = {}

    @classmethod
    def register(cls, scraper_class: type[BaseScraper]):
        """Register a scraper class."""
        cls._scrapers[scraper_class.platform_name] = scraper_class
        return scraper_class

    @classmethod
    def get(cls, platform_name: str) -> Optional[type[BaseScraper]]:
        """Get a scraper class by platform name."""
        return cls._scrapers.get(platform_name)

    @classmethod
    def list_platforms(cls) -> list[str]:
        """List all registered platforms."""
        return list(cls._scrapers.keys())

    @classmethod
    def create(cls, platform_name: str, config: Optional[dict] = None) -> Optional[BaseScraper]:
        """Create a scraper instance."""
        scraper_class = cls.get(platform_name)
        if scraper_class:
            return scraper_class(config)
        return None
