"""LinkedIn job scraper.

NOTE: LinkedIn has strong anti-scraping measures and requires authentication
for full access. This scraper provides basic functionality but may require
additional setup (cookies, authentication) for production use.

Consider using LinkedIn's official Jobs API for commercial applications:
https://docs.microsoft.com/en-us/linkedin/jobs/
"""

import logging
import time
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class LinkedInScraper(BaseScraper):
    """Scraper for LinkedIn job listings."""

    platform_name = "linkedin"
    base_url = "https://www.linkedin.com"
    max_results_per_search = 100
    results_per_page = 25
    rate_limit_seconds = 5.0  # LinkedIn is aggressive about rate limiting

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)
        # LinkedIn-specific settings
        self.use_guest_api = config.get("use_guest_api", True) if config else True

    def _build_search_url(self, query: SearchQuery, start: int = 0) -> str:
        """Build LinkedIn search URL."""
        # Use guest job search endpoint
        params = {
            "keywords": " ".join(query.keywords) if query.keywords else "",
            "location": query.location,
            "start": start,
            "sortBy": "DD",  # Date descending
        }

        if query.remote_only:
            params["f_WT"] = "2"  # Remote work type

        if query.job_type:
            jt_map = {
                "fulltime": "F",
                "parttime": "P",
                "contract": "C",
                "temporary": "T",
                "internship": "I",
            }
            if query.job_type.lower() in jt_map:
                params["f_JT"] = jt_map[query.job_type.lower()]

        return f"{self.base_url}/jobs/search?{urlencode(params)}"

    def _get_headers(self) -> dict:
        """Get headers for LinkedIn requests."""
        headers = super()._get_headers()
        headers.update({
            "Accept": "text/html,application/xhtml+xml",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        })
        return headers

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on LinkedIn."""
        from bs4 import BeautifulSoup

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            start = 0
            max_pages = self.max_results_per_search // self.results_per_page

            while pages_scraped < max_pages:
                url = self._build_search_url(query, start)
                logger.info(f"Fetching: {url}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code == 429:
                    errors.append("Rate limited by LinkedIn")
                    break
                elif response.status_code == 401:
                    errors.append("Authentication required - consider using LinkedIn API")
                    break
                elif response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards (guest view)
                job_cards = soup.find_all('div', class_='base-card')

                if not job_cards:
                    job_cards = soup.find_all('li', class_='jobs-search-results__list-item')

                if not job_cards:
                    logger.info(f"No more jobs found after {pages_scraped} pages")
                    break

                # Get total count
                if pages_scraped == 0:
                    count_elem = soup.find('span', class_='results-context-header__job-count')
                    if count_elem:
                        import re
                        match = re.search(r'([\d,]+)', count_elem.get_text())
                        if match:
                            total_found = int(match.group(1).replace(',', ''))

                for card in job_cards:
                    job = self._parse_job_card(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                start += self.results_per_page

                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping LinkedIn: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=self.platform_name,
            jobs=jobs,
            total_found=total_found or len(jobs),
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job_card(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse a LinkedIn job card."""
        try:
            # Title
            title_elem = card.find('h3', class_='base-search-card__title')
            if not title_elem:
                title_elem = card.find('a', class_='job-card-list__title')
            title = title_elem.get_text(strip=True) if title_elem else None

            # Company
            company_elem = card.find('h4', class_='base-search-card__subtitle')
            if not company_elem:
                company_elem = card.find('a', class_='job-card-container__company-name')
            company = company_elem.get_text(strip=True) if company_elem else "Unknown"

            # Location
            location_elem = card.find('span', class_='job-search-card__location')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            # URL
            link_elem = card.find('a', class_='base-card__full-link')
            if not link_elem:
                link_elem = card.find('a', href=True)
            url = link_elem.get('href', '') if link_elem else ""

            if not title or not url:
                return None

            # Clean up URL
            if '?' in url:
                url = url.split('?')[0]

            # Remote detection
            remote = False
            if location:
                remote = 'remote' in location.lower()
            badge_elem = card.find('span', class_='job-search-card__workplace-type')
            if badge_elem and 'remote' in badge_elem.get_text().lower():
                remote = True

            # Posted date
            posted_date = None
            date_elem = card.find('time', class_='job-search-card__listdate')
            if date_elem:
                date_str = date_elem.get('datetime')
                if date_str:
                    try:
                        posted_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    except ValueError:
                        pass

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                remote=remote,
                posted_date=posted_date,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing LinkedIn job card: {e}")
            return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
