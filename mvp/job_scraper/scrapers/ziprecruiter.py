"""ZipRecruiter job scraper."""

import logging
import time
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class ZipRecruiterScraper(BaseScraper):
    """Scraper for ZipRecruiter.com job listings."""

    platform_name = "ziprecruiter"
    base_url = "https://www.ziprecruiter.com"
    max_results_per_search = 100
    results_per_page = 20
    rate_limit_seconds = 3.0

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build ZipRecruiter search URL."""
        params = {
            "search": " ".join(query.keywords) if query.keywords else "",
            "location": query.location,
            "page": page,
        }

        if query.radius_miles and query.radius_miles > 0:
            params["radius"] = query.radius_miles

        if query.remote_only:
            params["refine_by_location_type"] = "only_remote"

        return f"{self.base_url}/jobs?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on ZipRecruiter."""
        from bs4 import BeautifulSoup

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            page = 1
            max_pages = self.max_results_per_search // self.results_per_page

            while pages_scraped < max_pages:
                url = self._build_search_url(query, page)
                logger.info(f"Fetching: {url}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed on page {page}: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code} on page {page}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards
                job_cards = soup.find_all('article', class_='job_result')

                if not job_cards:
                    # Try alternative selector
                    job_cards = soup.find_all('div', class_='job_content')

                if not job_cards:
                    logger.info(f"No more jobs found after {pages_scraped} pages")
                    break

                # Get total count from first page
                if pages_scraped == 0:
                    count_elem = soup.find('h1', class_='job_results_headline')
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
                page += 1

                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping ZipRecruiter: {e}")
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
        """Parse a ZipRecruiter job card."""
        try:
            # Title
            title_elem = card.find('h2', class_='title') or card.find('a', class_='job_link')
            title = title_elem.get_text(strip=True) if title_elem else None

            # Company
            company_elem = card.find('a', class_='t_org_link') or card.find('p', class_='company')
            company = company_elem.get_text(strip=True) if company_elem else "Unknown"

            # Location
            location_elem = card.find('span', class_='location') or card.find('p', class_='location')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            # URL
            link_elem = card.find('a', href=True, class_='job_link')
            if not link_elem:
                link_elem = card.find('a', href=True)
            url = link_elem.get('href', '') if link_elem else ""

            if url and not url.startswith('http'):
                url = f"{self.base_url}{url}"

            if not title or not url:
                return None

            # Salary
            salary_min, salary_max, salary_type = None, None, None
            salary_elem = card.find('span', class_='salary')
            if salary_elem:
                salary_min, salary_max, salary_type = self._parse_salary(
                    salary_elem.get_text(strip=True)
                )

            # Remote detection
            remote = 'remote' in location.lower() if location else False

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type=salary_type,
                remote=remote,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing ZipRecruiter job card: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary text."""
        import re

        salary_min, salary_max, salary_type = None, None, None

        if not salary_text:
            return salary_min, salary_max, salary_type

        text_lower = salary_text.lower()
        if 'hour' in text_lower or '/hr' in text_lower:
            salary_type = 'hourly'
        elif 'year' in text_lower or '/yr' in text_lower:
            salary_type = 'yearly'

        numbers = re.findall(r'[\$]?([\d,]+(?:\.\d+)?)', salary_text)
        numbers = [float(n.replace(',', '')) for n in numbers]

        if len(numbers) >= 2:
            salary_min = min(numbers)
            salary_max = max(numbers)
        elif len(numbers) == 1:
            salary_min = salary_max = numbers[0]

        return salary_min, salary_max, salary_type

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
