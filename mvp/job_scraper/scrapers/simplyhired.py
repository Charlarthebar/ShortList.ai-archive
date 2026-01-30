"""SimplyHired job scraper."""

import logging
import time
import re
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class SimplyHiredScraper(BaseScraper):
    """Scraper for SimplyHired.com job listings."""

    platform_name = "simplyhired"
    base_url = "https://www.simplyhired.com"
    max_results_per_search = 100
    results_per_page = 20
    rate_limit_seconds = 3.0

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build SimplyHired search URL."""
        params = {
            "l": query.location,
            "pn": page,
        }

        if query.keywords:
            params["q"] = " ".join(query.keywords)

        if query.radius_miles and query.radius_miles > 0:
            params["mi"] = query.radius_miles

        if query.job_type:
            jt_map = {
                "fulltime": "CF3CP",
                "parttime": "75GKK",
                "contract": "NJXCK",
                "temporary": "QJZM9",
            }
            if query.job_type.lower() in jt_map:
                params["fjt"] = jt_map[query.job_type.lower()]

        return f"{self.base_url}/search?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on SimplyHired."""
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
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards
                job_cards = soup.find_all('article', class_='SerpJob')
                if not job_cards:
                    job_cards = soup.find_all('div', {'data-testid': 'searchSerpJob'})
                if not job_cards:
                    job_cards = soup.find_all('li', class_='job')

                if not job_cards:
                    logger.info(f"No more jobs found after {pages_scraped} pages")
                    break

                # Get total count from first page
                if pages_scraped == 0:
                    count_elem = soup.find('span', {'data-testid': 'searchCountTitle'})
                    if not count_elem:
                        count_elem = soup.find('span', class_='results-count')
                    if count_elem:
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
            logger.error(f"Error scraping SimplyHired: {e}")
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
        """Parse a SimplyHired job card."""
        try:
            # Title
            title_elem = card.find('a', {'data-testid': 'searchSerpJobTitle'})
            if not title_elem:
                title_elem = card.find('h2', class_='jobposting-title')
                if title_elem:
                    title_elem = title_elem.find('a')
            if not title_elem:
                title_elem = card.find('a', class_='card-link')

            title = title_elem.get_text(strip=True) if title_elem else None

            # URL
            url = ""
            if title_elem and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href

            if not title or not url:
                return None

            # Company
            company_elem = card.find('span', {'data-testid': 'companyName'})
            if not company_elem:
                company_elem = card.find('span', class_='jobposting-company')
            company = company_elem.get_text(strip=True) if company_elem else "Unknown"

            # Location
            location_elem = card.find('span', {'data-testid': 'searchSerpJobLocation'})
            if not location_elem:
                location_elem = card.find('span', class_='jobposting-location')
            location = location_elem.get_text(strip=True) if location_elem else query.location

            # Salary
            salary_min, salary_max, salary_type = None, None, None
            salary_elem = card.find('span', {'data-testid': 'searchSerpJobSalaryEst'})
            if not salary_elem:
                salary_elem = card.find('span', class_='jobposting-salary')
            if salary_elem:
                salary_min, salary_max, salary_type = self._parse_salary(
                    salary_elem.get_text(strip=True)
                )

            # Remote
            remote = False
            if location and 'remote' in location.lower():
                remote = True
            remote_badge = card.find('span', class_='remote')
            if remote_badge:
                remote = True

            # Posted date
            posted_date = None
            date_elem = card.find('span', {'data-testid': 'searchSerpJobDateStamp'})
            if date_elem:
                posted_date = self._parse_date(date_elem.get_text(strip=True))

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
                posted_date=posted_date,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing SimplyHired job card: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary text."""
        salary_min, salary_max, salary_type = None, None, None

        if not salary_text:
            return salary_min, salary_max, salary_type

        text_lower = salary_text.lower()
        if 'hour' in text_lower or '/hr' in text_lower or 'an hour' in text_lower:
            salary_type = 'hourly'
        elif 'year' in text_lower or '/yr' in text_lower or 'a year' in text_lower:
            salary_type = 'yearly'

        numbers = re.findall(r'[\$]?([\d,]+(?:\.\d+)?)', salary_text)
        numbers = [float(n.replace(',', '')) for n in numbers]

        if len(numbers) >= 2:
            salary_min = min(numbers)
            salary_max = max(numbers)
        elif len(numbers) == 1:
            salary_min = salary_max = numbers[0]

        return salary_min, salary_max, salary_type

    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """Parse relative date text."""
        text_lower = date_text.lower()
        now = datetime.now()

        if 'today' in text_lower or 'just' in text_lower:
            return now
        elif 'yesterday' in text_lower:
            return now - timedelta(days=1)

        match = re.search(r'(\d+)\s*day', text_lower)
        if match:
            return now - timedelta(days=int(match.group(1)))

        match = re.search(r'(\d+)\s*hour', text_lower)
        if match:
            return now - timedelta(hours=int(match.group(1)))

        return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
