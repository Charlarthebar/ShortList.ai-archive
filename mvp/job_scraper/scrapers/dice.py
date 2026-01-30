"""Dice.com job scraper for technology jobs."""

import logging
import time
import re
from typing import Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@ScraperRegistry.register
class DiceScraper(BaseScraper):
    """Scraper for Dice.com - specializes in tech/IT jobs."""

    platform_name = "dice"
    base_url = "https://www.dice.com"
    api_url = "https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
    max_results_per_search = 100
    results_per_page = 20
    rate_limit_seconds = 2.0

    def _build_search_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build Dice API search URL."""
        params = {
            "q": " ".join(query.keywords) if query.keywords else "",
            "location": query.location,
            "latitude": "",
            "longitude": "",
            "countryCode2": "US",
            "radius": query.radius_miles or 30,
            "radiusUnit": "mi",
            "page": page,
            "pageSize": self.results_per_page,
            "facets": "employmentType|postedDate|workFromHomeAvailability",
            "fields": "id|jobId|guid|summary|title|postedDate|modifiedDate|company.name|"
                      "salary|locations|employmentType|workFromHomeAvailability|"
                      "isRemote|companyPageUrl|detailsPageUrl",
            "culture": "en",
            "recommendations": "true",
            "interactionId": "0",
            "fj": "true",
            "includeRemote": "true",
        }

        if query.remote_only:
            params["workFromHomeAvailability"] = "TRUE"

        return f"{self.api_url}?{urlencode(params)}"

    def _build_html_url(self, query: SearchQuery, page: int = 1) -> str:
        """Build HTML fallback URL."""
        params = {
            "q": " ".join(query.keywords) if query.keywords else "",
            "location": query.location,
            "radius": query.radius_miles or 30,
            "page": page,
        }
        return f"{self.base_url}/jobs?{urlencode(params)}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on Dice."""
        # Try API first
        result = self._search_api(query)

        if not result.jobs and result.errors:
            logger.info("API failed, trying HTML scraping")
            result = self._search_html(query)

        return result

    def _search_api(self, query: SearchQuery) -> ScrapeResult:
        """Search using Dice API."""
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
                logger.info(f"Fetching Dice API: page {page}")

                try:
                    # API needs specific headers
                    headers = {
                        "Accept": "application/json",
                        "x-api-key": "1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8",
                        "Origin": "https://www.dice.com",
                        "Referer": "https://www.dice.com/",
                    }
                    response = self._make_request(url, headers=headers)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"API returned {response.status_code}")
                    break

                try:
                    data = response.json()
                except Exception as e:
                    errors.append(f"Failed to parse JSON: {e}")
                    break

                # Get total count
                if pages_scraped == 0:
                    total_found = data.get("meta", {}).get("totalHits", 0)
                    logger.info(f"Dice: Found {total_found} total jobs")

                # Parse jobs
                items = data.get("data", [])
                if not items:
                    break

                for item in items:
                    job = self._parse_api_job(item, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(items) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error querying Dice API: {e}")
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

    def _search_html(self, query: SearchQuery) -> ScrapeResult:
        """Fallback HTML scraping."""
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
                url = self._build_html_url(query, page)
                logger.info(f"Fetching Dice HTML: {url}")

                try:
                    response = self._make_request(url)
                except Exception as e:
                    errors.append(f"Request failed: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job cards
                job_cards = soup.find_all('div', {'data-cy': 'search-result-card'})
                if not job_cards:
                    job_cards = soup.find_all('dhi-search-card')

                if not job_cards:
                    break

                for card in job_cards:
                    job = self._parse_html_job(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped += 1
                page += 1

                if len(job_cards) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error scraping Dice HTML: {e}")
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

    def _parse_api_job(self, item: dict, query: SearchQuery) -> Optional[Job]:
        """Parse a job from API response."""
        try:
            title = item.get("title", "")
            if not title:
                return None

            company_data = item.get("company", {})
            company = company_data.get("name", "Unknown") if isinstance(company_data, dict) else str(company_data)

            # Location
            locations = item.get("locations", [])
            if locations:
                loc = locations[0]
                if isinstance(loc, dict):
                    location = f"{loc.get('city', '')}, {loc.get('state', '')}".strip(", ")
                else:
                    location = str(loc)
            else:
                location = query.location

            # URL
            url = item.get("detailsPageUrl", "")
            if not url:
                job_id = item.get("id", item.get("jobId", ""))
                if job_id:
                    url = f"{self.base_url}/job-detail/{job_id}"

            if not url:
                return None

            # Salary
            salary_min, salary_max, salary_type = None, None, None
            salary_str = item.get("salary", "")
            if salary_str:
                salary_min, salary_max, salary_type = self._parse_salary(salary_str)

            # Job type
            job_type = None
            emp_type = item.get("employmentType", "")
            if emp_type:
                if "full" in emp_type.lower():
                    job_type = "full-time"
                elif "contract" in emp_type.lower():
                    job_type = "contract"
                elif "part" in emp_type.lower():
                    job_type = "part-time"

            # Remote
            remote = item.get("isRemote", False)
            if not remote:
                remote = item.get("workFromHomeAvailability", "") == "TRUE"

            # Posted date
            posted_date = None
            date_str = item.get("postedDate", "")
            if date_str:
                try:
                    posted_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_type=salary_type,
                job_type=job_type,
                remote=remote,
                posted_date=posted_date,
                industry="Technology",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Dice API job: {e}")
            return None

    def _parse_html_job(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse a job from HTML."""
        try:
            title_elem = card.find('a', {'data-cy': 'card-title-link'})
            title = title_elem.get_text(strip=True) if title_elem else None

            url = ""
            if title_elem and title_elem.get('href'):
                href = title_elem.get('href')
                if href.startswith('/'):
                    url = f"{self.base_url}{href}"
                else:
                    url = href

            if not title or not url:
                return None

            company_elem = card.find('a', {'data-cy': 'search-result-company-name'})
            company = company_elem.get_text(strip=True) if company_elem else "Unknown"

            location_elem = card.find('span', {'data-cy': 'search-result-location'})
            location = location_elem.get_text(strip=True) if location_elem else query.location

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                industry="Technology",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Dice HTML job: {e}")
            return None

    def _parse_salary(self, salary_text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Parse salary text."""
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
        return self._parse_api_job(raw_data, query)
