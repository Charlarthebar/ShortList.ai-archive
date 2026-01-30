"""Greenhouse ATS scraper for tech companies.

Many tech companies use Greenhouse for recruiting. This scraper
handles the standard Greenhouse job board API.

Greenhouse boards have a standard API:
- Jobs list: https://boards-api.greenhouse.io/v1/boards/{company}/jobs
- Job details: https://boards-api.greenhouse.io/v1/boards/{company}/jobs/{id}
"""

import logging
import time
import re
from typing import Optional
from datetime import datetime
from dataclasses import dataclass

from .base import BaseScraper, ScraperRegistry
from core.models import Job, SearchQuery, ScrapeResult

logger = logging.getLogger(__name__)


@dataclass
class GreenhouseEmployer:
    """Configuration for a Greenhouse employer."""
    name: str
    board_token: str
    location_filter: Optional[str] = None


# NC tech companies using Greenhouse
NC_GREENHOUSE_EMPLOYERS = {
    "redhat": GreenhouseEmployer(
        name="Red Hat",
        board_token="redhat",
        location_filter="NC"
    ),
    "epic_games": GreenhouseEmployer(
        name="Epic Games",
        board_token="epicgames",
        location_filter="NC"
    ),
    "pendo": GreenhouseEmployer(
        name="Pendo",
        board_token="pendo",
        location_filter="NC"
    ),
    "bandwidth": GreenhouseEmployer(
        name="Bandwidth",
        board_token="bandwidth",
        location_filter="NC"
    ),
    "allscripts": GreenhouseEmployer(
        name="Allscripts/Veradigm",
        board_token="allscripts",
        location_filter="NC"
    ),
}


class GreenhouseScraper(BaseScraper):
    """Generic scraper for Greenhouse-based career sites."""

    platform_name = "greenhouse"
    api_base = "https://boards-api.greenhouse.io/v1/boards"
    max_results_per_search = 500
    rate_limit_seconds = 1.0

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)

        employer_key = config.get("employer") if config else None
        if employer_key and employer_key in NC_GREENHOUSE_EMPLOYERS:
            self.employer = NC_GREENHOUSE_EMPLOYERS[employer_key]
        else:
            self.employer = None

        if config and "employer_config" in config:
            ec = config["employer_config"]
            self.employer = GreenhouseEmployer(
                name=ec.get("name", "Unknown"),
                board_token=ec["board_token"],
                location_filter=ec.get("location_filter")
            )

    @property
    def base_url(self) -> str:
        if not self.employer:
            return ""
        return f"{self.api_base}/{self.employer.board_token}"

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on Greenhouse career site."""
        if not self.employer:
            return ScrapeResult(
                query=query,
                platform=self.platform_name,
                jobs=[],
                total_found=0,
                pages_scraped=0,
                errors=["No employer configured"],
                duration_seconds=0
            )

        jobs = []
        errors = []
        start_time = time.time()

        try:
            url = f"{self.base_url}/jobs?content=true"
            logger.info(f"Fetching {self.employer.name} jobs from Greenhouse")

            try:
                headers = {"Accept": "application/json"}
                response = self._make_request(url, headers=headers)
            except Exception as e:
                errors.append(f"Request failed: {e}")
                return ScrapeResult(
                    query=query,
                    platform=f"{self.platform_name}_{self.employer.board_token}",
                    jobs=[],
                    total_found=0,
                    pages_scraped=0,
                    errors=errors,
                    duration_seconds=time.time() - start_time
                )

            if response.status_code != 200:
                errors.append(f"HTTP {response.status_code}")
                return ScrapeResult(
                    query=query,
                    platform=f"{self.platform_name}_{self.employer.board_token}",
                    jobs=[],
                    total_found=0,
                    pages_scraped=0,
                    errors=errors,
                    duration_seconds=time.time() - start_time
                )

            try:
                data = response.json()
            except Exception as e:
                errors.append(f"Failed to parse JSON: {e}")
                return ScrapeResult(
                    query=query,
                    platform=f"{self.platform_name}_{self.employer.board_token}",
                    jobs=[],
                    total_found=0,
                    pages_scraped=0,
                    errors=errors,
                    duration_seconds=time.time() - start_time
                )

            job_list = data.get("jobs", [])
            logger.info(f"{self.employer.name}: Found {len(job_list)} total jobs")

            for job_data in job_list:
                job = self._parse_job(job_data, query)
                if job and self._is_nc_job(job):
                    # Apply keyword filter if provided
                    if query.keywords:
                        title_lower = job.title.lower()
                        if any(kw.lower() in title_lower for kw in query.keywords):
                            jobs.append(job)
                    else:
                        jobs.append(job)

        except Exception as e:
            logger.error(f"Error querying {self.employer.name if self.employer else 'Greenhouse'}: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=f"{self.platform_name}_{self.employer.board_token}" if self.employer else self.platform_name,
            jobs=jobs,
            total_found=len(jobs),
            pages_scraped=1,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job(self, job_data: dict, query: SearchQuery) -> Optional[Job]:
        """Parse a Greenhouse job."""
        try:
            title = job_data.get("title", "")
            if not title:
                return None

            # Location
            location_data = job_data.get("location", {})
            if isinstance(location_data, dict):
                location = location_data.get("name", "")
            else:
                location = str(location_data)

            # URL
            url = job_data.get("absolute_url", "")
            if not url:
                job_id = job_data.get("id")
                if job_id:
                    url = f"https://boards.greenhouse.io/{self.employer.board_token}/jobs/{job_id}"

            if not url:
                return None

            # Posted date
            posted_date = None
            updated_at = job_data.get("updated_at", "")
            if updated_at:
                try:
                    posted_date = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                except ValueError:
                    pass

            # Departments
            departments = job_data.get("departments", [])
            dept_names = [d.get("name", "") for d in departments if isinstance(d, dict)]
            dept_str = ", ".join(dept_names) if dept_names else ""

            company = self.employer.name if self.employer else "Unknown"
            if dept_str:
                company = f"{company} - {dept_str}"

            return Job(
                title=title,
                company=company,
                location=location,
                platform=self.platform_name,
                url=url,
                posted_date=posted_date,
                industry="Technology",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Greenhouse job: {e}")
            return None

    def _is_nc_job(self, job: Job) -> bool:
        """Check if job is in NC."""
        if not job.location:
            return False

        location_lower = job.location.lower()
        nc_indicators = [
            "nc", "north carolina", "raleigh", "durham", "cary", "morrisville",
            "chapel hill", "research triangle", "rtp", "charlotte", "remote"
        ]
        return any(ind in location_lower for ind in nc_indicators)


@ScraperRegistry.register
class RedHatScraper(GreenhouseScraper):
    """Red Hat careers scraper (IBM subsidiary, Raleigh HQ)."""
    platform_name = "redhat"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "redhat"
        super().__init__(config)


@ScraperRegistry.register
class EpicGamesScraper(GreenhouseScraper):
    """Epic Games careers scraper (Cary HQ)."""
    platform_name = "epicgames"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "epic_games"
        super().__init__(config)


@ScraperRegistry.register
class PendoScraper(GreenhouseScraper):
    """Pendo careers scraper (Raleigh)."""
    platform_name = "pendo"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "pendo"
        super().__init__(config)


@ScraperRegistry.register
class BandwidthScraper(GreenhouseScraper):
    """Bandwidth careers scraper (Raleigh)."""
    platform_name = "bandwidth"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "bandwidth"
        super().__init__(config)


# SAS Institute uses a custom career site
@ScraperRegistry.register
class SASScraper(BaseScraper):
    """SAS Institute careers scraper (Cary HQ - largest private employer in Triangle)."""

    platform_name = "sas"
    base_url = "https://careers.sas.com"
    api_url = "https://careers.sas.com/api/jobs"
    max_results_per_search = 200
    rate_limit_seconds = 2.0

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute search on SAS careers."""
        from bs4 import BeautifulSoup

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            # Try the main careers page
            url = f"{self.base_url}/search-jobs/north%20carolina"
            logger.info(f"Fetching SAS careers")

            try:
                response = self._make_request(url)
            except Exception as e:
                errors.append(f"Request failed: {e}")
                return ScrapeResult(
                    query=query,
                    platform=self.platform_name,
                    jobs=[],
                    total_found=0,
                    pages_scraped=0,
                    errors=errors,
                    duration_seconds=time.time() - start_time
                )

            if response.status_code != 200:
                errors.append(f"HTTP {response.status_code}")
            else:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings
                job_cards = soup.find_all('li', class_='job-listing')
                if not job_cards:
                    job_cards = soup.find_all('div', class_='job')
                if not job_cards:
                    job_cards = soup.find_all('a', class_='job-link')

                for card in job_cards:
                    job = self._parse_job_card(card, query)
                    if job:
                        jobs.append(job)

                pages_scraped = 1

        except Exception as e:
            logger.error(f"Error scraping SAS: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=self.platform_name,
            jobs=jobs,
            total_found=len(jobs),
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job_card(self, card, query: SearchQuery) -> Optional[Job]:
        """Parse SAS job card."""
        try:
            title_elem = card.find('a') if card.name != 'a' else card
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

            loc_elem = card.find('span', class_='location')
            location = loc_elem.get_text(strip=True) if loc_elem else "Cary, NC"

            return Job(
                title=title,
                company="SAS Institute",
                location=location,
                platform=self.platform_name,
                url=url,
                industry="Technology",
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing SAS job: {e}")
            return None

    def _parse_job(self, raw_data: dict, query: SearchQuery) -> Optional[Job]:
        pass
