"""Workday career site scraper.

Many large employers use Workday for their career sites. This scraper
provides a generic implementation that can be configured for different
Workday-based career sites.

Workday sites have a standard API structure:
- Search: POST https://{company}.wd{n}.myworkdayjobs.com/wday/cxs/{company}/{site}/jobs
- Details: GET https://{company}.wd{n}.myworkdayjobs.com/wday/cxs/{company}/{site}/job/{id}
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
class WorkdayEmployer:
    """Configuration for a Workday employer."""
    name: str
    company_code: str
    site_code: str
    workday_instance: int = 5  # wd1, wd5, etc.
    location_filter: Optional[str] = None  # Filter to NC jobs
    base_domain: str = "myworkdayjobs.com"  # Some employers use myworkdaysite.com


# Major NC employers using Workday
# Note: These endpoints may need periodic updates as companies change their Workday configs
NC_WORKDAY_EMPLOYERS = {
    "bank_of_america": WorkdayEmployer(
        name="Bank of America",
        company_code="ghr",
        site_code="lateral-us",
        workday_instance=1,
        location_filter="North Carolina"
    ),
    "wells_fargo": WorkdayEmployer(
        name="Wells Fargo",
        company_code="wf",
        site_code="WellsFargoJobs",
        workday_instance=1,
        location_filter="NC",
        base_domain="myworkdaysite.com"  # Uses different domain
    ),
    "lowes": WorkdayEmployer(
        name="Lowe's",
        company_code="lowes",
        site_code="LWS_External_CS",
        workday_instance=5,
        location_filter="NC"
    ),
    "atrium_health": WorkdayEmployer(
        name="Atrium Health / Advocate Health",
        company_code="aah",
        site_code="External",
        workday_instance=5,
        location_filter="NC"
    ),
    "truist": WorkdayEmployer(
        name="Truist",
        company_code="truist",
        site_code="Careers",
        workday_instance=1,
        location_filter="NC"
    ),
    "food_lion": WorkdayEmployer(
        name="Food Lion",
        company_code="aholddelhaize",
        site_code="USCareers",
        workday_instance=1,
        location_filter="NC"
    ),
    "spectrum": WorkdayEmployer(
        name="Spectrum/Charter",
        company_code="chartercom",
        site_code="Spectrum",
        workday_instance=5,
        location_filter="NC"
    ),
    "labcorp": WorkdayEmployer(
        name="Labcorp",
        company_code="labcorp",
        site_code="External",
        workday_instance=1,
        location_filter="NC"
    ),
    "fidelity": WorkdayEmployer(
        name="Fidelity Investments",
        company_code="fmr",
        site_code="FidelityCareers",
        workday_instance=1,
        location_filter="NC"
    ),
}


class WorkdayScraper(BaseScraper):
    """Generic scraper for Workday-based career sites."""

    platform_name = "workday"
    max_results_per_search = 500  # Fetch many to filter by state
    results_per_page = 20  # Workday APIs often limit to 20
    rate_limit_seconds = 1.0

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config)

        # Get employer configuration
        employer_key = config.get("employer") if config else None
        if employer_key and employer_key in NC_WORKDAY_EMPLOYERS:
            self.employer = NC_WORKDAY_EMPLOYERS[employer_key]
        else:
            # Default to first employer or use custom config
            self.employer = None

        # Allow custom employer config
        if config and "employer_config" in config:
            ec = config["employer_config"]
            self.employer = WorkdayEmployer(
                name=ec.get("name", "Unknown"),
                company_code=ec["company_code"],
                site_code=ec["site_code"],
                workday_instance=ec.get("workday_instance", 5),
                location_filter=ec.get("location_filter")
            )

    @property
    def base_url(self) -> str:
        if not self.employer:
            return ""
        domain = self.employer.base_domain
        # myworkdaysite.com uses wd{n}.domain, myworkdayjobs.com uses {company}.wd{n}.domain
        if domain == "myworkdaysite.com":
            return f"https://wd{self.employer.workday_instance}.{domain}"
        return f"https://{self.employer.company_code}.wd{self.employer.workday_instance}.{domain}"

    def _get_api_url(self) -> str:
        """Get the Workday API endpoint."""
        return f"{self.base_url}/wday/cxs/{self.employer.company_code}/{self.employer.site_code}/jobs"

    def _get_headers(self) -> dict:
        """Get headers for Workday API requests."""
        # Don't use parent headers - they include browser navigation headers
        # that confuse the Workday API (Sec-Fetch-*, Upgrade-Insecure-Requests, etc.)
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

    def _make_request(self, url: str, method: str = "GET", **kwargs) -> "requests.Response":
        """Override base _make_request to use simpler request handling for Workday API."""
        import requests

        headers = kwargs.pop("headers", None) or self._get_headers()
        timeout = kwargs.pop("timeout", 30)
        kwargs.pop("proxies", None)  # Don't use proxies for Workday

        self._rate_limit()

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            timeout=timeout,
            **kwargs
        )

        if response.status_code != 200:
            logger.warning(f"{self.platform_name}: HTTP {response.status_code} for {url}")

        return response

    def search(self, query: SearchQuery) -> ScrapeResult:
        """Execute a search on Workday career site."""
        if not self.employer:
            return ScrapeResult(
                query=query,
                platform=f"{self.platform_name}",
                jobs=[],
                total_found=0,
                pages_scraped=0,
                errors=["No employer configured"],
                duration_seconds=0
            )

        jobs = []
        errors = []
        pages_scraped = 0
        total_found = 0
        start_time = time.time()

        try:
            offset = 0
            max_results = self.max_results_per_search

            while offset < max_results:
                # Build search payload - Workday API is picky about format
                payload = {
                    "appliedFacets": {},
                    "limit": self.results_per_page,
                    "offset": offset,
                    "searchText": ""
                }

                # Only add keywords to searchText (not location - we filter client-side)
                # Workday's text search on location is unreliable
                if query.keywords:
                    payload["searchText"] = " ".join(query.keywords)

                logger.info(f"Fetching {self.employer.name} jobs: offset {offset}")

                try:
                    response = self._make_request(
                        self._get_api_url(),
                        method="POST",
                        json=payload
                    )
                except Exception as e:
                    errors.append(f"Request failed at offset {offset}: {e}")
                    break

                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    break

                try:
                    data = response.json()
                except Exception as e:
                    errors.append(f"Failed to parse JSON: {e}")
                    break

                # Get total count
                if pages_scraped == 0:
                    total_found = data.get("total", 0)
                    logger.info(f"{self.employer.name}: Found {total_found} total jobs")
                    max_results = min(max_results, total_found)

                # Parse jobs
                job_postings = data.get("jobPostings", [])
                if not job_postings:
                    break

                for posting in job_postings:
                    job = self._parse_job(posting, query)
                    if job:
                        # Filter by NC if location filter didn't work via API
                        if self._is_nc_job(job):
                            jobs.append(job)

                pages_scraped += 1
                offset += self.results_per_page

                if len(job_postings) < self.results_per_page:
                    break

        except Exception as e:
            logger.error(f"Error querying {self.employer.name if self.employer else 'Workday'}: {e}")
            errors.append(str(e))

        duration = time.time() - start_time

        return ScrapeResult(
            query=query,
            platform=f"{self.platform_name}_{self.employer.name.lower().replace(' ', '_')}" if self.employer else self.platform_name,
            jobs=jobs,
            total_found=total_found,
            pages_scraped=pages_scraped,
            errors=errors,
            duration_seconds=duration
        )

    def _parse_job(self, posting: dict, query: SearchQuery) -> Optional[Job]:
        """Parse a Workday job posting."""
        try:
            title = posting.get("title", "")
            if not title:
                return None

            # External path for job URL
            external_path = posting.get("externalPath", "")
            if external_path:
                # myworkdaysite.com uses recruiting/{company}/{site} path
                if self.employer.base_domain == "myworkdaysite.com":
                    url = f"{self.base_url}/recruiting/{self.employer.company_code}/{self.employer.site_code}{external_path}"
                else:
                    url = f"{self.base_url}/en-US{external_path}"
            else:
                url = ""

            if not url:
                return None

            # Location
            location_data = posting.get("locationsText", "")
            location = location_data if location_data else query.location

            # Posted date
            posted_date = None
            posted_on = posting.get("postedOn")
            if posted_on:
                try:
                    posted_date = datetime.strptime(posted_on, "%Y-%m-%d")
                except ValueError:
                    pass

            return Job(
                title=title,
                company=self.employer.name if self.employer else "Unknown",
                location=location,
                platform=self.platform_name,
                url=url,
                posted_date=posted_date,
                search_group=query.group_name,
                search_term=query.location
            )

        except Exception as e:
            logger.debug(f"Error parsing Workday job: {e}")
            return None

    def _is_nc_job(self, job: Job) -> bool:
        """Check if a job is in North Carolina."""
        if not job.location:
            return True  # Include if no location (will be filtered later)

        location_lower = job.location.lower()
        nc_indicators = [
            ", nc", "north carolina", "charlotte", "raleigh", "durham",
            "greensboro", "winston-salem", "fayetteville", "cary", "wilmington",
            "high point", "greenville", "asheville", "concord", "gastonia",
            "jacksonville", "chapel hill", "huntersville", "apex", "wake forest"
        ]
        return any(ind in location_lower for ind in nc_indicators)


# Create individual registered scrapers for each major employer
@ScraperRegistry.register
class BankOfAmericaScraper(WorkdayScraper):
    """Bank of America careers scraper."""
    platform_name = "bofa"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "bank_of_america"
        super().__init__(config)


@ScraperRegistry.register
class WellsFargoScraper(WorkdayScraper):
    """Wells Fargo careers scraper."""
    platform_name = "wellsfargo"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "wells_fargo"
        super().__init__(config)


@ScraperRegistry.register
class LowesScraper(WorkdayScraper):
    """Lowe's careers scraper."""
    platform_name = "lowes"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "lowes"
        super().__init__(config)


@ScraperRegistry.register
class AtriumHealthScraper(WorkdayScraper):
    """Atrium Health careers scraper."""
    platform_name = "atrium"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "atrium_health"
        super().__init__(config)


@ScraperRegistry.register
class TruistScraper(WorkdayScraper):
    """Truist Bank careers scraper."""
    platform_name = "truist"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "truist"
        super().__init__(config)


@ScraperRegistry.register
class FidelityScraper(WorkdayScraper):
    """Fidelity Investments careers scraper."""
    platform_name = "fidelity"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        config["employer"] = "fidelity"
        super().__init__(config)
