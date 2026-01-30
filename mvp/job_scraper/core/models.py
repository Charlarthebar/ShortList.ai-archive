"""Data models for job scraping."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import hashlib


@dataclass
class Job:
    """Represents a single job listing."""
    title: str
    company: str
    location: str
    platform: str
    url: str

    # Optional fields
    description: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_type: Optional[str] = None  # hourly, yearly, etc.
    job_type: Optional[str] = None  # full-time, part-time, etc.
    posted_date: Optional[datetime] = None
    industry: Optional[str] = None
    remote: bool = False

    # Metadata
    scraped_at: datetime = field(default_factory=datetime.now)
    search_group: Optional[str] = None
    search_term: Optional[str] = None

    @property
    def unique_id(self) -> str:
        """Generate a unique ID for deduplication."""
        # Normalize for comparison
        normalized = f"{self.title.lower().strip()}|{self.company.lower().strip()}|{self.location.lower().strip()}"
        return hashlib.md5(normalized.encode()).hexdigest()

    @property
    def url_hash(self) -> str:
        """Hash of the URL for quick dedup."""
        return hashlib.md5(self.url.encode()).hexdigest()

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "unique_id": self.unique_id,
            "title": self.title,
            "company": self.company,
            "location": self.location,
            "platform": self.platform,
            "url": self.url,
            "description": self.description,
            "salary_min": self.salary_min,
            "salary_max": self.salary_max,
            "salary_type": self.salary_type,
            "job_type": self.job_type,
            "posted_date": self.posted_date.isoformat() if self.posted_date else None,
            "industry": self.industry,
            "remote": self.remote,
            "scraped_at": self.scraped_at.isoformat(),
            "search_group": self.search_group,
            "search_term": self.search_term
        }


@dataclass
class SearchQuery:
    """Represents a search query to execute."""
    location: str
    radius_miles: int = 25
    keywords: Optional[list[str]] = None
    job_type: Optional[str] = None
    remote_only: bool = False

    # Metadata
    group_name: str = ""
    phase: int = 1


@dataclass
class ScrapeResult:
    """Result from a single scrape operation."""
    query: SearchQuery
    platform: str
    jobs: list[Job]
    total_found: int
    pages_scraped: int
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass
class StateResult:
    """Aggregated result for an entire state."""
    state: str
    state_abbrev: str
    total_jobs: int
    unique_jobs: int
    duplicates_removed: int
    coverage_estimate: float
    jobs: list[Job]
    scrape_results: list[ScrapeResult]
    errors: list[str]
    duration_seconds: float
    scraped_at: datetime = field(default_factory=datetime.now)
