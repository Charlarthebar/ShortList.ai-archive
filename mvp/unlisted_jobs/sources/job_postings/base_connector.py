#!/usr/bin/env python3
"""
Base ATS Connector
==================

Abstract base class for all ATS (Applicant Tracking System) connectors.
Defines the standard interface for fetching and parsing job postings.

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Dict, Any, Optional
import hashlib
import json

logger = logging.getLogger(__name__)


@dataclass
class JobPosting:
    """
    Standardized job posting data structure.

    All ATS connectors normalize their data to this format before ingestion.
    """
    # Required fields
    external_id: str  # Unique ID from the ATS (stable across fetches)
    title: str
    company_name: str

    # Location fields
    location_raw: str = None
    city: str = None
    state: str = None
    country: str = "United States"
    is_remote: bool = False

    # Job details
    description: str = None
    requirements: str = None
    department: str = None
    employment_type: str = None  # full-time, part-time, contract, etc.

    # Salary information (if available)
    salary_min: float = None
    salary_max: float = None
    salary_currency: str = "USD"
    salary_period: str = "annual"  # annual, hourly, monthly

    # Metadata
    posted_date: datetime = None
    url: str = None
    ats_type: str = None  # greenhouse, lever, etc.

    # Raw data for provenance
    raw_data: Dict[str, Any] = field(default_factory=dict)

    # Tracking
    fetched_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        if self.posted_date:
            data['posted_date'] = self.posted_date.isoformat()
        data['fetched_at'] = self.fetched_at.isoformat()
        return data

    def content_hash(self) -> str:
        """
        Generate content hash for deduplication.

        Used to identify if a posting's content has changed.
        """
        content = f"{self.title}|{self.company_name}|{self.location_raw}|{self.description}"
        return hashlib.md5(content.encode()).hexdigest()[:16]


class BaseATSConnector(ABC):
    """
    Abstract base class for ATS connectors.

    All connectors must implement:
    - fetch_jobs(): Retrieve all active job postings
    - parse_job(): Convert raw ATS data to JobPosting format
    """

    # ATS identifier (must be overridden)
    ATS_TYPE: str = "unknown"

    # Default request settings
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 1.0

    def __init__(self, company_id: str, company_name: str, base_url: str = None):
        """
        Initialize connector for a specific company.

        Args:
            company_id: Unique identifier for the company (used in URLs)
            company_name: Human-readable company name
            base_url: Optional custom base URL (for self-hosted ATS)
        """
        self.company_id = company_id
        self.company_name = company_name
        self.base_url = base_url or self._get_default_base_url()
        self.session = None

    @abstractmethod
    def _get_default_base_url(self) -> str:
        """Return the default base URL for this ATS type."""
        pass

    @abstractmethod
    def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all active job postings from the company's career page.

        Returns:
            List of JobPosting objects
        """
        pass

    @abstractmethod
    def parse_job(self, raw_job: Dict[str, Any]) -> JobPosting:
        """
        Parse raw job data from ATS into standardized JobPosting format.

        Args:
            raw_job: Raw job data from ATS API/page

        Returns:
            Standardized JobPosting object
        """
        pass

    def get_job_url(self, job_id: str) -> str:
        """Generate the public URL for a specific job posting."""
        return f"{self.base_url}/jobs/{job_id}"

    def _extract_location(self, location_str: str) -> Dict[str, str]:
        """
        Extract city, state, country from location string.

        Handles common formats:
        - "San Francisco, CA"
        - "New York, NY, USA"
        - "Remote"
        - "San Francisco, CA - Remote"
        """
        if not location_str:
            return {"city": None, "state": None, "is_remote": False}

        location_str = location_str.strip()
        is_remote = "remote" in location_str.lower()

        # Remove common suffixes
        clean_location = location_str
        for suffix in [" - Remote", " (Remote)", ", Remote", " - Hybrid"]:
            clean_location = clean_location.replace(suffix, "").replace(suffix.lower(), "")

        # Split by comma
        parts = [p.strip() for p in clean_location.split(",")]

        city = None
        state = None

        if len(parts) >= 2:
            city = parts[0]
            state = parts[1].strip()
            # Remove USA/US suffix if present
            if state.upper() in ["USA", "US", "UNITED STATES"]:
                state = parts[-2] if len(parts) > 2 else None
            # Normalize state to 2-letter code
            state = self._normalize_state(state)
        elif len(parts) == 1 and not is_remote:
            city = parts[0]

        return {
            "city": city,
            "state": state,
            "is_remote": is_remote
        }

    def _normalize_state(self, state: str) -> str:
        """Normalize state name to 2-letter code."""
        if not state:
            return None

        state = state.strip().upper()

        # If already 2 letters, return as-is
        if len(state) == 2:
            return state

        # Common state name mappings
        state_map = {
            "CALIFORNIA": "CA",
            "NEW YORK": "NY",
            "TEXAS": "TX",
            "FLORIDA": "FL",
            "WASHINGTON": "WA",
            "MASSACHUSETTS": "MA",
            "ILLINOIS": "IL",
            "PENNSYLVANIA": "PA",
            "OHIO": "OH",
            "GEORGIA": "GA",
            "NORTH CAROLINA": "NC",
            "MICHIGAN": "MI",
            "NEW JERSEY": "NJ",
            "VIRGINIA": "VA",
            "ARIZONA": "AZ",
            "COLORADO": "CO",
            "MINNESOTA": "MN",
            "OREGON": "OR",
            "UTAH": "UT",
            "MARYLAND": "MD",
            "CONNECTICUT": "CT",
            "INDIANA": "IN",
            "TENNESSEE": "TN",
            "MISSOURI": "MO",
            "WISCONSIN": "WI",
            "DISTRICT OF COLUMBIA": "DC",
            "D.C.": "DC",
        }

        return state_map.get(state, state[:2] if len(state) > 2 else state)

    def _parse_salary(self, salary_text: str) -> Dict[str, Any]:
        """
        Parse salary information from text.

        Handles formats like:
        - "$100,000 - $150,000"
        - "$50/hour"
        - "100k - 150k"
        - "$120,000/year"
        """
        import re

        if not salary_text:
            return {"min": None, "max": None, "period": "annual", "currency": "USD"}

        # Remove commas and currency symbols
        text = salary_text.replace(",", "").replace("$", "").strip()

        # Detect period
        period = "annual"
        if "/hour" in text.lower() or "hourly" in text.lower():
            period = "hourly"
        elif "/month" in text.lower() or "monthly" in text.lower():
            period = "monthly"

        # Extract numbers
        numbers = re.findall(r'(\d+(?:\.\d+)?)\s*k?', text.lower())

        salary_min = None
        salary_max = None

        if numbers:
            values = []
            for num in numbers:
                val = float(num)
                # If 'k' suffix or value is small, multiply by 1000
                if val < 1000 and 'k' in text.lower():
                    val *= 1000
                values.append(val)

            if len(values) >= 2:
                salary_min = min(values)
                salary_max = max(values)
            elif len(values) == 1:
                salary_min = values[0]
                salary_max = values[0]

        return {
            "min": salary_min,
            "max": salary_max,
            "period": period,
            "currency": "USD"
        }

    def _clean_html(self, html: str) -> str:
        """Remove HTML tags and clean up text."""
        if not html:
            return None

        import re

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html)

        # Decode common HTML entities
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    def __repr__(self):
        return f"{self.__class__.__name__}(company={self.company_name}, ats={self.ATS_TYPE})"
