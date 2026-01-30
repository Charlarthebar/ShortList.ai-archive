#!/usr/bin/env python3
"""
Base Connector Class
====================

Abstract base class for all data source connectors.
Provides common functionality for fetching, caching, and normalizing data.

All connectors should inherit from this class and implement:
- fetch_data() - Fetch raw data from source
- _normalize_columns() - Map source columns to standard names
- to_standard_format() - Convert to standard record format

Author: ShortList.ai
"""

import os
import logging
import time
import hashlib
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base class for data source connectors.

    Provides:
    - HTTP client with retry logic
    - File caching
    - Rate limiting
    - Standard record format conversion
    """

    # Override these in subclasses
    SOURCE_NAME = "base"
    SOURCE_URL = ""
    RELIABILITY_TIER = "B"  # A, B, or C
    CONFIDENCE_SCORE = 0.80

    def __init__(self, cache_dir: str = None, rate_limit: float = 1.0):
        """
        Initialize connector.

        Args:
            cache_dir: Directory for caching downloaded files
            rate_limit: Seconds between API requests
        """
        self.cache_dir = cache_dir or f"./data/{self.SOURCE_NAME}_cache"
        self.rate_limit = rate_limit
        self._last_request_time = 0

        # Create cache directory
        os.makedirs(self.cache_dir, exist_ok=True)

        # Set up HTTP session with retry logic
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic."""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            'User-Agent': 'ShortList.ai Employment Research Tool/1.0 (Research purposes)'
        })

        return session

    def _rate_limit_wait(self):
        """Wait if needed to respect rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with rate limiting."""
        self._rate_limit_wait()
        return self.session.get(url, timeout=60, **kwargs)

    def _post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request with rate limiting."""
        self._rate_limit_wait()
        return self.session.post(url, timeout=60, **kwargs)

    def _get_cache_path(self, key: str, extension: str = ".csv") -> str:
        """Get cache file path for a given key."""
        # Create a safe filename from the key
        safe_key = hashlib.md5(key.encode()).hexdigest()[:16]
        return os.path.join(self.cache_dir, f"{self.SOURCE_NAME}_{safe_key}{extension}")

    def _is_cache_valid(self, cache_path: str, max_age_days: int = 30) -> bool:
        """Check if cache file exists and is not too old."""
        if not os.path.exists(cache_path):
            return False

        file_age = time.time() - os.path.getmtime(cache_path)
        max_age_seconds = max_age_days * 24 * 60 * 60
        return file_age < max_age_seconds

    def _download_file(self, url: str, dest_path: str) -> bool:
        """Download file with progress logging."""
        try:
            logger.info(f"Downloading from {url}")
            response = self._get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0 and downloaded % (1024 * 1024) == 0:
                        pct = (downloaded / total_size) * 100
                        logger.info(f"Downloaded {pct:.1f}%")

            logger.info(f"Downloaded to {dest_path}")
            return True

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False

    @abstractmethod
    def fetch_data(self, limit: Optional[int] = None, **kwargs) -> pd.DataFrame:
        """
        Fetch data from source.

        Args:
            limit: Optional limit on number of records (for testing)
            **kwargs: Source-specific parameters

        Returns:
            DataFrame with raw source data
        """
        pass

    @abstractmethod
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names to standard format.

        Args:
            df: DataFrame with source-specific columns

        Returns:
            DataFrame with normalized column names
        """
        pass

    def to_standard_format(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to standard record format.

        Standard fields:
        - raw_company: Employer/organization name
        - raw_location: City, State format
        - raw_title: Job title
        - raw_description: Job description (if available)
        - raw_salary_min: Minimum annual salary
        - raw_salary_max: Maximum annual salary
        - raw_salary_text: Original salary string
        - source_url: Source data URL
        - source_document_id: Unique ID for deduplication
        - as_of_date: Date record was current
        - raw_data: Additional fields as dict
        - confidence_score: Data reliability (0-1)
        - job_status: 'filled' or 'active'

        Override this method for source-specific conversion.
        """
        records = []

        for idx, row in df.iterrows():
            record = self._row_to_standard(row, idx)
            if record:
                records.append(record)

        logger.info(f"Converted {len(records)} records to standard format")
        return records

    def _row_to_standard(self, row: pd.Series, idx: int) -> Optional[Dict[str, Any]]:
        """
        Convert a single row to standard format.
        Override in subclasses for source-specific logic.
        """
        return {
            'raw_company': str(row.get('employer_name', '')).strip() or None,
            'raw_location': self._format_location(row),
            'raw_title': str(row.get('job_title', '')).strip() or None,
            'raw_description': None,
            'raw_salary_min': self._parse_salary(row.get('salary_min')),
            'raw_salary_max': self._parse_salary(row.get('salary_max')),
            'raw_salary_text': row.get('salary_text'),
            'source_url': self.SOURCE_URL,
            'source_document_id': f"{self.SOURCE_NAME}_{idx}",
            'as_of_date': datetime.now().date().isoformat(),
            'raw_data': {},
            'confidence_score': self.CONFIDENCE_SCORE,
            'job_status': 'filled',
        }

    def _format_location(self, row: pd.Series) -> Optional[str]:
        """Format location as 'City, State'."""
        city = str(row.get('city', '')).strip()
        state = str(row.get('state', '')).strip()

        if city and state:
            return f"{city}, {state}"
        elif city:
            return city
        elif state:
            return state
        return None

    def _parse_salary(self, value) -> Optional[float]:
        """Parse salary value to float."""
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            # Remove common formatting
            cleaned = str(value).replace(',', '').replace('$', '').strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_val) -> Optional[str]:
        """Parse date to ISO format string."""
        if date_val is None or pd.isna(date_val):
            return None
        if isinstance(date_val, pd.Timestamp):
            return date_val.date().isoformat()
        if isinstance(date_val, datetime):
            return date_val.date().isoformat()
        try:
            return pd.to_datetime(date_val).date().isoformat()
        except:
            return None

    def filter_by_state(self, df: pd.DataFrame, state: str) -> pd.DataFrame:
        """Filter DataFrame to a specific state."""
        if 'state' not in df.columns:
            logger.warning(f"No 'state' column found, cannot filter by state")
            return df

        state_upper = state.upper()
        mask = df['state'].str.upper() == state_upper
        filtered = df[mask]
        logger.info(f"Filtered to {len(filtered)} records for state {state}")
        return filtered

    def _load_sample_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load sample data for testing when fetch fails.
        Override in subclasses to provide realistic sample data.
        """
        logger.warning(f"Using sample data for {self.SOURCE_NAME}")
        return pd.DataFrame()

    def explain_data(self) -> str:
        """
        Return a human-readable explanation of this data source.
        Override in subclasses.
        """
        return f"""
{self.SOURCE_NAME.upper()} Data Source
{'=' * (len(self.SOURCE_NAME) + 12)}

Source URL: {self.SOURCE_URL}
Reliability Tier: {self.RELIABILITY_TIER}
Confidence Score: {self.CONFIDENCE_SCORE}

Override explain_data() in subclass for detailed documentation.
"""


class GovernmentPayrollConnector(BaseConnector):
    """
    Base class for government payroll connectors.

    Common patterns for federal, state, and local payroll data.
    """

    RELIABILITY_TIER = "A"
    CONFIDENCE_SCORE = 0.95  # Official government data

    def _row_to_standard(self, row: pd.Series, idx: int) -> Optional[Dict[str, Any]]:
        """Convert payroll row to standard format."""
        return {
            'raw_company': str(row.get('department', row.get('agency', ''))).strip() or None,
            'raw_location': self._format_location(row),
            'raw_title': str(row.get('job_title', row.get('position_title', ''))).strip() or None,
            'raw_description': None,
            'raw_salary_min': self._parse_salary(row.get('annual_salary', row.get('salary'))),
            'raw_salary_max': self._parse_salary(row.get('annual_salary', row.get('salary'))),
            'raw_salary_text': str(row.get('annual_salary', '')),
            'source_url': self.SOURCE_URL,
            'source_document_id': self._generate_payroll_id(row, idx),
            'as_of_date': self._parse_date(row.get('pay_year', row.get('fiscal_year'))),
            'raw_data': {
                'employee_name': row.get('employee_name'),
                'department': row.get('department'),
                'agency': row.get('agency'),
            },
            'confidence_score': self.CONFIDENCE_SCORE,
            'job_status': 'filled',
        }

    def _generate_payroll_id(self, row: pd.Series, idx: int) -> str:
        """Generate unique ID for payroll record."""
        components = [
            self.SOURCE_NAME,
            str(row.get('employee_name', ''))[:20],
            str(row.get('job_title', row.get('position_title', '')))[:20],
            str(row.get('pay_year', row.get('fiscal_year', ''))),
        ]
        key = '_'.join(c.replace(' ', '') for c in components if c)
        return hashlib.md5(key.encode()).hexdigest()[:16]


class LicensedProfessionalConnector(BaseConnector):
    """
    Base class for licensed professional connectors.

    Common patterns for healthcare, legal, trades, education licensing data.
    """

    RELIABILITY_TIER = "A"
    CONFIDENCE_SCORE = 0.90  # Official licensing data

    def _row_to_standard(self, row: pd.Series, idx: int) -> Optional[Dict[str, Any]]:
        """Convert licensing row to standard format."""
        # Build name from components
        first = str(row.get('first_name', '')).strip()
        last = str(row.get('last_name', '')).strip()
        full_name = f"{first} {last}".strip()

        return {
            'raw_company': str(row.get('employer_name', row.get('practice_name', ''))).strip() or None,
            'raw_location': self._format_location(row),
            'raw_title': str(row.get('license_type', row.get('credential_type', ''))).strip() or None,
            'raw_description': None,
            'raw_salary_min': None,  # Licensing data typically doesn't include salary
            'raw_salary_max': None,
            'raw_salary_text': None,
            'source_url': self.SOURCE_URL,
            'source_document_id': self._generate_license_id(row, idx),
            'as_of_date': self._parse_date(row.get('issue_date', row.get('last_updated'))),
            'raw_data': {
                'license_number': row.get('license_number'),
                'license_status': row.get('license_status', row.get('status')),
                'expiration_date': self._parse_date(row.get('expiration_date')),
                'full_name': full_name if full_name else None,
                'specialty': row.get('specialty', row.get('taxonomy_description')),
            },
            'confidence_score': self.CONFIDENCE_SCORE,
            'job_status': 'filled',
        }

    def _generate_license_id(self, row: pd.Series, idx: int) -> str:
        """Generate unique ID for license record."""
        license_num = str(row.get('license_number', ''))
        state = str(row.get('state', ''))
        license_type = str(row.get('license_type', ''))

        if license_num and state:
            key = f"{self.SOURCE_NAME}_{state}_{license_num}"
        else:
            key = f"{self.SOURCE_NAME}_{idx}"

        return hashlib.md5(key.encode()).hexdigest()[:16]


class NonprofitConnector(BaseConnector):
    """
    Base class for nonprofit data connectors (990 filings).
    """

    RELIABILITY_TIER = "B"
    CONFIDENCE_SCORE = 0.80  # Tax filing data, some inference involved

    def _row_to_standard(self, row: pd.Series, idx: int) -> Optional[Dict[str, Any]]:
        """Convert nonprofit row to standard format."""
        return {
            'raw_company': str(row.get('organization_name', row.get('org_name', ''))).strip() or None,
            'raw_location': self._format_location(row),
            'raw_title': str(row.get('position_title', row.get('title', ''))).strip() or None,
            'raw_description': None,
            'raw_salary_min': self._parse_salary(row.get('compensation', row.get('total_compensation'))),
            'raw_salary_max': self._parse_salary(row.get('compensation', row.get('total_compensation'))),
            'raw_salary_text': str(row.get('compensation', '')),
            'source_url': self.SOURCE_URL,
            'source_document_id': self._generate_nonprofit_id(row, idx),
            'as_of_date': self._parse_date(row.get('tax_period', row.get('filing_year'))),
            'raw_data': {
                'ein': row.get('ein'),
                'is_officer': row.get('is_officer', False),
                'hours_per_week': row.get('hours_per_week'),
                'total_employees': row.get('total_employees'),
            },
            'confidence_score': self.CONFIDENCE_SCORE,
            'job_status': 'filled',
        }

    def _generate_nonprofit_id(self, row: pd.Series, idx: int) -> str:
        """Generate unique ID for nonprofit record."""
        ein = str(row.get('ein', ''))
        title = str(row.get('position_title', row.get('title', '')))[:20]
        year = str(row.get('filing_year', row.get('tax_period', '')))

        key = f"{self.SOURCE_NAME}_{ein}_{title}_{year}"
        return hashlib.md5(key.encode()).hexdigest()[:16]
