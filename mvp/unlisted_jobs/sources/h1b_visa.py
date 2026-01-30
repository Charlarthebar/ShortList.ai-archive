#!/usr/bin/env python3
"""
H-1B Visa Data Connector
=========================

Fetches H-1B visa application data from the US Department of Labor.

This is a TIER A source (high reliability) because:
- Official government data
- Includes actual salaries (prevailing wage or offered wage)
- Includes company names, job titles, and locations
- Legally required filings, audited data

Data Source: https://www.dol.gov/agencies/eta/foreign-labor/performance
Alternative: https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub

The H-1B program requires employers to file Labor Condition Applications (LCAs)
which include:
- Employer name and address
- Job title
- Prevailing wage or actual wage
- Work location
- SOC code (occupation code)

This gives us HIGH-QUALITY observed data for tech/skilled positions.

Author: ShortList.ai
"""

import os
import logging
import requests
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import zipfile
import io

logger = logging.getLogger(__name__)


class H1BVisaConnector:
    """
    Connector for H-1B visa LCA disclosure data.

    Downloads and processes H-1B Labor Condition Application data
    from the Department of Labor.
    """

    # DOL disclosure data URLs (updated annually)
    # These are the official H-1B LCA disclosure datasets
    DATA_URLS = {
        2024: "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2024_Q4.xlsx",
        2023: "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2023.xlsx",
        2022: "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY2022.xlsx",
    }

    # Alternative: Use the H-1B Employer Data Hub API
    # https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub

    def __init__(self, cache_dir: str = "./data/h1b_cache"):
        """
        Initialize H-1B connector.

        Args:
            cache_dir: Directory to cache downloaded files
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def fetch_year(self, year: int = 2024, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch H-1B data for a specific year.

        Args:
            year: Fiscal year (2022, 2023, 2024)
            limit: Optional limit on number of records (for testing)

        Returns:
            DataFrame with normalized columns
        """
        if year not in self.DATA_URLS:
            raise ValueError(f"Year {year} not available. Available: {list(self.DATA_URLS.keys())}")

        url = self.DATA_URLS[year]
        cache_file = os.path.join(self.cache_dir, f"h1b_{year}.xlsx")

        # Check cache first
        if os.path.exists(cache_file):
            logger.info(f"Loading H-1B data from cache: {cache_file}")
            df = pd.read_excel(cache_file, nrows=limit)
        else:
            logger.info(f"Downloading H-1B data for {year}...")
            logger.info(f"URL: {url}")
            logger.warning("This may take a few minutes (file is ~100MB)...")

            try:
                # Download with progress
                response = requests.get(url, stream=True, timeout=300)
                response.raise_for_status()

                # Save to cache
                with open(cache_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info(f"Downloaded and cached to {cache_file}")

                # Load from cache
                df = pd.read_excel(cache_file, nrows=limit)

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download H-1B data: {e}")
                logger.info("Trying alternative method: sample data...")
                return self._load_sample_data(limit)

        logger.info(f"Loaded {len(df)} H-1B records for {year}")

        # Normalize column names (they vary by year)
        df = self._normalize_columns(df, year)

        return df

    def _normalize_columns(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Normalize column names across different years.

        The DOL changes column names slightly each year.
        """
        # Common column mappings
        column_mapping = {
            # Employer
            'EMPLOYER_NAME': 'employer_name',
            'Employer Name': 'employer_name',
            'TRADE_NAME_DBA': 'employer_dba',

            # Job title
            'JOB_TITLE': 'job_title',
            'Job Title': 'job_title',

            # Location
            'WORKSITE_CITY': 'city',
            'Worksite City': 'city',
            'WORKSITE_STATE': 'state',
            'Worksite State': 'state',
            'WORKSITE_POSTAL_CODE': 'zip_code',

            # Salary
            'WAGE_RATE_OF_PAY_FROM': 'wage_from',
            'Wage Rate of Pay From': 'wage_from',
            'WAGE_RATE_OF_PAY_TO': 'wage_to',
            'Wage Rate of Pay To': 'wage_to',
            'WAGE_UNIT_OF_PAY': 'wage_unit',
            'Wage Unit of Pay': 'wage_unit',
            'PREVAILING_WAGE': 'prevailing_wage',
            'Prevailing Wage': 'prevailing_wage',

            # SOC code (occupation)
            'SOC_CODE': 'soc_code',
            'SOC Code': 'soc_code',
            'SOC_TITLE': 'soc_title',
            'SOC Title': 'soc_title',

            # Dates
            'RECEIVED_DATE': 'received_date',
            'Decision Date': 'decision_date',
            'CASE_STATUS': 'case_status',
            'Case Status': 'case_status',

            # Full-time position
            'FULL_TIME_POSITION': 'full_time',
            'Full Time Position': 'full_time',
        }

        # Rename columns that exist
        rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
        df = df.rename(columns=rename_dict)

        return df

    def to_standard_format(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert H-1B DataFrame to standard source format.

        Returns:
            List of dicts in standard format for pipeline ingestion
        """
        records = []

        for idx, row in df.iterrows():
            # Convert salary to annual
            salary_min, salary_max = self._normalize_salary(
                row.get('wage_from'),
                row.get('wage_to'),
                row.get('wage_unit', 'Year'),
                row.get('prevailing_wage')
            )

            # Only include certified applications
            if row.get('case_status', '').upper() != 'CERTIFIED':
                continue

            # Skip if missing critical fields
            if pd.isna(row.get('employer_name')) or pd.isna(row.get('job_title')):
                continue

            record = {
                'raw_company': str(row.get('employer_name', '')).strip(),
                'raw_location': f"{row.get('city', '')}, {row.get('state', '')}".strip(', '),
                'raw_title': str(row.get('job_title', '')).strip(),
                'raw_description': None,  # H-1B data doesn't include descriptions
                'raw_salary_min': salary_min,
                'raw_salary_max': salary_max,
                'raw_salary_text': f"{row.get('wage_from')} {row.get('wage_unit', 'Year')}",
                'source_url': 'https://www.dol.gov/agencies/eta/foreign-labor/performance',
                'source_document_id': f"h1b_{row.get('received_date', 'unknown')}_{idx}",
                'as_of_date': self._parse_date(row.get('received_date')),
                'raw_data': {
                    'soc_code': row.get('soc_code'),
                    'soc_title': row.get('soc_title'),
                    'full_time': row.get('full_time'),
                    'case_status': row.get('case_status'),
                    'city': row.get('city'),
                    'state': row.get('state'),
                    'zip_code': row.get('zip_code'),
                }
            }

            records.append(record)

        logger.info(f"Converted {len(records)} H-1B records to standard format")
        return records

    def _normalize_salary(self, wage_from, wage_to, wage_unit, prevailing_wage) -> tuple:
        """
        Normalize salary to annual amounts.

        Returns:
            (salary_min, salary_max) as annual amounts
        """
        # Use prevailing wage as fallback
        if pd.isna(wage_from):
            wage_from = prevailing_wage
        if pd.isna(wage_to):
            wage_to = wage_from

        if pd.isna(wage_from):
            return (None, None)

        # Convert to float
        try:
            wage_from = float(wage_from)
            wage_to = float(wage_to) if not pd.isna(wage_to) else wage_from
        except (ValueError, TypeError):
            return (None, None)

        # Convert to annual
        wage_unit = str(wage_unit).upper() if not pd.isna(wage_unit) else 'YEAR'

        if 'HOUR' in wage_unit:
            # Hourly to annual (assume 40hrs/week, 52 weeks)
            wage_from = wage_from * 40 * 52
            wage_to = wage_to * 40 * 52
        elif 'WEEK' in wage_unit:
            wage_from = wage_from * 52
            wage_to = wage_to * 52
        elif 'MONTH' in wage_unit:
            wage_from = wage_from * 12
            wage_to = wage_to * 12
        elif 'BI-WEEK' in wage_unit or 'BIWEEK' in wage_unit:
            wage_from = wage_from * 26
            wage_to = wage_to * 26
        # else assume annual

        return (round(wage_from, 2), round(wage_to, 2))

    def _parse_date(self, date_val):
        """Parse date string or timestamp to ISO format string."""
        if pd.isna(date_val):
            return None
        if isinstance(date_val, pd.Timestamp):
            return date_val.date().isoformat()
        if isinstance(date_val, datetime):
            return date_val.date().isoformat()
        try:
            return pd.to_datetime(date_val).date().isoformat()
        except:
            return None

    def _load_sample_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load sample H-1B data for testing when download fails.
        """
        logger.info("Creating sample H-1B data for testing...")

        sample_data = {
            'EMPLOYER_NAME': [
                'Google LLC',
                'Microsoft Corporation',
                'Amazon.com Services LLC',
                'Meta Platforms Inc',
                'Apple Inc',
                'Massachusetts Institute of Technology',
                'Harvard University',
                'Brigham and Womens Hospital',
            ],
            'JOB_TITLE': [
                'Software Engineer',
                'Senior Software Engineer',
                'Data Scientist',
                'Product Manager',
                'Machine Learning Engineer',
                'Research Scientist',
                'Postdoctoral Research Fellow',
                'Physician',
            ],
            'WORKSITE_CITY': [
                'Mountain View',
                'Redmond',
                'Seattle',
                'Menlo Park',
                'Cupertino',
                'Cambridge',
                'Cambridge',
                'Boston',
            ],
            'WORKSITE_STATE': [
                'CA', 'WA', 'WA', 'CA', 'CA', 'MA', 'MA', 'MA'
            ],
            'WAGE_RATE_OF_PAY_FROM': [
                150000, 145000, 135000, 160000, 155000, 85000, 65000, 220000
            ],
            'WAGE_RATE_OF_PAY_TO': [
                180000, 175000, 160000, 190000, 185000, 95000, 75000, 280000
            ],
            'WAGE_UNIT_OF_PAY': [
                'Year', 'Year', 'Year', 'Year', 'Year', 'Year', 'Year', 'Year'
            ],
            'PREVAILING_WAGE': [
                140000, 135000, 125000, 150000, 145000, 80000, 60000, 200000
            ],
            'SOC_CODE': [
                '15-1252', '15-1252', '15-2051', '11-2032', '15-1252', '19-1029', '19-1029', '29-1216'
            ],
            'SOC_TITLE': [
                'Software Developers', 'Software Developers', 'Data Scientists',
                'Product Managers', 'Software Developers', 'Research Scientists',
                'Research Scientists', 'Physicians'
            ],
            'CASE_STATUS': ['CERTIFIED'] * 8,
            'RECEIVED_DATE': ['2024-01-15'] * 8,
            'FULL_TIME_POSITION': ['Y'] * 8,
        }

        df = pd.DataFrame(sample_data)

        if limit:
            df = df.head(limit)

        logger.info(f"Created {len(df)} sample H-1B records")
        return df


def demo():
    """Demo the H-1B connector."""
    logging.basicConfig(level=logging.INFO)

    print("="*60)
    print("H-1B VISA DATA CONNECTOR DEMO")
    print("="*60)

    connector = H1BVisaConnector()

    # Fetch sample data (limit to 100 for demo)
    print("\nFetching H-1B data (this may take a moment)...")
    df = connector.fetch_year(year=2024, limit=100)

    print(f"\n✓ Loaded {len(df)} records")
    print(f"\nColumns: {list(df.columns)}")

    # Convert to standard format
    print("\nConverting to standard format...")
    records = connector.to_standard_format(df)

    print(f"✓ Converted {len(records)} certified records")

    # Show sample
    if records:
        print("\n" + "="*60)
        print("SAMPLE RECORD:")
        print("="*60)
        sample = records[0]
        for key, value in sample.items():
            if key != 'raw_data':
                print(f"{key:20s}: {value}")
        print()

    # Show salary distribution
    salaries = [r['raw_salary_min'] for r in records if r['raw_salary_min']]
    if salaries:
        import statistics
        print("="*60)
        print("SALARY DISTRIBUTION:")
        print("="*60)
        print(f"Count:  {len(salaries)}")
        print(f"Min:    ${min(salaries):,.0f}")
        print(f"Median: ${statistics.median(salaries):,.0f}")
        print(f"Max:    ${max(salaries):,.0f}")
        print()

    # Show top companies
    companies = {}
    for r in records:
        comp = r['raw_company']
        companies[comp] = companies.get(comp, 0) + 1

    print("="*60)
    print("TOP EMPLOYERS:")
    print("="*60)
    for comp, count in sorted(companies.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"{count:3d}  {comp}")


if __name__ == "__main__":
    demo()
