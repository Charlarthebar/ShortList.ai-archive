#!/usr/bin/env python3
"""
NPI Registry Healthcare Provider Connector
==========================================

Fetches healthcare provider data from the National Plan and Provider
Enumeration System (NPPES) NPI Registry.

This is a TIER A source (high reliability) because:
- Official CMS government data
- Includes all licensed healthcare providers in the US
- Legally required registration
- Free API with no authentication required

Data Source: https://npiregistry.cms.hhs.gov/api/
Bulk Download: https://download.cms.gov/nppes/NPI_Files.html

The NPI Registry includes:
- Physicians (MDs, DOs)
- Nurses (RNs, NPs, LPNs)
- Pharmacists
- Physical/Occupational Therapists
- Dentists
- Psychologists
- And many more healthcare providers

Estimated MA coverage: ~150,000 providers

Author: ShortList.ai
"""

import os
import logging
import zipfile
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime

from .base import LicensedProfessionalConnector

logger = logging.getLogger(__name__)


# Healthcare Provider Taxonomy mapping (common codes to readable titles)
# Full list: https://taxonomy.nucc.org/
TAXONOMY_TO_TITLE = {
    # Physicians
    '207R00000X': 'Internal Medicine Physician',
    '207Q00000X': 'Family Medicine Physician',
    '207RC0000X': 'Cardiovascular Disease Physician',
    '207RG0100X': 'Gastroenterologist',
    '207RH0000X': 'Hematologist',
    '207RI0200X': 'Infectious Disease Physician',
    '207RN0300X': 'Nephrologist',
    '207RP1001X': 'Pulmonologist',
    '207V00000X': 'Obstetrics & Gynecology Physician',
    '207X00000X': 'Orthopedic Surgeon',
    '208000000X': 'Pediatrician',
    '2084N0400X': 'Neurologist',
    '2084P0800X': 'Psychiatrist',
    '208600000X': 'Surgeon',
    '208D00000X': 'General Practice Physician',
    '208M00000X': 'Hospitalist',
    '207T00000X': 'Neurological Surgeon',
    '208100000X': 'Physical Medicine & Rehabilitation Physician',
    '2086S0120X': 'Pediatric Surgeon',
    '2086S0122X': 'Plastic Surgeon',
    '207W00000X': 'Ophthalmologist',
    '207Y00000X': 'Otolaryngologist',
    '2081P2900X': 'Pain Medicine Physician',
    '207L00000X': 'Anesthesiologist',
    '207K00000X': 'Allergy & Immunology Physician',
    '207N00000X': 'Dermatologist',
    '207P00000X': 'Emergency Medicine Physician',
    '208G00000X': 'Thoracic Surgeon',
    '2086S0102X': 'Vascular Surgeon',

    # Nurses
    '163W00000X': 'Registered Nurse',
    '363L00000X': 'Nurse Practitioner',
    '363A00000X': 'Physician Assistant',
    '164W00000X': 'Licensed Practical Nurse',
    '367500000X': 'Certified Registered Nurse Anesthetist',
    '364S00000X': 'Clinical Nurse Specialist',

    # Pharmacists
    '183500000X': 'Pharmacist',
    '183700000X': 'Pharmacy Technician',

    # Mental Health
    '103T00000X': 'Psychologist',
    '101Y00000X': 'Counselor',
    '104100000X': 'Social Worker',
    '106H00000X': 'Marriage & Family Therapist',

    # Therapy
    '225100000X': 'Physical Therapist',
    '225X00000X': 'Occupational Therapist',
    '231H00000X': 'Audiologist',
    '235Z00000X': 'Speech-Language Pathologist',

    # Dental
    '122300000X': 'Dentist',
    '1223G0001X': 'General Dentist',
    '1223P0221X': 'Pediatric Dentist',
    '1223S0112X': 'Oral Surgeon',
    '1223X0008X': 'Oral & Maxillofacial Pathologist',

    # Other
    '111N00000X': 'Chiropractor',
    '133V00000X': 'Dietitian/Nutritionist',
    '152W00000X': 'Optometrist',
    '156F00000X': 'Podiatrist',
    '174400000X': 'Specialist',
    '146N00000X': 'Emergency Medical Technician',
    '347B00000X': 'Radiology Technician',
    '246Q00000X': 'Medical Laboratory Technician',
}


class NPIRegistryConnector(LicensedProfessionalConnector):
    """
    Connector for NPPES NPI Registry data.

    Provides access to all registered healthcare providers in the US.
    Supports both API queries and bulk download processing.
    """

    SOURCE_NAME = "npi_registry"
    SOURCE_URL = "https://npiregistry.cms.hhs.gov/"
    RELIABILITY_TIER = "A"
    CONFIDENCE_SCORE = 0.90

    # API endpoint (no key required, 10 requests/second max)
    API_BASE = "https://npiregistry.cms.hhs.gov/api/"

    # Bulk download URL (monthly updates, ~8GB compressed)
    BULK_DOWNLOAD_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_"

    def __init__(self, cache_dir: str = None, rate_limit: float = 0.2):
        """
        Initialize NPI connector.

        Args:
            cache_dir: Directory for caching data
            rate_limit: Seconds between API requests (default 0.2 = 5/sec)
        """
        super().__init__(cache_dir or "./data/npi_cache", rate_limit)

    def fetch_data(self, state: str = "MA", limit: Optional[int] = None,
                   use_bulk: bool = False, **kwargs) -> pd.DataFrame:
        """
        Fetch NPI data for a state.

        Args:
            state: Two-letter state code (default: MA)
            limit: Maximum records to fetch (for testing)
            use_bulk: If True, use bulk download instead of API

        Returns:
            DataFrame with NPI provider data
        """
        # Check for pre-downloaded data files first
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        possible_paths = [
            os.path.join(base_dir, "data", f"npi_{state.lower()}_healthcare.csv"),
            os.path.join(base_dir, "data", f"npi_ma_healthcare.csv"),
            os.path.join(base_dir, "data", f"npi_{state.lower()}_healthcare.csv"),
            os.path.join(base_dir, "data", "npi_ma_healthcare.csv"),
        ]

        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Found NPI data at: {path}")
                df = pd.read_csv(path, nrows=limit)
                df = self._normalize_columns(df)
                logger.info(f"Loaded {len(df)} NPI records from file")
                return df

        if use_bulk:
            return self._fetch_bulk_data(state, limit)
        else:
            return self._fetch_via_api(state, limit)

    def _fetch_via_api(self, state: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch NPI data via API.

        Note: API has pagination limit of 1200 results per query.
        For full state coverage, use bulk download instead.
        """
        logger.info(f"Fetching NPI data via API for state: {state}")

        all_records = []
        skip = 0
        page_size = 200  # API max is 200 per request

        # Limit iterations to avoid runaway queries
        max_iterations = (limit or 10000) // page_size + 1

        for i in range(max_iterations):
            params = {
                'version': '2.1',
                'state': state,
                'limit': page_size,
                'skip': skip,
                'enumeration_type': 'NPI-1',  # Individual providers only
            }

            try:
                response = self._get(f"{self.API_BASE}", params=params)
                response.raise_for_status()
                data = response.json()

                results = data.get('results', [])
                if not results:
                    break

                all_records.extend(results)
                logger.info(f"Fetched {len(all_records)} NPI records...")

                if limit and len(all_records) >= limit:
                    all_records = all_records[:limit]
                    break

                skip += page_size

                # Check if we've hit the API result limit
                result_count = data.get('result_count', 0)
                if skip >= result_count:
                    break

            except Exception as e:
                logger.error(f"API error: {e}")
                break

        if not all_records:
            logger.warning("No records fetched from API, using sample data")
            return self._load_sample_data(limit)

        # Convert to DataFrame
        df = self._api_results_to_dataframe(all_records)
        logger.info(f"Fetched {len(df)} NPI records for {state}")
        return df

    def _api_results_to_dataframe(self, results: List[Dict]) -> pd.DataFrame:
        """Convert API results to DataFrame."""
        records = []

        for r in results:
            basic = r.get('basic', {})
            addresses = r.get('addresses', [])
            taxonomies = r.get('taxonomies', [])

            # Get practice location (location_purpose = 'LOCATION')
            practice_addr = next(
                (a for a in addresses if a.get('address_purpose') == 'LOCATION'),
                addresses[0] if addresses else {}
            )

            # Get primary taxonomy
            primary_tax = next(
                (t for t in taxonomies if t.get('primary')),
                taxonomies[0] if taxonomies else {}
            )

            records.append({
                'npi': r.get('number'),
                'first_name': basic.get('first_name', ''),
                'last_name': basic.get('last_name', ''),
                'credential': basic.get('credential', ''),
                'gender': basic.get('gender', ''),
                'sole_proprietor': basic.get('sole_proprietor', ''),
                'enumeration_date': basic.get('enumeration_date'),
                'last_updated': basic.get('last_updated'),
                'status': basic.get('status', 'A'),
                'city': practice_addr.get('city', ''),
                'state': practice_addr.get('state', ''),
                'postal_code': practice_addr.get('postal_code', ''),
                'taxonomy_code': primary_tax.get('code', ''),
                'taxonomy_description': primary_tax.get('desc', ''),
                'license_number': primary_tax.get('license', ''),
                'license_state': primary_tax.get('state', ''),
            })

        return pd.DataFrame(records)

    def _fetch_bulk_data(self, state: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch NPI data from bulk download.

        Note: Bulk file is ~8GB compressed, ~50GB uncompressed.
        Only use this for full national data collection.
        """
        # Check for cached state extract
        cache_path = self._get_cache_path(f"npi_bulk_{state}", ".csv")

        if self._is_cache_valid(cache_path, max_age_days=30):
            logger.info(f"Loading cached bulk data from {cache_path}")
            df = pd.read_csv(cache_path, nrows=limit, low_memory=False)
            return self._normalize_bulk_columns(df)

        logger.warning("Bulk download not yet cached.")
        logger.info("For bulk data, download from: https://download.cms.gov/nppes/NPI_Files.html")
        logger.info("Using API method instead...")

        return self._fetch_via_api(state, limit)

    def _normalize_bulk_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize bulk download column names."""
        column_mapping = {
            'NPI': 'npi',
            'Provider First Name': 'first_name',
            'Provider Last Name (Legal Name)': 'last_name',
            'Provider Credential Text': 'credential',
            'Provider Gender Code': 'gender',
            'Provider Business Practice Location Address City Name': 'city',
            'Provider Business Practice Location Address State Name': 'state',
            'Provider Business Practice Location Address Postal Code': 'postal_code',
            'Healthcare Provider Taxonomy Code_1': 'taxonomy_code',
            'Provider License Number_1': 'license_number',
            'Provider License Number State Code_1': 'license_state',
            'NPI Enumeration Date': 'enumeration_date',
            'Last Update Date': 'last_updated',
        }

        rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
        return df.rename(columns=rename_dict)

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names (API data is already normalized)."""
        return df

    def to_standard_format(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert NPI data to standard format."""
        records = []

        for idx, row in df.iterrows():
            # Skip inactive providers
            if str(row.get('status', 'A')).upper() != 'A':
                continue

            # Map taxonomy to job title
            taxonomy_code = str(row.get('taxonomy_code', ''))
            job_title = TAXONOMY_TO_TITLE.get(
                taxonomy_code,
                row.get('taxonomy_description', 'Healthcare Provider')
            )

            # Build full name
            first = str(row.get('first_name', '')).strip()
            last = str(row.get('last_name', '')).strip()
            credential = str(row.get('credential', '')).strip()
            full_name = f"{first} {last}".strip()
            if credential:
                full_name = f"{full_name}, {credential}"

            record = {
                'raw_company': None,  # NPI doesn't reliably include employer
                'raw_location': self._format_location(row),
                'raw_title': job_title,
                'raw_description': row.get('taxonomy_description'),
                'raw_salary_min': None,  # NPI doesn't include salary
                'raw_salary_max': None,
                'raw_salary_text': None,
                'source_url': self.SOURCE_URL,
                'source_document_id': f"npi_{row.get('npi', idx)}",
                'as_of_date': self._parse_date(row.get('last_updated', row.get('enumeration_date'))),
                'raw_data': {
                    'npi': row.get('npi'),
                    'full_name': full_name,
                    'credential': credential,
                    'taxonomy_code': taxonomy_code,
                    'taxonomy_description': row.get('taxonomy_description'),
                    'license_number': row.get('license_number'),
                    'license_state': row.get('license_state'),
                    'gender': row.get('gender'),
                    'sole_proprietor': row.get('sole_proprietor'),
                },
                'confidence_score': self.CONFIDENCE_SCORE,
                'job_status': 'filled',
            }

            records.append(record)

        logger.info(f"Converted {len(records)} NPI records to standard format")
        return records

    def _load_sample_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load sample NPI data for testing."""
        logger.info("Creating sample NPI data...")

        sample_data = {
            'npi': [
                '1234567890', '1234567891', '1234567892', '1234567893',
                '1234567894', '1234567895', '1234567896', '1234567897',
                '1234567898', '1234567899',
            ],
            'first_name': [
                'John', 'Jane', 'Robert', 'Maria', 'David',
                'Sarah', 'Michael', 'Emily', 'James', 'Jennifer',
            ],
            'last_name': [
                'Smith', 'Johnson', 'Williams', 'Brown', 'Jones',
                'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
            ],
            'credential': [
                'MD', 'MD', 'DO', 'NP', 'PA-C',
                'RN', 'PharmD', 'DPT', 'PsyD', 'DMD',
            ],
            'gender': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F'],
            'city': [
                'Boston', 'Cambridge', 'Worcester', 'Springfield', 'Lowell',
                'Boston', 'Cambridge', 'Somerville', 'Newton', 'Brookline',
            ],
            'state': ['MA'] * 10,
            'postal_code': [
                '02101', '02139', '01608', '01103', '01851',
                '02115', '02140', '02144', '02459', '02445',
            ],
            'taxonomy_code': [
                '207R00000X', '207Q00000X', '208000000X', '363L00000X', '363A00000X',
                '163W00000X', '183500000X', '225100000X', '103T00000X', '122300000X',
            ],
            'taxonomy_description': [
                'Internal Medicine', 'Family Medicine', 'Pediatrics',
                'Nurse Practitioner', 'Physician Assistant',
                'Registered Nurse', 'Pharmacist', 'Physical Therapist',
                'Psychologist', 'Dentist',
            ],
            'license_number': [f'MA{i}12345' for i in range(10)],
            'license_state': ['MA'] * 10,
            'enumeration_date': ['2015-01-15'] * 10,
            'last_updated': ['2024-06-01'] * 10,
            'status': ['A'] * 10,
        }

        df = pd.DataFrame(sample_data)

        if limit:
            df = df.head(limit)

        logger.info(f"Created {len(df)} sample NPI records")
        return df

    def explain_data(self) -> str:
        """Explain NPI data source."""
        return """
NPI REGISTRY - National Provider Identifier System
===================================================

WHAT IT IS:
The National Plan and Provider Enumeration System (NPPES) assigns
unique 10-digit National Provider Identifiers (NPIs) to all healthcare
providers in the United States.

WHAT IT TELLS US:
- Name and credentials of every licensed healthcare provider
- Practice location (city, state, zip)
- Provider type/specialty via taxonomy codes
- License information
- Whether they're actively practicing

COVERAGE:
- ~7 million total NPIs issued
- ~6 million individual providers (NPI Type 1)
- ~150,000 providers in Massachusetts alone

DATA INCLUDES:
- Physicians (MDs, DOs) - ~1.1 million nationally
- Nurse Practitioners - ~350,000
- Registered Nurses - ~4 million (via state boards)
- Pharmacists - ~330,000
- Physical/Occupational Therapists - ~500,000
- Dentists - ~200,000
- Psychologists - ~100,000
- And many more specialties

WHY IT MATTERS FOR FILLED JOBS:
This gives us CONFIRMED filled positions - these are people
currently licensed and practicing in healthcare. Every active
NPI represents a real job that someone holds.

RELIABILITY: TIER A (0.90 confidence)
- Official government data (CMS)
- Legally required registration
- Updated monthly

LIMITATIONS:
- No salary information
- Employer info often missing or incomplete
- Some NPIs are inactive but still in system

SOURCE: https://npiregistry.cms.hhs.gov/
API DOCS: https://npiregistry.cms.hhs.gov/api-page
BULK DATA: https://download.cms.gov/nppes/NPI_Files.html
"""


def demo():
    """Demo the NPI connector."""
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("NPI REGISTRY HEALTHCARE PROVIDER CONNECTOR DEMO")
    print("=" * 60)

    connector = NPIRegistryConnector()

    # Fetch MA providers (limited for demo)
    print("\nFetching MA healthcare providers...")
    df = connector.fetch_data(state="MA", limit=100)

    print(f"\n✓ Loaded {len(df)} records")
    print(f"\nColumns: {list(df.columns)}")

    # Convert to standard format
    print("\nConverting to standard format...")
    records = connector.to_standard_format(df)

    print(f"✓ Converted {len(records)} active provider records")

    # Show sample
    if records:
        print("\n" + "=" * 60)
        print("SAMPLE RECORD:")
        print("=" * 60)
        sample = records[0]
        for key, value in sample.items():
            if key != 'raw_data':
                print(f"{key:20s}: {value}")
        print("\nraw_data:")
        for key, value in sample.get('raw_data', {}).items():
            print(f"  {key:18s}: {value}")

    # Show provider type distribution
    provider_types = {}
    for r in records:
        ptype = r['raw_title']
        provider_types[ptype] = provider_types.get(ptype, 0) + 1

    print("\n" + "=" * 60)
    print("PROVIDER TYPE DISTRIBUTION:")
    print("=" * 60)
    for ptype, count in sorted(provider_types.items(), key=lambda x: x[1], reverse=True)[:15]:
        print(f"{count:3d}  {ptype}")

    # Show locations
    locations = {}
    for r in records:
        loc = r['raw_location']
        if loc:
            locations[loc] = locations.get(loc, 0) + 1

    print("\n" + "=" * 60)
    print("TOP LOCATIONS:")
    print("=" * 60)
    for loc, count in sorted(locations.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"{count:3d}  {loc}")


if __name__ == "__main__":
    demo()
