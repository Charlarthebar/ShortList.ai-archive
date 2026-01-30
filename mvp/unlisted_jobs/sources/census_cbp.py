#!/usr/bin/env python3
"""
Census County Business Patterns (CBP) Connector
=================================================

Fetches establishment counts by industry and employment size class from Census CBP API.
This provides the foundation for inferring the "unknown" jobs at private employers.

Key insight: CBP tells us there are X establishments of size Y in industry Z.
We can create synthetic employer archetypes to represent these establishments.

Data source: https://www.census.gov/programs-surveys/cbp.html
API docs: https://www.census.gov/data/developers/data-sets/cbp-zbp/cbp-api.html

Author: ShortList.ai
"""

import os
import json
import requests
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
import time

logger = logging.getLogger(__name__)

# Census CBP API base URL
CBP_API_BASE = "https://api.census.gov/data/2023/cbp"

# Massachusetts FIPS code
MA_FIPS = "25"

# Employment size class definitions
SIZE_CLASSES = {
    "210": {"label": "<5 employees", "min": 1, "max": 4, "midpoint": 2.5},
    "220": {"label": "5-9 employees", "min": 5, "max": 9, "midpoint": 7},
    "230": {"label": "10-19 employees", "min": 10, "max": 19, "midpoint": 15},
    "241": {"label": "20-49 employees", "min": 20, "max": 49, "midpoint": 35},
    "242": {"label": "50-99 employees", "min": 50, "max": 99, "midpoint": 75},
    "251": {"label": "100-249 employees", "min": 100, "max": 249, "midpoint": 175},
    "252": {"label": "250-499 employees", "min": 250, "max": 499, "midpoint": 375},
    "254": {"label": "500-999 employees", "min": 500, "max": 999, "midpoint": 750},
    "260": {"label": "1000+ employees", "min": 1000, "max": 5000, "midpoint": 2000},
}

# NAICS to industry mapping for job inference
NAICS_INDUSTRY_MAP = {
    "11": "agriculture",
    "21": "mining",
    "22": "utilities",
    "23": "construction",
    "31-33": "manufacturing",
    "42": "wholesale",
    "44-45": "retail",
    "48-49": "transportation",
    "51": "information",
    "52": "finance",
    "53": "real_estate",
    "54": "professional_services",
    "55": "management",
    "56": "administrative",
    "61": "education",
    "62": "healthcare",
    "71": "entertainment",
    "72": "hospitality",
    "81": "other_services",
}

# 2-digit NAICS codes to query
NAICS_CODES = [
    "11", "21", "22", "23", "31", "32", "33",  # Goods-producing
    "42", "44", "45", "48", "49",  # Trade/Transportation
    "51", "52", "53", "54", "55", "56",  # Information/Finance/Professional
    "61", "62", "71", "72", "81",  # Education/Healthcare/Services
]


@dataclass
class EstablishmentClass:
    """Represents a class of establishments (e.g., 500 healthcare practices with <5 employees)."""
    naics_code: str
    naics_label: str
    industry: str
    size_class: str
    size_label: str
    num_establishments: int
    total_employment: int
    avg_employees_per_establishment: float
    state: str
    state_fips: str
    data_year: int = 2023


class CensusCBPConnector:
    """
    Connector for Census County Business Patterns API.
    """

    def __init__(self, state_fips: str = MA_FIPS, cache_dir: str = "./data/cache"):
        self.state_fips = state_fips
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _fetch_api(self, params: Dict) -> List[List]:
        """Fetch data from CBP API with caching."""
        # Build URL
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{CBP_API_BASE}?{param_str}"

        # Check cache
        cache_key = url.replace("/", "_").replace(":", "_").replace("?", "_")[-100:]
        cache_path = os.path.join(self.cache_dir, f"cbp_{cache_key}.json")

        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                return json.load(f)

        # Fetch from API
        logger.info(f"Fetching CBP data: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Cache result
        with open(cache_path, "w") as f:
            json.dump(data, f)

        time.sleep(0.5)  # Rate limiting
        return data

    def fetch_industry_size_distribution(self, naics_code: str) -> List[EstablishmentClass]:
        """
        Fetch establishment counts by size class for a specific industry.

        Returns list of EstablishmentClass objects representing each size bucket.
        """
        results = []

        params = {
            "get": "NAME,NAICS2017,NAICS2017_LABEL,EMP,ESTAB,EMPSZES,EMPSZES_LABEL",
            "for": f"state:{self.state_fips}",
            "NAICS2017": naics_code,
        }

        try:
            data = self._fetch_api(params)
        except Exception as e:
            logger.warning(f"Failed to fetch NAICS {naics_code}: {e}")
            return results

        headers = data[0]
        emp_idx = headers.index("EMP")
        estab_idx = headers.index("ESTAB")
        empszes_idx = headers.index("EMPSZES")
        label_idx = headers.index("NAICS2017_LABEL")

        for row in data[1:]:
            size_code = row[empszes_idx]

            # Skip the "all establishments" row
            if size_code == "001":
                continue

            if size_code not in SIZE_CLASSES:
                continue

            emp = int(row[emp_idx]) if row[emp_idx] else 0
            estab = int(row[estab_idx]) if row[estab_idx] else 0

            if estab == 0:
                continue

            size_info = SIZE_CLASSES[size_code]

            # Map NAICS to our industry categories
            industry = "other"
            for prefix, ind in NAICS_INDUSTRY_MAP.items():
                if naics_code.startswith(prefix.split("-")[0]):
                    industry = ind
                    break

            est_class = EstablishmentClass(
                naics_code=naics_code,
                naics_label=row[label_idx],
                industry=industry,
                size_class=size_code,
                size_label=size_info["label"],
                num_establishments=estab,
                total_employment=emp,
                avg_employees_per_establishment=emp / estab if estab > 0 else 0,
                state="Massachusetts",
                state_fips=self.state_fips,
            )

            results.append(est_class)

        return results

    def fetch_all_industries(self) -> List[EstablishmentClass]:
        """Fetch establishment data for all industries."""
        all_results = []

        for naics in NAICS_CODES:
            logger.info(f"Fetching NAICS {naics}...")
            results = self.fetch_industry_size_distribution(naics)
            all_results.extend(results)

        logger.info(f"Fetched {len(all_results)} establishment classes")
        return all_results

    def to_dataframe(self, data: List[EstablishmentClass]) -> pd.DataFrame:
        """Convert establishment classes to DataFrame."""
        records = []
        for est in data:
            records.append({
                "naics_code": est.naics_code,
                "naics_label": est.naics_label,
                "industry": est.industry,
                "size_class": est.size_class,
                "size_label": est.size_label,
                "num_establishments": est.num_establishments,
                "total_employment": est.total_employment,
                "avg_employees": round(est.avg_employees_per_establishment, 1),
                "state": est.state,
                "data_year": est.data_year,
            })
        return pd.DataFrame(records)


def fetch_ma_cbp_data(output_dir: str = "./data/ma_jobs") -> pd.DataFrame:
    """
    Fetch all Massachusetts CBP data and save to CSV.

    Returns DataFrame with establishment counts by industry and size class.
    """
    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("CENSUS COUNTY BUSINESS PATTERNS - MASSACHUSETTS")
    print("=" * 70)

    connector = CensusCBPConnector()

    print("\nFetching establishment data by industry and size class...")
    data = connector.fetch_all_industries()

    df = connector.to_dataframe(data)

    # Summary stats
    total_establishments = df["num_establishments"].sum()
    total_employment = df["total_employment"].sum()

    print(f"\nTotal establishment classes: {len(df)}")
    print(f"Total establishments: {total_establishments:,}")
    print(f"Total employment: {total_employment:,}")

    # By industry
    print("\nBy Industry:")
    by_industry = df.groupby("industry").agg({
        "num_establishments": "sum",
        "total_employment": "sum"
    }).sort_values("total_employment", ascending=False)

    for industry, row in by_industry.iterrows():
        print(f"  {industry:<25} {row['total_employment']:>10,} employees  {row['num_establishments']:>8,} establishments")

    # By size class
    print("\nBy Size Class:")
    by_size = df.groupby("size_label").agg({
        "num_establishments": "sum",
        "total_employment": "sum"
    }).sort_values("total_employment", ascending=False)

    for size, row in by_size.iterrows():
        print(f"  {size:<25} {row['total_employment']:>10,} employees  {row['num_establishments']:>8,} establishments")

    # Save
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "census_cbp_ma.csv")
    df.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")

    return df


if __name__ == "__main__":
    fetch_ma_cbp_data()
