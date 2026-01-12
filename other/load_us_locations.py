#!/usr/bin/env python3
"""
US Locations Loader
==================

Utility to load comprehensive list of US ZIP codes and cities for processing.
This can be used to generate a list of all locations to scrape.
"""

import csv
import json
import requests
from typing import List, Dict
import pandas as pd


def load_us_zip_codes_from_csv(csv_path: str = "us_zip_codes.csv") -> List[str]:
    """Load US ZIP codes from a CSV file.

    Expected CSV format: zip_code, city, state, latitude, longitude
    You can download US ZIP code data from:
    - https://www.unitedstateszipcodes.org/zip-code-database/
    - https://simplemaps.com/data/us-zips
    """
    zip_codes = []
    try:
        df = pd.read_csv(csv_path)
        if 'zip_code' in df.columns:
            zip_codes = df['zip_code'].astype(str).tolist()
        elif 'zip' in df.columns:
            zip_codes = df['zip'].astype(str).tolist()
        else:
            # Assume first column is ZIP code
            zip_codes = df.iloc[:, 0].astype(str).tolist()

        # Filter to valid 5-digit ZIP codes
        zip_codes = [z.zfill(5) for z in zip_codes if z.isdigit() and len(z) <= 5]
        return list(set(zip_codes))  # Remove duplicates
    except FileNotFoundError:
        print(f"CSV file {csv_path} not found. Using sample ZIP codes.")
        return get_sample_zip_codes()
    except Exception as e:
        print(f"Error loading ZIP codes: {e}")
        return get_sample_zip_codes()


def get_sample_zip_codes() -> List[str]:
    """Return a sample of US ZIP codes for testing."""
    return [
        # Major cities
        "10001", "10002", "10003",  # New York, NY
        "90210", "90211", "90001",  # Los Angeles, CA
        "60601", "60602", "60603",  # Chicago, IL
        "77001", "77002", "77003",  # Houston, TX
        "33101", "33102", "33109",  # Miami, FL
        "98101", "98102", "98103",  # Seattle, WA
        "80201", "80202", "80203",  # Denver, CO
        "30301", "30302", "30303",  # Atlanta, GA
        "02139", "02138", "02140",  # Cambridge, MA
        "02459", "02460", "02461",  # Newton, MA
        "19101", "19102", "19103",  # Philadelphia, PA
        "94102", "94103", "94104",  # San Francisco, CA
        "78701", "78702", "78703",  # Austin, TX
        "02101", "02102", "02103",  # Boston, MA
        "20001", "20002", "20003",  # Washington, DC
    ]


def load_us_cities() -> List[Dict[str, str]]:
    """Load list of major US cities.

    Returns list of dicts with 'city' and 'state' keys.
    """
    major_cities = [
        {"city": "New York", "state": "NY"},
        {"city": "Los Angeles", "state": "CA"},
        {"city": "Chicago", "state": "IL"},
        {"city": "Houston", "state": "TX"},
        {"city": "Phoenix", "state": "AZ"},
        {"city": "Philadelphia", "state": "PA"},
        {"city": "San Antonio", "state": "TX"},
        {"city": "San Diego", "state": "CA"},
        {"city": "Dallas", "state": "TX"},
        {"city": "San Jose", "state": "CA"},
        {"city": "Austin", "state": "TX"},
        {"city": "Jacksonville", "state": "FL"},
        {"city": "Fort Worth", "state": "TX"},
        {"city": "Columbus", "state": "OH"},
        {"city": "Charlotte", "state": "NC"},
        {"city": "San Francisco", "state": "CA"},
        {"city": "Indianapolis", "state": "IN"},
        {"city": "Seattle", "state": "WA"},
        {"city": "Denver", "state": "CO"},
        {"city": "Washington", "state": "DC"},
        {"city": "Boston", "state": "MA"},
        {"city": "El Paso", "state": "TX"},
        {"city": "Detroit", "state": "MI"},
        {"city": "Nashville", "state": "TN"},
        {"city": "Portland", "state": "OR"},
        {"city": "Oklahoma City", "state": "OK"},
        {"city": "Las Vegas", "state": "NV"},
        {"city": "Memphis", "state": "TN"},
        {"city": "Louisville", "state": "KY"},
        {"city": "Baltimore", "state": "MD"},
    ]
    return major_cities


def get_all_us_states() -> List[str]:
    """Return list of all US state abbreviations."""
    return [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    ]


if __name__ == "__main__":
    # Example usage
    print("Loading US locations...")

    # Try to load from CSV, fallback to samples
    zip_codes = load_us_zip_codes_from_csv()
    print(f"Loaded {len(zip_codes)} ZIP codes")

    # Save to JSON for easy import
    with open("us_locations.json", "w") as f:
        json.dump({
            "zip_codes": zip_codes,
            "cities": load_us_cities(),
            "states": get_all_us_states()
        }, f, indent=2)

    print("Saved locations to us_locations.json")
