#!/usr/bin/env python3
"""
CBP-Based Full Coverage Job Inference
======================================

Uses Census County Business Patterns data to achieve near-complete coverage
of Massachusetts employment by creating synthetic employer archetypes for
establishments not in our known employer database.

Approach:
1. Load CBP data (employees by industry and size class)
2. Subtract known employers (observed + already inferred)
3. Create synthetic archetypes for remaining establishments
4. Apply occupation distributions and BLS salary data

This gives us ~100% coverage of private sector employment with confidence
scores that reflect the statistical nature of the inference.

Author: ShortList.ai
"""

import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from dataclasses import dataclass
import logging
import requests

logger = logging.getLogger(__name__)

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

# NAICS to industry category mapping
NAICS_TO_INDUSTRY = {
    "11": "agriculture",
    "21": "mining",
    "22": "utilities",
    "23": "construction",
    "31-33": "manufacturing",
    "42": "wholesale",
    "44-45": "retail",
    "48-49": "transportation",
    "51": "technology",  # Information
    "52": "finance",
    "53": "real_estate",
    "54": "professional_services",
    "55": "management",
    "56": "administrative",
    "61": "higher_education",  # Educational services
    "62": "healthcare",
    "71": "entertainment",
    "72": "hospitality",
    "81": "other",  # Other services
    "99": "other",  # Unclassified
}

# Industry-occupation distribution (what % of each industry is each occupation)
INDUSTRY_OCCUPATION_MIX = {
    'healthcare': {
        'registered_nurses': 0.22,
        'physicians': 0.06,
        'medical_technicians': 0.12,
        'healthcare_support': 0.18,
        'administrative': 0.15,
        'management': 0.05,
        'facilities': 0.05,
        'other_healthcare': 0.17,
    },
    'higher_education': {
        'faculty': 0.22,
        'research_staff': 0.15,
        'administrative': 0.20,
        'student_services': 0.10,
        'facilities': 0.10,
        'it_staff': 0.08,
        'management': 0.05,
        'other_education': 0.10,
    },
    'technology': {
        'software_developers': 0.35,
        'data_scientists': 0.08,
        'product_managers': 0.08,
        'designers': 0.05,
        'sales': 0.12,
        'management': 0.08,
        'administrative': 0.10,
        'support': 0.06,
        'other_tech': 0.08,
    },
    'finance': {
        'financial_analysts': 0.15,
        'accountants': 0.12,
        'software_developers': 0.12,
        'customer_service': 0.12,
        'sales': 0.10,
        'management': 0.10,
        'administrative': 0.15,
        'compliance': 0.08,
        'other_finance': 0.06,
    },
    'professional_services': {
        'consultants': 0.25,
        'software_developers': 0.15,
        'accountants': 0.12,
        'lawyers': 0.10,
        'management': 0.10,
        'administrative': 0.12,
        'sales': 0.08,
        'other_professional': 0.08,
    },
    'manufacturing': {
        'production_workers': 0.40,
        'engineers': 0.15,
        'technicians': 0.12,
        'quality_control': 0.08,
        'management': 0.08,
        'administrative': 0.08,
        'logistics': 0.05,
        'other_manufacturing': 0.04,
    },
    'retail': {
        'sales_associates': 0.45,
        'cashiers': 0.15,
        'stock_handlers': 0.12,
        'management': 0.10,
        'customer_service': 0.08,
        'administrative': 0.05,
        'other_retail': 0.05,
    },
    'hospitality': {
        'food_service': 0.40,
        'cooks': 0.20,
        'servers': 0.15,
        'management': 0.08,
        'housekeeping': 0.07,
        'front_desk': 0.05,
        'other_hospitality': 0.05,
    },
    'construction': {
        'construction_workers': 0.45,
        'carpenters': 0.12,
        'electricians': 0.10,
        'plumbers': 0.08,
        'management': 0.08,
        'administrative': 0.08,
        'engineers': 0.05,
        'other_construction': 0.04,
    },
    'transportation': {
        'drivers': 0.40,
        'warehouse_workers': 0.25,
        'logistics': 0.10,
        'management': 0.08,
        'administrative': 0.08,
        'mechanics': 0.05,
        'other_transport': 0.04,
    },
    'administrative': {
        'administrative_assistants': 0.30,
        'customer_service': 0.20,
        'data_entry': 0.15,
        'security': 0.10,
        'janitors': 0.10,
        'management': 0.08,
        'other_admin': 0.07,
    },
    'wholesale': {
        'sales_reps': 0.30,
        'warehouse_workers': 0.25,
        'logistics': 0.15,
        'administrative': 0.12,
        'management': 0.10,
        'other_wholesale': 0.08,
    },
    'real_estate': {
        'real_estate_agents': 0.35,
        'property_managers': 0.20,
        'administrative': 0.18,
        'maintenance': 0.12,
        'management': 0.08,
        'other_realestate': 0.07,
    },
    'entertainment': {
        'performers': 0.20,
        'recreation_workers': 0.25,
        'customer_service': 0.15,
        'food_service': 0.15,
        'management': 0.10,
        'administrative': 0.08,
        'other_entertainment': 0.07,
    },
    'utilities': {
        'technicians': 0.35,
        'engineers': 0.20,
        'operators': 0.15,
        'management': 0.10,
        'administrative': 0.10,
        'customer_service': 0.05,
        'other_utilities': 0.05,
    },
    'management': {
        'executives': 0.30,
        'managers': 0.25,
        'analysts': 0.20,
        'administrative': 0.15,
        'it_staff': 0.05,
        'other_management': 0.05,
    },
    'other': {
        'service_workers': 0.30,
        'administrative': 0.20,
        'technicians': 0.15,
        'sales': 0.15,
        'management': 0.10,
        'other_general': 0.10,
    },
    'agriculture': {
        'farm_workers': 0.50,
        'equipment_operators': 0.20,
        'management': 0.10,
        'administrative': 0.10,
        'other_ag': 0.10,
    },
    'mining': {
        'extraction_workers': 0.40,
        'equipment_operators': 0.25,
        'engineers': 0.15,
        'management': 0.10,
        'other_mining': 0.10,
    },
}

# Occupation to SOC code and salary data
OCCUPATION_SALARY_DATA = {
    # Healthcare
    'registered_nurses': {'soc': '29-1141', 'title': 'Registered Nurses', 'p10': 63000, 'median': 89000, 'p90': 116000},
    'physicians': {'soc': '29-1216', 'title': 'Physicians', 'p10': 120000, 'median': 220000, 'p90': 350000},
    'medical_technicians': {'soc': '29-2000', 'title': 'Medical Technicians', 'p10': 38000, 'median': 55000, 'p90': 80000},
    'healthcare_support': {'soc': '31-0000', 'title': 'Healthcare Support Workers', 'p10': 30000, 'median': 38000, 'p90': 52000},
    'other_healthcare': {'soc': '29-0000', 'title': 'Healthcare Workers', 'p10': 35000, 'median': 55000, 'p90': 90000},

    # Technology
    'software_developers': {'soc': '15-1252', 'title': 'Software Developers', 'p10': 80000, 'median': 130000, 'p90': 190000},
    'data_scientists': {'soc': '15-2051', 'title': 'Data Scientists', 'p10': 75000, 'median': 120000, 'p90': 180000},
    'product_managers': {'soc': '15-1299', 'title': 'Product Managers', 'p10': 85000, 'median': 140000, 'p90': 200000},
    'designers': {'soc': '27-1024', 'title': 'Graphic Designers', 'p10': 45000, 'median': 65000, 'p90': 100000},
    'support': {'soc': '15-1232', 'title': 'Computer Support Specialists', 'p10': 45000, 'median': 65000, 'p90': 95000},
    'it_staff': {'soc': '15-1299', 'title': 'IT Specialists', 'p10': 55000, 'median': 85000, 'p90': 130000},
    'other_tech': {'soc': '15-0000', 'title': 'Technology Workers', 'p10': 55000, 'median': 90000, 'p90': 150000},

    # Finance
    'financial_analysts': {'soc': '13-2051', 'title': 'Financial Analysts', 'p10': 55000, 'median': 95000, 'p90': 160000},
    'accountants': {'soc': '13-2011', 'title': 'Accountants', 'p10': 50000, 'median': 78000, 'p90': 130000},
    'compliance': {'soc': '13-1041', 'title': 'Compliance Officers', 'p10': 50000, 'median': 75000, 'p90': 120000},
    'other_finance': {'soc': '13-0000', 'title': 'Financial Workers', 'p10': 45000, 'median': 70000, 'p90': 120000},

    # Education
    'faculty': {'soc': '25-1000', 'title': 'Faculty/Professors', 'p10': 50000, 'median': 85000, 'p90': 180000},
    'research_staff': {'soc': '19-0000', 'title': 'Research Scientists', 'p10': 55000, 'median': 85000, 'p90': 140000},
    'student_services': {'soc': '21-1012', 'title': 'Student Services', 'p10': 38000, 'median': 55000, 'p90': 80000},
    'other_education': {'soc': '25-0000', 'title': 'Education Workers', 'p10': 35000, 'median': 55000, 'p90': 90000},

    # Professional Services
    'consultants': {'soc': '13-1111', 'title': 'Consultants', 'p10': 60000, 'median': 100000, 'p90': 180000},
    'lawyers': {'soc': '23-1011', 'title': 'Lawyers', 'p10': 75000, 'median': 145000, 'p90': 250000},
    'other_professional': {'soc': '13-0000', 'title': 'Professional Workers', 'p10': 50000, 'median': 80000, 'p90': 140000},

    # Manufacturing
    'production_workers': {'soc': '51-0000', 'title': 'Production Workers', 'p10': 32000, 'median': 45000, 'p90': 65000},
    'engineers': {'soc': '17-2000', 'title': 'Engineers', 'p10': 70000, 'median': 100000, 'p90': 150000},
    'technicians': {'soc': '17-3000', 'title': 'Technicians', 'p10': 45000, 'median': 65000, 'p90': 95000},
    'quality_control': {'soc': '51-9061', 'title': 'Quality Control', 'p10': 35000, 'median': 48000, 'p90': 70000},
    'other_manufacturing': {'soc': '51-0000', 'title': 'Manufacturing Workers', 'p10': 32000, 'median': 45000, 'p90': 65000},

    # Retail
    'sales_associates': {'soc': '41-2031', 'title': 'Sales Associates', 'p10': 25000, 'median': 32000, 'p90': 48000},
    'cashiers': {'soc': '41-2011', 'title': 'Cashiers', 'p10': 24000, 'median': 30000, 'p90': 38000},
    'stock_handlers': {'soc': '53-7065', 'title': 'Stock Handlers', 'p10': 28000, 'median': 35000, 'p90': 48000},
    'other_retail': {'soc': '41-0000', 'title': 'Retail Workers', 'p10': 25000, 'median': 35000, 'p90': 50000},

    # Hospitality
    'food_service': {'soc': '35-0000', 'title': 'Food Service Workers', 'p10': 24000, 'median': 32000, 'p90': 45000},
    'cooks': {'soc': '35-2014', 'title': 'Cooks', 'p10': 28000, 'median': 38000, 'p90': 55000},
    'servers': {'soc': '35-3031', 'title': 'Servers', 'p10': 22000, 'median': 30000, 'p90': 45000},
    'housekeeping': {'soc': '37-2012', 'title': 'Housekeeping', 'p10': 26000, 'median': 32000, 'p90': 42000},
    'front_desk': {'soc': '43-4081', 'title': 'Front Desk', 'p10': 28000, 'median': 35000, 'p90': 48000},
    'other_hospitality': {'soc': '35-0000', 'title': 'Hospitality Workers', 'p10': 25000, 'median': 33000, 'p90': 45000},

    # Construction
    'construction_workers': {'soc': '47-0000', 'title': 'Construction Workers', 'p10': 35000, 'median': 52000, 'p90': 80000},
    'carpenters': {'soc': '47-2031', 'title': 'Carpenters', 'p10': 40000, 'median': 58000, 'p90': 85000},
    'electricians': {'soc': '47-2111', 'title': 'Electricians', 'p10': 45000, 'median': 68000, 'p90': 100000},
    'plumbers': {'soc': '47-2152', 'title': 'Plumbers', 'p10': 45000, 'median': 65000, 'p90': 95000},
    'other_construction': {'soc': '47-0000', 'title': 'Construction Workers', 'p10': 35000, 'median': 52000, 'p90': 80000},

    # Transportation
    'drivers': {'soc': '53-3032', 'title': 'Truck Drivers', 'p10': 35000, 'median': 52000, 'p90': 75000},
    'warehouse_workers': {'soc': '53-7062', 'title': 'Warehouse Workers', 'p10': 30000, 'median': 40000, 'p90': 55000},
    'logistics': {'soc': '13-1081', 'title': 'Logistics Specialists', 'p10': 42000, 'median': 62000, 'p90': 95000},
    'mechanics': {'soc': '49-3023', 'title': 'Mechanics', 'p10': 38000, 'median': 55000, 'p90': 78000},
    'other_transport': {'soc': '53-0000', 'title': 'Transportation Workers', 'p10': 32000, 'median': 45000, 'p90': 65000},

    # Administrative
    'administrative_assistants': {'soc': '43-6014', 'title': 'Administrative Assistants', 'p10': 35000, 'median': 48000, 'p90': 68000},
    'customer_service': {'soc': '43-4051', 'title': 'Customer Service Reps', 'p10': 30000, 'median': 40000, 'p90': 55000},
    'data_entry': {'soc': '43-9021', 'title': 'Data Entry Keyers', 'p10': 30000, 'median': 38000, 'p90': 50000},
    'security': {'soc': '33-9032', 'title': 'Security Guards', 'p10': 30000, 'median': 38000, 'p90': 52000},
    'janitors': {'soc': '37-2011', 'title': 'Janitors', 'p10': 28000, 'median': 35000, 'p90': 48000},
    'other_admin': {'soc': '43-0000', 'title': 'Administrative Workers', 'p10': 32000, 'median': 42000, 'p90': 60000},

    # General
    'management': {'soc': '11-0000', 'title': 'Managers', 'p10': 60000, 'median': 100000, 'p90': 180000},
    'administrative': {'soc': '43-0000', 'title': 'Administrative Staff', 'p10': 35000, 'median': 48000, 'p90': 70000},
    'facilities': {'soc': '37-0000', 'title': 'Facilities Staff', 'p10': 30000, 'median': 42000, 'p90': 60000},
    'sales': {'soc': '41-0000', 'title': 'Sales Workers', 'p10': 30000, 'median': 55000, 'p90': 120000},
    'sales_reps': {'soc': '41-4012', 'title': 'Sales Representatives', 'p10': 40000, 'median': 70000, 'p90': 130000},

    # Other
    'service_workers': {'soc': '39-0000', 'title': 'Service Workers', 'p10': 26000, 'median': 35000, 'p90': 50000},
    'farm_workers': {'soc': '45-2092', 'title': 'Farm Workers', 'p10': 28000, 'median': 35000, 'p90': 48000},
    'equipment_operators': {'soc': '53-7032', 'title': 'Equipment Operators', 'p10': 35000, 'median': 48000, 'p90': 68000},
    'operators': {'soc': '51-8000', 'title': 'Plant Operators', 'p10': 45000, 'median': 65000, 'p90': 95000},
    'extraction_workers': {'soc': '47-5000', 'title': 'Extraction Workers', 'p10': 40000, 'median': 55000, 'p90': 80000},
    'performers': {'soc': '27-2000', 'title': 'Performers/Artists', 'p10': 28000, 'median': 50000, 'p90': 100000},
    'recreation_workers': {'soc': '39-9032', 'title': 'Recreation Workers', 'p10': 26000, 'median': 35000, 'p90': 52000},
    'real_estate_agents': {'soc': '41-9022', 'title': 'Real Estate Agents', 'p10': 30000, 'median': 55000, 'p90': 120000},
    'property_managers': {'soc': '11-9141', 'title': 'Property Managers', 'p10': 45000, 'median': 65000, 'p90': 100000},
    'maintenance': {'soc': '49-9071', 'title': 'Maintenance Workers', 'p10': 35000, 'median': 48000, 'p90': 68000},
    'executives': {'soc': '11-1011', 'title': 'Executives', 'p10': 100000, 'median': 180000, 'p90': 350000},
    'managers': {'soc': '11-1021', 'title': 'General Managers', 'p10': 60000, 'median': 100000, 'p90': 160000},
    'analysts': {'soc': '13-1111', 'title': 'Business Analysts', 'p10': 55000, 'median': 85000, 'p90': 130000},
    'other_general': {'soc': '00-0000', 'title': 'Other Workers', 'p10': 30000, 'median': 45000, 'p90': 70000},
    'other_ag': {'soc': '45-0000', 'title': 'Agricultural Workers', 'p10': 28000, 'median': 35000, 'p90': 50000},
    'other_mining': {'soc': '47-5000', 'title': 'Mining Workers', 'p10': 40000, 'median': 55000, 'p90': 80000},
    'other_utilities': {'soc': '51-8000', 'title': 'Utility Workers', 'p10': 45000, 'median': 65000, 'p90': 95000},
    'other_management': {'soc': '11-0000', 'title': 'Management Workers', 'p10': 60000, 'median': 100000, 'p90': 160000},
    'other_wholesale': {'soc': '41-0000', 'title': 'Wholesale Workers', 'p10': 35000, 'median': 50000, 'p90': 80000},
    'other_realestate': {'soc': '41-0000', 'title': 'Real Estate Workers', 'p10': 35000, 'median': 50000, 'p90': 80000},
    'other_entertainment': {'soc': '27-0000', 'title': 'Entertainment Workers', 'p10': 28000, 'median': 42000, 'p90': 70000},
}

# Massachusetts cities for geographic distribution (weighted by population)
MA_CITIES = [
    ("Boston", 0.25),
    ("Cambridge", 0.08),
    ("Worcester", 0.06),
    ("Springfield", 0.05),
    ("Lowell", 0.04),
    ("Quincy", 0.03),
    ("Newton", 0.03),
    ("Somerville", 0.03),
    ("Brookline", 0.02),
    ("Framingham", 0.02),
    ("Waltham", 0.02),
    ("Malden", 0.02),
    ("Medford", 0.02),
    ("Lynn", 0.02),
    ("Brockton", 0.02),
    ("Salem", 0.02),
    ("Other MA", 0.27),  # Remainder
]


def fetch_cbp_data() -> pd.DataFrame:
    """Fetch CBP establishment data by industry and size class."""
    print("Fetching Census CBP data for Massachusetts...")

    base_url = "https://api.census.gov/data/2023/cbp"
    results = []

    # Fetch data for each industry
    all_naics = list(NAICS_TO_INDUSTRY.keys())

    for naics in all_naics:
        url = f"{base_url}?get=NAME,NAICS2017,NAICS2017_LABEL,EMP,ESTAB,EMPSZES,EMPSZES_LABEL&for=state:25&NAICS2017={naics}"

        try:
            response = requests.get(url, timeout=30)
            data = response.json()

            headers = data[0]
            emp_idx = headers.index("EMP")
            estab_idx = headers.index("ESTAB")
            empszes_idx = headers.index("EMPSZES")
            label_idx = headers.index("NAICS2017_LABEL")

            for row in data[1:]:
                size_code = row[empszes_idx]
                if size_code not in SIZE_CLASSES:
                    continue

                emp = int(row[emp_idx]) if row[emp_idx] else 0
                estab = int(row[estab_idx]) if row[estab_idx] else 0

                if estab == 0:
                    continue

                results.append({
                    "naics_code": naics,
                    "naics_label": row[label_idx],
                    "industry": NAICS_TO_INDUSTRY.get(naics, "other"),
                    "size_class": size_code,
                    "size_label": SIZE_CLASSES[size_code]["label"],
                    "size_min": SIZE_CLASSES[size_code]["min"],
                    "size_max": SIZE_CLASSES[size_code]["max"],
                    "size_midpoint": SIZE_CLASSES[size_code]["midpoint"],
                    "num_establishments": estab,
                    "total_employment": emp,
                })

        except Exception as e:
            logger.warning(f"Failed to fetch NAICS {naics}: {e}")

    df = pd.DataFrame(results)
    print(f"  Fetched {len(df)} establishment classes, {df['total_employment'].sum():,} total employees")
    return df


def subtract_known_employers(cbp_df: pd.DataFrame, observed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Subtract known/observed employers from CBP totals to avoid double counting.

    Returns adjusted CBP data with remaining "unknown" establishments.
    """
    # Summarize observed jobs by industry
    if 'industry' in observed_df.columns:
        observed_by_industry = observed_df.groupby('industry')['estimated_headcount'].sum() if 'estimated_headcount' in observed_df.columns else observed_df.groupby('industry').size()
    else:
        observed_by_industry = pd.Series(dtype=int)

    # For simplicity, reduce CBP totals proportionally by observed coverage
    adjusted = cbp_df.copy()

    for idx, row in adjusted.iterrows():
        industry = row['industry']
        observed_count = observed_by_industry.get(industry, 0)
        cbp_total = cbp_df[cbp_df['industry'] == industry]['total_employment'].sum()

        if cbp_total > 0:
            # Calculate remaining percentage (cap at 0)
            remaining_pct = max(0, 1 - (observed_count / cbp_total))
            adjusted.at[idx, 'total_employment'] = int(row['total_employment'] * remaining_pct)

    return adjusted


def generate_synthetic_jobs(cbp_df: pd.DataFrame, subtract_observed: int = 0) -> pd.DataFrame:
    """
    Generate synthetic job records from CBP establishment data.

    Creates job archetypes for each industry/size class combination,
    distributed across occupations and locations.
    """
    print("Generating synthetic job archetypes from CBP data...")

    all_jobs = []
    np.random.seed(42)  # Reproducibility

    for _, est_row in cbp_df.iterrows():
        industry = est_row['industry']
        total_emp = int(est_row['total_employment'])
        num_establishments = est_row['num_establishments']
        size_label = est_row['size_label']
        naics_label = est_row['naics_label']

        if total_emp < 1:
            continue

        # Get occupation mix for this industry
        occ_mix = INDUSTRY_OCCUPATION_MIX.get(industry, INDUSTRY_OCCUPATION_MIX['other'])

        # Generate jobs for each occupation
        for occupation, pct in occ_mix.items():
            occ_emp = int(total_emp * pct)
            if occ_emp < 1:
                continue

            # Get salary data
            salary_data = OCCUPATION_SALARY_DATA.get(occupation, {
                'soc': '00-0000',
                'title': occupation.replace('_', ' ').title(),
                'p10': 35000,
                'median': 50000,
                'p90': 80000
            })

            # Distribute across cities
            for city, city_pct in MA_CITIES:
                city_emp = int(occ_emp * city_pct)
                if city_emp < 1:
                    continue

                # Confidence scoring
                # - Size class confidence: larger = more accurate (public data usually for big companies)
                size_conf = 0.30 + (est_row['size_min'] / 1000) * 0.3  # 0.30 - 0.60
                size_conf = min(0.60, size_conf)

                # - Salary confidence from BLS data
                salary_conf = 0.85

                # - Location confidence (we're distributing statistically)
                location_conf = 0.50

                # - Overall confidence
                overall_conf = size_conf * salary_conf * location_conf * 0.8  # Additional uncertainty factor

                all_jobs.append({
                    'employer_name': f"[{naics_label[:30]}] {size_label}",
                    'city': city,
                    'state': 'MA',
                    'job_title': salary_data['title'],
                    'soc_code': salary_data['soc'],
                    'occupation': occupation,
                    'estimated_headcount': city_emp,
                    'salary_min': salary_data['p10'],
                    'salary_median': salary_data['median'],
                    'salary_max': salary_data['p90'],
                    'salary_confidence': round(salary_conf, 3),
                    'location_confidence': round(location_conf, 3),
                    'overall_confidence': round(overall_conf, 3),
                    'industry': industry,
                    'size_class': size_label,
                    'record_type': 'cbp_synthetic',
                    'source': f'census_cbp_2023:{est_row["naics_code"]}',
                })

    df = pd.DataFrame(all_jobs)
    print(f"  Generated {len(df)} synthetic job archetypes")
    print(f"  Total synthetic positions: {df['estimated_headcount'].sum():,}")

    return df


def expand_archetypes_to_individuals(archetypes_df: pd.DataFrame,
                                      max_per_archetype: int = 50) -> pd.DataFrame:
    """
    Expand job archetypes into individual job records.

    Limits expansion to keep dataset manageable while maintaining
    proportions for analysis.
    """
    print("Expanding archetypes to individual records...")

    np.random.seed(42)
    records = []

    for _, row in archetypes_df.iterrows():
        n_expand = min(row['estimated_headcount'], max_per_archetype)

        for _ in range(n_expand):
            # Vary salary within range
            salary_std = (row['salary_max'] - row['salary_min']) / 4
            salary = np.random.normal(row['salary_median'], salary_std)
            salary = max(row['salary_min'], min(row['salary_max'], salary))

            records.append({
                'employer_name': row['employer_name'],
                'city': row['city'],
                'state': row['state'],
                'job_title': row['job_title'],
                'soc_code': row['soc_code'],
                'estimated_salary': round(salary, 0),
                'salary_confidence': row['salary_confidence'],
                'location_confidence': row['location_confidence'],
                'overall_confidence': row['overall_confidence'],
                'industry': row['industry'],
                'record_type': 'cbp_synthetic_individual',
                'source': row['source'],
            })

    df = pd.DataFrame(records)
    print(f"  Expanded to {len(df)} individual records")
    return df


def run_full_cbp_inference(output_dir: str = "./data/ma_jobs") -> Tuple[pd.DataFrame, Dict]:
    """
    Run the full CBP-based inference pipeline.

    Returns:
        Tuple of (jobs DataFrame, statistics dict)
    """
    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("CBP-BASED FULL COVERAGE JOB INFERENCE")
    print("=" * 70)

    # 1. Fetch CBP data
    cbp_df = fetch_cbp_data()

    # 2. Generate synthetic jobs
    synthetic_df = generate_synthetic_jobs(cbp_df)

    # 3. Save archetypes
    os.makedirs(output_dir, exist_ok=True)

    archetype_path = os.path.join(output_dir, "cbp_synthetic_archetypes.csv")
    synthetic_df.to_csv(archetype_path, index=False)
    print(f"\nSaved archetypes to: {archetype_path}")

    # 4. Save CBP raw data
    cbp_path = os.path.join(output_dir, "census_cbp_ma.csv")
    cbp_df.to_csv(cbp_path, index=False)

    # Statistics
    stats = {
        'cbp_total_employees': int(cbp_df['total_employment'].sum()),
        'synthetic_archetypes': len(synthetic_df),
        'synthetic_positions': int(synthetic_df['estimated_headcount'].sum()),
        'avg_confidence': round(synthetic_df['overall_confidence'].mean(), 3),
        'by_industry': synthetic_df.groupby('industry')['estimated_headcount'].sum().to_dict(),
        'by_city': synthetic_df.groupby('city')['estimated_headcount'].sum().to_dict(),
    }

    # Summary
    print("\n" + "=" * 70)
    print("INFERENCE SUMMARY")
    print("=" * 70)

    print(f"\nCBP Total Employees (MA): {stats['cbp_total_employees']:,}")
    print(f"Synthetic Archetypes: {stats['synthetic_archetypes']:,}")
    print(f"Synthetic Positions: {stats['synthetic_positions']:,}")
    print(f"Average Confidence: {stats['avg_confidence']:.1%}")

    print("\nBy Industry (top 10):")
    for industry, count in sorted(stats['by_industry'].items(), key=lambda x: -x[1])[:10]:
        print(f"  {industry:<25} {count:>10,}")

    print("\nBy City (top 10):")
    for city, count in sorted(stats['by_city'].items(), key=lambda x: -x[1])[:10]:
        print(f"  {city:<25} {count:>10,}")

    return synthetic_df, stats


if __name__ == "__main__":
    run_full_cbp_inference()
