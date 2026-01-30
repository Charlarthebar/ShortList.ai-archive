#!/usr/bin/env python3
"""
Populate Metro Areas and Map Locations
======================================

Populates the metro_areas table from OEWS data and maps existing
locations to their corresponding metros based on city/state matching.

Author: ShortList.ai
Date: 2026-01-13
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

# Setup environment for database
os.environ['DB_USER'] = os.environ.get('DB_USER', 'noahhopkins')

from database import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# State abbreviation to full name mapping
STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
    'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam'
}


def populate_metros_from_oews(conn):
    """Populate metro_areas table from oews_areas."""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get metro areas from OEWS (areatype_code = 'M')
    cur.execute("""
        SELECT area_code, area_name, state_code
        FROM oews_areas
        WHERE areatype_code = 'M'
    """)

    metros = cur.fetchall()
    logger.info(f"Found {len(metros)} metro areas in OEWS")

    inserted = 0
    for metro in metros:
        # Extract state from area_name (format: "City-City, ST" or "City-City, ST-ST")
        area_name = metro['area_name']
        state = None
        if ', ' in area_name:
            state_part = area_name.split(', ')[-1]
            # Handle multi-state metros like "NY-NJ-PA"
            state = state_part.split('-')[0].strip()

        try:
            cur.execute("""
                INSERT INTO metro_areas (cbsa_code, name, state)
                VALUES (%s, %s, %s)
                ON CONFLICT (cbsa_code) DO UPDATE SET name = EXCLUDED.name
            """, (metro['area_code'], area_name, state))
            inserted += 1
        except Exception as e:
            logger.warning(f"Error inserting metro {area_name}: {e}")

    conn.commit()
    logger.info(f"Inserted/updated {inserted} metro areas")
    return inserted


def map_locations_to_metros(conn):
    """Map existing locations to their metro areas."""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get all metros with their principal cities
    cur.execute("SELECT id, name, state FROM metro_areas")
    metros = cur.fetchall()

    # Build a lookup of city patterns to metro IDs
    city_to_metro = {}
    for metro in metros:
        metro_name = metro['name']
        metro_id = metro['id']
        metro_state = metro['state']

        # Extract cities from metro name (before the comma)
        if ', ' in metro_name:
            cities_part = metro_name.split(', ')[0]
            cities = [c.strip() for c in cities_part.split('-')]

            for city in cities:
                # Store as (city_lower, state) -> metro_id
                if metro_state:
                    key = (city.lower(), metro_state)
                    city_to_metro[key] = metro_id

    logger.info(f"Built lookup with {len(city_to_metro)} city-state combinations")

    # Get locations without metro_id
    cur.execute("""
        SELECT id, city, state FROM locations
        WHERE metro_id IS NULL AND city IS NOT NULL AND state IS NOT NULL
    """)
    locations = cur.fetchall()
    logger.info(f"Found {len(locations)} locations to map")

    mapped = 0
    for loc in locations:
        city = loc['city'].strip() if loc['city'] else None
        state = loc['state'].strip().upper() if loc['state'] else None

        if not city or not state:
            continue

        # Try exact match first
        key = (city.lower(), state)
        metro_id = city_to_metro.get(key)

        # If no exact match, try fuzzy matching for common variations
        if not metro_id:
            # Try without common suffixes
            city_clean = city.lower()
            for suffix in [' city', ' township', ' village', ' town']:
                if city_clean.endswith(suffix):
                    city_clean = city_clean[:-len(suffix)]
                    key = (city_clean, state)
                    metro_id = city_to_metro.get(key)
                    if metro_id:
                        break

        if metro_id:
            cur.execute("""
                UPDATE locations SET metro_id = %s WHERE id = %s
            """, (metro_id, loc['id']))
            mapped += 1

    conn.commit()
    logger.info(f"Mapped {mapped} locations to metros")
    return mapped


def map_locations_by_state_proximity(conn):
    """
    For unmapped locations, try to map to the largest metro in the same state.
    This is a fallback for locations not in a principal city.
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get the largest metro in each state (by OEWS employment)
    cur.execute("""
        WITH metro_employment AS (
            SELECT
                ma.id as metro_id,
                ma.state,
                ma.name,
                COALESCE(SUM(oe.employment), 0) as total_employment
            FROM metro_areas ma
            LEFT JOIN oews_estimates oe ON ma.cbsa_code = oe.area_code
            WHERE ma.state IS NOT NULL
            GROUP BY ma.id, ma.state, ma.name
        )
        SELECT DISTINCT ON (state)
            metro_id, state, name, total_employment
        FROM metro_employment
        WHERE total_employment > 0
        ORDER BY state, total_employment DESC
    """)

    state_default_metro = {row['state']: row['metro_id'] for row in cur.fetchall()}
    logger.info(f"Found default metros for {len(state_default_metro)} states")

    # Map remaining locations
    cur.execute("""
        SELECT id, state FROM locations
        WHERE metro_id IS NULL AND state IS NOT NULL
    """)
    locations = cur.fetchall()

    mapped = 0
    for loc in locations:
        state = loc['state'].strip().upper() if loc['state'] else None
        if state and state in state_default_metro:
            cur.execute("""
                UPDATE locations SET metro_id = %s WHERE id = %s
            """, (state_default_metro[state], loc['id']))
            mapped += 1

    conn.commit()
    logger.info(f"Mapped {mapped} additional locations using state defaults")
    return mapped


def main():
    config = Config()
    conn = psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password
    )

    try:
        # Step 1: Populate metros from OEWS
        logger.info("Step 1: Populating metro areas from OEWS...")
        metros_inserted = populate_metros_from_oews(conn)

        # Step 2: Map locations to metros by city name
        logger.info("\nStep 2: Mapping locations to metros by city name...")
        mapped_by_city = map_locations_to_metros(conn)

        # Step 3: Map remaining locations by state default
        logger.info("\nStep 3: Mapping remaining locations by state default...")
        mapped_by_state = map_locations_by_state_proximity(conn)

        # Summary
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM metro_areas")
        total_metros = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM locations WHERE metro_id IS NOT NULL")
        locations_with_metro = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM locations")
        total_locations = cur.fetchone()[0]

        print("\n" + "=" * 60)
        print("METRO POPULATION SUMMARY")
        print("=" * 60)
        print(f"Metro areas created:      {total_metros:,}")
        print(f"Locations mapped:         {locations_with_metro:,} / {total_locations:,}")
        print(f"  - By city name:         {mapped_by_city:,}")
        print(f"  - By state default:     {mapped_by_state:,}")
        print(f"Coverage:                 {locations_with_metro/total_locations*100:.1f}%")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
