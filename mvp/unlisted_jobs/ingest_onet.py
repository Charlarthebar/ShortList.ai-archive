#!/usr/bin/env python3
"""
O*NET Database Integration Script
==================================

Ingests O*NET 30.1 database to enrich job data with:
- Occupation descriptions
- Skills and knowledge requirements
- Education requirements (Job Zones)
- Alternate titles for matching

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import csv
import logging
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('onet_ingestion.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

ONET_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'onet', 'db_30_1_text')


def read_tsv(filename: str) -> List[Dict]:
    """Read a tab-separated file and return list of dicts."""
    filepath = os.path.join(ONET_DATA_DIR, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        return list(reader)


def create_onet_tables(cursor):
    """Create O*NET reference tables in the database."""
    log.info("Creating O*NET reference tables...")

    # Main occupations table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onet_occupations (
            soc_code VARCHAR(10) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            job_zone INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Alternate titles for matching
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onet_alternate_titles (
            id SERIAL PRIMARY KEY,
            soc_code VARCHAR(10) REFERENCES onet_occupations(soc_code),
            alternate_title VARCHAR(255) NOT NULL,
            short_title VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onet_alt_title ON onet_alternate_titles(alternate_title)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onet_alt_soc ON onet_alternate_titles(soc_code)
    """)

    # Skills by occupation
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onet_skills (
            id SERIAL PRIMARY KEY,
            soc_code VARCHAR(10) REFERENCES onet_occupations(soc_code),
            skill_name VARCHAR(100) NOT NULL,
            importance NUMERIC(4,2),  -- Scale 1-5
            level NUMERIC(4,2),       -- Scale 0-7
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onet_skills_soc ON onet_skills(soc_code)
    """)

    # Knowledge by occupation
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onet_knowledge (
            id SERIAL PRIMARY KEY,
            soc_code VARCHAR(10) REFERENCES onet_occupations(soc_code),
            knowledge_area VARCHAR(100) NOT NULL,
            importance NUMERIC(4,2),
            level NUMERIC(4,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onet_knowledge_soc ON onet_knowledge(soc_code)
    """)

    # Education requirements
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onet_education (
            id SERIAL PRIMARY KEY,
            soc_code VARCHAR(10) REFERENCES onet_occupations(soc_code),
            education_level VARCHAR(100) NOT NULL,
            percentage NUMERIC(5,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onet_edu_soc ON onet_education(soc_code)
    """)

    # Job Zone reference
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onet_job_zones (
            zone INTEGER PRIMARY KEY,
            education VARCHAR(255),
            experience VARCHAR(255),
            training VARCHAR(255),
            examples TEXT
        )
    """)

    log.info("O*NET tables created successfully")


def ingest_occupations(cursor) -> int:
    """Ingest main occupation data."""
    log.info("Ingesting occupation data...")

    # Read occupation data
    occupations = read_tsv('Occupation Data.txt')

    # Read job zones
    job_zones = read_tsv('Job Zones.txt')
    zone_map = {row['O*NET-SOC Code']: int(row['Job Zone']) for row in job_zones}

    count = 0
    for occ in occupations:
        soc_code = occ['O*NET-SOC Code']
        title = occ['Title']
        description = occ['Description']
        job_zone = zone_map.get(soc_code)

        cursor.execute("""
            INSERT INTO onet_occupations (soc_code, title, description, job_zone)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (soc_code) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                job_zone = EXCLUDED.job_zone
        """, (soc_code, title, description, job_zone))
        count += 1

    log.info(f"  Ingested {count} occupations")
    return count


def ingest_alternate_titles(cursor) -> int:
    """Ingest alternate titles for occupation matching."""
    log.info("Ingesting alternate titles...")

    # Clear existing
    cursor.execute("DELETE FROM onet_alternate_titles")

    titles = read_tsv('Alternate Titles.txt')

    count = 0
    for row in titles:
        soc_code = row['O*NET-SOC Code']
        alt_title = row['Alternate Title']
        short_title = row.get('Short Title', '')

        cursor.execute("""
            INSERT INTO onet_alternate_titles (soc_code, alternate_title, short_title)
            VALUES (%s, %s, %s)
        """, (soc_code, alt_title, short_title or None))
        count += 1

    log.info(f"  Ingested {count} alternate titles")
    return count


def ingest_skills(cursor) -> int:
    """Ingest skills data (importance and level scales)."""
    log.info("Ingesting skills data...")

    # Clear existing
    cursor.execute("DELETE FROM onet_skills")

    skills = read_tsv('Skills.txt')

    # Group by SOC code and skill name, get importance (IM) and level (LV) values
    skill_data = defaultdict(dict)
    for row in skills:
        if row.get('Recommend Suppress') == 'Y':
            continue
        soc_code = row['O*NET-SOC Code']
        skill_name = row['Element Name']
        scale_id = row['Scale ID']
        try:
            value = float(row['Data Value'])
        except (ValueError, TypeError):
            continue

        key = (soc_code, skill_name)
        if scale_id == 'IM':
            skill_data[key]['importance'] = value
        elif scale_id == 'LV':
            skill_data[key]['level'] = value

    count = 0
    for (soc_code, skill_name), values in skill_data.items():
        importance = values.get('importance')
        level = values.get('level')

        cursor.execute("""
            INSERT INTO onet_skills (soc_code, skill_name, importance, level)
            VALUES (%s, %s, %s, %s)
        """, (soc_code, skill_name, importance, level))
        count += 1

    log.info(f"  Ingested {count} skill records")
    return count


def ingest_knowledge(cursor) -> int:
    """Ingest knowledge data."""
    log.info("Ingesting knowledge data...")

    # Clear existing
    cursor.execute("DELETE FROM onet_knowledge")

    knowledge = read_tsv('Knowledge.txt')

    # Group by SOC code and knowledge area
    knowledge_data = defaultdict(dict)
    for row in knowledge:
        if row.get('Recommend Suppress') == 'Y':
            continue
        soc_code = row['O*NET-SOC Code']
        area = row['Element Name']
        scale_id = row['Scale ID']
        try:
            value = float(row['Data Value'])
        except (ValueError, TypeError):
            continue

        key = (soc_code, area)
        if scale_id == 'IM':
            knowledge_data[key]['importance'] = value
        elif scale_id == 'LV':
            knowledge_data[key]['level'] = value

    count = 0
    for (soc_code, area), values in knowledge_data.items():
        importance = values.get('importance')
        level = values.get('level')

        cursor.execute("""
            INSERT INTO onet_knowledge (soc_code, knowledge_area, importance, level)
            VALUES (%s, %s, %s, %s)
        """, (soc_code, area, importance, level))
        count += 1

    log.info(f"  Ingested {count} knowledge records")
    return count


def ingest_education(cursor) -> int:
    """Ingest education requirements."""
    log.info("Ingesting education requirements...")

    # Clear existing
    cursor.execute("DELETE FROM onet_education")

    # Education category labels
    edu_categories = {
        '1': 'Less than high school diploma',
        '2': 'High school diploma or equivalent',
        '3': 'Post-secondary certificate',
        '4': 'Some college, no degree',
        '5': "Associate's degree",
        '6': "Bachelor's degree",
        '7': 'Post-baccalaureate certificate',
        '8': "Master's degree",
        '9': 'Post-master\'s certificate',
        '10': 'First professional degree',
        '11': 'Doctoral degree',
        '12': 'Post-doctoral training'
    }

    education = read_tsv('Education, Training, and Experience.txt')

    count = 0
    for row in education:
        if row.get('Element ID') != '2.D.1':  # Only "Required Level of Education"
            continue
        if row.get('Recommend Suppress') == 'Y':
            continue

        soc_code = row['O*NET-SOC Code']
        category = row.get('Category', '')
        edu_level = edu_categories.get(category, f'Unknown ({category})')

        try:
            percentage = float(row['Data Value'])
        except (ValueError, TypeError):
            continue

        if percentage > 0:
            cursor.execute("""
                INSERT INTO onet_education (soc_code, education_level, percentage)
                VALUES (%s, %s, %s)
            """, (soc_code, edu_level, percentage))
            count += 1

    log.info(f"  Ingested {count} education records")
    return count


def ingest_job_zone_reference(cursor):
    """Ingest Job Zone reference data."""
    log.info("Ingesting Job Zone reference data...")

    job_zone_data = [
        (1, 'Little or no preparation needed', 'Little or no previous work-related skill, knowledge, or experience',
         'No previous work experience needed', 'Food preparation workers, dishwashers'),
        (2, 'Some preparation needed', 'Some previous work-related skill, knowledge, or experience usually needed',
         'Usually requires training', 'Customer service reps, tellers, general office clerks'),
        (3, 'Medium preparation needed', 'Previous work-related skill, knowledge, or experience required',
         'May require vocational training', 'Electricians, dental assistants, medical secretaries'),
        (4, 'Considerable preparation needed', 'High level of skill, knowledge, or experience needed',
         'Requires bachelor\'s degree', 'Accountants, graphic designers, most sales managers'),
        (5, 'Extensive preparation needed', 'Extensive skill, knowledge, or experience needed',
         'May require graduate degree', 'Physicians, lawyers, college professors, engineers')
    ]

    cursor.execute("DELETE FROM onet_job_zones")
    for zone, education, experience, training, examples in job_zone_data:
        cursor.execute("""
            INSERT INTO onet_job_zones (zone, education, experience, training, examples)
            VALUES (%s, %s, %s, %s, %s)
        """, (zone, education, experience, training, examples))

    log.info("  Job Zone reference data ingested")


def create_title_to_soc_mapping(cursor):
    """Create a view/function to help match job titles to SOC codes."""
    log.info("Creating title-to-SOC mapping helpers...")

    # Create a materialized view of normalized titles for faster lookup
    cursor.execute("""
        DROP MATERIALIZED VIEW IF EXISTS onet_title_lookup CASCADE
    """)

    cursor.execute("""
        CREATE MATERIALIZED VIEW onet_title_lookup AS
        SELECT
            soc_code,
            LOWER(title) as normalized_title,
            'primary' as title_type
        FROM onet_occupations
        UNION ALL
        SELECT
            soc_code,
            LOWER(alternate_title) as normalized_title,
            'alternate' as title_type
        FROM onet_alternate_titles
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onet_lookup_title ON onet_title_lookup(normalized_title)
    """)

    log.info("  Title lookup materialized view created")


def show_sample_data(cursor):
    """Display sample data to verify ingestion."""
    log.info("\n" + "=" * 60)
    log.info("SAMPLE O*NET DATA")
    log.info("=" * 60)

    # Show sample occupations
    cursor.execute("""
        SELECT soc_code, title, job_zone,
               LEFT(description, 100) || '...' as description_preview
        FROM onet_occupations
        ORDER BY soc_code
        LIMIT 5
    """)
    log.info("\nSample Occupations:")
    for row in cursor.fetchall():
        log.info(f"  {row[0]}: {row[1]} (Zone {row[2]})")

    # Show top skills for Software Developer
    cursor.execute("""
        SELECT skill_name, importance, level
        FROM onet_skills
        WHERE soc_code = '15-1252.00'
        ORDER BY importance DESC
        LIMIT 5
    """)
    log.info("\nTop Skills for Software Developers (15-1252.00):")
    for row in cursor.fetchall():
        log.info(f"  {row[0]}: importance={row[1]}, level={row[2]}")

    # Show education requirements for Registered Nurses
    cursor.execute("""
        SELECT education_level, percentage
        FROM onet_education
        WHERE soc_code = '29-1141.00'
        AND percentage > 5
        ORDER BY percentage DESC
    """)
    log.info("\nEducation Requirements for Registered Nurses (29-1141.00):")
    for row in cursor.fetchall():
        log.info(f"  {row[0]}: {row[1]:.1f}%")

    # Show title matching potential
    cursor.execute("""
        SELECT COUNT(DISTINCT soc_code) as occ_count,
               COUNT(*) as total_titles
        FROM onet_title_lookup
    """)
    row = cursor.fetchone()
    log.info(f"\nTitle Lookup: {row[1]:,} titles mapping to {row[0]} occupations")


def main():
    """Main function to ingest O*NET data."""
    log.info("=" * 60)
    log.info("O*NET DATABASE INTEGRATION")
    log.info("=" * 60)

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        # Create tables
        create_onet_tables(cursor)
        conn.commit()

        # Ingest data
        occ_count = ingest_occupations(cursor)
        conn.commit()

        title_count = ingest_alternate_titles(cursor)
        conn.commit()

        skill_count = ingest_skills(cursor)
        conn.commit()

        knowledge_count = ingest_knowledge(cursor)
        conn.commit()

        edu_count = ingest_education(cursor)
        conn.commit()

        ingest_job_zone_reference(cursor)
        conn.commit()

        create_title_to_soc_mapping(cursor)
        conn.commit()

        # Summary
        log.info("\n" + "=" * 60)
        log.info("O*NET INGESTION SUMMARY")
        log.info("=" * 60)
        log.info(f"  Occupations:      {occ_count:,}")
        log.info(f"  Alternate Titles: {title_count:,}")
        log.info(f"  Skill Records:    {skill_count:,}")
        log.info(f"  Knowledge Records:{knowledge_count:,}")
        log.info(f"  Education Records:{edu_count:,}")

        show_sample_data(cursor)

    except Exception as e:
        log.error(f"Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        db.release_connection(conn)

    log.info("\nO*NET integration complete!")


if __name__ == "__main__":
    main()
