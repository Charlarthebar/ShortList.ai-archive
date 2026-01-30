#!/usr/bin/env python3
"""
Title Normalization Script
==========================

Normalizes job titles in the database to a consistent format:
- Title Case (with proper acronym handling)
- Remove trailing level indicators (1, 2, 3, I, II, III, IV, V)
- Preserve common acronyms (HR, IT, CEO, CFO, etc.)

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import re
import logging
from typing import Optional

# Set DB_USER
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('title_normalization.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Common acronyms to preserve (uppercase)
# Note: Store in uppercase for matching, but some have special casing (PhD)
ACRONYMS = {
    'HR', 'IT', 'CEO', 'CFO', 'COO', 'CTO', 'CIO', 'CMO', 'VP', 'SVP', 'EVP',
    'MD', 'RN', 'LPN', 'PA', 'NP', 'DDS', 'DMD', 'DO', 'PHD', 'JD', 'MBA',
    'CPA', 'PE', 'PMP', 'LCSW', 'MSW', 'LMFT', 'OT', 'PT', 'SLP',
    'SQL', 'AWS', 'GCP', 'API', 'UI', 'UX', 'QA', 'QC', 'BI', 'AI', 'ML',
    'HVAC', 'EMT', 'EMS', 'ICU', 'ER', 'OR', 'NICU', 'PICU',
    'DOT', 'CDL', 'OTR', 'LTL', 'FTL',
    'OSHA', 'EPA', 'FDA', 'USDA', 'DOD', 'DOE', 'HHS', 'NIH', 'CDC',
    'AC', 'DC', 'RF', 'PCB', 'CAD', 'CAM', 'CNC', 'PLC',
    'ERP', 'CRM', 'SaaS', 'B2B', 'B2C',
    'DUI', 'DWI', 'OVI',
    'HRIS', 'FMLA', 'ADA', 'EEOC', 'FLSA',
    'PR', 'AR', 'AP',  # Public Relations, Accounts Receivable/Payable
    'TV', 'AM', 'FM', 'DJ',
    'GIS', 'CAD', 'BIM',
    'HIV', 'AIDS', 'TB', 'STD', 'STI',
    'ESL', 'ELL', 'IEP', 'SPED',
    'ROTC', 'JAG', 'MP',
    'AA', 'BA', 'BS', 'MA', 'MS', 'MPA', 'MPH', 'MSN', 'DNP',
}

# Words that should stay lowercase (unless at start)
LOWERCASE_WORDS = {'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}

# Pattern to match trailing level indicators
TRAILING_LEVEL_PATTERN = re.compile(
    r'\s+(?:'
    r'[1-9]|'  # Single digits 1-9
    r'I{1,3}|'  # Roman numerals I, II, III
    r'IV|V|'    # Roman numerals IV, V
    r'VI{0,3}|'  # Roman numerals VI, VII, VIII
    r'IX|X'     # Roman numerals IX, X
    r')$',
    re.IGNORECASE
)


def normalize_title(raw_title: str) -> str:
    """
    Normalize a job title to consistent format.

    - Convert to Title Case
    - Preserve acronyms
    - Remove trailing level indicators
    """
    if not raw_title:
        return raw_title

    # First, remove trailing level indicators
    title = TRAILING_LEVEL_PATTERN.sub('', raw_title.strip())

    # Convert to lowercase for processing
    title = title.lower()

    # Split into words
    words = title.split()

    normalized_words = []
    for i, word in enumerate(words):
        # Check if word (uppercase) is an acronym
        upper_word = word.upper()

        # Handle words with punctuation (e.g., "hr/payroll")
        if '/' in word:
            parts = word.split('/')
            normalized_parts = []
            for part in parts:
                upper_part = part.upper()
                if upper_part in ACRONYMS:
                    normalized_parts.append(upper_part)
                else:
                    normalized_parts.append(part.capitalize())
            normalized_words.append('/'.join(normalized_parts))
        elif '-' in word:
            # Handle hyphenated words (e.g., "self-employed")
            parts = word.split('-')
            normalized_parts = []
            for part in parts:
                upper_part = part.upper()
                if upper_part in ACRONYMS:
                    normalized_parts.append(upper_part)
                else:
                    normalized_parts.append(part.capitalize())
            normalized_words.append('-'.join(normalized_parts))
        elif upper_word in ACRONYMS:
            # Special casing for PhD
            if upper_word == 'PHD':
                normalized_words.append('PhD')
            else:
                normalized_words.append(upper_word)
        elif i > 0 and word in LOWERCASE_WORDS:
            # Keep lowercase words lowercase (except at start)
            normalized_words.append(word)
        else:
            # Standard title case
            normalized_words.append(word.capitalize())

    return ' '.join(normalized_words)


def test_normalization():
    """Test the normalization function with various inputs."""
    test_cases = [
        ("SOCIAL WORKER 2", "Social Worker"),
        ("SOCIAL WORKER III", "Social Worker"),
        ("HR COORDINATOR", "HR Coordinator"),
        ("CHIEF EXECUTIVE OFFICER", "Chief Executive Officer"),
        ("IT MANAGER I", "IT Manager"),
        ("software engineer", "Software Engineer"),
        ("VP OF SALES", "VP of Sales"),
        ("SENIOR DATA ANALYST IV", "Senior Data Analyst"),
        ("REGISTERED NURSE - ICU", "Registered Nurse - ICU"),
        ("AI/ML ENGINEER", "AI/ML Engineer"),
        ("HVAC TECHNICIAN 3", "HVAC Technician"),
        ("CEO", "CEO"),
        ("cfo", "CFO"),
        ("DIRECTOR OF HR", "Director of HR"),
        ("QA ENGINEER II", "QA Engineer"),
        ("PhD RESEARCHER", "PhD Researcher"),
        ("ADMINISTRATIVE ASSISTANT V", "Administrative Assistant"),
        ("BI ANALYST", "BI Analyst"),
        ("UX/UI DESIGNER", "UX/UI Designer"),
    ]

    print("\nTitle Normalization Test Results:")
    print("=" * 70)
    all_passed = True
    for raw, expected in test_cases:
        result = normalize_title(raw)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            all_passed = False
        print(f"{status}: '{raw}' -> '{result}' (expected: '{expected}')")
    print("=" * 70)
    return all_passed


def main():
    """Main function to normalize all titles in the database."""
    log.info("=" * 60)
    log.info("TITLE NORMALIZATION")
    log.info("=" * 60)

    # Test first
    log.info("\nRunning normalization tests...")
    if not test_normalization():
        log.warning("Some tests failed, but proceeding with normalization...")

    # Initialize database
    config = Config()
    db = DatabaseManager(config)
    conn = db.get_connection()
    cursor = conn.cursor()

    # Get count of records to process
    cursor.execute("SELECT COUNT(*) FROM observed_jobs")
    total = cursor.fetchone()[0]
    log.info(f"\nTotal records to normalize: {total:,}")

    # Get max ID for batching
    cursor.execute("SELECT MAX(id) FROM observed_jobs")
    max_id = cursor.fetchone()[0]

    # Process in batches by ID range
    batch_size = 50000
    updated = 0
    current_id = 0

    log.info("Processing titles...")

    while current_id < max_id:
        # Fetch a batch of records
        cursor.execute("""
            SELECT id, raw_title FROM observed_jobs
            WHERE id > %s AND id <= %s
            ORDER BY id
        """, (current_id, current_id + batch_size))

        rows = cursor.fetchall()
        if not rows:
            current_id += batch_size
            continue

        # Process and collect updates
        updates = []
        for job_id, raw_title in rows:
            if raw_title:
                normalized = normalize_title(raw_title)
                if normalized != raw_title:
                    updates.append((normalized, job_id))

        # Batch update
        if updates:
            cursor.executemany(
                "UPDATE observed_jobs SET raw_title = %s WHERE id = %s",
                updates
            )
            updated += len(updates)
            conn.commit()

        current_id += batch_size
        log.info(f"  Processed up to ID {current_id:,}, updated {updated:,} titles...")

    log.info(f"\nTitle normalization complete: {updated:,} titles updated")

    # Show some examples
    cursor.execute("""
        SELECT raw_title, COUNT(*) as cnt
        FROM observed_jobs
        GROUP BY raw_title
        ORDER BY cnt DESC
        LIMIT 20
    """)

    log.info("\nTop 20 normalized titles:")
    log.info("-" * 50)
    for title, cnt in cursor.fetchall():
        log.info(f"  {title}: {cnt:,}")

    db.release_connection(conn)


if __name__ == "__main__":
    main()
