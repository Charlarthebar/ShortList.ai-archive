#!/usr/bin/env python3
"""
Import Cambridge jobs and ONET skills into the database.

This script:
1. Loads ONET skills from onet_skills.csv
2. Loads Cambridge jobs from cambridge_jobs.csv
3. Links jobs to their required skills
4. Uses OpenAI to classify job role types and experience levels
"""

import os
import sys
import csv
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime
import re

# Load environment variables
load_dotenv()

# Database connection - matches app.py config
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
ONET_SKILLS_PATH = os.path.join(PROJECT_ROOT, "Charlie's Work", "onet_skills.csv")
CAMBRIDGE_JOBS_PATH = os.path.join(PROJECT_ROOT, "Charlie's Work", "cambridge_jobs.csv")

# Role type mapping for classification
ROLE_TYPES = [
    'software_engineer',
    'data_scientist',
    'data_analyst',
    'product_manager',
    'engineering_manager',
    'sales',
    'marketing',
    'design',
    'operations',
    'finance',
    'hr',
    'support'
]


def get_db():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)


def load_onet_skills():
    """Load ONET skills from CSV into database.

    Note: The existing onet_skills table has columns: id, soc_code, skill_name, importance, level, created_at
    We only need to ensure the skill_name values exist for matching purposes.
    """
    print(f"Loading ONET skills from {ONET_SKILLS_PATH}...")

    conn = get_db()
    skills_loaded = 0
    skills_existing = 0

    with open(ONET_SKILLS_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        with conn.cursor() as cur:
            for row in reader:
                skill_name = row.get('skill_name', '').strip()

                if not skill_name:
                    continue

                # Check if skill already exists
                cur.execute("SELECT id FROM onet_skills WHERE skill_name = %s LIMIT 1", (skill_name,))
                if cur.fetchone():
                    skills_existing += 1
                else:
                    try:
                        # Insert with just skill_name (other columns are nullable or have defaults)
                        cur.execute("""
                            INSERT INTO onet_skills (skill_name)
                            VALUES (%s)
                        """, (skill_name,))
                        skills_loaded += 1
                    except Exception as e:
                        print(f"Error inserting skill '{skill_name}': {e}")
                        conn.rollback()

    conn.commit()
    conn.close()
    print(f"ONET skills: {skills_existing} already existed, {skills_loaded} newly added.")
    return skills_loaded + skills_existing


def get_skill_id_map():
    """Get mapping of skill names to skill IDs."""
    conn = get_db()
    skill_map = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, skill_name FROM onet_skills")
        for row in cur.fetchall():
            # Store both original and lowercase for matching
            skill_map[row['skill_name'].lower()] = row['id']

    conn.close()
    return skill_map


def classify_job_with_ai(client, title, description):
    """Use OpenAI to classify job role type and experience level."""
    prompt = f"""Analyze this job posting and return a JSON object with:
- role_type: one of {ROLE_TYPES}
- experience_level: one of ['intern', 'entry', 'mid', 'senior']
- work_arrangement: one of ['remote', 'hybrid', 'onsite'] (look for keywords in description)

Job Title: {title}

Job Description (first 1000 chars):
{description[:1000] if description else 'No description'}

Return ONLY valid JSON, no other text."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=100
        )

        result_text = response.choices[0].message.content.strip()
        # Clean up JSON if wrapped in markdown
        if result_text.startswith('```'):
            result_text = re.sub(r'^```json?\n?', '', result_text)
            result_text = re.sub(r'\n?```$', '', result_text)

        result = json.loads(result_text)
        return {
            'role_type': result.get('role_type', 'operations'),
            'experience_level': result.get('experience_level', 'mid'),
            'work_arrangement': result.get('work_arrangement', 'onsite')
        }
    except Exception as e:
        print(f"AI classification error: {e}")
        # Fallback: infer from title
        return infer_job_attributes(title, description)


def infer_job_attributes(title, description):
    """Fallback: infer job attributes from title/description keywords."""
    title_lower = (title or '').lower()
    desc_lower = (description or '').lower()

    # Role type inference
    role_type = 'operations'  # default
    if any(kw in title_lower for kw in ['software', 'developer', 'engineer', 'swe', 'backend', 'frontend', 'full stack']):
        role_type = 'software_engineer'
    elif any(kw in title_lower for kw in ['data scientist', 'machine learning', 'ml engineer', 'ai ']):
        role_type = 'data_scientist'
    elif any(kw in title_lower for kw in ['data analyst', 'business analyst', 'analytics']):
        role_type = 'data_analyst'
    elif any(kw in title_lower for kw in ['product manager', 'product owner', 'pm ']):
        role_type = 'product_manager'
    elif any(kw in title_lower for kw in ['engineering manager', 'tech lead', 'director of engineering']):
        role_type = 'engineering_manager'
    elif any(kw in title_lower for kw in ['sales', 'account executive', 'business development', 'bdr', 'sdr']):
        role_type = 'sales'
    elif any(kw in title_lower for kw in ['marketing', 'content', 'seo', 'growth']):
        role_type = 'marketing'
    elif any(kw in title_lower for kw in ['design', 'ux', 'ui ', 'graphic']):
        role_type = 'design'
    elif any(kw in title_lower for kw in ['finance', 'accounting', 'cfo', 'controller', 'bookkeeper']):
        role_type = 'finance'
    elif any(kw in title_lower for kw in ['hr ', 'human resources', 'people', 'recruiter', 'talent']):
        role_type = 'hr'
    elif any(kw in title_lower for kw in ['support', 'customer success', 'customer service']):
        role_type = 'support'

    # Experience level inference
    experience_level = 'mid'  # default
    if any(kw in title_lower for kw in ['intern', 'internship']):
        experience_level = 'intern'
    elif any(kw in title_lower for kw in ['junior', 'entry', 'associate', ' i ', ' 1 ']):
        experience_level = 'entry'
    elif any(kw in title_lower for kw in ['senior', 'sr ', 'lead', 'principal', 'staff', 'director', 'vp ', 'chief']):
        experience_level = 'senior'

    # Work arrangement inference
    work_arrangement = 'onsite'  # default
    if '[remote]' in title_lower or 'fully remote' in desc_lower or 'work from anywhere' in desc_lower:
        work_arrangement = 'remote'
    elif 'hybrid' in desc_lower or 'flexible' in desc_lower:
        work_arrangement = 'hybrid'

    return {
        'role_type': role_type,
        'experience_level': experience_level,
        'work_arrangement': work_arrangement
    }


def load_cambridge_jobs(use_ai=True, limit=None):
    """Load Cambridge jobs from CSV into database."""
    print(f"Loading Cambridge jobs from {CAMBRIDGE_JOBS_PATH}...")

    # Initialize OpenAI client if using AI classification
    client = None
    if use_ai:
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            client = OpenAI(api_key=api_key)
            print("Using OpenAI for job classification")
        else:
            print("No OpenAI API key found, using keyword-based classification")
            use_ai = False

    # Get skill ID mapping
    skill_map = get_skill_id_map()
    print(f"Loaded {len(skill_map)} skills for matching")

    conn = get_db()
    jobs_loaded = 0
    skills_linked = 0

    with open(CAMBRIDGE_JOBS_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            if limit and i >= limit:
                break

            title = row.get('title', '').strip()
            employer = row.get('employer', '').strip()
            location = row.get('location', '').strip()
            description = row.get('description', '').strip()
            skills_str = row.get('skills', '').strip()
            source_url = row.get('url', '').strip()
            posted_date_str = row.get('posted_date', '').strip()
            is_remote = row.get('is_remote', '0').strip()

            # Parse salary
            salary_min = None
            salary_max = None
            try:
                if row.get('salary_min'):
                    salary_min = int(float(row['salary_min']))
                if row.get('salary_max'):
                    salary_max = int(float(row['salary_max']))
            except (ValueError, TypeError):
                pass

            # Create salary_range string for display
            salary_range = None
            if salary_min and salary_max:
                salary_range = f"${salary_min:,} - ${salary_max:,}"
            elif salary_min:
                salary_range = f"${salary_min:,}+"
            elif salary_max:
                salary_range = f"Up to ${salary_max:,}"

            # Parse posted date
            posted_date = None
            if posted_date_str:
                try:
                    posted_date = datetime.fromisoformat(posted_date_str.replace('Z', '+00:00'))
                except:
                    pass

            # Classify job
            if use_ai and client and i % 50 == 0:
                # Use AI for every 50th job to save API costs, infer the rest
                attrs = classify_job_with_ai(client, title, description)
            else:
                attrs = infer_job_attributes(title, description)

            # Override work_arrangement if is_remote flag is set
            if is_remote == '1':
                attrs['work_arrangement'] = 'remote'

            try:
                with conn.cursor() as cur:
                    # Insert job
                    cur.execute("""
                        INSERT INTO watchable_positions
                        (title, company_name, location, description, salary_range, salary_min, salary_max,
                         role_type, experience_level, work_arrangement, source_url, posted_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'open')
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """, (
                        title, employer, location, description, salary_range, salary_min, salary_max,
                        attrs['role_type'], attrs['experience_level'], attrs['work_arrangement'],
                        source_url, posted_date
                    ))

                    result = cur.fetchone()
                    if result:
                        position_id = result[0]
                        jobs_loaded += 1

                        # Link skills to job
                        if skills_str:
                            skills_list = [s.strip() for s in skills_str.split(';') if s.strip()]
                            for skill_name in skills_list:
                                skill_id = skill_map.get(skill_name.lower())
                                if skill_id:
                                    try:
                                        cur.execute("""
                                            INSERT INTO job_required_skills (position_id, skill_id)
                                            VALUES (%s, %s)
                                            ON CONFLICT DO NOTHING
                                        """, (position_id, skill_id))
                                        skills_linked += 1
                                    except Exception as e:
                                        pass  # Skip duplicate skills

                        if jobs_loaded % 100 == 0:
                            print(f"  Loaded {jobs_loaded} jobs...")
                            conn.commit()

            except Exception as e:
                print(f"Error inserting job '{title}': {e}")

    conn.commit()
    conn.close()
    print(f"Loaded {jobs_loaded} jobs with {skills_linked} skill links.")
    return jobs_loaded


def run_schema_migrations():
    """Run the schema.sql to ensure tables exist."""
    print("Running schema migrations...")
    schema_path = os.path.join(SCRIPT_DIR, 'schema.sql')

    conn = get_db()
    with conn.cursor() as cur:
        with open(schema_path, 'r') as f:
            cur.execute(f.read())
    conn.commit()
    conn.close()
    print("Schema migrations complete.")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Import Cambridge jobs and ONET skills')
    parser.add_argument('--skills-only', action='store_true', help='Only import ONET skills')
    parser.add_argument('--jobs-only', action='store_true', help='Only import Cambridge jobs')
    parser.add_argument('--no-ai', action='store_true', help='Skip AI classification, use keyword matching only')
    parser.add_argument('--limit', type=int, help='Limit number of jobs to import')
    parser.add_argument('--skip-migrations', action='store_true', help='Skip schema migrations')

    args = parser.parse_args()

    if not args.skip_migrations:
        run_schema_migrations()

    if not args.jobs_only:
        load_onet_skills()

    if not args.skills_only:
        load_cambridge_jobs(use_ai=not args.no_ai, limit=args.limit)

    print("\nImport complete!")


if __name__ == '__main__':
    main()
