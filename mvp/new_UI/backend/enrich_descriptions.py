#!/usr/bin/env python3
"""
Use AI to enrich short job descriptions.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI
import time

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def enrich_description(client, title, company, location, existing_desc, salary_range=None):
    """Use GPT to create a richer job description."""

    salary_info = f"\nSalary: {salary_range}" if salary_range else ""
    existing_info = f"\n\nExisting description snippet: {existing_desc}" if existing_desc else ""

    prompt = f"""Write a professional job description for this position. Be specific and realistic based on what this role typically involves at companies like this.

Job Title: {title}
Company: {company}
Location: {location}{salary_info}{existing_info}

Write 2-3 paragraphs covering:
1. Role overview - what this position does day-to-day
2. Key responsibilities and what success looks like
3. The type of candidate who would thrive in this role

Keep it professional, specific to this type of role, and around 150-200 words. Don't use bullet points. Don't make up specific company details you don't know - focus on the role itself."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=400
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"  Error generating description: {e}")
        return None


def enrich_short_descriptions(limit=50, min_length=300):
    """Find and enrich jobs with short descriptions."""

    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("Error: OPENAI_API_KEY not found")
        return

    client = OpenAI(api_key=api_key)
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find jobs with short descriptions
        cur.execute("""
            SELECT id, title, company_name, location, description, salary_range
            FROM watchable_positions
            WHERE (description IS NULL OR LENGTH(description) < %s)
              AND title IS NOT NULL
              AND company_name IS NOT NULL
            ORDER BY
              CASE WHEN salary_range IS NOT NULL THEN 0 ELSE 1 END,
              LENGTH(COALESCE(description, ''))
            LIMIT %s
        """, (min_length, limit))

        jobs = cur.fetchall()
        print(f"Found {len(jobs)} jobs with descriptions < {min_length} chars")

        enriched = 0
        for i, job in enumerate(jobs):
            print(f"\n[{i+1}/{len(jobs)}] {job['title']} at {job['company_name']}")
            print(f"  Current: {(job['description'] or '')[:80]}...")

            new_desc = enrich_description(
                client,
                job['title'],
                job['company_name'],
                job['location'],
                job['description'],
                job['salary_range']
            )

            if new_desc and len(new_desc) > len(job['description'] or ''):
                cur.execute("""
                    UPDATE watchable_positions
                    SET description = %s
                    WHERE id = %s
                """, (new_desc, job['id']))
                conn.commit()
                enriched += 1
                print(f"  Enriched: {new_desc[:80]}...")
            else:
                print(f"  Skipped (no improvement)")

            # Rate limiting
            time.sleep(0.5)

        print(f"\n{'='*50}")
        print(f"Enriched {enriched} job descriptions")

    conn.close()
    return enriched


def preview_enrichment(n=3):
    """Preview what enrichment would look like for a few jobs."""

    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("Error: OPENAI_API_KEY not found")
        return

    client = OpenAI(api_key=api_key)
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, title, company_name, location, description, salary_range
            FROM watchable_positions
            WHERE LENGTH(COALESCE(description, '')) < 300
              AND title IS NOT NULL
              AND company_name IS NOT NULL
            ORDER BY RANDOM()
            LIMIT %s
        """, (n,))

        jobs = cur.fetchall()

        for job in jobs:
            print("=" * 60)
            print(f"TITLE: {job['title']}")
            print(f"COMPANY: {job['company_name']}")
            print(f"LOCATION: {job['location']}")
            print(f"SALARY: {job['salary_range']}")
            print(f"\nCURRENT DESCRIPTION ({len(job['description'] or '')} chars):")
            print(job['description'] or "(none)")

            new_desc = enrich_description(
                client,
                job['title'],
                job['company_name'],
                job['location'],
                job['description'],
                job['salary_range']
            )

            print(f"\nENRICHED DESCRIPTION ({len(new_desc or '')} chars):")
            print(new_desc)
            print()

    conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Enrich job descriptions with AI')
    parser.add_argument('--preview', type=int, default=0,
                        help='Preview enrichment for N random jobs without saving')
    parser.add_argument('--limit', type=int, default=50,
                        help='Maximum number of jobs to enrich')
    parser.add_argument('--min-length', type=int, default=300,
                        help='Only enrich descriptions shorter than this')

    args = parser.parse_args()

    if args.preview > 0:
        preview_enrichment(args.preview)
    else:
        enrich_short_descriptions(limit=args.limit, min_length=args.min_length)
