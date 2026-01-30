#!/usr/bin/env python3
"""
Example Usage of the Comprehensive Job Database
================================================

Demonstrates key workflows:
1. Setting up the database
2. Ingesting observed data
3. Creating archetypes
4. Querying results
5. Understanding observed vs. inferred

Author: ShortList.ai
"""

import os
from datetime import date
from database import DatabaseManager, Config
from title_normalizer import TitleNormalizer, seed_canonical_roles


def setup_example():
    """Setup example database and seed initial data."""
    print("="*60)
    print("STEP 1: INITIALIZE DATABASE")
    print("="*60)

    # Initialize database manager
    config = Config(
        db_name="jobs_comprehensive",
        db_user="postgres"
    )
    db = DatabaseManager(config)
    db.initialize_pool()

    # Initialize schema (if not already done)
    try:
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        db.execute_schema_file(schema_path)
        print("✓ Schema initialized")
    except Exception as e:
        print(f"Schema already exists or error: {e}")

    # Seed canonical roles
    print("\nSeeding canonical roles...")
    seed_canonical_roles(db)
    print("✓ Canonical roles seeded")

    return db


def example_1_insert_observed_job(db: DatabaseManager):
    """
    Example 1: Insert an observed job from a payroll source.

    This is REAL DATA - an actual job we observed from a reliable source.
    """
    print("\n" + "="*60)
    print("EXAMPLE 1: INSERT OBSERVED JOB (PAYROLL)")
    print("="*60)

    # Step 1: Get or create company
    company_id = db.insert_company(
        name="Massachusetts Institute of Technology",
        domain="mit.edu",
        ein="042103594",
        industry="Education",
        size_category="large"
    )
    print(f"✓ Company ID: {company_id}")

    # Step 2: Get or create metro area
    metro_id = db.insert_metro_area(
        cbsa_code="71650",
        name="Boston-Cambridge-Nashua, MA-NH",
        state="MA",
        population=4900000,
        cost_of_living_index=1.42
    )
    print(f"✓ Metro ID: {metro_id}")

    # Step 3: Get or create location
    location_id = db.insert_location(
        city="Cambridge",
        state="MA",
        metro_id=metro_id,
        latitude=42.3736,
        longitude=-71.1097,
        zip_code="02139"
    )
    print(f"✓ Location ID: {location_id}")

    # Step 4: Normalize the job title
    normalizer = TitleNormalizer(db)
    title_result = normalizer.parse_title("Senior Software Engineer")

    print(f"\nTitle Normalization:")
    print(f"  Raw Title: Senior Software Engineer")
    print(f"  Canonical Role: {title_result.canonical_role_name}")
    print(f"  Canonical Role ID: {title_result.canonical_role_id}")
    print(f"  Seniority: {title_result.seniority}")
    print(f"  Title Confidence: {title_result.title_confidence:.2f}")
    print(f"  Seniority Confidence: {title_result.seniority_confidence:.2f}")

    # Step 5: Create the observed job
    job_data = {
        'company_id': company_id,
        'location_id': location_id,
        'canonical_role_id': title_result.canonical_role_id,
        'raw_title': "Senior Software Engineer",
        'raw_company': "Massachusetts Institute of Technology",
        'raw_location': "Cambridge, MA",
        'title_confidence': title_result.title_confidence,
        'seniority': title_result.seniority,
        'seniority_confidence': title_result.seniority_confidence,
        'employment_type': 'FT',
        'description': 'Develops and maintains software systems for research computing.',
        'salary_point': 145000,
        'salary_currency': 'USD',
        'salary_period': 'annual',
        'salary_type': 'base',
        'source_id': 1,  # Assuming source ID 1 = state_payroll
        'source_type': 'payroll',
        'observation_weight': 0.95,  # High weight for payroll
        'status': 'filled',  # Payroll = filled position
        'metadata': {
            'department': 'Information Systems & Technology',
            'pay_year': 2024,
            'source_document': 'MA State Payroll 2024'
        }
    }

    job_id = db.insert_observed_job(job_data)
    print(f"\n✓ Observed Job ID: {job_id}")
    print(f"  Record Type: OBSERVED (payroll)")
    print(f"  This is REAL DATA from a Tier A source")

    return job_id, company_id, metro_id, title_result.canonical_role_id


def example_2_create_archetype(db: DatabaseManager, company_id: int,
                              metro_id: int, role_id: int):
    """
    Example 2: Create an archetype from observed data.

    This is an OBSERVED archetype - high-confidence aggregate from real rows.
    """
    print("\n" + "="*60)
    print("EXAMPLE 2: CREATE OBSERVED ARCHETYPE")
    print("="*60)

    archetype_data = {
        'company_id': company_id,
        'metro_id': metro_id,
        'canonical_role_id': role_id,
        'seniority': 'senior',
        'record_type': 'observed',  # This is observed, not inferred

        # Salary distribution (from observed payroll rows)
        'salary_p25': 130000,
        'salary_p50': 145000,
        'salary_p75': 160000,
        'salary_mean': 145000,
        'salary_stddev': 15000,
        'salary_currency': 'USD',
        'salary_method': 'direct_observation',

        # Headcount (we observed N people in payroll)
        'headcount_p50': 5,

        # Description
        'description': 'Senior Software Engineers at MIT develop and maintain research computing infrastructure, working with cutting-edge technologies.',
        'description_confidence': 0.85,

        # Evidence summary
        'observed_count': 5,  # We saw 5 payroll rows
        'evidence_summary': {
            'payroll_rows': 5,
            'source': 'MA State Payroll 2024'
        },

        # Confidence (high because from payroll)
        'composite_confidence': 0.90,
        'confidence_components': {
            'salary_confidence': 0.95,
            'headcount_confidence': 0.90,
            'existence_confidence': 0.95
        },

        # Provenance
        'top_sources': {
            'state_payroll': 0.95
        },
        'evidence_date_latest': date.today()
    }

    archetype_id = db.upsert_job_archetype(archetype_data)
    print(f"✓ Archetype ID: {archetype_id}")
    print(f"  Record Type: OBSERVED (high-confidence aggregate)")
    print(f"  Based on 5 payroll rows")
    print(f"  Confidence: 0.90")

    return archetype_id


def example_3_create_inferred_archetype(db: DatabaseManager, company_id: int,
                                       metro_id: int):
    """
    Example 3: Create an INFERRED archetype.

    This is for a role where we don't have direct observations,
    but we infer its existence from OEWS data and company patterns.
    """
    print("\n" + "="*60)
    print("EXAMPLE 3: CREATE INFERRED ARCHETYPE")
    print("="*60)

    # Get or create a different role (Data Scientist)
    conn = db.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM canonical_roles WHERE name = 'Data Scientist'
            """)
            result = cursor.fetchone()
            data_scientist_role_id = result[0] if result else None
    finally:
        db.release_connection(conn)

    if not data_scientist_role_id:
        print("⚠ Data Scientist role not found, skipping")
        return None

    print(f"Inferring archetype for: Data Scientist (mid-level) at MIT")
    print("Evidence:")
    print("  - OEWS says Boston metro has 8,500 data scientists")
    print("  - MIT is a large employer in this metro")
    print("  - MIT's industry typically employs data scientists")
    print("  - Inferring likely presence with ~60% confidence")

    archetype_data = {
        'company_id': company_id,
        'metro_id': metro_id,
        'canonical_role_id': data_scientist_role_id,
        'seniority': 'mid',
        'record_type': 'inferred',  # THIS IS INFERRED, NOT OBSERVED

        # Salary distribution (from model, not direct observation)
        'salary_p25': 105000,
        'salary_p50': 120000,
        'salary_p75': 140000,
        'salary_mean': 120000,
        'salary_stddev': 18000,
        'salary_currency': 'USD',
        'salary_method': 'hierarchical_bayesian_with_oews_prior',

        # Headcount (allocated from OEWS)
        'headcount_p10': 2,
        'headcount_p50': 5,
        'headcount_p90': 10,
        'headcount_method': 'share_of_evidence_allocation',

        # Description (generated)
        'description': 'Mid-level Data Scientists analyze research data, build predictive models, and support scientific computing initiatives.',
        'description_sources': ['onet_template', 'industry_education_flavor'],
        'description_confidence': 0.60,

        # Evidence summary (NO direct observations)
        'observed_count': 0,
        'evidence_summary': {
            'oews_prior': True,
            'company_size': 'large',
            'industry_typical': True
        },

        # Confidence (lower because inferred)
        'composite_confidence': 0.60,
        'confidence_components': {
            'salary_confidence': 0.55,
            'headcount_confidence': 0.50,
            'existence_confidence': 0.70
        },

        # Provenance
        'top_sources': {
            'oews_macro': 0.40,
            'industry_patterns': 0.30,
            'company_size': 0.30
        },
        'evidence_date_latest': date.today()
    }

    archetype_id = db.upsert_job_archetype(archetype_data)
    print(f"\n✓ Inferred Archetype ID: {archetype_id}")
    print(f"  Record Type: INFERRED (model-generated fill-in)")
    print(f"  Based on OEWS priors and company patterns")
    print(f"  Confidence: 0.60 (lower than observed)")
    print(f"  ⚠ This is NOT observed data - it's an inference")

    return archetype_id


def example_4_query_results(db: DatabaseManager, company_id: int):
    """
    Example 4: Query and understand results.
    """
    print("\n" + "="*60)
    print("EXAMPLE 4: QUERY RESULTS")
    print("="*60)

    # Get all archetypes for the company
    archetypes = db.get_archetypes_by_company(company_id)

    print(f"\nFound {len(archetypes)} archetypes for company ID {company_id}:")
    print()

    for arch in archetypes:
        print(f"Role: {arch['role_name']} ({arch['seniority']})")
        print(f"  Record Type: {arch['record_type'].upper()}")
        print(f"  Salary P50: ${arch['salary_p50']:,.0f}" if arch['salary_p50'] else "  Salary: N/A")
        print(f"  Headcount P50: {arch['headcount_p50']}" if arch['headcount_p50'] else "  Headcount: N/A")
        print(f"  Confidence: {arch['composite_confidence']:.2f}")
        print(f"  Observed Count: {arch['observed_count']}")
        print()

    # Show the difference between observed and inferred
    observed = [a for a in archetypes if a['record_type'] == 'observed']
    inferred = [a for a in archetypes if a['record_type'] == 'inferred']

    print("="*60)
    print("UNDERSTANDING OBSERVED vs. INFERRED")
    print("="*60)
    print(f"\nOBSERVED archetypes: {len(observed)}")
    print("  - Based on real payroll rows, job postings, visa filings")
    print("  - High confidence (typically 0.7-0.95)")
    print("  - Defensible: can point to source documents")
    print()
    print(f"INFERRED archetypes: {len(inferred)}")
    print("  - Generated by models to fill gaps")
    print("  - Lower confidence (typically 0.3-0.7)")
    print("  - Clearly labeled as inferred in the data")
    print()
    print("WHY THIS MATTERS:")
    print("  - Product can show users 'verified' vs 'estimated' jobs")
    print("  - Investors can audit the dataset")
    print("  - You never misrepresent inference as observation")


def main():
    """Run all examples."""
    print("="*60)
    print("COMPREHENSIVE JOB DATABASE - EXAMPLE USAGE")
    print("="*60)

    # Setup
    db = setup_example()

    # Example 1: Insert observed job
    job_id, company_id, metro_id, role_id = example_1_insert_observed_job(db)

    # Example 2: Create observed archetype
    observed_arch_id = example_2_create_archetype(db, company_id, metro_id, role_id)

    # Example 3: Create inferred archetype
    inferred_arch_id = example_3_create_inferred_archetype(db, company_id, metro_id)

    # Example 4: Query and understand results
    example_4_query_results(db, company_id)

    # Cleanup
    db.close_all_connections()

    print("\n" + "="*60)
    print("EXAMPLES COMPLETE")
    print("="*60)
    print("\nNext steps:")
    print("  1. Explore the database: psql jobs_comprehensive")
    print("  2. View observed jobs: SELECT * FROM observed_jobs;")
    print("  3. View archetypes: SELECT * FROM job_archetypes;")
    print("  4. Check provenance: SELECT * FROM archetype_evidence;")
    print("  5. Run full pipeline: python pipeline.py --mode full")


if __name__ == "__main__":
    main()
