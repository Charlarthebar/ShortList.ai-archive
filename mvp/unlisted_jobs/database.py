#!/usr/bin/env python3
"""
Database Manager for Comprehensive Job Database
================================================

Manages PostgreSQL database connections and provides ORM-like
access to the job database following the archetype-based schema.

Author: ShortList.ai
"""

import os
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, date
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor, Json
from psycopg2 import pool
import json

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Database configuration."""
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "jobs_comprehensive")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")

    # Connection pool settings
    min_connections: int = 1
    max_connections: int = 10


class DatabaseManager:
    """
    Database manager with connection pooling and schema management.

    Implements the comprehensive job database schema following the
    12-phase plan with archetype-based design.
    """

    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.pool = None

    def initialize_pool(self):
        """Initialize connection pool."""
        try:
            self.pool = pool.ThreadedConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            logger.info(f"Database pool initialized: {self.config.db_name}")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool."""
        if not self.pool:
            self.initialize_pool()
        return self.pool.getconn()

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        if self.pool:
            self.pool.putconn(conn)

    def close_all_connections(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("All database connections closed")

    def execute_schema_file(self, schema_path: str):
        """Execute SQL schema file."""
        conn = self.get_connection()
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            with conn.cursor() as cursor:
                cursor.execute(schema_sql)
            conn.commit()
            logger.info(f"Schema executed successfully from {schema_path}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing schema: {e}")
            raise
        finally:
            self.release_connection(conn)

    # ========================================================================
    # PHASE 3: Company and Location Management
    # ========================================================================

    def insert_company(self, name: str, domain: str = None, ein: str = None,
                      industry: str = None, size_category: str = None) -> int:
        """
        Insert or get company ID.

        Returns: company_id
        """
        conn = self.get_connection()
        try:
            normalized_name = self._normalize_company_name(name)

            with conn.cursor() as cursor:
                # Check if exists
                cursor.execute("""
                    SELECT id FROM companies WHERE normalized_name = %s
                """, (normalized_name,))
                result = cursor.fetchone()

                if result:
                    return result[0]

                # Insert new
                cursor.execute("""
                    INSERT INTO companies (name, normalized_name, domain, ein, industry, size_category)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (name, normalized_name, domain, ein, industry, size_category))

                company_id = cursor.fetchone()[0]
                conn.commit()
                return company_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting company {name}: {e}")
            raise
        finally:
            self.release_connection(conn)

    def insert_company_alias(self, company_id: int, alias: str, source: str = None,
                           confidence: float = 1.0) -> int:
        """Insert company alias for entity matching."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO company_aliases (company_id, alias, source, confidence)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (alias, company_id) DO NOTHING
                    RETURNING id
                """, (company_id, alias, source, confidence))
                result = cursor.fetchone()
                conn.commit()
                return result[0] if result else None
        finally:
            self.release_connection(conn)

    def insert_metro_area(self, cbsa_code: str, name: str, state: str = None,
                         population: int = None, cost_of_living_index: float = None) -> int:
        """Insert or get metro area ID."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO metro_areas (cbsa_code, name, state, population, cost_of_living_index)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (cbsa_code) DO UPDATE SET
                        name = EXCLUDED.name,
                        population = EXCLUDED.population,
                        cost_of_living_index = EXCLUDED.cost_of_living_index
                    RETURNING id
                """, (cbsa_code, name, state, population, cost_of_living_index))
                metro_id = cursor.fetchone()[0]
                conn.commit()
                return metro_id
        finally:
            self.release_connection(conn)

    def insert_location(self, city: str, state: str, metro_id: int = None,
                       latitude: float = None, longitude: float = None,
                       zip_code: str = None, is_remote: bool = False) -> int:
        """Insert or get location ID."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO locations (city, state, metro_id, latitude, longitude, zip_code, is_remote)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city, state, country) DO UPDATE SET
                        metro_id = EXCLUDED.metro_id,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude
                    RETURNING id
                """, (city, state, metro_id, latitude, longitude, zip_code, is_remote))
                location_id = cursor.fetchone()[0]
                conn.commit()
                return location_id
        finally:
            self.release_connection(conn)

    # ========================================================================
    # PHASE 4: Canonical Roles and Title Mapping
    # ========================================================================

    def insert_canonical_role(self, name: str, soc_code: str = None, onet_code: str = None,
                             role_family: str = None, category: str = None,
                             description: str = None, typical_skills: List[str] = None) -> int:
        """Insert or get canonical role ID."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO canonical_roles (soc_code, onet_code, name, role_family, category, description, typical_skills)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (soc_code) DO UPDATE SET
                        name = EXCLUDED.name,
                        role_family = EXCLUDED.role_family
                    RETURNING id
                """, (soc_code, onet_code, name, role_family, category, description, typical_skills))
                role_id = cursor.fetchone()[0]
                conn.commit()
                return role_id
        finally:
            self.release_connection(conn)

    def insert_title_mapping_rule(self, pattern: str, canonical_role_id: int,
                                  seniority_level: str = None, confidence: float = 0.9,
                                  rule_type: str = 'regex', priority: int = 100) -> int:
        """Insert title mapping rule."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO title_mapping_rules (pattern, canonical_role_id, seniority_level, confidence, rule_type, priority)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (pattern, canonical_role_id, seniority_level, confidence, rule_type, priority))
                rule_id = cursor.fetchone()[0]
                conn.commit()
                return rule_id
        finally:
            self.release_connection(conn)

    # ========================================================================
    # PHASE 2 & 5: Source Data and Observed Jobs
    # ========================================================================

    def insert_source_data_raw(self, source_id: int, raw_data: Dict[str, Any]) -> int:
        """Insert raw source data."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO source_data_raw (
                        source_id, raw_company, raw_location, raw_title, raw_description,
                        raw_salary_min, raw_salary_max, raw_salary_text,
                        source_url, source_document_id, as_of_date, raw_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    source_id,
                    raw_data.get('raw_company'),
                    raw_data.get('raw_location'),
                    raw_data.get('raw_title'),
                    raw_data.get('raw_description'),
                    raw_data.get('raw_salary_min'),
                    raw_data.get('raw_salary_max'),
                    raw_data.get('raw_salary_text'),
                    raw_data.get('source_url'),
                    raw_data.get('source_document_id'),
                    raw_data.get('as_of_date', date.today()),
                    Json(raw_data)
                ))
                raw_id = cursor.fetchone()[0]
                conn.commit()
                return raw_id
        finally:
            self.release_connection(conn)

    def insert_observed_job(self, job_data: Dict[str, Any]) -> int:
        """
        Insert an observed job (row-level evidence).

        This is for OBSERVED data only - actual job postings, payroll rows,
        visa filings, etc. NOT inferred/filled data.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO observed_jobs (
                        company_id, location_id, canonical_role_id,
                        raw_title, raw_company, raw_location,
                        title_confidence, seniority, seniority_confidence,
                        employment_type, description, requirements,
                        salary_min, salary_max, salary_point,
                        salary_currency, salary_period, salary_type,
                        source_id, source_data_id, source_type, observation_weight,
                        record_type, status, posted_date, filled_date,
                        first_seen, last_seen, metadata
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """, (
                    job_data.get('company_id'),
                    job_data.get('location_id'),
                    job_data.get('canonical_role_id'),
                    job_data.get('raw_title'),
                    job_data.get('raw_company'),
                    job_data.get('raw_location'),
                    job_data.get('title_confidence', 0.5),
                    job_data.get('seniority'),
                    job_data.get('seniority_confidence', 0.5),
                    job_data.get('employment_type'),
                    job_data.get('description'),
                    job_data.get('requirements'),
                    job_data.get('salary_min'),
                    job_data.get('salary_max'),
                    job_data.get('salary_point'),
                    job_data.get('salary_currency', 'USD'),
                    job_data.get('salary_period', 'annual'),
                    job_data.get('salary_type', 'base'),
                    job_data.get('source_id'),
                    job_data.get('source_data_id'),
                    job_data.get('source_type'),
                    job_data.get('observation_weight', 0.5),
                    'observed',  # Always 'observed' for this table
                    job_data.get('status', 'active'),
                    job_data.get('posted_date'),
                    job_data.get('filled_date'),
                    job_data.get('first_seen', datetime.now()),
                    job_data.get('last_seen', datetime.now()),
                    Json(job_data.get('metadata', {}))
                ))
                job_id = cursor.fetchone()[0]
                conn.commit()
                logger.debug(f"Inserted observed job ID {job_id}")
                return job_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting observed job: {e}")
            raise
        finally:
            self.release_connection(conn)

    # ========================================================================
    # PHASE 6: Compensation Observations
    # ========================================================================

    def insert_compensation_observation(self, comp_data: Dict[str, Any]) -> int:
        """Insert compensation observation."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO compensation_observations (
                        company_id, metro_id, location_id, canonical_role_id, seniority,
                        pay_type, value_min, value_max, value_point, currency, annualized_base,
                        source_id, source_type, observation_weight, observed_date, employment_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    comp_data.get('company_id'),
                    comp_data.get('metro_id'),
                    comp_data.get('location_id'),
                    comp_data.get('canonical_role_id'),
                    comp_data.get('seniority'),
                    comp_data.get('pay_type', 'base'),
                    comp_data.get('value_min'),
                    comp_data.get('value_max'),
                    comp_data.get('value_point'),
                    comp_data.get('currency', 'USD'),
                    comp_data.get('annualized_base'),
                    comp_data.get('source_id'),
                    comp_data.get('source_type'),
                    comp_data.get('observation_weight', 0.5),
                    comp_data.get('observed_date', date.today()),
                    comp_data.get('employment_type')
                ))
                comp_id = cursor.fetchone()[0]
                conn.commit()
                return comp_id
        finally:
            self.release_connection(conn)

    # ========================================================================
    # PHASE 8: OEWS Macro Priors
    # ========================================================================

    def insert_oews_estimate(self, oews_data: Dict[str, Any]) -> int:
        """Insert BLS OEWS employment estimate."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO oews_estimates (
                        metro_id, cbsa_code, canonical_role_id, soc_code,
                        employment_count, employment_rse,
                        wage_mean, wage_median, wage_p10, wage_p25, wage_p75, wage_p90,
                        reference_year, reference_period
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (metro_id, canonical_role_id, reference_year) DO UPDATE SET
                        employment_count = EXCLUDED.employment_count,
                        wage_mean = EXCLUDED.wage_mean,
                        wage_median = EXCLUDED.wage_median
                    RETURNING id
                """, (
                    oews_data.get('metro_id'),
                    oews_data.get('cbsa_code'),
                    oews_data.get('canonical_role_id'),
                    oews_data.get('soc_code'),
                    oews_data.get('employment_count'),
                    oews_data.get('employment_rse'),
                    oews_data.get('wage_mean'),
                    oews_data.get('wage_median'),
                    oews_data.get('wage_p10'),
                    oews_data.get('wage_p25'),
                    oews_data.get('wage_p75'),
                    oews_data.get('wage_p90'),
                    oews_data.get('reference_year'),
                    oews_data.get('reference_period')
                ))
                oews_id = cursor.fetchone()[0]
                conn.commit()
                return oews_id
        finally:
            self.release_connection(conn)

    # ========================================================================
    # PHASE 9: Job Archetypes (Core Output)
    # ========================================================================

    def upsert_job_archetype(self, archetype_data: Dict[str, Any]) -> int:
        """
        Insert or update a job archetype.

        This is the core output table: Company × Metro × Role × Seniority
        Can be either 'observed' (high-confidence aggregate) or 'inferred' (fill-in).
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO job_archetypes (
                        company_id, metro_id, canonical_role_id, seniority, record_type,
                        headcount_p10, headcount_p50, headcount_p90, headcount_method,
                        salary_p25, salary_p50, salary_p75, salary_mean, salary_stddev,
                        salary_currency, salary_method,
                        description, description_sources, description_confidence,
                        employment_type,
                        observed_count, filled_probability_weighted_count, evidence_summary,
                        composite_confidence, confidence_components, top_sources,
                        evidence_date_earliest, evidence_date_latest
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (company_id, metro_id, canonical_role_id, seniority, record_type)
                    DO UPDATE SET
                        headcount_p10 = EXCLUDED.headcount_p10,
                        headcount_p50 = EXCLUDED.headcount_p50,
                        headcount_p90 = EXCLUDED.headcount_p90,
                        salary_p25 = EXCLUDED.salary_p25,
                        salary_p50 = EXCLUDED.salary_p50,
                        salary_p75 = EXCLUDED.salary_p75,
                        description = EXCLUDED.description,
                        composite_confidence = EXCLUDED.composite_confidence,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    archetype_data['company_id'],
                    archetype_data['metro_id'],
                    archetype_data['canonical_role_id'],
                    archetype_data['seniority'],
                    archetype_data['record_type'],
                    archetype_data.get('headcount_p10'),
                    archetype_data.get('headcount_p50'),
                    archetype_data.get('headcount_p90'),
                    archetype_data.get('headcount_method'),
                    archetype_data.get('salary_p25'),
                    archetype_data.get('salary_p50'),
                    archetype_data.get('salary_p75'),
                    archetype_data.get('salary_mean'),
                    archetype_data.get('salary_stddev'),
                    archetype_data.get('salary_currency', 'USD'),
                    archetype_data.get('salary_method'),
                    archetype_data.get('description'),
                    archetype_data.get('description_sources'),
                    archetype_data.get('description_confidence'),
                    archetype_data.get('employment_type'),
                    archetype_data.get('observed_count', 0),
                    archetype_data.get('filled_probability_weighted_count'),
                    Json(archetype_data.get('evidence_summary', {})),
                    archetype_data.get('composite_confidence'),
                    Json(archetype_data.get('confidence_components', {})),
                    Json(archetype_data.get('top_sources', {})),
                    archetype_data.get('evidence_date_earliest'),
                    archetype_data.get('evidence_date_latest')
                ))
                archetype_id = cursor.fetchone()[0]
                conn.commit()
                logger.debug(f"Upserted archetype ID {archetype_id} (record_type={archetype_data['record_type']})")
                return archetype_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error upserting archetype: {e}")
            raise
        finally:
            self.release_connection(conn)

    def insert_archetype_evidence(self, archetype_id: int, evidence_type: str,
                                  evidence_id: int, evidence_weight: float,
                                  source_id: int = None, contributed_to: List[str] = None) -> int:
        """Link archetype to its evidence for provenance."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO archetype_evidence (
                        archetype_id, evidence_type, evidence_id, evidence_weight,
                        source_id, contributed_to
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    archetype_id,
                    evidence_type,
                    evidence_id,
                    evidence_weight,
                    source_id,
                    contributed_to
                ))
                evidence_link_id = cursor.fetchone()[0]
                conn.commit()
                return evidence_link_id
        finally:
            self.release_connection(conn)

    # ========================================================================
    # Licensed Professionals (NPI, Bar, Teachers, Trades)
    # ========================================================================

    def insert_licensed_professional(self, prof_data: Dict[str, Any]) -> int:
        """
        Insert a licensed professional record.

        Used for NPI healthcare providers, attorneys, teachers, trades, etc.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO licensed_professionals (
                        license_type, license_number, state,
                        first_name, last_name, credential,
                        raw_title, taxonomy_code,
                        employer_name, employer_city, employer_state,
                        license_status, issue_date, expiration_date,
                        source, source_url, source_document_id,
                        confidence_score, raw_data
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (license_type, license_number, state) DO UPDATE SET
                        raw_title = EXCLUDED.raw_title,
                        employer_name = EXCLUDED.employer_name,
                        license_status = EXCLUDED.license_status,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    prof_data['license_type'],
                    prof_data['license_number'],
                    prof_data['state'],
                    prof_data.get('first_name'),
                    prof_data.get('last_name'),
                    prof_data.get('credential'),
                    prof_data.get('raw_title'),
                    prof_data.get('taxonomy_code'),
                    prof_data.get('employer_name'),
                    prof_data.get('employer_city'),
                    prof_data.get('employer_state'),
                    prof_data.get('license_status', 'active'),
                    prof_data.get('issue_date'),
                    prof_data.get('expiration_date'),
                    prof_data['source'],
                    prof_data.get('source_url'),
                    prof_data.get('source_document_id'),
                    prof_data.get('confidence_score', 0.90),
                    Json(prof_data.get('raw_data', {}))
                ))
                prof_id = cursor.fetchone()[0]
                conn.commit()
                return prof_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting licensed professional: {e}")
            raise
        finally:
            self.release_connection(conn)

    def batch_insert_licensed_professionals(self, professionals: List[Dict[str, Any]]) -> int:
        """Batch insert licensed professionals for efficiency."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO licensed_professionals (
                        license_type, license_number, state,
                        first_name, last_name, credential,
                        raw_title, taxonomy_code,
                        employer_name, employer_city, employer_state,
                        license_status, issue_date, expiration_date,
                        source, source_url, source_document_id,
                        confidence_score, raw_data
                    ) VALUES (
                        %(license_type)s, %(license_number)s, %(state)s,
                        %(first_name)s, %(last_name)s, %(credential)s,
                        %(raw_title)s, %(taxonomy_code)s,
                        %(employer_name)s, %(employer_city)s, %(employer_state)s,
                        %(license_status)s, %(issue_date)s, %(expiration_date)s,
                        %(source)s, %(source_url)s, %(source_document_id)s,
                        %(confidence_score)s, %(raw_data)s
                    )
                    ON CONFLICT (license_type, license_number, state) DO UPDATE SET
                        raw_title = EXCLUDED.raw_title,
                        employer_name = EXCLUDED.employer_name,
                        license_status = EXCLUDED.license_status,
                        updated_at = CURRENT_TIMESTAMP
                """
                # Prepare data with defaults
                prepared = []
                for p in professionals:
                    prepared.append({
                        'license_type': p['license_type'],
                        'license_number': p['license_number'],
                        'state': p['state'],
                        'first_name': p.get('first_name'),
                        'last_name': p.get('last_name'),
                        'credential': p.get('credential'),
                        'raw_title': p.get('raw_title'),
                        'taxonomy_code': p.get('taxonomy_code'),
                        'employer_name': p.get('employer_name'),
                        'employer_city': p.get('employer_city'),
                        'employer_state': p.get('employer_state'),
                        'license_status': p.get('license_status', 'active'),
                        'issue_date': p.get('issue_date'),
                        'expiration_date': p.get('expiration_date'),
                        'source': p['source'],
                        'source_url': p.get('source_url'),
                        'source_document_id': p.get('source_document_id'),
                        'confidence_score': p.get('confidence_score', 0.90),
                        'raw_data': Json(p.get('raw_data', {}))
                    })

                execute_batch(cursor, query, prepared, page_size=1000)
                conn.commit()
                logger.info(f"Batch inserted {len(professionals)} licensed professionals")
                return len(professionals)
        except Exception as e:
            conn.rollback()
            logger.error(f"Error batch inserting licensed professionals: {e}")
            raise
        finally:
            self.release_connection(conn)

    # ========================================================================
    # Company Headcounts (from SEC, 990, etc.)
    # ========================================================================

    def insert_company_headcount(self, headcount_data: Dict[str, Any]) -> int:
        """
        Insert a company headcount observation.

        Used for employee counts from 990 filings, SEC 10-K, news, etc.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO company_headcounts (
                        company_id, company_name, ein, cik,
                        employee_count, employee_count_is_estimate,
                        fiscal_year, fiscal_period, geography,
                        source, source_url, source_document_id, as_of_date,
                        confidence_score, raw_data
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (company_name, fiscal_year, source) DO UPDATE SET
                        employee_count = EXCLUDED.employee_count,
                        as_of_date = EXCLUDED.as_of_date,
                        raw_data = EXCLUDED.raw_data
                    RETURNING id
                """, (
                    headcount_data.get('company_id'),
                    headcount_data['company_name'],
                    headcount_data.get('ein'),
                    headcount_data.get('cik'),
                    headcount_data['employee_count'],
                    headcount_data.get('employee_count_is_estimate', False),
                    headcount_data.get('fiscal_year'),
                    headcount_data.get('fiscal_period'),
                    headcount_data.get('geography', 'US'),
                    headcount_data['source'],
                    headcount_data.get('source_url'),
                    headcount_data.get('source_document_id'),
                    headcount_data.get('as_of_date', date.today()),
                    headcount_data.get('confidence_score', 0.80),
                    Json(headcount_data.get('raw_data', {}))
                ))
                headcount_id = cursor.fetchone()[0]
                conn.commit()
                return headcount_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting company headcount: {e}")
            raise
        finally:
            self.release_connection(conn)

    # ========================================================================
    # Query Methods
    # ========================================================================

    def get_archetypes_by_company(self, company_id: int, record_type: str = None) -> List[Dict]:
        """Get all archetypes for a company."""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT a.*, c.name as company_name, m.name as metro_name, r.name as role_name
                    FROM job_archetypes a
                    LEFT JOIN companies c ON a.company_id = c.id
                    LEFT JOIN metro_areas m ON a.metro_id = m.id
                    LEFT JOIN canonical_roles r ON a.canonical_role_id = r.id
                    WHERE a.company_id = %s
                """
                params = [company_id]

                if record_type:
                    query += " AND a.record_type = %s"
                    params.append(record_type)

                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        finally:
            self.release_connection(conn)

    def get_coverage_summary(self) -> List[Dict]:
        """Get coverage summary from view."""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM coverage_summary LIMIT 100")
                return [dict(row) for row in cursor.fetchall()]
        finally:
            self.release_connection(conn)

    # ========================================================================
    # Utilities
    # ========================================================================

    @staticmethod
    def _normalize_company_name(name: str) -> str:
        """Normalize company name for matching."""
        if not name:
            return ""

        # Remove common suffixes
        suffixes = ['Inc.', 'Inc', 'LLC', 'L.L.C.', 'Corp.', 'Corporation',
                   'Ltd.', 'Limited', 'Co.', 'Company']
        normalized = name
        for suffix in suffixes:
            normalized = normalized.replace(suffix, '')

        # Remove punctuation, lowercase, strip whitespace
        normalized = ''.join(c for c in normalized if c.isalnum() or c.isspace())
        normalized = normalized.lower().strip()

        return normalized
