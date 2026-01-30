-- ============================================================================
-- COMPREHENSIVE JOB DATABASE SCHEMA
-- Based on the 12-phase implementation plan
-- ============================================================================
--
-- CORE PRINCIPLES:
-- 1. Observed vs. Inferred is a first-class concept
-- 2. Archetypes (Company × Metro × Role × Seniority) not individual seats
-- 3. Confidence scoring and provenance built into every table
-- 4. Distribution-based estimates, not point estimates
--
-- ============================================================================

-- ============================================================================
-- PHASE 3: IDENTITY GRAPH - Companies and Locations
-- ============================================================================

-- Companies table with stable IDs
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,  -- For matching
    domain TEXT,  -- Website domain for matching
    ein TEXT,  -- Employer ID Number (nonprofits, some companies)
    industry TEXT,
    size_category TEXT,  -- small/medium/large/enterprise
    is_public BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(normalized_name)
);

CREATE INDEX idx_companies_domain ON companies(domain);
CREATE INDEX idx_companies_ein ON companies(ein);
CREATE INDEX idx_companies_name ON companies(normalized_name);

-- Company aliases for entity matching
CREATE TABLE IF NOT EXISTS company_aliases (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    source TEXT,  -- Where this alias was observed
    confidence DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(alias, company_id)
);

CREATE INDEX idx_company_aliases_alias ON company_aliases(alias);

-- Metro areas (CBSA-based)
CREATE TABLE IF NOT EXISTS metro_areas (
    id SERIAL PRIMARY KEY,
    cbsa_code TEXT UNIQUE,
    name TEXT NOT NULL,
    state TEXT,
    country TEXT DEFAULT 'US',
    population INTEGER,
    cost_of_living_index DECIMAL(5,2),  -- For salary adjustments
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Normalized locations
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    city TEXT,
    state TEXT,
    country TEXT DEFAULT 'US',
    metro_id INTEGER REFERENCES metro_areas(id),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    zip_code TEXT,
    is_remote BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, state, country)
);

CREATE INDEX idx_locations_metro ON locations(metro_id);
CREATE INDEX idx_locations_geo ON locations(latitude, longitude);


-- ============================================================================
-- PHASE 4: CANONICAL ROLE ONTOLOGY
-- ============================================================================

-- Canonical roles (SOC/O*NET aligned)
CREATE TABLE IF NOT EXISTS canonical_roles (
    id SERIAL PRIMARY KEY,
    soc_code TEXT,  -- SOC occupation code
    onet_code TEXT,  -- O*NET code
    name TEXT NOT NULL,
    role_family TEXT,  -- Engineering, Sales, Healthcare, etc.
    category TEXT,  -- Major occupation group
    description TEXT,
    typical_skills TEXT[],  -- Array of typical skills
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(soc_code)
);

CREATE INDEX idx_canonical_roles_family ON canonical_roles(role_family);
CREATE INDEX idx_canonical_roles_category ON canonical_roles(category);

-- Title to role mapping rules
CREATE TABLE IF NOT EXISTS title_mapping_rules (
    id SERIAL PRIMARY KEY,
    pattern TEXT NOT NULL,  -- Regex or exact match
    canonical_role_id INTEGER REFERENCES canonical_roles(id),
    seniority_level TEXT,  -- intern/entry/mid/senior/lead/manager/director/exec
    confidence DECIMAL(3,2) DEFAULT 0.9,
    rule_type TEXT DEFAULT 'regex',  -- regex/exact/ml
    priority INTEGER DEFAULT 100,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_title_mapping_pattern ON title_mapping_rules(pattern);


-- ============================================================================
-- PHASE 2: SOURCE ACQUISITION LAYER
-- ============================================================================

-- Source registry
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    type TEXT NOT NULL,  -- payroll/posting/visa/cba/macro/directory
    reliability_tier TEXT,  -- A/B/C/D
    base_reliability DECIMAL(3,2) DEFAULT 0.5,
    terms_classification TEXT,  -- Legal/compliance notes
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw source data envelope
CREATE TABLE IF NOT EXISTS source_data_raw (
    id BIGSERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    raw_company TEXT,
    raw_location TEXT,
    raw_title TEXT,
    raw_description TEXT,
    raw_salary_min DECIMAL(12,2),
    raw_salary_max DECIMAL(12,2),
    raw_salary_text TEXT,
    source_url TEXT,
    source_document_id TEXT,
    as_of_date DATE,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB,  -- Full raw data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_source_data_source ON source_data_raw(source_id);
CREATE INDEX idx_source_data_dates ON source_data_raw(first_seen, last_seen);
CREATE INDEX idx_source_data_raw_json ON source_data_raw USING GIN(raw_data);


-- ============================================================================
-- PHASE 1 & 5-6: OBSERVED EVIDENCE (Row-level observations)
-- ============================================================================

-- Observed job rows (what we actually saw)
CREATE TABLE IF NOT EXISTS observed_jobs (
    id BIGSERIAL PRIMARY KEY,

    -- Identity
    company_id INTEGER REFERENCES companies(id),
    location_id INTEGER REFERENCES locations(id),
    canonical_role_id INTEGER REFERENCES canonical_roles(id),

    -- Raw observed fields
    raw_title TEXT NOT NULL,
    raw_company TEXT,
    raw_location TEXT,
    title_confidence DECIMAL(3,2),
    seniority TEXT,  -- intern/entry/mid/senior/lead/manager/director/exec
    seniority_confidence DECIMAL(3,2),

    -- Job details
    employment_type TEXT,  -- FT/PT/contract
    description TEXT,
    requirements TEXT,

    -- Compensation (as observed)
    salary_min DECIMAL(12,2),
    salary_max DECIMAL(12,2),
    salary_point DECIMAL(12,2),
    salary_currency TEXT DEFAULT 'USD',
    salary_period TEXT,  -- annual/hourly
    salary_type TEXT,  -- base/total/hourly

    -- Source and provenance
    source_id INTEGER REFERENCES sources(id),
    source_data_id BIGINT REFERENCES source_data_raw(id),
    source_type TEXT NOT NULL,  -- payroll/posting_observed/visa/cba/etc
    observation_weight DECIMAL(3,2) DEFAULT 0.5,

    -- Lifecycle tracking
    record_type TEXT DEFAULT 'observed' CHECK (record_type = 'observed'),
    status TEXT DEFAULT 'active',  -- active/filled/closed/expired
    posted_date TIMESTAMP,
    filled_date TIMESTAMP,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_observed_jobs_company ON observed_jobs(company_id);
CREATE INDEX idx_observed_jobs_location ON observed_jobs(location_id);
CREATE INDEX idx_observed_jobs_role ON observed_jobs(canonical_role_id);
CREATE INDEX idx_observed_jobs_source ON observed_jobs(source_id);
CREATE INDEX idx_observed_jobs_status ON observed_jobs(status, last_seen);
CREATE INDEX idx_observed_jobs_metadata ON observed_jobs USING GIN(metadata);


-- ============================================================================
-- PHASE 5: JOB POSTING LIFECYCLE TRACKING
-- ============================================================================

-- Job posting lifecycle events
CREATE TABLE IF NOT EXISTS posting_lifecycle (
    id BIGSERIAL PRIMARY KEY,
    external_id TEXT NOT NULL,  -- job_id from ATS/board
    source_id INTEGER REFERENCES sources(id),
    company_id INTEGER REFERENCES companies(id),
    canonical_role_id INTEGER REFERENCES canonical_roles(id),

    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    disappeared_date TIMESTAMP,

    -- Filled vs closed inference
    filled_probability DECIMAL(4,3),  -- 0.000 to 1.000
    closure_reason TEXT,  -- filled/canceled/expired/other

    posting_duration_days INTEGER,

    -- Features for classification
    company_posting_cadence JSONB,  -- Historical patterns
    role_scarcity_signals JSONB,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(external_id, source_id)
);

CREATE INDEX idx_posting_lifecycle_company ON posting_lifecycle(company_id);
CREATE INDEX idx_posting_lifecycle_disappeared ON posting_lifecycle(disappeared_date);


-- ============================================================================
-- PHASE 6: COMPENSATION OBSERVATIONS
-- ============================================================================

-- Unified compensation observations
CREATE TABLE IF NOT EXISTS compensation_observations (
    id BIGSERIAL PRIMARY KEY,

    -- Links
    company_id INTEGER REFERENCES companies(id),
    metro_id INTEGER REFERENCES metro_areas(id),
    location_id INTEGER REFERENCES locations(id),
    canonical_role_id INTEGER REFERENCES canonical_roles(id),
    seniority TEXT,

    -- Compensation data
    pay_type TEXT,  -- base/total/hourly
    value_min DECIMAL(12,2),
    value_max DECIMAL(12,2),
    value_point DECIMAL(12,2),
    currency TEXT DEFAULT 'USD',
    annualized_base DECIMAL(12,2),  -- Normalized to annual base

    -- Provenance
    source_id INTEGER REFERENCES sources(id),
    source_type TEXT,
    observation_weight DECIMAL(3,2) DEFAULT 0.5,
    observed_date DATE,

    -- Context
    employment_type TEXT,  -- FT/PT/contract

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_comp_obs_company ON compensation_observations(company_id);
CREATE INDEX idx_comp_obs_metro ON compensation_observations(metro_id);
CREATE INDEX idx_comp_obs_role ON compensation_observations(canonical_role_id);


-- ============================================================================
-- PHASE 8: MACRO PRIORS (OEWS, QCEW)
-- ============================================================================

-- BLS OEWS metro × occupation employment
CREATE TABLE IF NOT EXISTS oews_estimates (
    id SERIAL PRIMARY KEY,

    metro_id INTEGER REFERENCES metro_areas(id),
    cbsa_code TEXT,
    canonical_role_id INTEGER REFERENCES canonical_roles(id),
    soc_code TEXT,

    -- Employment counts
    employment_count INTEGER,
    employment_rse DECIMAL(5,2),  -- Relative standard error

    -- Wage distributions
    wage_mean DECIMAL(10,2),
    wage_median DECIMAL(10,2),
    wage_p10 DECIMAL(10,2),
    wage_p25 DECIMAL(10,2),
    wage_p75 DECIMAL(10,2),
    wage_p90 DECIMAL(10,2),

    -- Metadata
    reference_year INTEGER,
    reference_period TEXT,  -- e.g., "May 2024"

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(metro_id, canonical_role_id, reference_year)
);

CREATE INDEX idx_oews_metro ON oews_estimates(metro_id);
CREATE INDEX idx_oews_role ON oews_estimates(canonical_role_id);


-- ============================================================================
-- PHASE 9: JOB ARCHETYPES (Company × Metro × Role × Seniority)
-- ============================================================================

-- The core archetype table
CREATE TABLE IF NOT EXISTS job_archetypes (
    id BIGSERIAL PRIMARY KEY,

    -- Archetype dimensions
    company_id INTEGER REFERENCES companies(id),
    metro_id INTEGER REFERENCES metro_areas(id),
    canonical_role_id INTEGER REFERENCES canonical_roles(id),
    seniority TEXT NOT NULL,

    -- Record type flag
    record_type TEXT NOT NULL CHECK (record_type IN ('observed', 'inferred')),

    -- Headcount estimates (distributions)
    headcount_p10 INTEGER,
    headcount_p50 INTEGER,
    headcount_p90 INTEGER,
    headcount_method TEXT,

    -- Salary estimates (distributions)
    salary_p25 DECIMAL(12,2),
    salary_p50 DECIMAL(12,2),
    salary_p75 DECIMAL(12,2),
    salary_mean DECIMAL(12,2),
    salary_stddev DECIMAL(12,2),
    salary_currency TEXT DEFAULT 'USD',
    salary_method TEXT,

    -- Description
    description TEXT,
    description_sources TEXT[],
    description_confidence DECIMAL(3,2),

    -- Employment type
    employment_type TEXT,  -- FT/PT/contract

    -- Evidence summary
    observed_count INTEGER DEFAULT 0,
    filled_probability_weighted_count DECIMAL(8,2),
    evidence_summary JSONB,  -- Counts by evidence type

    -- Confidence and provenance
    composite_confidence DECIMAL(3,2),
    confidence_components JSONB,
    top_sources JSONB,  -- Top contributing sources by weight

    -- Dates
    evidence_date_earliest DATE,
    evidence_date_latest DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint on archetype
    UNIQUE(company_id, metro_id, canonical_role_id, seniority, record_type)
);

CREATE INDEX idx_archetypes_company ON job_archetypes(company_id);
CREATE INDEX idx_archetypes_metro ON job_archetypes(metro_id);
CREATE INDEX idx_archetypes_role ON job_archetypes(canonical_role_id);
CREATE INDEX idx_archetypes_type ON job_archetypes(record_type);
CREATE INDEX idx_archetypes_confidence ON job_archetypes(composite_confidence);


-- ============================================================================
-- PHASE 10: PROVENANCE AND AUDITABILITY
-- ============================================================================

-- Evidence links: ties archetypes back to source observations
CREATE TABLE IF NOT EXISTS archetype_evidence (
    id BIGSERIAL PRIMARY KEY,
    archetype_id BIGINT REFERENCES job_archetypes(id) ON DELETE CASCADE,

    -- What evidence contributed
    evidence_type TEXT NOT NULL,  -- payroll/posting/visa/cba/posting_lifecycle/oews
    evidence_id BIGINT,  -- ID in the respective evidence table
    evidence_weight DECIMAL(5,3),

    -- Source info
    source_id INTEGER REFERENCES sources(id),
    source_document_id TEXT,

    -- What it contributed to
    contributed_to TEXT[],  -- headcount/salary/description/existence

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_archetype_evidence_archetype ON archetype_evidence(archetype_id);
CREATE INDEX idx_archetype_evidence_type ON archetype_evidence(evidence_type);


-- ============================================================================
-- PHASE 7: DESCRIPTION GENERATION
-- ============================================================================

-- Canonical role description templates
CREATE TABLE IF NOT EXISTS role_description_templates (
    id SERIAL PRIMARY KEY,
    canonical_role_id INTEGER REFERENCES canonical_roles(id),

    responsibilities TEXT[],
    required_skills TEXT[],
    tools_technologies TEXT[],
    qualifications TEXT[],

    source TEXT,  -- onet/manual/learned
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Industry × Role phrase banks (learned from observed postings)
CREATE TABLE IF NOT EXISTS industry_role_phrases (
    id SERIAL PRIMARY KEY,
    canonical_role_id INTEGER REFERENCES canonical_roles(id),
    industry TEXT,
    seniority TEXT,

    phrase_type TEXT,  -- skill/responsibility/tool/requirement
    phrase TEXT,
    frequency INTEGER,
    weight DECIMAL(5,3),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_industry_phrases_role ON industry_role_phrases(canonical_role_id, industry);


-- Company description style (only when enough evidence)
CREATE TABLE IF NOT EXISTS company_description_style (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),

    min_postings_required INTEGER DEFAULT 10,
    postings_analyzed INTEGER,

    common_requirements TEXT[],
    common_tools TEXT[],
    tone_markers JSONB,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================================
-- PHASE 11: HUMAN REVIEW QUEUE
-- ============================================================================

-- Human review queue for low-confidence items
CREATE TABLE IF NOT EXISTS review_queue (
    id BIGSERIAL PRIMARY KEY,

    item_type TEXT NOT NULL,  -- title_mapping/company_match/archetype_verification
    item_id BIGINT,

    issue_description TEXT,
    current_value TEXT,
    suggested_value TEXT,
    confidence DECIMAL(3,2),

    status TEXT DEFAULT 'pending',  -- pending/approved/rejected/fixed
    reviewed_by TEXT,
    reviewed_at TIMESTAMP,
    resolution_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_review_queue_status ON review_queue(status);
CREATE INDEX idx_review_queue_type ON review_queue(item_type);


-- ============================================================================
-- PHASE 12: OPERATIONAL METADATA
-- ============================================================================

-- Pipeline run log
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,

    run_type TEXT,  -- full/incremental/source_update
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status TEXT DEFAULT 'running',  -- running/completed/failed

    -- Metrics
    sources_processed INTEGER,
    raw_records_ingested INTEGER,
    observed_jobs_created INTEGER,
    observed_jobs_updated INTEGER,
    archetypes_created INTEGER,
    archetypes_updated INTEGER,

    -- Quality metrics
    coverage_metrics JSONB,
    quality_metrics JSONB,

    error_log TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Quality metrics tracking
CREATE TABLE IF NOT EXISTS quality_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,

    -- Coverage metrics
    observed_jobs_by_source JSONB,
    archetypes_by_metro JSONB,
    archetypes_by_industry JSONB,

    -- Quality metrics (title mapping)
    title_mapping_high_confidence_pct DECIMAL(5,2),
    title_mapping_manual_review_count INTEGER,

    -- Quality metrics (pay)
    pay_mae DECIMAL(10,2),
    pay_calibration_error DECIMAL(5,2),

    -- Quality metrics (description)
    description_relevance_score DECIMAL(3,2),

    -- Honesty metric
    inferred_properly_labeled_pct DECIMAL(5,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(metric_date)
);


-- ============================================================================
-- HELPER VIEWS
-- ============================================================================

-- View: All jobs (observed + inferred archetypes)
CREATE OR REPLACE VIEW all_jobs AS
SELECT
    'observed' as job_type,
    o.id,
    c.name as company_name,
    m.name as metro_name,
    r.name as role_name,
    o.seniority,
    o.raw_title as title,
    o.salary_min,
    o.salary_max,
    o.status,
    o.record_type,
    NULL as headcount,
    o.created_at
FROM observed_jobs o
LEFT JOIN companies c ON o.company_id = c.id
LEFT JOIN locations l ON o.location_id = l.id
LEFT JOIN metro_areas m ON l.metro_id = m.id
LEFT JOIN canonical_roles r ON o.canonical_role_id = r.id

UNION ALL

SELECT
    'archetype' as job_type,
    a.id,
    c.name as company_name,
    m.name as metro_name,
    r.name as role_name,
    a.seniority,
    r.name || ' - ' || a.seniority as title,
    a.salary_p25 as salary_min,
    a.salary_p75 as salary_max,
    a.record_type as status,
    a.record_type,
    a.headcount_p50 as headcount,
    a.created_at
FROM job_archetypes a
LEFT JOIN companies c ON a.company_id = c.id
LEFT JOIN metro_areas m ON a.metro_id = m.id
LEFT JOIN canonical_roles r ON a.canonical_role_id = r.id;


-- View: Coverage summary
CREATE OR REPLACE VIEW coverage_summary AS
SELECT
    m.name as metro,
    r.role_family,
    COUNT(DISTINCT a.id) as archetype_count,
    SUM(a.headcount_p50) as estimated_total_employment,
    AVG(a.composite_confidence) as avg_confidence,
    COUNT(DISTINCT CASE WHEN a.salary_p50 IS NOT NULL THEN a.id END) as with_salary_count,
    COUNT(DISTINCT CASE WHEN a.description IS NOT NULL THEN a.id END) as with_description_count
FROM job_archetypes a
JOIN metro_areas m ON a.metro_id = m.id
JOIN canonical_roles r ON a.canonical_role_id = r.id
WHERE a.record_type = 'inferred'
GROUP BY m.name, r.role_family
ORDER BY estimated_total_employment DESC;


-- ============================================================================
-- INITIAL SEED DATA
-- ============================================================================

-- ============================================================================
-- LICENSED PROFESSIONALS TABLE (NPI, Bar, Teachers, Trades)
-- ============================================================================

-- Licensed professionals from regulatory databases
CREATE TABLE IF NOT EXISTS licensed_professionals (
    id BIGSERIAL PRIMARY KEY,

    -- License info
    license_type TEXT NOT NULL,  -- npi/bar/teacher/electrician/plumber/real_estate
    license_number TEXT NOT NULL,
    state TEXT NOT NULL,

    -- Person info
    first_name TEXT,
    last_name TEXT,
    credential TEXT,  -- MD, RN, JD, etc.

    -- Professional info
    raw_title TEXT,  -- Job title/specialty
    taxonomy_code TEXT,  -- NPI taxonomy or equivalent
    employer_name TEXT,
    employer_city TEXT,
    employer_state TEXT,

    -- Status
    license_status TEXT,  -- active/inactive/expired
    issue_date DATE,
    expiration_date DATE,

    -- Source and provenance
    source TEXT NOT NULL,
    source_url TEXT,
    source_document_id TEXT,

    -- Confidence
    confidence_score REAL DEFAULT 0.90,

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint
    UNIQUE(license_type, license_number, state)
);

CREATE INDEX idx_licensed_prof_type ON licensed_professionals(license_type);
CREATE INDEX idx_licensed_prof_state ON licensed_professionals(state);
CREATE INDEX idx_licensed_prof_employer ON licensed_professionals(employer_name);
CREATE INDEX idx_licensed_prof_title ON licensed_professionals(raw_title);


-- ============================================================================
-- COMPANY HEADCOUNTS TABLE (from SEC, 990, news)
-- ============================================================================

-- Company employee counts from various sources
CREATE TABLE IF NOT EXISTS company_headcounts (
    id BIGSERIAL PRIMARY KEY,

    -- Company identifiers
    company_id INTEGER REFERENCES companies(id),
    company_name TEXT NOT NULL,
    ein TEXT,  -- Employer ID (nonprofits)
    cik TEXT,  -- SEC CIK (public companies)

    -- Headcount data
    employee_count INTEGER NOT NULL,
    employee_count_is_estimate BOOLEAN DEFAULT FALSE,

    -- Context
    fiscal_year INTEGER,
    fiscal_period TEXT,  -- Q1/Q2/Q3/Q4/FY
    geography TEXT,  -- US/Global/specific state

    -- Source and provenance
    source TEXT NOT NULL,  -- sec_10k/propublica_990/news/website
    source_url TEXT,
    source_document_id TEXT,
    as_of_date DATE,

    -- Confidence
    confidence_score REAL DEFAULT 0.80,

    -- Metadata
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint
    UNIQUE(company_name, fiscal_year, source)
);

CREATE INDEX idx_headcounts_company ON company_headcounts(company_id);
CREATE INDEX idx_headcounts_ein ON company_headcounts(ein);
CREATE INDEX idx_headcounts_name ON company_headcounts(company_name);
CREATE INDEX idx_headcounts_year ON company_headcounts(fiscal_year);


-- ============================================================================
-- INITIAL SEED DATA
-- ============================================================================

-- Insert source types
INSERT INTO sources (name, type, reliability_tier, base_reliability, is_active) VALUES
-- Tier A: Government Payroll
('state_payroll', 'payroll', 'A', 0.95, TRUE),
('ma_state_payroll', 'payroll', 'A', 0.95, TRUE),
('boston_payroll', 'payroll', 'A', 0.95, TRUE),
('cambridge_payroll', 'payroll', 'A', 0.95, TRUE),
('federal_opm', 'payroll', 'A', 0.95, TRUE),
('university_payroll', 'payroll', 'A', 0.95, TRUE),
('hospital_payroll', 'payroll', 'A', 0.95, TRUE),

-- Tier A: Visa Data
('h1b_visa', 'visa', 'A', 0.85, TRUE),
('perm_visa', 'visa', 'A', 0.85, TRUE),

-- Tier A: Licensed Professionals
('npi_registry', 'license', 'A', 0.90, TRUE),
('ma_bar', 'license', 'A', 0.90, TRUE),
('ma_teacher', 'license', 'A', 0.85, TRUE),
('ma_trades', 'license', 'A', 0.85, TRUE),

-- Tier A: Union/CBA
('cba_pay_table', 'cba', 'A', 0.90, TRUE),

-- Tier B: Nonprofits
('propublica_990', 'nonprofit', 'B', 0.80, TRUE),
('irs_990', 'nonprofit', 'B', 0.75, TRUE),

-- Tier B: Job Postings
('indeed_observed', 'posting', 'B', 0.70, TRUE),
('linkedin_observed', 'posting', 'B', 0.70, TRUE),
('greenhouse_ats', 'posting', 'B', 0.75, TRUE),
('adzuna', 'posting', 'B', 0.70, TRUE),

-- Tier B: Inferred
('posting_disappeared', 'posting_lifecycle', 'B', 0.60, TRUE),

-- Tier C: Directories and Macro
('company_directory', 'directory', 'C', 0.50, TRUE),
('oews_macro', 'macro', 'C', 0.40, TRUE),
('qcew_macro', 'macro', 'C', 0.40, TRUE),
('bls_oews', 'macro', 'C', 0.40, TRUE)
ON CONFLICT (name) DO NOTHING;

-- Insert seniority levels as a reference
-- (These will be used in the seniority column across tables)
-- intern, entry, mid, senior, lead, manager, director, exec

COMMENT ON TABLE job_archetypes IS 'Core archetype table: Company × Metro × Role × Seniority. Both observed (high-confidence aggregations) and inferred (fill-in) archetypes.';
COMMENT ON TABLE observed_jobs IS 'Individual job observations from sources. These are OBSERVED, not inferred. Row-level evidence.';
COMMENT ON TABLE compensation_observations IS 'Unified table of all salary observations across sources. Used to calibrate salary models.';
COMMENT ON TABLE archetype_evidence IS 'Provenance: links archetypes back to the evidence that created them. Auditability.';
COMMENT ON COLUMN job_archetypes.record_type IS 'observed = high-confidence aggregation from observed rows. inferred = fill-in from modeling.';
COMMENT ON COLUMN job_archetypes.composite_confidence IS 'Overall confidence score 0-1 for this archetype. See confidence_components for breakdown.';
