-- ============================================================================
-- BLS OEWS (Occupational Employment and Wage Statistics) SCHEMA
-- ============================================================================
-- The macro priors backbone for headcount and salary inference.
--
-- OEWS provides:
--   - Employment by occupation × metro area
--   - Wage percentiles (P10, P25, P50, P75, P90)
--   - Location quotients
--
-- Data is from: https://download.bls.gov/pub/time.series/oe/
-- ============================================================================

-- ============================================================================
-- REFERENCE TABLES (loaded from BLS mapping files)
-- ============================================================================

-- Geographic areas (metros, states, national)
CREATE TABLE IF NOT EXISTS oews_areas (
    area_code VARCHAR(7) PRIMARY KEY,
    state_code VARCHAR(2),
    areatype_code CHAR(1),  -- N=National, S=State, M=Metro
    area_name TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_oews_areas_state ON oews_areas(state_code);
CREATE INDEX IF NOT EXISTS idx_oews_areas_type ON oews_areas(areatype_code);

-- Occupations (SOC codes)
CREATE TABLE IF NOT EXISTS oews_occupations (
    occ_code VARCHAR(6) PRIMARY KEY,
    occ_title TEXT NOT NULL,
    occ_description TEXT,
    display_level INTEGER,  -- Hierarchy level
    is_selectable BOOLEAN DEFAULT TRUE
);

-- Industries (NAICS)
CREATE TABLE IF NOT EXISTS oews_industries (
    industry_code VARCHAR(6) PRIMARY KEY,
    industry_title TEXT NOT NULL,
    display_level INTEGER
);


-- ============================================================================
-- MAIN ESTIMATES TABLE
-- ============================================================================

-- Store OEWS estimates with one row per area × occupation × industry
-- For cross-industry totals, industry_code = '000000'
CREATE TABLE IF NOT EXISTS oews_estimates (
    id SERIAL PRIMARY KEY,

    -- Reference keys
    area_code VARCHAR(7) NOT NULL,      -- Metro/State/National
    occ_code VARCHAR(6) NOT NULL,       -- SOC occupation code
    industry_code VARCHAR(6) DEFAULT '000000',  -- NAICS, '000000' = all industries

    -- Survey info
    year INTEGER NOT NULL,

    -- Employment estimates
    employment INTEGER,                 -- Total employment
    employment_rse DECIMAL(5,1),        -- Relative standard error (%)
    employment_per_1000 DECIMAL(10,3),  -- Employment per 1,000 jobs
    location_quotient DECIMAL(6,2),     -- LQ vs national

    -- Annual wage estimates (most useful for our purposes)
    wage_annual_mean INTEGER,
    wage_annual_p10 INTEGER,
    wage_annual_p25 INTEGER,
    wage_annual_median INTEGER,         -- P50
    wage_annual_p75 INTEGER,
    wage_annual_p90 INTEGER,

    -- Hourly wage estimates
    wage_hourly_mean DECIMAL(8,2),
    wage_hourly_p10 DECIMAL(8,2),
    wage_hourly_p25 DECIMAL(8,2),
    wage_hourly_median DECIMAL(8,2),
    wage_hourly_p75 DECIMAL(8,2),
    wage_hourly_p90 DECIMAL(8,2),

    wage_rse DECIMAL(5,1),              -- Wage relative standard error (%)

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(area_code, occ_code, industry_code, year)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_oews_area ON oews_estimates(area_code);
CREATE INDEX IF NOT EXISTS idx_oews_occ ON oews_estimates(occ_code);
CREATE INDEX IF NOT EXISTS idx_oews_year ON oews_estimates(year);
CREATE INDEX IF NOT EXISTS idx_oews_area_occ ON oews_estimates(area_code, occ_code);


-- ============================================================================
-- MAPPING TABLE: SOC codes to our canonical roles
-- ============================================================================

-- Maps SOC occupation codes to our canonical_roles
-- Many-to-many because some SOC codes map to multiple roles and vice versa
CREATE TABLE IF NOT EXISTS oews_role_mapping (
    id SERIAL PRIMARY KEY,
    occ_code VARCHAR(6) NOT NULL REFERENCES oews_occupations(occ_code),
    canonical_role_id INTEGER NOT NULL REFERENCES canonical_roles(id),
    confidence DECIMAL(3,2) DEFAULT 1.0,  -- How confident is this mapping?
    is_primary BOOLEAN DEFAULT TRUE,       -- Is this the primary mapping?
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(occ_code, canonical_role_id)
);

CREATE INDEX IF NOT EXISTS idx_oews_role_mapping_occ ON oews_role_mapping(occ_code);
CREATE INDEX IF NOT EXISTS idx_oews_role_mapping_role ON oews_role_mapping(canonical_role_id);


-- ============================================================================
-- VIEW: OEWS priors joined with our canonical roles
-- ============================================================================

-- Convenient view for getting OEWS priors for our roles
CREATE OR REPLACE VIEW oews_role_priors AS
SELECT
    oe.area_code,
    oa.area_name,
    oa.areatype_code,
    cr.id as canonical_role_id,
    cr.name as canonical_role,
    oo.occ_code,
    oo.occ_title as soc_title,
    oe.year,
    oe.employment,
    oe.wage_annual_median,
    oe.wage_annual_p25,
    oe.wage_annual_p75,
    oe.wage_annual_p10,
    oe.wage_annual_p90,
    oe.location_quotient,
    orm.confidence as mapping_confidence
FROM oews_estimates oe
JOIN oews_areas oa ON oe.area_code = oa.area_code
JOIN oews_occupations oo ON oe.occ_code = oo.occ_code
JOIN oews_role_mapping orm ON oe.occ_code = orm.occ_code
JOIN canonical_roles cr ON orm.canonical_role_id = cr.id
WHERE oe.industry_code = '000000'  -- Cross-industry totals only
  AND orm.is_primary = TRUE;


-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Get employment and salary priors for Software Engineers in San Francisco
-- SELECT * FROM oews_role_priors
-- WHERE canonical_role = 'Software Engineer'
--   AND area_name LIKE '%San Francisco%'
--   AND year = 2024;

-- Get total employment by role family for a metro
-- SELECT
--     canonical_role,
--     SUM(employment) as total_employment,
--     AVG(wage_annual_median) as avg_median_wage
-- FROM oews_role_priors
-- WHERE area_code = '0041860'  -- San Francisco-Oakland-Berkeley
--   AND year = 2024
-- GROUP BY canonical_role
-- ORDER BY total_employment DESC;

-- Compare our inferred headcount to OEWS totals (validation)
-- SELECT
--     orp.area_name,
--     orp.canonical_role,
--     orp.employment as oews_employment,
--     ja.total_headcount as inferred_headcount,
--     (ja.total_headcount::float / NULLIF(orp.employment, 0)) as coverage_ratio
-- FROM oews_role_priors orp
-- LEFT JOIN (
--     SELECT metro, canonical_role_id, SUM(headcount_p50) as total_headcount
--     FROM job_archetypes
--     GROUP BY metro, canonical_role_id
-- ) ja ON orp.area_name ILIKE '%' || ja.metro || '%'
--      AND orp.canonical_role_id = ja.canonical_role_id
-- WHERE orp.year = 2024;
