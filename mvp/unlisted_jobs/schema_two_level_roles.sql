-- ============================================================================
-- TWO-LEVEL ROLE SYSTEM SCHEMA
-- ============================================================================
-- Level 1: Canonical Roles (coarse, stable, model-friendly)
-- Level 2: Specialization Tags (domain, tech_stack, function)
-- ============================================================================

-- ============================================================================
-- CONTROLLED VOCABULARIES FOR TAGS
-- ============================================================================

-- Domain tags: What industry/vertical/problem space
CREATE TABLE domain_tags (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    category TEXT,  -- e.g., 'Industry', 'Problem Space', 'Sector'
    description TEXT,
    aliases TEXT[],  -- Alternative names that map to this tag
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed domain tags
INSERT INTO domain_tags (name, category, aliases) VALUES
    -- Industries
    ('Healthcare', 'Industry', ARRAY['Health', 'Medical', 'Clinical', 'Pharma', 'Biotech']),
    ('Fintech', 'Industry', ARRAY['Financial Services', 'Banking', 'Payments', 'Insurance']),
    ('E-commerce', 'Industry', ARRAY['Retail', 'Marketplace', 'Shopping']),
    ('Gaming', 'Industry', ARRAY['Games', 'Game Dev']),
    ('Adtech', 'Industry', ARRAY['Advertising', 'Marketing Tech']),
    ('Edtech', 'Industry', ARRAY['Education', 'Learning']),
    ('Govtech', 'Industry', ARRAY['Government', 'Public Sector', 'Civic Tech']),
    ('Real Estate', 'Industry', ARRAY['Proptech', 'Property']),
    ('Logistics', 'Industry', ARRAY['Supply Chain', 'Shipping', 'Transportation']),
    ('Media', 'Industry', ARRAY['Entertainment', 'Streaming', 'Content']),
    -- Problem Spaces
    ('Security', 'Problem Space', ARRAY['Cybersecurity', 'InfoSec', 'AppSec']),
    ('AI/ML', 'Problem Space', ARRAY['Machine Learning', 'Artificial Intelligence', 'Deep Learning']),
    ('Data', 'Problem Space', ARRAY['Analytics', 'BI', 'Data Science']),
    ('Cloud', 'Problem Space', ARRAY['Cloud Computing', 'Cloud Native']),
    ('Mobile', 'Problem Space', ARRAY['iOS', 'Android', 'Mobile Apps']),
    ('Crypto', 'Problem Space', ARRAY['Blockchain', 'Web3', 'DeFi']);

-- Tech stack tags: What tools/technologies
CREATE TABLE tech_stack_tags (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    category TEXT,  -- e.g., 'Language', 'Framework', 'Infrastructure', 'Database', 'Cloud'
    description TEXT,
    aliases TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed tech stack tags (representative sample)
INSERT INTO tech_stack_tags (name, category, aliases) VALUES
    -- Languages
    ('Python', 'Language', ARRAY['py']),
    ('JavaScript', 'Language', ARRAY['JS', 'Node', 'NodeJS']),
    ('TypeScript', 'Language', ARRAY['TS']),
    ('Java', 'Language', ARRAY['JVM']),
    ('Go', 'Language', ARRAY['Golang']),
    ('Rust', 'Language', NULL),
    ('C++', 'Language', ARRAY['CPP']),
    ('SQL', 'Language', NULL),
    -- Frameworks
    ('React', 'Framework', ARRAY['ReactJS', 'React.js']),
    ('Django', 'Framework', NULL),
    ('FastAPI', 'Framework', NULL),
    ('Spring', 'Framework', ARRAY['Spring Boot']),
    ('Rails', 'Framework', ARRAY['Ruby on Rails', 'RoR']),
    -- Infrastructure
    ('Kubernetes', 'Infrastructure', ARRAY['K8s', 'k8s']),
    ('Docker', 'Infrastructure', ARRAY['Containers']),
    ('Terraform', 'Infrastructure', ARRAY['TF', 'IaC']),
    ('AWS', 'Cloud', ARRAY['Amazon Web Services', 'EC2', 'S3', 'Lambda']),
    ('GCP', 'Cloud', ARRAY['Google Cloud', 'Google Cloud Platform']),
    ('Azure', 'Cloud', ARRAY['Microsoft Azure']),
    -- Databases
    ('PostgreSQL', 'Database', ARRAY['Postgres', 'PG']),
    ('MySQL', 'Database', NULL),
    ('MongoDB', 'Database', ARRAY['Mongo']),
    ('Redis', 'Database', NULL),
    ('Elasticsearch', 'Database', ARRAY['ES', 'Elastic']),
    -- Data Tools
    ('Spark', 'Data', ARRAY['Apache Spark', 'PySpark']),
    ('Snowflake', 'Data', NULL),
    ('Airflow', 'Data', ARRAY['Apache Airflow']),
    ('dbt', 'Data', ARRAY['data build tool']),
    ('Kafka', 'Data', ARRAY['Apache Kafka']);

-- Function tags: What team/focus area
CREATE TABLE function_tags (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    category TEXT,  -- e.g., 'Team Type', 'Focus Area'
    description TEXT,
    aliases TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed function tags
INSERT INTO function_tags (name, category, aliases) VALUES
    -- Engineering Teams
    ('Platform', 'Team Type', ARRAY['Platform Engineering', 'Developer Platform']),
    ('Infrastructure', 'Team Type', ARRAY['Infra', 'Core Infrastructure']),
    ('Backend', 'Team Type', ARRAY['Server-side', 'API']),
    ('Frontend', 'Team Type', ARRAY['UI', 'Client-side', 'Web']),
    ('Fullstack', 'Team Type', ARRAY['Full Stack', 'Full-stack']),
    ('DevOps', 'Team Type', ARRAY['SRE', 'Reliability']),
    ('QA', 'Team Type', ARRAY['Quality', 'Testing', 'Test Engineering']),
    -- Focus Areas
    ('Growth', 'Focus Area', ARRAY['Growth Engineering', 'Experimentation']),
    ('Core', 'Focus Area', ARRAY['Core Product', 'Core Systems']),
    ('Internal Tools', 'Focus Area', ARRAY['Tooling', 'Developer Experience', 'DX']),
    ('Compliance', 'Focus Area', ARRAY['Regulatory', 'GRC']),
    ('Integrations', 'Focus Area', ARRAY['Partnerships', 'API Integrations']),
    ('Search', 'Focus Area', ARRAY['Discovery', 'Relevance']),
    ('Recommendations', 'Focus Area', ARRAY['Personalization', 'RecSys']),
    ('Payments', 'Focus Area', ARRAY['Billing', 'Checkout', 'Transactions']),
    ('Identity', 'Focus Area', ARRAY['Auth', 'Authentication', 'IAM']),
    ('Messaging', 'Focus Area', ARRAY['Notifications', 'Communications']),
    ('Observability', 'Focus Area', ARRAY['Monitoring', 'Logging', 'Tracing']);


-- ============================================================================
-- JOB SPECIALIZATIONS (Level 2 tags attached to observed_jobs)
-- ============================================================================

-- Junction table: observed_jobs <-> domain_tags
CREATE TABLE job_domain_tags (
    id SERIAL PRIMARY KEY,
    observed_job_id INTEGER REFERENCES observed_jobs(id) ON DELETE CASCADE,
    domain_tag_id INTEGER REFERENCES domain_tags(id) ON DELETE CASCADE,
    confidence FLOAT DEFAULT 1.0,  -- How confident are we in this tag?
    source TEXT,  -- Where did this tag come from? 'title', 'description', 'company', 'inferred'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(observed_job_id, domain_tag_id)
);

-- Junction table: observed_jobs <-> tech_stack_tags
CREATE TABLE job_tech_tags (
    id SERIAL PRIMARY KEY,
    observed_job_id INTEGER REFERENCES observed_jobs(id) ON DELETE CASCADE,
    tech_tag_id INTEGER REFERENCES tech_stack_tags(id) ON DELETE CASCADE,
    confidence FLOAT DEFAULT 1.0,
    source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(observed_job_id, tech_tag_id)
);

-- Junction table: observed_jobs <-> function_tags
CREATE TABLE job_function_tags (
    id SERIAL PRIMARY KEY,
    observed_job_id INTEGER REFERENCES observed_jobs(id) ON DELETE CASCADE,
    function_tag_id INTEGER REFERENCES function_tags(id) ON DELETE CASCADE,
    confidence FLOAT DEFAULT 1.0,
    source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(observed_job_id, function_tag_id)
);

-- Indexes for efficient querying
CREATE INDEX idx_job_domain_tags_job ON job_domain_tags(observed_job_id);
CREATE INDEX idx_job_domain_tags_tag ON job_domain_tags(domain_tag_id);
CREATE INDEX idx_job_tech_tags_job ON job_tech_tags(observed_job_id);
CREATE INDEX idx_job_tech_tags_tag ON job_tech_tags(tech_tag_id);
CREATE INDEX idx_job_function_tags_job ON job_function_tags(observed_job_id);
CREATE INDEX idx_job_function_tags_tag ON job_function_tags(function_tag_id);


-- ============================================================================
-- CONVENIENCE VIEW: Denormalized job with all tags
-- ============================================================================

CREATE OR REPLACE VIEW jobs_with_tags AS
SELECT
    oj.id,
    oj.raw_title,
    cr.name as canonical_role,
    oj.seniority,
    c.name as company,
    l.city,
    l.state,
    oj.salary_point,
    -- Aggregate tags as arrays
    ARRAY_AGG(DISTINCT dt.name) FILTER (WHERE dt.name IS NOT NULL) as domain_tags,
    ARRAY_AGG(DISTINCT tt.name) FILTER (WHERE tt.name IS NOT NULL) as tech_tags,
    ARRAY_AGG(DISTINCT ft.name) FILTER (WHERE ft.name IS NOT NULL) as function_tags
FROM observed_jobs oj
LEFT JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
LEFT JOIN companies c ON oj.company_id = c.id
LEFT JOIN locations l ON oj.location_id = l.id
LEFT JOIN job_domain_tags jdt ON oj.id = jdt.observed_job_id
LEFT JOIN domain_tags dt ON jdt.domain_tag_id = dt.id
LEFT JOIN job_tech_tags jtt ON oj.id = jtt.observed_job_id
LEFT JOIN tech_stack_tags tt ON jtt.tech_tag_id = tt.id
LEFT JOIN job_function_tags jft ON oj.id = jft.observed_job_id
LEFT JOIN function_tags ft ON jft.function_tag_id = ft.id
GROUP BY oj.id, oj.raw_title, cr.name, oj.seniority, c.name, l.city, l.state, oj.salary_point;


-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Find all Backend Engineers working on Payments in Fintech
-- SELECT * FROM jobs_with_tags
-- WHERE canonical_role = 'Software Engineer'
--   AND 'Backend' = ANY(function_tags)
--   AND ('Fintech' = ANY(domain_tags) OR 'Payments' = ANY(function_tags));

-- Find all jobs requiring Kubernetes experience
-- SELECT * FROM jobs_with_tags
-- WHERE 'Kubernetes' = ANY(tech_tags);

-- Salary analysis by domain
-- SELECT
--     dt.name as domain,
--     cr.name as role,
--     AVG(oj.salary_point) as avg_salary,
--     COUNT(*) as job_count
-- FROM observed_jobs oj
-- JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
-- JOIN job_domain_tags jdt ON oj.id = jdt.observed_job_id
-- JOIN domain_tags dt ON jdt.domain_tag_id = dt.id
-- WHERE oj.salary_point IS NOT NULL
-- GROUP BY dt.name, cr.name
-- ORDER BY avg_salary DESC;
