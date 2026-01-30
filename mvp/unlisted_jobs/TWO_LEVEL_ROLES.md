# Two-Level Role System

## Overview

The role system has two levels to balance model stability with job specificity:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ LEVEL 1: Canonical Roles (coarse, stable, model-friendly)              │
│ ─────────────────────────────────────────────────────────              │
│ • 50-150 roles total                                                   │
│ • Maps to SOC/O*NET codes                                              │
│ • Enough volume per role for ML models                                 │
│ • Examples: Software Engineer, Data Scientist, Product Manager         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ LEVEL 2: Specialization Tags (fine-grained, flexible)                  │
│ ─────────────────────────────────────────────────────                  │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │   DOMAIN     │  │  TECH STACK  │  │   FUNCTION   │                  │
│  │  (vertical)  │  │   (tools)    │  │ (team/focus) │                  │
│  ├──────────────┤  ├──────────────┤  ├──────────────┤                  │
│  │ Healthcare   │  │ Python       │  │ Platform     │                  │
│  │ Fintech      │  │ Kubernetes   │  │ Backend      │                  │
│  │ Security     │  │ React        │  │ Growth       │                  │
│  │ AI/ML        │  │ Snowflake    │  │ Payments     │                  │
│  │ E-commerce   │  │ AWS          │  │ Infrastructure│                  │
│  │ ...          │  │ ...          │  │ ...          │                  │
│  └──────────────┘  └──────────────┘  └──────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Example

A job titled "Senior Staff ML Platform Engineer - Payments" becomes:

| Dimension | Value |
|-----------|-------|
| **Canonical Role** | Software Engineer |
| **Seniority** | Staff |
| **Domain Tags** | AI/ML, Fintech |
| **Tech Tags** | (from description) |
| **Function Tags** | Platform, Payments |

## Why This Design?

### Problem with Flat Role Systems

Option A: Too few roles (30-50)
- ✅ Stable for modeling
- ❌ Loses important distinctions (ML Engineer vs Backend Engineer)

Option B: Too many roles (500+)
- ✅ Captures all nuances
- ❌ Sparse data per role, hard to maintain
- ❌ Blurs line between "role" and "specialization"

### Two-Level Solution

- Level 1 stays stable (~100 roles) with statistical mass
- Level 2 captures specificity without fragmenting core roles
- Tags are composable (a job can have multiple)
- Tags are extensible (add new ones without touching role system)

## Schema

### Tables

```sql
-- Level 1 (existing)
canonical_roles (id, name, soc_code, ...)

-- Level 2 controlled vocabularies
domain_tags (id, name, category, aliases)
tech_stack_tags (id, name, category, aliases)
function_tags (id, name, category, aliases)

-- Level 2 junction tables
job_domain_tags (observed_job_id, domain_tag_id, confidence, source)
job_tech_tags (observed_job_id, tech_tag_id, confidence, source)
job_function_tags (observed_job_id, function_tag_id, confidence, source)
```

### Key Fields

- **confidence**: How sure are we? (0.0 - 1.0)
  - Title match: 0.9
  - Description match (multiple): 0.7-0.85
  - Inferred from company: 0.6

- **source**: Where did this tag come from?
  - `title`: Extracted from job title
  - `description`: Extracted from job description
  - `company`: Inferred from company industry
  - `manual`: Human-assigned

## Tag Extraction

Tags are extracted by `TagExtractor` class:

```python
from tag_extractor import TagExtractor

extractor = TagExtractor()

# From title only
tags = extractor.extract_from_title("Senior Backend Engineer, Payments")
# domain: [('Fintech', 0.7)]
# function: [('Backend', 0.9), ('Payments', 0.9)]

# From all sources
tags = extractor.extract_all(
    title="ML Engineer",
    description="Build recommendation systems using PyTorch...",
    company_name="Netflix",
    industry="Media"
)
```

## Query Patterns

### Find all Backend Engineers in Fintech doing Payments

```sql
SELECT * FROM jobs_with_tags
WHERE canonical_role = 'Software Engineer'
  AND 'Backend' = ANY(function_tags)
  AND ('Fintech' = ANY(domain_tags) OR 'Payments' = ANY(function_tags));
```

### Salary by Domain

```sql
SELECT
    dt.name as domain,
    cr.name as role,
    AVG(oj.salary_point) as avg_salary,
    COUNT(*) as job_count
FROM observed_jobs oj
JOIN canonical_roles cr ON oj.canonical_role_id = cr.id
JOIN job_domain_tags jdt ON oj.id = jdt.observed_job_id
JOIN domain_tags dt ON jdt.domain_tag_id = dt.id
WHERE oj.salary_point IS NOT NULL
GROUP BY dt.name, cr.name
ORDER BY avg_salary DESC;
```

### Tech stack popularity by company size

```sql
SELECT
    tt.name as tech,
    c.size_category,
    COUNT(*) as job_count
FROM observed_jobs oj
JOIN companies c ON oj.company_id = c.id
JOIN job_tech_tags jtt ON oj.id = jtt.observed_job_id
JOIN tech_stack_tags tt ON jtt.tech_tag_id = tt.id
GROUP BY tt.name, c.size_category
ORDER BY job_count DESC;
```

## Integration with Job Archetypes

The `job_archetypes` table should use **canonical_role_id** (Level 1) as the primary dimension, not tags.

Tags can be used for:
- Filtering training data for archetype models
- Enriching archetype outputs
- Finding similar archetypes

```sql
-- Archetype for "Fintech Backend Engineer at Series B"
SELECT ja.*
FROM job_archetypes ja
JOIN canonical_roles cr ON ja.canonical_role_id = cr.id
WHERE cr.name = 'Software Engineer'
  AND ja.company_stage = 'Series B'
  -- Would need to add domain as archetype dimension if we want this
```

## Files

| File | Purpose |
|------|---------|
| `schema_two_level_roles.sql` | Database schema for Level 2 |
| `tag_extractor.py` | Extracts tags from title/description |
| `TWO_LEVEL_ROLES.md` | This documentation |

## Next Steps

1. Run `schema_two_level_roles.sql` to create tables
2. Backfill tags for existing jobs using `TagExtractor`
3. Update ingestion pipelines to extract tags on insert
4. Build tag analytics dashboard
