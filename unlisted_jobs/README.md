## Comprehensive Job Database System

A production-grade job database system that builds a complete view of the labor market, including both **observed** (actively listed) and **inferred** (filled/unlisted) positions.

### Core Philosophy

1. **Observed vs. Inferred** is a first-class concept
   - Never store inferred data as if it were observed
   - Every field tagged with provenance
   - Confidence scoring built into the data model

2. **Archetypes, not Seats**
   - Default to Company × Metro × Role × Seniority aggregates
   - Generate synthetic seat-level rows only when needed
   - Distributions, not point estimates

3. **Defensible and Auditable**
   - Full provenance tracking
   - Evidence logging for every inference
   - Human-in-the-loop review queue
   - Weekly quality metrics

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCE ACQUISITION LAYER                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Payroll  │  │ Job Board│  │  Visa    │  │   OEWS   │       │
│  │  (Tier A)│  │ (Tier B) │  │ (Tier A) │  │ (Tier C) │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                      NORMALIZATION LAYER                         │
│  • Company identity resolution                                   │
│  • Location standardization (CBSA metro areas)                   │
│  • Title → Canonical Role mapping (SOC/O*NET)                    │
│  • Seniority detection                                           │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                      OBSERVED JOBS TABLE                         │
│  Row-level observations: payroll rows, postings, visa filings   │
│  Status: observed_count, filled_probability, evidence_summary   │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                    INFERENCE & MODELING                          │
│  • Salary estimation (hierarchical Bayesian model)               │
│  • Headcount allocation (OEWS → company-level)                   │
│  • Description generation (template + industry flavor)           │
│  • Filled-job classifier (posting lifecycle)                     │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                     JOB ARCHETYPES TABLE                         │
│  Company × Metro × Role × Seniority                              │
│  record_type = 'observed' | 'inferred'                           │
│  Salary distributions (P25/P50/P75)                              │
│  Headcount distributions (P10/P50/P90)                           │
│  Confidence scores + provenance                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

See [`schema.sql`](./schema.sql) for the full schema.

### Core Tables

1. **`companies`** - Stable company identifiers
2. **`metro_areas`** - CBSA metropolitan areas
3. **`locations`** - Normalized city/state/metro
4. **`canonical_roles`** - SOC/O*NET aligned roles
5. **`observed_jobs`** - Row-level observations (OBSERVED ONLY)
6. **`job_archetypes`** - Aggregated archetypes (observed + inferred)
7. **`compensation_observations`** - Unified salary observations
8. **`oews_estimates`** - BLS macro priors
9. **`archetype_evidence`** - Provenance links

### Key Design Decisions

**Archetype-first design**
- Archetypes are Company × Metro × Role × Seniority
- Observed archetypes = high-confidence aggregations from observed rows
- Inferred archetypes = fill-ins from modeling
- Synthetic seat-level rows generated only when needed

**Distribution-based estimates**
- Salary: P25/P50/P75 + mean/stddev
- Headcount: P10/P50/P90
- Never store point estimates as truth

**Provenance and confidence**
- Every archetype links back to evidence via `archetype_evidence`
- Confidence scores broken down by component
- `top_sources` JSONB tracks contributing sources by weight

---

## Installation

### Prerequisites

- Python 3.9+
- PostgreSQL 13+
- pip packages: see `requirements.txt`

### Setup

```bash
# 1. Create database
createdb jobs_comprehensive

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Initialize schema
python -c "from database import DatabaseManager; db = DatabaseManager(); db.execute_schema_file('schema.sql')"

# 4. Seed canonical roles
python title_normalizer.py  # or use seed_canonical_roles()
```

---

## Usage

### Basic Pipeline

```python
from database import DatabaseManager, Config
from pipeline import JobDatabasePipeline

# Initialize
config = Config(
    db_name="jobs_comprehensive",
    db_user="postgres"
)
db = DatabaseManager(config)
pipeline = JobDatabasePipeline(db)

# Run end-to-end pipeline
pipeline.run()
```

### Insert Observed Job

```python
# Example: Insert a payroll row
job_data = {
    'company_id': 123,
    'location_id': 456,
    'canonical_role_id': 789,
    'raw_title': 'Senior Software Engineer',
    'seniority': 'senior',
    'seniority_confidence': 0.95,
    'salary_point': 150000,
    'salary_currency': 'USD',
    'source_id': 1,  # state_payroll
    'source_type': 'payroll',
    'observation_weight': 0.95,
    'status': 'filled',
}

job_id = db.insert_observed_job(job_data)
```

### Create Inferred Archetype

```python
# Example: Inferred archetype from modeling
archetype_data = {
    'company_id': 123,
    'metro_id': 456,
    'canonical_role_id': 789,
    'seniority': 'senior',
    'record_type': 'inferred',
    'headcount_p50': 15,
    'salary_p25': 130000,
    'salary_p50': 150000,
    'salary_p75': 175000,
    'salary_method': 'hierarchical_bayesian',
    'description': 'Senior Software Engineers at Acme Corp...',
    'composite_confidence': 0.75,
    'evidence_summary': {
        'payroll_rows': 2,
        'visa_filings': 1,
        'postings_observed': 5,
        'oews_prior': True
    }
}

archetype_id = db.upsert_job_archetype(archetype_data)
```

### Query Archetypes

```python
# Get all archetypes for a company
archetypes = db.get_archetypes_by_company(company_id=123)

# Filter by record type
observed_archetypes = db.get_archetypes_by_company(
    company_id=123,
    record_type='observed'
)

inferred_archetypes = db.get_archetypes_by_company(
    company_id=123,
    record_type='inferred'
)

# Coverage summary
coverage = db.get_coverage_summary()
```

---

## Data Sources by Tier

### Tier A (Highest Reliability)
- **State/municipal payroll** (0.95)
- **University/hospital payroll** (0.95)
- **CBA pay tables** (0.90)
- **H-1B visa filings** (0.85)

### Tier B (Medium Reliability)
- **Observed job postings** (0.70-0.75)
- **ATS feeds** (Greenhouse, Lever) (0.75)
- **Posting lifecycle** (filled inference) (0.60)

### Tier C (Macro Priors)
- **OEWS employment estimates** (0.40)
- **QCEW** (0.40)

---

## Key Workflows

### 1. Weekly Pipeline Run

```bash
python pipeline.py --mode full
```

- Ingests new data from all sources
- Normalizes companies, locations, titles
- Updates observed jobs
- Runs salary/headcount models
- Generates/updates inferred archetypes
- Computes quality metrics

### 2. Human Review Queue

```bash
python pipeline.py --review-queue
```

- Shows low-confidence title mappings
- Shows ambiguous company matches
- Allows manual approval/correction

### 3. Quality Metrics

```bash
python pipeline.py --metrics
```

- Coverage by metro/industry
- Title mapping confidence distribution
- Salary model MAE on holdout
- Honesty metric (% inferred properly labeled)

---

## Testing the System

### Unit Tests

```bash
pytest tests/
```

### Integration Test

```bash
python tests/test_end_to_end.py
```

Runs a mini pipeline:
1. Seeds canonical roles
2. Inserts sample companies and metros
3. Ingests sample payroll data
4. Creates observed jobs
5. Runs inference
6. Creates inferred archetypes
7. Validates provenance

---

## Phase-by-Phase Implementation

The system implements the 12-phase plan:

| Phase | Component | Status | File |
|-------|-----------|--------|------|
| 1 | Database schema | ✅ | `schema.sql` |
| 2 | Source acquisition | ✅ | `sources/` |
| 3 | Identity graph | ✅ | `database.py` |
| 4 | Title normalization | ✅ | `title_normalizer.py` |
| 5 | Evidence model | ✅ | `evidence.py` |
| 6 | Salary estimation | 🚧 | `models/salary_model.py` |
| 7 | Description generation | 🚧 | `models/description_gen.py` |
| 8 | Headcount estimation | 🚧 | `models/headcount_model.py` |
| 9 | Archetype synthesis | ✅ | `pipeline.py` |
| 10 | Confidence scoring | ✅ | `confidence.py` |
| 11 | Product integration | 📋 | TBD |
| 12 | Operations | 🚧 | `pipeline.py`, dashboards TBD |

Legend: ✅ Complete | 🚧 In Progress | 📋 Planned

---

## Important Rules

### DO
- ✅ Tag every inferred field as inferred
- ✅ Store distributions, not point estimates
- ✅ Link every archetype to evidence
- ✅ Use hierarchical models that shrink to priors when data is sparse
- ✅ Put low-confidence items in review queue

### DON'T
- ❌ Store inferred data as if it were observed
- ❌ Generate company-specific details without evidence
- ❌ Hard-label "filled" without probability
- ❌ Skip provenance tracking

---

## Quality Metrics (Weekly Tracking)

### Coverage
- Observed jobs by source and metro
- Archetypes with non-null pay + description

### Quality (Title)
- % mapped to canonical roles with confidence > 0.7

### Quality (Pay)
- MAE / calibration error on holdout observed salaries

### Quality (Description)
- Relevance scoring (human rubric + embedding similarity)

### Honesty
- % of inferred fields properly labeled

---

## Contributing

1. All changes must preserve observed vs. inferred separation
2. New sources must include reliability tier classification
3. Models must output distributions and confidence scores
4. Add tests for new functionality
5. Update quality metrics as needed

---

## FAQ

**Q: Why archetypes instead of individual job "seats"?**

A: Most use cases need aggregates. Generating millions of synthetic seat rows is wasteful and misleading. Archetypes give you Company × Role distributions, which is what matters for market intelligence. If you need seat-level for a specific product feature, generate them on-demand.

**Q: Why separate observed_jobs and job_archetypes tables?**

A: Observed jobs are row-level evidence (1 row = 1 observation). Archetypes are aggregations or inferences. Keeping them separate ensures you always know what's real vs. what's modeled.

**Q: Can I query both observed and inferred together?**

A: Yes, use the `all_jobs` view or query `job_archetypes` (which includes both record_type='observed' and record_type='inferred').

**Q: What if a company has no observed data?**

A: The archetype will be inferred entirely from OEWS priors and industry patterns. The confidence score will be low (~0.2-0.4) and the provenance will show "oews_prior" as the main evidence source.

**Q: How do I know if an archetype is trustworthy?**

A: Check `composite_confidence` (0-1 scale). Also inspect `evidence_summary` and `top_sources` to see what it's based on. High-confidence archetypes (>0.7) have multiple Tier A or B sources.

---

## License

MIT License. See LICENSE file.

---

## Contact

ShortList.ai Team
- Noah Hopkins (noahhopkins@mit.edu)
- Charles Lai
- Joshua Tang

For questions or issues, please file a GitHub issue or contact the team.
