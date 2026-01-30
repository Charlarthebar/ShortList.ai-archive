# ShortList.ai

ShortList.ai is a two-sided job marketplace that rethinks hiring as a continuous matching problem, not a one-time application event. Unlike traditional job boards that index only open roles and optimize for volume, ShortList.ai builds a continually updated view of all jobs (open and filled) and matches them to candidates based on skills, interests, and career trajectory.

## Team Members

- Charles Lai (Massachusetts Institute of Technology - Computer Science and Mathematics)
- Joshua Tang (Massachusetts Institute of Technology - Computer Science, Economics, and Data Science)
- Noah Hopkins (Massachusetts Institute of Technology - Computer Science and Finance)

---

## Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 14+
- Node.js 18+ (for frontend development)

### Running the UI

```bash
# From the repository root
cd mvp/new_UI

# Option 1: Use the run script (starts both backend and frontend)
./run.sh

# Option 2: Run services manually
# Terminal 1 - Backend API:
cd backend && python3 app.py

# Terminal 2 - Frontend:
cd frontend && python3 -m http.server 8000
```

The services will be available at:
- **Backend API**: http://localhost:5002
- **Frontend**: http://localhost:8000

### Database Setup

```bash
# 1. Create the database
createdb jobs_db

# 2. Initialize schema (from new_UI directory)
cd mvp/new_UI
./run.sh --setup

# Or manually:
psql -d jobs_db -f backend/schema.sql
```

### Environment Variables

Copy `.env.template` to `.env` and configure:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=jobs_db
DB_USER=your_username
DB_PASSWORD=your_password
OPENAI_API_KEY=your_openai_key  # For AI features
```

---

## Repository Structure

```
ShortList.ai/
├── mvp/                          # Core MVP code
│   ├── new_UI/                   # Web application (FastAPI + React)
│   ├── unlisted_jobs/            # Job database & data ingestion
│   ├── ai_screening_interview/   # AI interview engine
│   ├── Charlie's Work/           # Jobs collector & data processing
│   ├── job_scraper/              # Multi-state job scraping tool
│   └── interactive-web-agent/    # Browser automation tools
│
├── other/                        # Experimental job aggregator (separate system)
├── data/                         # Shared data files
├── docs/                         # Documentation
├── demo/                         # Demo files
├── slides/                       # Presentation slides
└── logs/                         # Application logs
```

---

## MVP Components

### 1. Web Application (`mvp/new_UI/`)

The main user-facing application with a FastAPI backend and React frontend.

**Backend (`backend/`):**
- `app.py` - Main FastAPI application with all API endpoints
- `interview_service.py` - AI-powered screening interview service
- `scoring_engine.py` - Job-candidate matching and scoring
- `semantic_matcher.py` - Skills-based semantic matching
- `insights_generator.py` - AI-generated career insights
- `digest_service.py` - Email digest generation
- `skill_extractor.py` - Extract skills from resumes/descriptions
- `import_cambridge_jobs.py` - Import jobs from Cambridge collector
- `schema.sql` - Database schema for the UI

**Frontend (`frontend/`):**
- `index.html` - Main HTML entry point
- `app.js` - React application (bundled)
- `styles.css` - Application styles

**Key Features:**
- Job search and browsing
- Candidate profile management
- AI-powered screening interviews
- Job-candidate matching scores
- Career insights and recommendations

### 2. Job Database System (`mvp/unlisted_jobs/`)

A comprehensive job database that tracks both observed (listed) and inferred (filled/unlisted) positions.

**Core Components:**
- `database.py` - Database connection and query management
- `pipeline.py` - Main data ingestion orchestrator
- `title_normalizer.py` - Job title to canonical role mapping
- `schema.sql` - Main database schema

**Data Ingestion Scripts:**
- `ingest_oes.py` - Bureau of Labor Statistics OES data
- `ingest_onet.py` - O*NET occupation database
- `ingest_h1b.py` - H-1B visa data
- `ingest_perm.py` - PERM labor certification data
- `ingest_ma_payroll.py` - Massachusetts state payroll
- `ingest_state_payrolls.py` - Multi-state payroll data
- `ingest_kaggle_*.py` - Kaggle job datasets

**Data Sources (`sources/`):**
- `bls_oews.py` - BLS Occupational Employment Statistics
- `census_cbp.py` - Census County Business Patterns
- `federal_opm.py` - Federal OPM employment data
- `h1b_visa.py` - H-1B visa filings
- `healthcare_npi.py` - Healthcare provider data
- `ma_state_payroll.py` - MA state employee data
- `job_postings/` - ATS connectors (Greenhouse, Lever, Workday, etc.)

**ML Models:**
- `salary_inference.py` - Salary prediction model
- `phase6_salary_model.py` - Hierarchical salary model
- `phase7_headcount_model.py` - Headcount allocation
- `phase8_archetype_inference.py` - Job archetype inference

**Scheduled Tasks:**
- `refresh_postings.py` - Cron-based job posting refresh
- `detect_openings.py` - Detect when filled jobs reopen
- `send_notifications.py` - User notification system

### 3. AI Screening Interview (`mvp/ai_screening_interview/`)

AI-powered candidate screening system.

- `ai_screening_interview.py` - Main interview engine
- `interview_backend.py` - Backend API for interview service
- Sample resumes and job descriptions for testing

**Features:**
- Dynamic question generation based on job requirements
- Real-time candidate evaluation
- Structured output with scores and recommendations

### 4. Jobs Data Collector (`mvp/Charlie's Work/`)

Real-time job data collection from multiple sources.

- `cambridge_jobs_collector.py` - Cambridge area jobs
- `nationwide_jobs_collector.py` - Nationwide job collection
- `github_collector.py` - GitHub job postings
- `extract_skills.py` - Skill extraction from job descriptions
- `schedule_scraping.py` - Automated scraping scheduler

**Data Storage:**
- `cambridge_jobs.db` - SQLite database for Cambridge jobs
- `nationwide_jobs.db` - SQLite database for nationwide jobs

### 5. Multi-State Job Scraper (`mvp/job_scraper/`)

Systematic job aggregation tool targeting 90%+ coverage.

- `main.py` - Main orchestrator
- `scrape_jobs_api.py` - API-based job scraping
- `scrapers/` - Platform-specific scrapers (Indeed, LinkedIn, etc.)
- `core/` - Iteration logic, deduplication, data models

**Usage:**
```bash
python main.py --state north_carolina
python main.py --all-states
```

---

## Database Architecture

The system uses PostgreSQL with the following key tables:

**Core Tables:**
- `companies` - Employer information
- `metro_areas` - CBSA metropolitan areas
- `locations` - Normalized city/state/metro
- `canonical_roles` - SOC/O*NET aligned job roles
- `observed_jobs` - Actual job observations
- `job_archetypes` - Company x Metro x Role aggregates

**Platform Tables (UI):**
- `users` - User accounts
- `candidates` - Candidate profiles
- `jobs` - Job listings
- `applications` - Job applications
- `interviews` - Interview records

---

## Running Data Pipelines

### Initialize the Job Database

```bash
cd mvp/unlisted_jobs

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create database and initialize schema
createdb jobs_comprehensive
python pipeline.py --init-schema --seed-roles

# Run full pipeline
python pipeline.py --mode full
```

### Refresh Job Postings

```bash
# Manual refresh
python refresh_postings.py

# Specific ATS only
python refresh_postings.py --ats greenhouse

# Dry run (no database changes)
python refresh_postings.py --dry-run
```

### Run Jobs Collector

```bash
cd "mvp/Charlie's Work"

# Cambridge area jobs
python cambridge_jobs_collector.py

# Nationwide jobs
python nationwide_jobs_collector.py
```

---

## API Endpoints

The backend provides the following key endpoints:

**Jobs:**
- `GET /api/jobs` - List jobs with filtering
- `GET /api/jobs/{id}` - Get job details
- `POST /api/jobs/search` - Search jobs

**Candidates:**
- `GET /api/candidates` - List candidates
- `POST /api/candidates` - Create candidate profile
- `GET /api/candidates/{id}/matches` - Get job matches

**Interviews:**
- `POST /api/interviews/start` - Start AI interview
- `POST /api/interviews/{id}/respond` - Submit response
- `GET /api/interviews/{id}/results` - Get interview results

**Matching:**
- `POST /api/match/score` - Score candidate-job match
- `GET /api/insights/{candidate_id}` - Get career insights

---

## Configuration Files

- `.env` - Environment variables (create from `.env.template`)
- `mvp/unlisted_jobs/config.example.json` - Data pipeline config
- `mvp/new_UI/backend/requirements.txt` - Backend Python dependencies

---

## Development

### Backend Development

```bash
cd mvp/new_UI/backend
pip install -r requirements.txt
python app.py  # Runs on port 5002
```

### Running Tests

```bash
# Test database connection
python -c "from database import DatabaseManager; db = DatabaseManager(); db.initialize_pool(); print('Connected')"

# Test title normalizer
python mvp/unlisted_jobs/title_normalizer.py

# Test ATS connectors
python mvp/unlisted_jobs/test_all_connectors.py
```

---

## Project Philosophy

1. **Observed vs. Inferred** - Never store inferred data as if it were observed. Every field tagged with provenance and confidence scores.

2. **Archetypes, not Seats** - Default to Company x Metro x Role x Seniority aggregates. Generate synthetic seat-level rows only when needed.

3. **Continuous Matching** - Job search is not a one-time event. Users can subscribe to filled positions and get notified when they reopen.

4. **Skill-Based Matching** - Profiles evolve over time with skill development, not just static resumes.

---

## License

This repository serves as the system of record for the ShortList.ai project and contains all code, documentation, research, and MVP artifacts in accordance with course requirements.
