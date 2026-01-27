-- Schema additions for new ShortList UI
-- Run this to ensure the necessary columns exist

-- Add resume_path column to platform_users for storing user's saved resume
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'platform_users' AND column_name = 'resume_path') THEN
        ALTER TABLE platform_users ADD COLUMN resume_path VARCHAR(500);
    END IF;
END $$;

-- Add columns to seeker_profiles if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'experience_level') THEN
        ALTER TABLE seeker_profiles ADD COLUMN experience_level VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'work_preference') THEN
        ALTER TABLE seeker_profiles ADD COLUMN work_preference VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'profile_complete') THEN
        ALTER TABLE seeker_profiles ADD COLUMN profile_complete BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- Add columns to watchable_positions if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'role_type') THEN
        ALTER TABLE watchable_positions ADD COLUMN role_type VARCHAR(50);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'description') THEN
        ALTER TABLE watchable_positions ADD COLUMN description TEXT;
    END IF;
END $$;

-- Add columns to shortlist_applications if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'experience_level') THEN
        ALTER TABLE shortlist_applications ADD COLUMN experience_level VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'work_preference') THEN
        ALTER TABLE shortlist_applications ADD COLUMN work_preference VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'resume_path') THEN
        ALTER TABLE shortlist_applications ADD COLUMN resume_path VARCHAR(500);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'status') THEN
        ALTER TABLE shortlist_applications ADD COLUMN status VARCHAR(20) DEFAULT 'pending';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'applied_at') THEN
        ALTER TABLE shortlist_applications ADD COLUMN applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'eligibility_data') THEN
        ALTER TABLE shortlist_applications ADD COLUMN eligibility_data JSONB;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'interview_status') THEN
        ALTER TABLE shortlist_applications ADD COLUMN interview_status VARCHAR(20) DEFAULT 'pending';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'interview_transcript') THEN
        ALTER TABLE shortlist_applications ADD COLUMN interview_transcript JSONB;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'interview_evaluation') THEN
        ALTER TABLE shortlist_applications ADD COLUMN interview_evaluation JSONB;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'interview_completed_at') THEN
        ALTER TABLE shortlist_applications ADD COLUMN interview_completed_at TIMESTAMP;
    END IF;
END $$;

-- Create shortlist_applications table if it doesn't exist
CREATE TABLE IF NOT EXISTS shortlist_applications (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES platform_users(id),
    position_id INTEGER REFERENCES watchable_positions(id),
    experience_level VARCHAR(20),
    work_preference VARCHAR(20),
    resume_path VARCHAR(500),
    status VARCHAR(20) DEFAULT 'pending',
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    eligibility_data JSONB,
    interview_status VARCHAR(20) DEFAULT 'pending',
    interview_transcript JSONB,
    interview_evaluation JSONB,
    interview_completed_at TIMESTAMP,
    UNIQUE(user_id, position_id)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_shortlist_apps_user ON shortlist_applications(user_id);
CREATE INDEX IF NOT EXISTS idx_shortlist_apps_position ON shortlist_applications(position_id);

-- ============================================================================
-- FIT QUESTIONS TABLES
-- ============================================================================

-- Fit questions table - stores the question definitions
CREATE TABLE IF NOT EXISTS fit_questions (
    id SERIAL PRIMARY KEY,
    role_type VARCHAR(50),  -- NULL means applies to all roles, or specific like 'software_engineer', 'sales'
    question_text TEXT NOT NULL,
    question_type VARCHAR(20) NOT NULL,  -- 'multiple_choice' or 'free_response'
    options JSONB,  -- Array of {label, value} for multiple choice questions
    display_order INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Application fit responses - stores user answers to fit questions
-- Note: question_id is VARCHAR because questions are hardcoded in Python, not stored in DB
CREATE TABLE IF NOT EXISTS application_fit_responses (
    id SERIAL PRIMARY KEY,
    application_id INTEGER REFERENCES shortlist_applications(id) ON DELETE CASCADE,
    question_id VARCHAR(50) NOT NULL,  -- String ID like 'tech_requirements', 'sales_motion'
    response_value VARCHAR(10),  -- 'A', 'B', 'C', 'D' for multiple choice
    response_text TEXT,  -- For free response questions
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(application_id, question_id)
);

-- Migration: If table exists with INTEGER question_id, alter it to VARCHAR
DO $$
BEGIN
    -- Check if the column exists and is integer type
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'application_fit_responses'
        AND column_name = 'question_id'
        AND data_type = 'integer'
    ) THEN
        -- Drop the foreign key constraint if it exists
        ALTER TABLE application_fit_responses DROP CONSTRAINT IF EXISTS application_fit_responses_question_id_fkey;
        -- Drop the unique constraint
        ALTER TABLE application_fit_responses DROP CONSTRAINT IF EXISTS application_fit_responses_application_id_question_id_key;
        -- Alter the column type
        ALTER TABLE application_fit_responses ALTER COLUMN question_id TYPE VARCHAR(50);
        -- Re-add the unique constraint
        ALTER TABLE application_fit_responses ADD CONSTRAINT application_fit_responses_application_id_question_id_key UNIQUE(application_id, question_id);
    END IF;
END $$;

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_fit_responses_app ON application_fit_responses(application_id);
CREATE INDEX IF NOT EXISTS idx_fit_questions_role ON fit_questions(role_type);

-- ============================================================================
-- ONET SKILLS & RESUME-BASED MATCHING TABLES
-- ============================================================================

-- ONET skills reference table
CREATE TABLE IF NOT EXISTS onet_skills (
    id SERIAL PRIMARY KEY,
    skill_name VARCHAR(200) UNIQUE NOT NULL,
    skill_type VARCHAR(50)  -- 'Core Skill', etc.
);

-- Candidate skills extracted from resumes
CREATE TABLE IF NOT EXISTS candidate_skills (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES onet_skills(id) ON DELETE CASCADE,
    confidence FLOAT DEFAULT 1.0,  -- AI confidence score (0-1)
    source VARCHAR(50) DEFAULT 'resume',  -- 'resume', 'manual', etc.
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, skill_id)
);

-- Job required skills (linking jobs to ONET skills)
CREATE TABLE IF NOT EXISTS job_required_skills (
    id SERIAL PRIMARY KEY,
    position_id INTEGER REFERENCES watchable_positions(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES onet_skills(id) ON DELETE CASCADE,
    UNIQUE(position_id, skill_id)
);

-- Create indexes for skill matching performance
CREATE INDEX IF NOT EXISTS idx_candidate_skills_user ON candidate_skills(user_id);
CREATE INDEX IF NOT EXISTS idx_candidate_skills_skill ON candidate_skills(skill_id);
CREATE INDEX IF NOT EXISTS idx_job_skills_position ON job_required_skills(position_id);
CREATE INDEX IF NOT EXISTS idx_job_skills_skill ON job_required_skills(skill_id);

-- Add additional columns to watchable_positions for better job matching
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'experience_level') THEN
        ALTER TABLE watchable_positions ADD COLUMN experience_level VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'work_arrangement') THEN
        ALTER TABLE watchable_positions ADD COLUMN work_arrangement VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'salary_min') THEN
        ALTER TABLE watchable_positions ADD COLUMN salary_min INTEGER;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'salary_max') THEN
        ALTER TABLE watchable_positions ADD COLUMN salary_max INTEGER;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'source_url') THEN
        ALTER TABLE watchable_positions ADD COLUMN source_url TEXT;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'posted_date') THEN
        ALTER TABLE watchable_positions ADD COLUMN posted_date TIMESTAMP;
    END IF;
END $$;

-- Add skills_extracted flag to seeker_profiles to track resume processing
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'skills_extracted') THEN
        ALTER TABLE seeker_profiles ADD COLUMN skills_extracted BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'skills_extracted_at') THEN
        ALTER TABLE seeker_profiles ADD COLUMN skills_extracted_at TIMESTAMP;
    END IF;
END $$;

-- ============================================================================
-- EMBEDDING-BASED SEMANTIC MATCHING
-- ============================================================================

-- Add embedding column to watchable_positions for semantic job matching
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'description_embedding') THEN
        ALTER TABLE watchable_positions ADD COLUMN description_embedding JSONB;
    END IF;
END $$;

-- Add structured profile data extracted from resume
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'resume_text') THEN
        ALTER TABLE seeker_profiles ADD COLUMN resume_text TEXT;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'resume_embedding') THEN
        ALTER TABLE seeker_profiles ADD COLUMN resume_embedding JSONB;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'seeker_profiles' AND column_name = 'extracted_profile') THEN
        ALTER TABLE seeker_profiles ADD COLUMN extracted_profile JSONB;
    END IF;
END $$;

-- Create index for faster embedding lookups (using GIN for JSONB)
CREATE INDEX IF NOT EXISTS idx_jobs_embedding ON watchable_positions USING GIN (description_embedding);
CREATE INDEX IF NOT EXISTS idx_seeker_embedding ON seeker_profiles USING GIN (resume_embedding);

-- ============================================================================
-- FIT SCORING & CANDIDATE INSIGHTS (Premium Employer Experience)
-- ============================================================================

-- Candidate fit scores with multi-bucket breakdown
CREATE TABLE IF NOT EXISTS candidate_fit_scores (
    id SERIAL PRIMARY KEY,
    application_id INTEGER UNIQUE REFERENCES shortlist_applications(id) ON DELETE CASCADE,

    -- Final Scores
    overall_fit_score INTEGER NOT NULL,  -- 0-100
    confidence_level VARCHAR(10) NOT NULL,  -- 'high', 'medium', 'low'

    -- Hard Filter Results
    hard_filters_passed BOOLEAN NOT NULL,
    hard_filter_breakdown JSONB,  -- {work_auth: true, location: true, start_date: true, seniority: true}

    -- Score Buckets (each 0-100)
    must_have_skills_score INTEGER,
    experience_alignment_score INTEGER,
    interview_performance_score INTEGER,
    fit_responses_score INTEGER,
    nice_to_have_skills_score INTEGER,

    -- Deductions
    deductions JSONB,  -- {weak_evidence: -5, inconsistencies: 0, unclear_answers: 0}
    score_breakdown JSONB,  -- Full explanation for transparency

    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fit_scores_app ON candidate_fit_scores(application_id);
CREATE INDEX IF NOT EXISTS idx_fit_scores_overall ON candidate_fit_scores(overall_fit_score DESC);

-- AI-generated insights for employer view
CREATE TABLE IF NOT EXISTS candidate_insights (
    id SERIAL PRIMARY KEY,
    application_id INTEGER UNIQUE REFERENCES shortlist_applications(id) ON DELETE CASCADE,

    -- One-liner Summary
    why_this_person TEXT,  -- "5 years React + startup experience + strong system design"

    -- Strengths & Risks (AI-generated)
    strengths JSONB,  -- [{text: "", evidence_source: "resume|interview|fit_responses", confidence: "high|medium|low"}]
    risks JSONB,  -- [{text: "", evidence_source: "", confidence: ""}]

    -- Suggested Interview Focus
    suggested_questions JSONB,  -- [{question: "", rationale: "", gap_area: "skills|experience|culture"}]

    -- Skill Chips for Display
    matched_skill_chips JSONB,  -- [{skill: "", is_must_have: true, source: "resume"}]

    -- Transcript Highlights
    interview_highlights JSONB,  -- [{quote: "", context: "", type: "strength|concern"}]

    -- Generation Metadata
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    llm_model VARCHAR(50),
    generation_version VARCHAR(10)
);

CREATE INDEX IF NOT EXISTS idx_insights_app ON candidate_insights(application_id);

-- Add fit score columns to shortlist_applications for quick access
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'fit_score') THEN
        ALTER TABLE shortlist_applications ADD COLUMN fit_score INTEGER;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'confidence_level') THEN
        ALTER TABLE shortlist_applications ADD COLUMN confidence_level VARCHAR(10);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'shortlist_applications' AND column_name = 'hard_filter_failed') THEN
        ALTER TABLE shortlist_applications ADD COLUMN hard_filter_failed BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_applications_fit_score ON shortlist_applications(position_id, fit_score DESC);

-- Add job requirements columns to watchable_positions
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'hard_requirements') THEN
        ALTER TABLE watchable_positions ADD COLUMN hard_requirements JSONB DEFAULT '{}';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'must_have_skills') THEN
        ALTER TABLE watchable_positions ADD COLUMN must_have_skills JSONB DEFAULT '[]';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'watchable_positions' AND column_name = 'nice_to_have_skills') THEN
        ALTER TABLE watchable_positions ADD COLUMN nice_to_have_skills JSONB DEFAULT '[]';
    END IF;
END $$;
