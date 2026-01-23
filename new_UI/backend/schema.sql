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
