-- Schema additions for new ShortList UI
-- Run this to ensure the necessary columns exist

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
