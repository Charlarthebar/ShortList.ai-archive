-- ============================================================================
-- SHORTLIST PLATFORM MIGRATION
-- Migrates from role-watching to shortlist system
-- ============================================================================
--
-- This migration:
-- 1. Adds monitoring flags to distinguish trackable vs historical roles
-- 2. Adds role configuration for employer-defined must-haves and thresholds
-- 3. Transforms job_watches into shortlist_applications with full application data
-- 4. Adds screening results storage
--
-- ============================================================================

-- ============================================================================
-- STEP 1: ADD MONITORING FLAGS TO POSITIONS
-- ============================================================================

-- Add is_monitored flag to watchable_positions
-- Only positions where is_monitored=true can promise "we'll notify you when it opens"
ALTER TABLE watchable_positions ADD COLUMN IF NOT EXISTS is_monitored BOOLEAN DEFAULT FALSE;
ALTER TABLE watchable_positions ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'ats';  -- 'ats', 'historical', 'manual'
ALTER TABLE watchable_positions ADD COLUMN IF NOT EXISTS data_as_of_date DATE;  -- For historical data

-- Update is_monitored based on whether company is in posting_targets
-- This should be run after the schema change
UPDATE watchable_positions wp
SET is_monitored = TRUE
WHERE EXISTS (
    SELECT 1 FROM posting_targets pt
    WHERE pt.company_id = wp.company_id AND pt.enabled = TRUE
);

CREATE INDEX IF NOT EXISTS idx_watchable_positions_monitored ON watchable_positions(is_monitored);

-- ============================================================================
-- STEP 2: ROLE CONFIGURATION (Employer Controls)
-- ============================================================================

-- Role-specific configuration set by employers
CREATE TABLE IF NOT EXISTS role_configurations (
    id SERIAL PRIMARY KEY,
    position_id INTEGER UNIQUE REFERENCES watchable_positions(id) ON DELETE CASCADE,

    -- Must-have requirements (objective pass/fail)
    require_work_auth BOOLEAN DEFAULT FALSE,
    allowed_work_auth TEXT[],  -- ['us_citizen', 'permanent_resident', 'f1_opt', 'needs_sponsorship']
    require_location_match BOOLEAN DEFAULT FALSE,
    allowed_locations TEXT[],
    require_experience_level BOOLEAN DEFAULT FALSE,
    allowed_experience_levels TEXT[],  -- ['intern', 'new_grad', 'entry', 'mid', 'senior']
    min_grad_year INTEGER,  -- e.g., 2024
    max_grad_year INTEGER,  -- e.g., 2026
    required_skills TEXT[],  -- Must have ALL of these

    -- Shortlist display settings
    score_threshold INTEGER DEFAULT 70,  -- Min AI score to show by default (0-100)
    volume_cap INTEGER,  -- Max candidates to review at once (NULL = no cap)

    -- Custom questions (in addition to standard project/fit questions)
    custom_questions JSONB DEFAULT '[]',  -- [{question: string, required: boolean}]

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_role_config_position ON role_configurations(position_id);


-- ============================================================================
-- STEP 3: SHORTLIST APPLICATIONS (Replaces job_watches for new system)
-- ============================================================================

-- Main shortlist applications table
CREATE TABLE IF NOT EXISTS shortlist_applications (
    id SERIAL PRIMARY KEY,

    -- Who applied
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,
    position_id INTEGER REFERENCES watchable_positions(id) ON DELETE CASCADE,

    -- Resume/profile (one of these required)
    resume_url TEXT,
    linkedin_url TEXT,

    -- Work authorization
    work_authorization TEXT NOT NULL CHECK (work_authorization IN (
        'us_citizen',
        'permanent_resident',
        'f1_opt',
        'f1_cpt',
        'h1b',
        'needs_sponsorship',
        'other'
    )),
    work_auth_details TEXT,  -- Additional details if "other"

    -- Experience level
    grad_year INTEGER,  -- Graduation year (for student/new grad)
    experience_level TEXT CHECK (experience_level IN ('intern', 'new_grad', 'entry', 'mid', 'senior', 'staff')),
    years_of_experience INTEGER,

    -- Availability
    start_availability DATE,
    availability_notes TEXT,

    -- Required short-answer questions
    project_response TEXT NOT NULL,  -- "Describe a project you built/did"
    fit_response TEXT NOT NULL,  -- "Why you're a fit for this role"

    -- Custom question responses (matches role_configurations.custom_questions)
    custom_responses JSONB DEFAULT '{}',

    -- ============================================
    -- SCREENING RESULTS
    -- ============================================

    -- Step A: Must-have gate (objective pass/fail)
    screening_passed BOOLEAN,  -- NULL = not yet screened
    screening_fail_reason TEXT,  -- e.g., "requires sponsorship", "availability doesn't match"
    screened_at TIMESTAMP,

    -- Step B: AI ranking (only for those who pass must-haves)
    ai_score INTEGER CHECK (ai_score >= 0 AND ai_score <= 100),  -- 0-100
    ai_strengths TEXT[],  -- 3 strengths
    ai_concern TEXT,  -- 1 concern/risk/gap
    ai_scored_at TIMESTAMP,
    ai_model_version TEXT,  -- Track which model was used

    -- Status tracking
    status TEXT DEFAULT 'pending' CHECK (status IN (
        'pending',      -- Just submitted, not yet screened
        'screened',     -- Passed must-haves, AI scored
        'rejected',     -- Failed must-haves
        'qualified',    -- Meets threshold, ready for employer review
        'reviewed',     -- Employer has viewed
        'contacted',    -- Employer reached out
        'archived'      -- Employer archived/dismissed
    )),

    -- Employer interaction
    employer_notes TEXT,
    reviewed_at TIMESTAMP,
    reviewed_by INTEGER REFERENCES platform_users(id),

    -- Notifications
    notified_role_opened BOOLEAN DEFAULT FALSE,
    notified_role_opened_at TIMESTAMP,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, position_id)
);

CREATE INDEX IF NOT EXISTS idx_shortlist_user ON shortlist_applications(user_id);
CREATE INDEX IF NOT EXISTS idx_shortlist_position ON shortlist_applications(position_id);
CREATE INDEX IF NOT EXISTS idx_shortlist_status ON shortlist_applications(status);
CREATE INDEX IF NOT EXISTS idx_shortlist_screening ON shortlist_applications(screening_passed);
CREATE INDEX IF NOT EXISTS idx_shortlist_score ON shortlist_applications(ai_score DESC) WHERE screening_passed = TRUE;
CREATE INDEX IF NOT EXISTS idx_shortlist_qualified ON shortlist_applications(position_id, ai_score DESC)
    WHERE screening_passed = TRUE AND status = 'qualified';


-- ============================================================================
-- STEP 4: UPDATE NOTIFICATION TRIGGERS FOR SHORTLIST
-- ============================================================================

-- Drop old trigger that notifies job_watches
DROP TRIGGER IF EXISTS trigger_notify_on_position_open ON watchable_positions;

-- New function to notify shortlist applicants when position opens
CREATE OR REPLACE FUNCTION notify_shortlist_on_open()
RETURNS TRIGGER AS $$
BEGIN
    -- Only trigger when status changes to 'open' from something else
    -- AND only if this position is monitored (not historical data)
    IF NEW.status = 'open' AND (OLD.status IS NULL OR OLD.status != 'open') AND NEW.is_monitored = TRUE THEN
        -- Notify candidates on shortlist
        INSERT INTO notifications (user_id, type, title, message, related_position_id, related_company_id, action_url)
        SELECT
            sa.user_id,
            'role_opened',
            NEW.title || ' at ' || NEW.company_name || ' is now open!',
            'A role you joined the shortlist for is now accepting applications.',
            NEW.id,
            NEW.company_id,
            '/roles/' || NEW.id
        FROM shortlist_applications sa
        WHERE sa.position_id = NEW.id
          AND sa.screening_passed = TRUE
          AND sa.notified_role_opened = FALSE;

        -- Mark candidates as notified
        UPDATE shortlist_applications
        SET notified_role_opened = TRUE,
            notified_role_opened_at = CURRENT_TIMESTAMP
        WHERE position_id = NEW.id
          AND screening_passed = TRUE
          AND notified_role_opened = FALSE;

        -- Notify employer about ready shortlist
        INSERT INTO notifications (user_id, type, title, message, related_position_id, related_company_id, action_url)
        SELECT DISTINCT
            ctm.user_id,
            'shortlist_ready',
            'Shortlist ready for ' || NEW.title,
            'This role just opened, and you have ' ||
                (SELECT COUNT(*) FROM shortlist_applications sa2
                 WHERE sa2.position_id = NEW.id AND sa2.screening_passed = TRUE AND sa2.ai_score >= COALESCE(rc.score_threshold, 70))
                || ' qualified candidates ready to review.',
            NEW.id,
            NEW.company_id,
            '/employer/roles/' || NEW.id || '/shortlist'
        FROM company_team_members ctm
        JOIN company_profiles cp ON ctm.company_profile_id = cp.id
        LEFT JOIN role_configurations rc ON rc.position_id = NEW.id
        WHERE cp.company_id = NEW.company_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_notify_shortlist_on_open
AFTER INSERT OR UPDATE OF status ON watchable_positions
FOR EACH ROW EXECUTE FUNCTION notify_shortlist_on_open();


-- ============================================================================
-- STEP 5: UPDATE STATS FUNCTION
-- ============================================================================

-- Function to update shortlist count on positions (replaces watcher_count trigger)
CREATE OR REPLACE FUNCTION update_shortlist_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE watchable_positions
        SET application_count = application_count + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = NEW.position_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE watchable_positions
        SET application_count = GREATEST(0, application_count - 1),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.position_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_shortlist_count ON shortlist_applications;
CREATE TRIGGER trigger_update_shortlist_count
AFTER INSERT OR DELETE ON shortlist_applications
FOR EACH ROW EXECUTE FUNCTION update_shortlist_count();


-- ============================================================================
-- STEP 6: HELPER VIEWS FOR EMPLOYER DASHBOARD
-- ============================================================================

-- View: Shortlist summary by position (for company-level dashboard)
CREATE OR REPLACE VIEW shortlist_summary AS
SELECT
    wp.id as position_id,
    wp.company_id,
    wp.company_name,
    wp.title,
    wp.status,
    wp.is_monitored,
    COUNT(sa.id) as total_applicants,
    COUNT(sa.id) FILTER (WHERE sa.screening_passed = TRUE) as passed_screening,
    COUNT(sa.id) FILTER (WHERE sa.screening_passed = TRUE AND sa.ai_score >= COALESCE(rc.score_threshold, 70)) as meets_threshold,
    AVG(sa.ai_score) FILTER (WHERE sa.screening_passed = TRUE) as avg_score
FROM watchable_positions wp
LEFT JOIN shortlist_applications sa ON wp.id = sa.position_id
LEFT JOIN role_configurations rc ON wp.id = rc.position_id
GROUP BY wp.id, wp.company_id, wp.company_name, wp.title, wp.status, wp.is_monitored, rc.score_threshold;


-- View: Ranked candidates for a position (for role-level dashboard)
CREATE OR REPLACE VIEW shortlist_candidates AS
SELECT
    sa.id as application_id,
    sa.position_id,
    sa.user_id,
    pu.first_name,
    pu.last_name,
    pu.email,
    sa.resume_url,
    sa.linkedin_url,
    sa.work_authorization,
    sa.grad_year,
    sa.experience_level,
    sa.start_availability,
    sa.project_response,
    sa.fit_response,
    sa.screening_passed,
    sa.screening_fail_reason,
    sa.ai_score,
    sa.ai_strengths,
    sa.ai_concern,
    sa.status,
    sa.employer_notes,
    sa.reviewed_at,
    sa.created_at as applied_at,
    rc.score_threshold,
    CASE WHEN sa.ai_score >= COALESCE(rc.score_threshold, 70) THEN TRUE ELSE FALSE END as meets_threshold
FROM shortlist_applications sa
JOIN platform_users pu ON sa.user_id = pu.id
LEFT JOIN role_configurations rc ON sa.position_id = rc.position_id
WHERE sa.screening_passed = TRUE
ORDER BY sa.ai_score DESC NULLS LAST;


-- ============================================================================
-- DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE role_configurations IS 'Employer-defined settings for each role: must-haves, score threshold, volume cap';
COMMENT ON TABLE shortlist_applications IS 'Candidate shortlist applications with screening results and AI scores';
COMMENT ON COLUMN watchable_positions.is_monitored IS 'TRUE if we actively monitor this employer''s job postings and can notify when role opens';
COMMENT ON COLUMN watchable_positions.data_source IS 'Source of role data: ats (live monitored), historical (payroll/manual import), manual (employer created)';
COMMENT ON COLUMN shortlist_applications.screening_passed IS 'Step A result: TRUE if passed all must-have requirements, FALSE if failed, NULL if not yet screened';
COMMENT ON COLUMN shortlist_applications.ai_score IS 'Step B result: 0-100 score from AI ranking (only for candidates who passed Step A)';
