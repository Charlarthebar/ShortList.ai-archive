-- ============================================================================
-- SHORTLIST PLATFORM SCHEMA
-- User profiles, role watching, company features, notifications
-- ============================================================================
--
-- This schema supports the role-watching MVP:
-- 1. Job seekers create profiles and watch roles at companies
-- 2. Companies see who's watching their positions
-- 3. Companies can invite candidates to apply
-- 4. Users get notified when watched positions open
--
-- ============================================================================

-- ============================================================================
-- USERS & AUTHENTICATION
-- ============================================================================

-- User accounts (both job seekers and company users)
CREATE TABLE IF NOT EXISTS platform_users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT,  -- NULL if using OAuth
    email_verified BOOLEAN DEFAULT FALSE,
    email_verified_at TIMESTAMP,

    -- User type
    user_type TEXT NOT NULL CHECK (user_type IN ('seeker', 'company')),

    -- Profile basics
    first_name TEXT,
    last_name TEXT,
    avatar_url TEXT,

    -- Settings
    notification_preferences JSONB DEFAULT '{"email": true, "in_app": true}',
    privacy_settings JSONB DEFAULT '{"profile_visible": true, "show_to_companies": true}',

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

CREATE INDEX idx_platform_users_email ON platform_users(email);
CREATE INDEX idx_platform_users_type ON platform_users(user_type);


-- OAuth connections
CREATE TABLE IF NOT EXISTS oauth_connections (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,  -- google, linkedin, github
    provider_user_id TEXT NOT NULL,
    access_token TEXT,
    refresh_token TEXT,
    token_expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(provider, provider_user_id)
);


-- ============================================================================
-- JOB SEEKER PROFILES
-- ============================================================================

-- Detailed job seeker profile
CREATE TABLE IF NOT EXISTS seeker_profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE REFERENCES platform_users(id) ON DELETE CASCADE,

    -- Professional info
    current_title TEXT,
    current_company TEXT,
    years_experience TEXT,  -- '0-2', '3-5', '6-10', '10+'

    -- Search status
    search_status TEXT DEFAULT 'just-looking'
        CHECK (search_status IN ('actively-looking', 'open-to-offers', 'just-looking')),

    -- Education
    education JSONB,  -- [{degree, school, year}]

    -- Skills
    skills TEXT[],  -- Array of skill names

    -- Preferences
    preferred_locations TEXT[],  -- Array of location strings
    work_arrangement TEXT[],  -- ['remote', 'hybrid', 'on-site']
    salary_min INTEGER,
    salary_max INTEGER,
    open_to_roles TEXT[],  -- Array of role types they're interested in

    -- Resume/portfolio
    resume_url TEXT,
    linkedin_url TEXT,
    portfolio_url TEXT,

    -- Computed/cached fields
    profile_completeness INTEGER DEFAULT 0,  -- 0-100%

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_seeker_profiles_status ON seeker_profiles(search_status);
CREATE INDEX idx_seeker_profiles_skills ON seeker_profiles USING GIN(skills);


-- ============================================================================
-- COMPANY PROFILES (Employer side)
-- ============================================================================

-- Company/employer profiles (linked to our companies table)
CREATE TABLE IF NOT EXISTS company_profiles (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),  -- Link to our main companies table

    -- Verification
    verified BOOLEAN DEFAULT FALSE,
    verified_at TIMESTAMP,
    verification_method TEXT,  -- 'email_domain', 'manual'

    -- Company info
    company_name TEXT NOT NULL,
    website TEXT,
    logo_url TEXT,
    description TEXT,
    industry TEXT,
    company_size TEXT,  -- 'startup', 'small', 'medium', 'large', 'enterprise'
    founded_year INTEGER,
    headquarters_location TEXT,

    -- Culture & benefits
    work_arrangements TEXT[],  -- ['remote', 'hybrid', 'on-site']
    benefits TEXT[],  -- ['health', '401k', 'unlimited_pto', ...]
    culture_description TEXT,

    -- Social links
    linkedin_url TEXT,
    glassdoor_url TEXT,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_company_profiles_company ON company_profiles(company_id);
CREATE INDEX idx_company_profiles_verified ON company_profiles(verified);


-- Company team members (who can manage company profile)
CREATE TABLE IF NOT EXISTS company_team_members (
    id SERIAL PRIMARY KEY,
    company_profile_id INTEGER REFERENCES company_profiles(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,
    role TEXT DEFAULT 'member' CHECK (role IN ('owner', 'admin', 'member')),
    invited_by INTEGER REFERENCES platform_users(id),
    invited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    accepted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_profile_id, user_id)
);


-- ============================================================================
-- ROLE WATCHING (Core Feature)
-- ============================================================================

-- Watchable positions (combines our job data with company positions)
-- This represents roles that can be watched, whether from ATS or manually created
CREATE TABLE IF NOT EXISTS watchable_positions (
    id SERIAL PRIMARY KEY,

    -- Link to existing data (if from ATS scrape)
    observed_job_id BIGINT REFERENCES observed_jobs(id),
    posting_lifecycle_id BIGINT REFERENCES posting_lifecycle(id),

    -- Or link to company-created position
    company_profile_id INTEGER REFERENCES company_profiles(id),

    -- Position details (denormalized for performance)
    company_id INTEGER REFERENCES companies(id),
    company_name TEXT NOT NULL,
    title TEXT NOT NULL,
    department TEXT,
    location TEXT,
    salary_range TEXT,
    employment_type TEXT,  -- 'full-time', 'part-time', 'contract'
    work_arrangement TEXT,  -- 'remote', 'hybrid', 'on-site'
    description TEXT,
    requirements TEXT[],

    -- Experience requirements (set by employer)
    experience_level TEXT CHECK (experience_level IN ('entry', 'mid', 'senior', 'staff', 'any')),
    min_years_experience INTEGER DEFAULT 0,
    max_years_experience INTEGER,
    required_skills TEXT[],  -- Must-have skills
    preferred_skills TEXT[], -- Nice-to-have skills

    -- Status
    status TEXT DEFAULT 'open' CHECK (status IN ('open', 'filled', 'paused', 'closed')),

    -- URL to apply
    apply_url TEXT,

    -- Stats (updated periodically)
    watcher_count INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    application_count INTEGER DEFAULT 0,

    -- Timestamps
    posted_at TIMESTAMP,
    filled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_watchable_positions_company ON watchable_positions(company_id);
CREATE INDEX idx_watchable_positions_status ON watchable_positions(status);
CREATE INDEX idx_watchable_positions_observed ON watchable_positions(observed_job_id);


-- Job watches (the core connection: user watches a position)
CREATE TABLE IF NOT EXISTS job_watches (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,
    position_id INTEGER REFERENCES watchable_positions(id) ON DELETE CASCADE,

    -- Watch context
    notes TEXT,  -- Personal notes from watcher
    priority INTEGER DEFAULT 0,  -- User can prioritize watches

    -- Notification settings for this watch
    notify_on_open BOOLEAN DEFAULT TRUE,
    notify_on_similar BOOLEAN DEFAULT TRUE,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, position_id)
);

CREATE INDEX idx_job_watches_user ON job_watches(user_id);
CREATE INDEX idx_job_watches_position ON job_watches(position_id);


-- Company invitations to candidates
CREATE TABLE IF NOT EXISTS candidate_invitations (
    id SERIAL PRIMARY KEY,

    -- Who's inviting
    company_profile_id INTEGER REFERENCES company_profiles(id) ON DELETE CASCADE,
    invited_by_user_id INTEGER REFERENCES platform_users(id),

    -- Who's being invited
    candidate_user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,

    -- For which position (optional - could be general invite)
    position_id INTEGER REFERENCES watchable_positions(id),

    -- Invitation details
    message TEXT,
    status TEXT DEFAULT 'pending'
        CHECK (status IN ('pending', 'viewed', 'accepted', 'declined', 'expired')),

    -- Response
    candidate_response TEXT,
    responded_at TIMESTAMP,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + INTERVAL '30 days'),

    UNIQUE(company_profile_id, candidate_user_id, position_id)
);

CREATE INDEX idx_invitations_candidate ON candidate_invitations(candidate_user_id);
CREATE INDEX idx_invitations_company ON candidate_invitations(company_profile_id);
CREATE INDEX idx_invitations_status ON candidate_invitations(status);


-- ============================================================================
-- NOTIFICATIONS
-- ============================================================================

-- Notification types for reference
-- vacancy: A watched position opened up
-- invite: Company invited you to apply
-- match: New job matches your profile
-- application: Someone applied to your position
-- response: Candidate responded to your invite

CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,

    -- Notification content
    type TEXT NOT NULL,  -- vacancy, invite, match, application, response
    title TEXT NOT NULL,
    message TEXT NOT NULL,

    -- Related entities
    related_position_id INTEGER REFERENCES watchable_positions(id),
    related_company_id INTEGER REFERENCES companies(id),
    related_user_id INTEGER REFERENCES platform_users(id),
    related_invitation_id INTEGER REFERENCES candidate_invitations(id),

    -- State
    read BOOLEAN DEFAULT FALSE,
    read_at TIMESTAMP,

    -- Action URL
    action_url TEXT,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_notifications_user ON notifications(user_id, created_at DESC);
CREATE INDEX idx_notifications_unread ON notifications(user_id, read) WHERE read = FALSE;


-- ============================================================================
-- MATCH SCORING
-- ============================================================================

-- Pre-computed match scores between seekers and positions
CREATE TABLE IF NOT EXISTS seeker_position_matches (
    id SERIAL PRIMARY KEY,
    seeker_profile_id INTEGER REFERENCES seeker_profiles(id) ON DELETE CASCADE,
    position_id INTEGER REFERENCES watchable_positions(id) ON DELETE CASCADE,

    -- Match score (0-100)
    match_score INTEGER NOT NULL,

    -- Score breakdown
    skills_match INTEGER,  -- 0-100
    location_match INTEGER,  -- 0-100
    salary_match INTEGER,  -- 0-100
    experience_match INTEGER,  -- 0-100

    -- Match reasons (for explainability)
    matching_skills TEXT[],
    match_reasons TEXT[],

    -- Timestamps
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(seeker_profile_id, position_id)
);

CREATE INDEX idx_matches_seeker ON seeker_position_matches(seeker_profile_id, match_score DESC);
CREATE INDEX idx_matches_position ON seeker_position_matches(position_id, match_score DESC);


-- ============================================================================
-- ACTIVITY TRACKING
-- ============================================================================

-- Track user activity for analytics and recommendations
CREATE TABLE IF NOT EXISTS user_activity (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES platform_users(id) ON DELETE CASCADE,

    activity_type TEXT NOT NULL,  -- view_position, watch, unwatch, apply, search, etc.

    -- Related entities
    position_id INTEGER REFERENCES watchable_positions(id),
    company_id INTEGER REFERENCES companies(id),
    search_query TEXT,

    -- Context
    metadata JSONB,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_activity_user ON user_activity(user_id, created_at DESC);
CREATE INDEX idx_activity_type ON user_activity(activity_type, created_at DESC);


-- ============================================================================
-- HELPER VIEWS
-- ============================================================================

-- View: Positions with watcher details (for companies)
CREATE OR REPLACE VIEW position_watchers AS
SELECT
    wp.id as position_id,
    wp.title,
    wp.company_name,
    wp.status,
    jw.user_id as watcher_user_id,
    pu.first_name,
    pu.last_name,
    sp.current_title,
    sp.current_company,
    sp.years_experience,
    sp.search_status,
    sp.skills,
    spm.match_score,
    jw.created_at as watching_since
FROM watchable_positions wp
JOIN job_watches jw ON wp.id = jw.position_id
JOIN platform_users pu ON jw.user_id = pu.id
LEFT JOIN seeker_profiles sp ON pu.id = sp.user_id
LEFT JOIN seeker_position_matches spm ON sp.id = spm.seeker_profile_id AND wp.id = spm.position_id
WHERE pu.privacy_settings->>'show_to_companies' = 'true';


-- View: User's watchlist with position details
CREATE OR REPLACE VIEW user_watchlist AS
SELECT
    jw.user_id,
    jw.id as watch_id,
    wp.id as position_id,
    wp.company_name,
    wp.title,
    wp.location,
    wp.salary_range,
    wp.status,
    wp.watcher_count,
    spm.match_score,
    jw.priority,
    jw.notes,
    jw.created_at as watching_since
FROM job_watches jw
JOIN watchable_positions wp ON jw.position_id = wp.id
JOIN platform_users pu ON jw.user_id = pu.id
LEFT JOIN seeker_profiles sp ON pu.id = sp.user_id
LEFT JOIN seeker_position_matches spm ON sp.id = spm.seeker_profile_id AND wp.id = spm.position_id;


-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function to update watcher count on positions
CREATE OR REPLACE FUNCTION update_watcher_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE watchable_positions
        SET watcher_count = watcher_count + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = NEW.position_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE watchable_positions
        SET watcher_count = GREATEST(0, watcher_count - 1),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.position_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_watcher_count
AFTER INSERT OR DELETE ON job_watches
FOR EACH ROW EXECUTE FUNCTION update_watcher_count();


-- Function to create vacancy notification when position opens
CREATE OR REPLACE FUNCTION notify_watchers_on_open()
RETURNS TRIGGER AS $$
BEGIN
    -- Only trigger when status changes to 'open' from something else
    IF NEW.status = 'open' AND (OLD.status IS NULL OR OLD.status != 'open') THEN
        INSERT INTO notifications (user_id, type, title, message, related_position_id, related_company_id, action_url)
        SELECT
            jw.user_id,
            'vacancy',
            NEW.title || ' is now open!',
            'A position you''re watching at ' || NEW.company_name || ' is now accepting applications.',
            NEW.id,
            NEW.company_id,
            '/positions/' || NEW.id
        FROM job_watches jw
        WHERE jw.position_id = NEW.id
          AND jw.notify_on_open = TRUE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_notify_on_position_open
AFTER INSERT OR UPDATE OF status ON watchable_positions
FOR EACH ROW EXECUTE FUNCTION notify_watchers_on_open();


-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert comment for documentation
COMMENT ON TABLE platform_users IS 'All users of the platform - both job seekers and company recruiters';
COMMENT ON TABLE seeker_profiles IS 'Extended profile for job seekers with skills, preferences, etc.';
COMMENT ON TABLE company_profiles IS 'Company/employer profiles for the hiring side';
COMMENT ON TABLE job_watches IS 'Core feature: tracks which users are watching which positions';
COMMENT ON TABLE candidate_invitations IS 'Companies can invite candidates to apply for positions';
COMMENT ON TABLE notifications IS 'In-app and email notifications for users';
COMMENT ON TABLE seeker_position_matches IS 'Pre-computed match scores for recommendations';
