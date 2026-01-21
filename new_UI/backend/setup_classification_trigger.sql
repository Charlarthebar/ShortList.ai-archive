-- Auto-classification trigger for watchable_positions
-- Run this SQL to set up automatic classification of new jobs

-- First, add experience_level column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'watchable_positions' AND column_name = 'experience_level'
    ) THEN
        ALTER TABLE watchable_positions ADD COLUMN experience_level VARCHAR(20);
    END IF;
END $$;

-- Create or replace the classification function
CREATE OR REPLACE FUNCTION classify_job_title()
RETURNS TRIGGER AS $$
DECLARE
    title_lower TEXT;
BEGIN
    title_lower := LOWER(NEW.title);

    -- Classify role_type if not already set
    IF NEW.role_type IS NULL THEN
        -- Engineering Manager (check first to catch managers before software_engineer)
        IF title_lower ~ '(engineering manager|eng manager|development manager|software manager|technical manager|head of engineering|director.*(engineering|software|development)|vp.*(engineering|software))' THEN
            NEW.role_type := 'engineering_manager';
        -- Software Engineer
        ELSIF title_lower ~ '(software engineer|software developer|backend engineer|frontend engineer|full.?stack|web developer|mobile.*(developer|engineer)|ios.*(developer|engineer)|android.*(developer|engineer)|platform engineer|systems engineer|devops|sre|site reliability|infrastructure engineer|cloud engineer|qa engineer|test engineer|sdet|automation engineer|security engineer|application engineer|embedded)' THEN
            NEW.role_type := 'software_engineer';
        -- Data Scientist
        ELSIF title_lower ~ '(data scientist|machine learning|ml engineer|ai engineer|research scientist|applied scientist|deep learning|nlp engineer|computer vision)' THEN
            NEW.role_type := 'data_scientist';
        -- Data Analyst
        ELSIF title_lower ~ '(data analyst|business analyst|analytics|bi analyst|business intelligence|reporting analyst|insights analyst|data engineer)' THEN
            NEW.role_type := 'data_analyst';
        -- Product Manager
        ELSIF title_lower ~ '(product manager|product owner|program manager|project manager|technical program|tpm|product lead|head of product)' THEN
            NEW.role_type := 'product_manager';
        -- Sales
        ELSIF title_lower ~ '(sales|account executive|business development|bdr|sdr|account manager|customer success|solutions consultant|solutions engineer|pre.?sales)' THEN
            NEW.role_type := 'sales';
        -- Marketing
        ELSIF title_lower ~ '(marketing|growth|content|brand|communications|pr |public relations|social media|seo|sem|demand gen)' THEN
            NEW.role_type := 'marketing';
        -- Design
        ELSIF title_lower ~ '(designer|ux|ui|user experience|user interface|product design|visual design|graphic design|creative|art director)' THEN
            NEW.role_type := 'design';
        -- Operations
        ELSIF title_lower ~ '(operations|supply chain|logistics|procurement|facilities|office manager|executive assistant|chief of staff|strategy|consulting)' THEN
            NEW.role_type := 'operations';
        -- Finance
        ELSIF title_lower ~ '(finance|accountant|accounting|controller|cfo|financial analyst|fp&a|treasury|audit|tax|payroll)' THEN
            NEW.role_type := 'finance';
        -- HR
        ELSIF title_lower ~ '(human resources|\mhr\M|recruiter|recruiting|talent|people ops|people operations|compensation|benefits|hrbp)' THEN
            NEW.role_type := 'hr';
        -- Support
        ELSIF title_lower ~ '(customer support|customer service|technical support|help desk|support engineer|support specialist|client services|implementation)' THEN
            NEW.role_type := 'support';
        END IF;
    END IF;

    -- Classify experience_level if not already set
    IF NEW.experience_level IS NULL THEN
        -- Intern
        IF title_lower ~ '\mintern\M|\minternship\M' THEN
            NEW.experience_level := 'intern';
        -- Senior/Lead/Staff/Principal
        ELSIF title_lower ~ '\msenior\M|\msr\.?\M|\mlead\M|\mprincipal\M|\mstaff\M|\mdirector\M|\mhead\M|\mvp\M|\mchief\M|\mmanager\M|\miii\M|\miv\M' THEN
            NEW.experience_level := 'senior';
        -- Entry/Junior/Associate
        ELSIF title_lower ~ '\mjunior\M|\mjr\.?\M|\mentry\M|\massociate\M|\mnew grad\M' THEN
            NEW.experience_level := 'entry';
        -- Mid-level
        ELSIF title_lower ~ '\mii\M|\mmid\M' THEN
            NEW.experience_level := 'mid';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS classify_job_on_insert ON watchable_positions;
DROP TRIGGER IF EXISTS classify_job_on_update ON watchable_positions;

-- Create triggers for INSERT and UPDATE
CREATE TRIGGER classify_job_on_insert
    BEFORE INSERT ON watchable_positions
    FOR EACH ROW
    EXECUTE FUNCTION classify_job_title();

CREATE TRIGGER classify_job_on_update
    BEFORE UPDATE OF title ON watchable_positions
    FOR EACH ROW
    WHEN (OLD.title IS DISTINCT FROM NEW.title)
    EXECUTE FUNCTION classify_job_title();

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Classification triggers created successfully!';
    RAISE NOTICE 'New jobs will be automatically classified on insert.';
    RAISE NOTICE 'Jobs will be re-classified if their title changes.';
END $$;
