#!/usr/bin/env python3
"""
Comprehensive job standardization:
1. Clean job titles - standardize abbreviations, remove locations
2. Restructure descriptions into clean, readable format with sections and bullets
"""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================================
# TITLE CLEANING
# ============================================================================

def clean_title(title):
    """Clean and standardize job title."""
    if not title:
        return title

    cleaned = title

    # First, remove parenthetical abbreviation duplicates like "(BDR)" or "(SRE)" when the full name is already there
    cleaned = re.sub(r'\s*\((?:BDR|SDR|SRE|SWE|PM|AE|AM|RN|PT|OT)\)\s*$', '', cleaned, flags=re.IGNORECASE)

    # Standardize abbreviations (only at word boundaries, not inside parentheses)
    abbreviation_map = [
        # These expand abbreviations at start of words or standalone
        (r'\bSr\.?\s', 'Senior '),
        (r'\bJr\.?\s', 'Junior '),
        (r'\bMgr\.?\b', 'Manager'),
        (r'\bMgmt\.?\b', 'Management'),
        (r'\bEngr\.?\b', 'Engineer'),
        (r'\bDev\.?\b', 'Developer'),
        (r'\bAdmin\.?\b', 'Administrator'),
        (r'\bAsst\.?\b', 'Assistant'),
        (r'\bAssoc\.?\b', 'Associate'),
        (r'\bExec\.?\b', 'Executive'),
        (r'\bDir\.?\b', 'Director'),
    ]

    for pattern, replacement in abbreviation_map:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)

    # Expand standalone abbreviations (not in parentheses) - be more careful
    standalone_expansions = [
        # Only expand if it's the whole title or clearly standalone
        (r'^SDR$', 'Sales Development Representative'),
        (r'^BDR$', 'Business Development Representative'),
        (r'^SWE$', 'Software Engineer'),
        (r'^SRE$', 'Site Reliability Engineer'),
        # Expand GTM when it's part of a title
        (r'\bGTM\b', 'Go-To-Market'),
    ]

    for pattern, replacement in standalone_expansions:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)

    # Remove location lists in parentheses at the end
    # Matches patterns like (Boston/Dallas/Chicago/...) or (Boston, MA / Dallas, TX / ...)
    cleaned = re.sub(r'\s*\([^)]*(?:/[^)]*){2,}\)\s*$', '', cleaned)

    # Remove single location in parentheses at end
    cleaned = re.sub(r'\s*\([^)]*,\s*[A-Z]{2}\s*\)\s*$', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*\([^)]*(?:Remote|Hybrid|On-?site)[^)]*\)\s*$', '', cleaned, flags=re.IGNORECASE)

    # Remove job codes like (JP13711), (#2464), etc.
    cleaned = re.sub(r'\s*\([A-Z]{2,}\d+\)\s*$', '', cleaned)
    cleaned = re.sub(r'\s*\(#?\d+\)\s*$', '', cleaned)
    cleaned = re.sub(r'\s*#\d+\s*', ' ', cleaned)

    # Remove bracketed prefixes
    cleaned = re.sub(r'^\s*\[(?:REMOTE|HYBRID|ON-?SITE|ONSITE|NEW)\]\s*', '', cleaned, flags=re.IGNORECASE)

    # Remove "Job ID: XXX" patterns
    cleaned = re.sub(r'\s*[-–]\s*(?:Job|Req|Requisition)\s*(?:ID|#)?:?\s*[A-Z0-9-]+\s*$', '', cleaned, flags=re.IGNORECASE)

    # Remove trailing location indicators
    cleaned = re.sub(r'\s*[-–]\s*(?:Remote|Hybrid|On-?site|USA|US|Nationwide)\s*$', '', cleaned, flags=re.IGNORECASE)

    # Remove city location at end after dash (be careful not to remove job-related words)
    # Only remove if it looks like a city (capitalized word) followed by nothing or state
    location_cities = r'(?:Boston|Chicago|Dallas|Denver|Dublin|London|Madrid|Paris|Tokyo|Toronto|Zurich|Warsaw|Singapore|Mumbai|Francisco|Angeles|Richmond|Scottsdale|NYC|SF)'
    cleaned = re.sub(rf'\s*[-–]\s*{location_cities}(?:,\s*[A-Z]{{2}})?\s*$', '', cleaned, flags=re.IGNORECASE)

    # Clean up multiple spaces and dashes
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'\s*[-–]\s*$', '', cleaned)
    cleaned = cleaned.strip()

    return cleaned


def clean_all_titles(dry_run=False):
    """Clean all job titles in the database."""
    conn = get_db()
    cleaned_count = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, title FROM watchable_positions WHERE title IS NOT NULL")
        jobs = cur.fetchall()
        print(f"Checking {len(jobs)} job titles...")

        for job in jobs:
            old_title = job['title']
            new_title = clean_title(old_title)

            if new_title != old_title:
                if dry_run:
                    print(f"  '{old_title}' → '{new_title}'")
                else:
                    cur.execute("UPDATE watchable_positions SET title = %s WHERE id = %s",
                                (new_title, job['id']))
                cleaned_count += 1

        if not dry_run:
            conn.commit()

    conn.close()
    print(f"Cleaned {cleaned_count} job titles")
    return cleaned_count


# ============================================================================
# DESCRIPTION RESTRUCTURING
# ============================================================================

def extract_sections_from_text(text):
    """Extract meaningful sections from a raw job description."""

    # Common section headers to look for
    section_patterns = [
        (r'(?:about\s+(?:the\s+)?(?:role|position|job|opportunity)|role\s+overview|position\s+summary|job\s+summary|overview)\s*[:\n]', 'overview'),
        (r'(?:what\s+you\'?ll?\s+do|responsibilities|key\s+responsibilities|duties|your\s+responsibilities|job\s+duties|essential\s+functions)\s*[:\n]', 'responsibilities'),
        (r'(?:what\s+(?:you\'?ll?\s+)?(?:need|bring)|requirements|qualifications|required\s+(?:skills|qualifications)|minimum\s+qualifications|who\s+you\s+are|what\s+we\'?re\s+looking\s+for)\s*[:\n]', 'requirements'),
        (r'(?:nice\s+to\s+have|preferred\s+(?:skills|qualifications)|desired\s+(?:skills|qualifications)|bonus\s+points)\s*[:\n]', 'preferred'),
        (r'(?:what\s+we\s+offer|benefits|perks|compensation|why\s+(?:join|work)\s+(?:us|here))\s*[:\n]', 'benefits'),
        (r'(?:about\s+(?:us|the\s+company|our\s+company)|company\s+(?:overview|description)|who\s+we\s+are)\s*[:\n]', 'company'),
        (r'(?:skills|required\s+skills|technical\s+skills)\s*[:\n]', 'skills'),
    ]

    sections = {}
    remaining_text = text

    # Try to extract sections
    for pattern, section_name in section_patterns:
        match = re.search(pattern, remaining_text, re.IGNORECASE)
        if match:
            start = match.end()
            # Find the next section header or end of text
            next_section = len(remaining_text)
            for other_pattern, _ in section_patterns:
                other_match = re.search(other_pattern, remaining_text[start:], re.IGNORECASE)
                if other_match:
                    next_section = min(next_section, start + other_match.start())

            section_content = remaining_text[start:next_section].strip()
            if section_content and len(section_content) > 20:
                sections[section_name] = section_content

    return sections


def extract_bullet_points(text):
    """Extract bullet points from text."""
    bullets = []

    # Look for existing bullet points
    bullet_patterns = [
        r'[•●○■□▪▸►]\s*(.+?)(?=\n|$)',
        r'[-–—]\s+(.+?)(?=\n|$)',
        r'\d+[.)]\s*(.+?)(?=\n|$)',
        r'\*\s+(.+?)(?=\n|$)',
    ]

    for pattern in bullet_patterns:
        matches = re.findall(pattern, text, re.MULTILINE)
        for match in matches:
            clean_bullet = match.strip()
            if clean_bullet and len(clean_bullet) > 10 and clean_bullet not in bullets:
                bullets.append(clean_bullet)

    # If no bullets found, try to split on sentences that look like list items
    if not bullets:
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        for sent in sentences:
            sent = sent.strip()
            if 20 < len(sent) < 300 and not sent.endswith(':'):
                bullets.append(sent)

    return bullets[:10]  # Limit to 10 bullets


def restructure_description(title, company, location, salary_range, raw_description):
    """Restructure a job description into a clean, readable format."""

    if not raw_description:
        return None

    # If already well-structured (has our formatting), skip
    if '**Key Responsibilities:**' in raw_description and '**Qualifications:**' in raw_description:
        return None

    # Clean the raw description first
    text = raw_description

    # Remove duplicate content (common in scraped descriptions)
    text = remove_duplicate_paragraphs(text)

    # Remove boilerplate
    text = remove_boilerplate(text)

    # Extract sections from the cleaned text
    sections = extract_sections_from_text(text)

    # Build the new structured description
    parts = []

    # Overview section - extract a clean summary
    overview = create_overview(title, company, text, sections)
    if overview:
        parts.append(overview)

    # Responsibilities section
    resp_bullets = extract_responsibilities(text, sections)
    if resp_bullets:
        parts.append("\n\n**Key Responsibilities:**")
        for bullet in resp_bullets[:6]:
            parts.append(f"• {bullet}")

    # Qualifications section
    qual_bullets = extract_qualifications(text, sections)
    if qual_bullets:
        parts.append("\n\n**Qualifications:**")
        for bullet in qual_bullets[:6]:
            parts.append(f"• {bullet}")

    # Skills section (if distinct from qualifications)
    skills_bullets = extract_skills(text, sections)
    if skills_bullets and len(qual_bullets or []) < 4:
        parts.append("\n\n**Skills:**")
        for bullet in skills_bullets[:4]:
            parts.append(f"• {bullet}")

    # Benefits (brief)
    benefits = extract_benefits(text, sections)
    if benefits:
        parts.append("\n\n**Benefits:**")
        for bullet in benefits[:4]:
            parts.append(f"• {bullet}")

    # About company (brief)
    company_info = extract_company_info(company, text, sections)
    if company_info:
        parts.append(f"\n\n**About {company}:**")
        parts.append(company_info)

    # If we didn't extract enough structure, use template-based approach
    if len(parts) < 3 or (len(resp_bullets or []) < 2 and len(qual_bullets or []) < 2):
        return generate_template_description(title, company, location, salary_range)

    # Position details
    if location or salary_range:
        parts.append("\n\n**Position Details:**")
        if location:
            parts.append(f"• Location: {location}")
        if salary_range:
            parts.append(f"• Compensation: {salary_range}")

    result = '\n'.join(parts)

    # Final cleanup
    result = re.sub(r'\n{3,}', '\n\n', result)
    result = result.strip()

    return result if len(result) > 600 else generate_template_description(title, company, location, salary_range)


def remove_duplicate_paragraphs(text):
    """Remove duplicate or near-duplicate paragraphs."""
    paragraphs = text.split('\n\n')
    seen = set()
    unique = []

    for p in paragraphs:
        # Normalize for comparison
        normalized = re.sub(r'\s+', ' ', p.strip().lower())[:200]
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(p)

    return '\n\n'.join(unique)


def remove_boilerplate(text):
    """Remove common boilerplate text."""
    # Remove "By clicking Apply" type text
    text = re.sub(r'By clicking the ["\']?Apply["\']? button.*?(?=\n\n|\Z)', '', text, flags=re.IGNORECASE | re.DOTALL)

    # Remove pay transparency sections
    text = re.sub(r'Pay Transparency details.*?(?=\n\n[A-Z]|\Z)', '', text, flags=re.IGNORECASE | re.DOTALL)

    # Remove EEO statements
    text = re.sub(r'(?:We are an |This company is an )?[Ee]qual [Oo]pportunity [Ee]mployer.*?(?=\n\n|\Z)', '', text, flags=re.DOTALL)

    # Remove "apply now" CTAs
    text = re.sub(r'(?:Apply now|Click here to apply|Submit your application).*?(?=\n\n|\Z)', '', text, flags=re.IGNORECASE | re.DOTALL)

    return text.strip()


def create_overview(title, company, text, sections):
    """Create a clean overview paragraph."""
    # Try to get from sections first
    overview = sections.get('overview', '')

    if not overview:
        # Look for a good opening paragraph
        paragraphs = re.split(r'\n\n+', text)
        for p in paragraphs[:3]:
            p = p.strip()
            # Skip if it's a header or list
            if p.endswith(':') or p.startswith(('•', '-', '*', '1.')):
                continue
            # Skip if too short or too long
            if len(p) < 100 or len(p) > 1000:
                continue
            # Skip if it's clearly not an overview
            skip_phrases = ['responsibilities:', 'requirements:', 'qualifications:', 'skills:', 'what you', 'who you are', 'minimum', 'preferred']
            if any(phrase in p.lower() for phrase in skip_phrases):
                continue
            overview = p
            break

    if not overview:
        return None

    # Clean up
    overview = re.sub(r'\s+', ' ', overview).strip()
    overview = re.sub(r'^(?:Job\s+Description|Position\s+Summary|Overview|About\s+(?:the\s+)?(?:Role|Position))[:\s]*', '', overview, flags=re.IGNORECASE)

    # Remove duplicate "Job Description:" that appears twice
    overview = re.sub(r'Job Description:\s*', '', overview, flags=re.IGNORECASE)

    # Truncate if too long
    if len(overview) > 500:
        overview = overview[:500].rsplit(' ', 1)[0] + '.'

    return overview if len(overview) > 50 else None


def extract_responsibilities(text, sections):
    """Extract responsibility bullets."""
    # Try from sections
    resp_text = sections.get('responsibilities', '')

    if resp_text:
        bullets = extract_bullet_points(resp_text)
        if bullets:
            return [clean_bullet(b) for b in bullets]

    # Try to find responsibility-like sentences
    responsibility_keywords = [
        'responsible for', 'manage', 'develop', 'lead', 'create', 'build',
        'design', 'implement', 'support', 'collaborate', 'work with',
        'drive', 'ensure', 'maintain', 'deliver', 'coordinate', 'analyze'
    ]

    bullets = []
    sentences = re.split(r'(?<=[.!])\s+(?=[A-Z])', text)

    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 30 or len(sent) > 250:
            continue
        if any(kw in sent.lower() for kw in responsibility_keywords):
            cleaned = clean_bullet(sent)
            if cleaned and cleaned not in bullets:
                bullets.append(cleaned)
                if len(bullets) >= 8:
                    break

    return bullets if bullets else None


def extract_qualifications(text, sections):
    """Extract qualification bullets."""
    qual_text = sections.get('requirements', '') or sections.get('qualifications', '')

    if qual_text:
        bullets = extract_bullet_points(qual_text)
        if bullets:
            return [clean_bullet(b) for b in bullets]

    # Try to find qualification-like sentences
    qual_keywords = [
        'years', 'experience', 'degree', 'bachelor', 'master', 'phd',
        'knowledge of', 'proficiency', 'familiar with', 'ability to',
        'strong', 'excellent', 'required', 'must have', 'certification'
    ]

    bullets = []
    sentences = re.split(r'(?<=[.!])\s+(?=[A-Z])', text)

    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 20 or len(sent) > 200:
            continue
        if any(kw in sent.lower() for kw in qual_keywords):
            cleaned = clean_bullet(sent)
            if cleaned and cleaned not in bullets:
                bullets.append(cleaned)
                if len(bullets) >= 8:
                    break

    return bullets if bullets else None


def extract_skills(text, sections):
    """Extract skills bullets."""
    skills_text = sections.get('skills', '')

    if skills_text:
        bullets = extract_bullet_points(skills_text)
        if bullets:
            return [clean_bullet(b) for b in bullets[:6]]

    return None


def extract_benefits(text, sections):
    """Extract benefits bullets."""
    benefits_text = sections.get('benefits', '')

    if benefits_text:
        bullets = extract_bullet_points(benefits_text)
        if bullets:
            return [clean_bullet(b) for b in bullets[:4]]

    return None


def extract_company_info(company, text, sections):
    """Extract brief company information."""
    company_text = sections.get('company', '')

    if company_text:
        # Keep it brief
        info = re.sub(r'\s+', ' ', company_text).strip()
        if len(info) > 250:
            info = info[:250].rsplit(' ', 1)[0] + '...'
        return info

    return None


def clean_bullet(text):
    """Clean a bullet point text."""
    text = text.strip()
    # Remove leading bullets/dashes
    text = re.sub(r'^[•●○■□▪▸►\-–—*]\s*', '', text)
    # Remove leading numbers
    text = re.sub(r'^\d+[.)]\s*', '', text)
    # Capitalize first letter
    if text and text[0].islower():
        text = text[0].upper() + text[1:]
    # Remove trailing period if it's a short phrase
    if text.endswith('.') and len(text) < 100 and '.' not in text[:-1]:
        text = text[:-1]
    return text


def generate_template_description(title, company, location, salary_range):
    """Generate a structured description using role-based templates."""
    from enrich_descriptions_v2 import generate_structured_description
    return generate_structured_description(title, company, location, salary_range)


def restructure_simple(title, company, location, salary_range, raw_description):
    """Simple restructuring for descriptions that don't have clear sections."""

    # Remove common cruft
    text = raw_description

    # Remove boilerplate headers
    text = re.sub(r'^(?:Job\s+Description|Position\s+Summary|Overview)[:\s]*\n*', '', text, flags=re.IGNORECASE)

    # Remove pay transparency boilerplate
    text = re.sub(r'Pay\s+Transparency\s+details.*?(?=\n\n|\Z)', '', text, flags=re.IGNORECASE | re.DOTALL)

    # Remove "apply by clicking" type text
    text = re.sub(r'(?:By\s+clicking|To\s+apply|Click\s+here).*?(?=\n\n|\Z)', '', text, flags=re.IGNORECASE | re.DOTALL)

    # Try to extract bullets from the whole text
    all_bullets = extract_bullet_points(text)

    # Split into paragraphs
    paragraphs = [p.strip() for p in re.split(r'\n\n+', text) if p.strip()]

    parts = []

    # Get overview (first substantial paragraph that's not a list)
    for p in paragraphs[:3]:
        if len(p) > 100 and not p.startswith(('•', '-', '*', '1.', '2.')):
            overview = p[:600].rsplit(' ', 1)[0] if len(p) > 600 else p
            parts.append(overview)
            break

    # Add responsibilities if we found bullets
    if all_bullets:
        resp_bullets = [b for b in all_bullets if any(kw in b.lower() for kw in
            ['manage', 'develop', 'lead', 'create', 'build', 'design', 'implement', 'support', 'collaborate', 'work with', 'responsible'])]
        if resp_bullets:
            parts.append("\n\n**Key Responsibilities:**")
            for b in resp_bullets[:6]:
                parts.append(f"• {b}")

        qual_bullets = [b for b in all_bullets if any(kw in b.lower() for kw in
            ['years', 'experience', 'degree', 'bachelor', 'master', 'knowledge', 'proficiency', 'skills', 'familiar', 'ability'])]
        if qual_bullets:
            parts.append("\n\n**Qualifications:**")
            for b in qual_bullets[:6]:
                parts.append(f"• {b}")

    # Position details
    if location or salary_range:
        parts.append("\n\n**Position Details:**")
        if location:
            parts.append(f"• Location: {location}")
        if salary_range:
            parts.append(f"• Compensation: {salary_range}")

    result = '\n'.join(parts)
    return result if len(result) > 400 else None


def needs_restructuring(desc):
    """Check if a description needs to be restructured."""
    if not desc:
        return False

    # Already well-structured with our format
    if '**Key Responsibilities:**' in desc:
        return False

    # If it doesn't have our structure, it probably needs restructuring
    # unless it's already nicely formatted with bullets
    if '**' not in desc:
        return True

    # Has some markdown but check if it's truly well-structured
    # Look for signs of poor structure
    if 'By clicking the "Apply" button' in desc:
        return True
    if 'Pay Transparency details' in desc:
        return True

    # Wall of text (long paragraph without breaks)
    paragraphs = desc.split('\n\n')
    for p in paragraphs:
        if len(p) > 1000:
            return True

    return False


def restructure_all_descriptions(limit=100, dry_run=False):
    """Restructure all descriptions that need it."""
    conn = get_db()
    restructured = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, title, company_name, location, salary_range, description
            FROM watchable_positions
            WHERE description IS NOT NULL AND LENGTH(description) > 500
            ORDER BY LENGTH(description) DESC
        """)

        jobs = cur.fetchall()
        print(f"Checking {len(jobs)} jobs for restructuring needs...")

        processed = 0
        for job in jobs:
            if limit and processed >= limit:
                break

            if not needs_restructuring(job['description']):
                continue

            new_desc = restructure_description(
                job['title'],
                job['company_name'],
                job['location'],
                job['salary_range'],
                job['description']
            )

            if new_desc and len(new_desc) >= 400:
                processed += 1

                if dry_run:
                    print(f"\n{'='*70}")
                    print(f"TITLE: {job['title']}")
                    print(f"COMPANY: {job['company_name']}")
                    print(f"\nOLD ({len(job['description'])} chars):")
                    print(job['description'][:500] + '...')
                    print(f"\nNEW ({len(new_desc)} chars):")
                    print(new_desc[:800] + '...' if len(new_desc) > 800 else new_desc)
                else:
                    cur.execute("UPDATE watchable_positions SET description = %s WHERE id = %s",
                                (new_desc, job['id']))
                    restructured += 1

                    if restructured % 100 == 0:
                        conn.commit()
                        print(f"  Restructured {restructured} descriptions...")

        if not dry_run:
            conn.commit()

    conn.close()
    print(f"Restructured {restructured} job descriptions")
    return restructured


# ============================================================================
# MAIN
# ============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Standardize job titles and descriptions')
    parser.add_argument('--titles', action='store_true', help='Clean job titles only')
    parser.add_argument('--descriptions', action='store_true', help='Restructure descriptions only')
    parser.add_argument('--all', action='store_true', help='Clean titles and restructure descriptions')
    parser.add_argument('--limit', type=int, default=100, help='Limit number of descriptions to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')

    args = parser.parse_args()

    if args.titles or args.all:
        print("\n" + "="*50)
        print("CLEANING JOB TITLES")
        print("="*50)
        clean_all_titles(dry_run=args.dry_run)

    if args.descriptions or args.all:
        print("\n" + "="*50)
        print("RESTRUCTURING DESCRIPTIONS")
        print("="*50)
        # Process in batches
        if args.limit == 0 or args.all:
            total = 0
            while True:
                count = restructure_all_descriptions(limit=500, dry_run=args.dry_run)
                total += count
                if count == 0:
                    break
                print(f"Total restructured so far: {total}")
            print(f"\nCompleted! Total restructured: {total}")
        else:
            restructure_all_descriptions(limit=args.limit, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
