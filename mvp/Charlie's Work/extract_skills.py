#!/usr/bin/env python3
"""
Skills Extractor for Cambridge Jobs
====================================
Extracts skills from job descriptions AND infers from job titles.
Uses O*NET skills taxonomy + title-to-skills mappings to ensure 100% coverage.

Output:
- Updates the 'skills' column in cambridge_jobs.db
- Exports updated cambridge_jobs.csv with FULL descriptions
"""

import sqlite3
import csv
import re
import os

DB_PATH = "cambridge_jobs.db"
CSV_PATH = "cambridge_jobs.csv"
ONET_SKILLS_PATH = "onet_skills.csv"


# =========================================================
# O*NET-BASED TITLE TO SKILLS MAPPINGS
# Based on O*NET occupation classifications and their skill requirements
# =========================================================

TITLE_SKILL_MAPPINGS = {
    # Engineering & Technical
    r'\b(software|developer|programmer|coder)\b': [
        'Programming', 'Critical Thinking', 'Complex Problem Solving',
        'Systems Analysis', 'Technology Design', 'Active Learning',
        'Reading Comprehension', 'Mathematics'
    ],
    r'\b(engineer)\b': [
        'Critical Thinking', 'Complex Problem Solving', 'Systems Analysis',
        'Mathematics', 'Active Learning', 'Judgment and Decision Making',
        'Reading Comprehension', 'Technology Design'
    ],
    r'\b(data scientist|machine learning|ml engineer|ai engineer)\b': [
        'Programming', 'Mathematics', 'Critical Thinking', 'Complex Problem Solving',
        'Systems Analysis', 'Active Learning', 'Science', 'Technology Design'
    ],
    r'\b(devops|sre|site reliability|infrastructure)\b': [
        'Programming', 'Systems Analysis', 'Troubleshooting', 'Complex Problem Solving',
        'Operations Monitoring', 'Technology Design', 'Critical Thinking'
    ],
    r'\b(architect)\b': [
        'Systems Analysis', 'Technology Design', 'Critical Thinking',
        'Complex Problem Solving', 'Judgment and Decision Making', 'Coordination'
    ],
    r'\b(qa|quality assurance|test|tester)\b': [
        'Quality Control Analysis', 'Critical Thinking', 'Troubleshooting',
        'Active Learning', 'Reading Comprehension', 'Monitoring'
    ],

    # Data & Analytics
    r'\b(analyst|analytics)\b': [
        'Critical Thinking', 'Reading Comprehension', 'Active Learning',
        'Complex Problem Solving', 'Mathematics', 'Writing', 'Systems Analysis'
    ],
    r'\b(data engineer)\b': [
        'Programming', 'Systems Analysis', 'Critical Thinking', 'Mathematics',
        'Technology Design', 'Complex Problem Solving', 'Active Learning'
    ],
    r'\b(business intelligence|bi developer)\b': [
        'Critical Thinking', 'Systems Analysis', 'Reading Comprehension',
        'Complex Problem Solving', 'Mathematics', 'Active Learning'
    ],

    # Management & Leadership
    r'\b(manager|director|head of|vp|vice president|chief)\b': [
        'Management of Personnel Resources', 'Coordination', 'Judgment and Decision Making',
        'Critical Thinking', 'Speaking', 'Active Listening', 'Negotiation',
        'Social Perceptiveness', 'Time Management'
    ],
    r'\b(project manager|program manager|pm)\b': [
        'Coordination', 'Time Management', 'Management of Personnel Resources',
        'Judgment and Decision Making', 'Critical Thinking', 'Speaking',
        'Active Listening', 'Negotiation', 'Monitoring'
    ],
    r'\b(team lead|tech lead|lead)\b': [
        'Coordination', 'Management of Personnel Resources', 'Instructing',
        'Critical Thinking', 'Speaking', 'Active Listening', 'Judgment and Decision Making'
    ],
    r'\b(supervisor)\b': [
        'Management of Personnel Resources', 'Coordination', 'Monitoring',
        'Instructing', 'Speaking', 'Active Listening', 'Time Management'
    ],
    r'\b(ceo|cto|cfo|coo|cio)\b': [
        'Management of Personnel Resources', 'Management of Financial Resources',
        'Judgment and Decision Making', 'Critical Thinking', 'Negotiation',
        'Systems Evaluation', 'Coordination', 'Speaking'
    ],

    # Sales & Marketing
    r'\b(sales|account executive|business development|bdr|sdr)\b': [
        'Persuasion', 'Negotiation', 'Speaking', 'Active Listening',
        'Social Perceptiveness', 'Service Orientation', 'Coordination'
    ],
    r'\b(marketing|brand|growth)\b': [
        'Writing', 'Speaking', 'Critical Thinking', 'Active Learning',
        'Social Perceptiveness', 'Persuasion', 'Coordination', 'Time Management'
    ],
    r'\b(product manager|product owner)\b': [
        'Critical Thinking', 'Coordination', 'Judgment and Decision Making',
        'Speaking', 'Active Listening', 'Systems Analysis', 'Persuasion',
        'Social Perceptiveness', 'Writing'
    ],

    # Finance & Accounting
    r'\b(accountant|accounting|cpa)\b': [
        'Mathematics', 'Critical Thinking', 'Reading Comprehension',
        'Active Learning', 'Monitoring', 'Time Management', 'Writing'
    ],
    r'\b(financial analyst|finance)\b': [
        'Mathematics', 'Critical Thinking', 'Reading Comprehension',
        'Complex Problem Solving', 'Judgment and Decision Making', 'Active Learning'
    ],
    r'\b(auditor|audit)\b': [
        'Critical Thinking', 'Reading Comprehension', 'Monitoring',
        'Mathematics', 'Judgment and Decision Making', 'Writing'
    ],
    r'\b(controller|treasurer)\b': [
        'Management of Financial Resources', 'Mathematics', 'Critical Thinking',
        'Monitoring', 'Judgment and Decision Making', 'Reading Comprehension'
    ],

    # Healthcare & Life Sciences
    r'\b(nurse|rn|lpn|nursing)\b': [
        'Service Orientation', 'Active Listening', 'Social Perceptiveness',
        'Critical Thinking', 'Monitoring', 'Coordination', 'Speaking', 'Science'
    ],
    r'\b(physician|doctor|md|medical director)\b': [
        'Critical Thinking', 'Science', 'Active Listening', 'Judgment and Decision Making',
        'Complex Problem Solving', 'Reading Comprehension', 'Social Perceptiveness'
    ],
    r'\b(pharmacist|pharmacy)\b': [
        'Science', 'Critical Thinking', 'Active Listening', 'Reading Comprehension',
        'Service Orientation', 'Monitoring', 'Judgment and Decision Making'
    ],
    r'\b(research scientist|scientist|researcher)\b': [
        'Science', 'Critical Thinking', 'Reading Comprehension', 'Active Learning',
        'Complex Problem Solving', 'Writing', 'Mathematics'
    ],
    r'\b(clinical|clinical research|cra|crc)\b': [
        'Science', 'Critical Thinking', 'Reading Comprehension', 'Monitoring',
        'Writing', 'Coordination', 'Active Learning', 'Quality Control Analysis'
    ],
    r'\b(regulatory|regulatory affairs)\b': [
        'Reading Comprehension', 'Writing', 'Critical Thinking', 'Active Learning',
        'Monitoring', 'Judgment and Decision Making', 'Coordination'
    ],
    r'\b(biotech|biotechnology|bioinformatics)\b': [
        'Science', 'Critical Thinking', 'Programming', 'Mathematics',
        'Reading Comprehension', 'Active Learning', 'Complex Problem Solving'
    ],
    r'\b(therapist|therapy|physical therapist|pt|occupational therapist|ot)\b': [
        'Service Orientation', 'Active Listening', 'Social Perceptiveness',
        'Speaking', 'Monitoring', 'Instructing', 'Critical Thinking'
    ],
    r'\b(medical assistant|ma)\b': [
        'Service Orientation', 'Active Listening', 'Social Perceptiveness',
        'Monitoring', 'Speaking', 'Coordination', 'Time Management'
    ],

    # Human Resources
    r'\b(hr|human resources|recruiter|recruiting|talent)\b': [
        'Active Listening', 'Speaking', 'Social Perceptiveness', 'Negotiation',
        'Judgment and Decision Making', 'Reading Comprehension', 'Writing', 'Coordination'
    ],
    r'\b(training|trainer|learning|development)\b': [
        'Instructing', 'Learning Strategies', 'Speaking', 'Active Listening',
        'Writing', 'Social Perceptiveness', 'Coordination'
    ],

    # Customer Service & Support
    r'\b(customer service|customer success|support|help desk)\b': [
        'Service Orientation', 'Active Listening', 'Speaking', 'Social Perceptiveness',
        'Critical Thinking', 'Coordination', 'Time Management', 'Troubleshooting'
    ],
    r'\b(technical support|tech support|it support)\b': [
        'Troubleshooting', 'Service Orientation', 'Active Listening', 'Speaking',
        'Critical Thinking', 'Technology Design', 'Complex Problem Solving'
    ],

    # Design & Creative
    r'\b(designer|ux|ui|user experience|user interface)\b': [
        'Critical Thinking', 'Active Learning', 'Complex Problem Solving',
        'Social Perceptiveness', 'Coordination', 'Time Management', 'Technology Design'
    ],
    r'\b(graphic design|visual design|creative)\b': [
        'Active Learning', 'Critical Thinking', 'Time Management',
        'Coordination', 'Social Perceptiveness'
    ],
    r'\b(writer|editor|content|copywriter)\b': [
        'Writing', 'Reading Comprehension', 'Critical Thinking', 'Active Learning',
        'Time Management', 'Social Perceptiveness'
    ],

    # Operations & Administration
    r'\b(operations|ops)\b': [
        'Coordination', 'Monitoring', 'Time Management', 'Critical Thinking',
        'Management of Material Resources', 'Judgment and Decision Making'
    ],
    r'\b(administrative|admin|assistant|secretary|receptionist)\b': [
        'Time Management', 'Coordination', 'Active Listening', 'Speaking',
        'Writing', 'Service Orientation', 'Reading Comprehension'
    ],
    r'\b(office manager)\b': [
        'Coordination', 'Management of Material Resources', 'Time Management',
        'Speaking', 'Active Listening', 'Monitoring', 'Service Orientation'
    ],
    r'\b(executive assistant|ea)\b': [
        'Time Management', 'Coordination', 'Active Listening', 'Speaking',
        'Writing', 'Judgment and Decision Making', 'Social Perceptiveness'
    ],

    # Legal
    r'\b(lawyer|attorney|legal|counsel|paralegal)\b': [
        'Reading Comprehension', 'Critical Thinking', 'Writing', 'Speaking',
        'Active Listening', 'Negotiation', 'Judgment and Decision Making', 'Persuasion'
    ],
    r'\b(compliance|compliance officer)\b': [
        'Reading Comprehension', 'Critical Thinking', 'Monitoring', 'Writing',
        'Judgment and Decision Making', 'Active Learning'
    ],

    # Education & Research
    r'\b(teacher|professor|instructor|educator)\b': [
        'Instructing', 'Learning Strategies', 'Speaking', 'Active Listening',
        'Social Perceptiveness', 'Writing', 'Reading Comprehension', 'Monitoring'
    ],
    r'\b(tutor|teaching assistant|ta)\b': [
        'Instructing', 'Active Listening', 'Speaking', 'Learning Strategies',
        'Social Perceptiveness', 'Monitoring'
    ],

    # Trades & Technical
    r'\b(technician|tech)\b': [
        'Troubleshooting', 'Equipment Maintenance', 'Repairing', 'Operation and Control',
        'Critical Thinking', 'Quality Control Analysis', 'Monitoring'
    ],
    r'\b(mechanic|maintenance)\b': [
        'Repairing', 'Equipment Maintenance', 'Troubleshooting', 'Operation and Control',
        'Critical Thinking', 'Equipment Selection', 'Installation'
    ],
    r'\b(electrician|electrical)\b': [
        'Troubleshooting', 'Repairing', 'Equipment Maintenance', 'Installation',
        'Critical Thinking', 'Operation and Control', 'Quality Control Analysis'
    ],
    r'\b(hvac)\b': [
        'Repairing', 'Troubleshooting', 'Equipment Maintenance', 'Installation',
        'Operation and Control', 'Equipment Selection', 'Critical Thinking'
    ],
    r'\b(carpenter|carpentry|construction)\b': [
        'Operation and Control', 'Critical Thinking', 'Coordination',
        'Equipment Selection', 'Quality Control Analysis', 'Mathematics'
    ],

    # Security & IT
    r'\b(security|cybersecurity|infosec|information security)\b': [
        'Critical Thinking', 'Complex Problem Solving', 'Systems Analysis',
        'Monitoring', 'Troubleshooting', 'Technology Design', 'Active Learning'
    ],
    r'\b(network|networking|network engineer)\b': [
        'Troubleshooting', 'Systems Analysis', 'Technology Design', 'Critical Thinking',
        'Complex Problem Solving', 'Installation', 'Monitoring'
    ],
    r'\b(system administrator|sysadmin|systems administrator)\b': [
        'Troubleshooting', 'Systems Analysis', 'Technology Design', 'Critical Thinking',
        'Operation and Control', 'Monitoring', 'Installation'
    ],
    r'\b(database administrator|dba)\b': [
        'Programming', 'Systems Analysis', 'Critical Thinking', 'Troubleshooting',
        'Technology Design', 'Complex Problem Solving', 'Monitoring'
    ],

    # Logistics & Supply Chain
    r'\b(logistics|supply chain|warehouse|inventory)\b': [
        'Coordination', 'Monitoring', 'Management of Material Resources',
        'Time Management', 'Critical Thinking', 'Mathematics'
    ],
    r'\b(driver|delivery|truck|cdl)\b': [
        'Operation and Control', 'Monitoring', 'Time Management',
        'Coordination', 'Service Orientation'
    ],
    r'\b(shipping|receiving|fulfillment)\b': [
        'Coordination', 'Time Management', 'Monitoring',
        'Management of Material Resources', 'Quality Control Analysis'
    ],

    # Food Service & Hospitality
    r'\b(chef|cook|culinary|kitchen)\b': [
        'Time Management', 'Coordination', 'Monitoring', 'Active Learning',
        'Management of Material Resources', 'Quality Control Analysis'
    ],
    r'\b(server|waiter|waitress|bartender)\b': [
        'Service Orientation', 'Active Listening', 'Speaking', 'Social Perceptiveness',
        'Coordination', 'Time Management'
    ],
    r'\b(hotel|hospitality|front desk)\b': [
        'Service Orientation', 'Active Listening', 'Speaking', 'Social Perceptiveness',
        'Coordination', 'Time Management', 'Monitoring'
    ],

    # Retail
    r'\b(retail|store|cashier|sales associate)\b': [
        'Service Orientation', 'Active Listening', 'Speaking', 'Social Perceptiveness',
        'Persuasion', 'Mathematics', 'Monitoring'
    ],

    # Consulting
    r'\b(consultant|consulting|advisory)\b': [
        'Critical Thinking', 'Complex Problem Solving', 'Speaking', 'Writing',
        'Active Listening', 'Social Perceptiveness', 'Persuasion', 'Systems Analysis'
    ],

    # Intern/Entry Level
    r'\b(intern|internship|co-op|trainee|apprentice)\b': [
        'Active Learning', 'Active Listening', 'Reading Comprehension',
        'Critical Thinking', 'Time Management', 'Writing'
    ],
}

# Default skills for jobs that don't match any pattern
DEFAULT_SKILLS = [
    'Critical Thinking', 'Active Listening', 'Reading Comprehension',
    'Speaking', 'Time Management', 'Coordination'
]


def load_skills_from_csv():
    """Load skills from onet_skills.csv file."""
    print("Loading O*NET skills from onet_skills.csv...")

    core_skills = set()
    tech_categories = set()
    tech_tools = set()

    if not os.path.exists(ONET_SKILLS_PATH):
        print(f"  Warning: {ONET_SKILLS_PATH} not found")
        return set(), set(), set()

    with open(ONET_SKILLS_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            skill = row.get('skill_name', '').strip()
            skill_type = row.get('skill_type', '').strip()

            if not skill:
                continue

            if skill_type == 'Core Skill':
                core_skills.add(skill.lower())
            elif skill_type == 'Technology Category':
                tech_categories.add(skill.lower())
            elif skill_type == 'Technology/Tool':
                tech_tools.add(skill.lower())

    print(f"  Loaded {len(core_skills)} core skills")
    print(f"  Loaded {len(tech_categories)} tech categories")
    print(f"  Loaded {len(tech_tools)} tech tools")

    return core_skills, tech_categories, tech_tools


def build_skills_taxonomy():
    """Build comprehensive skills taxonomy from O*NET + common skills."""
    print("Building skills taxonomy...")

    # Load from onet_skills.csv
    core_skills, tech_categories, tech_tools = load_skills_from_csv()

    # Combine all O*NET skills
    taxonomy = core_skills | tech_categories | tech_tools

    # Add common skills not always in O*NET
    common_skills = {
        # Programming languages
        "python", "java", "javascript", "typescript", "c++", "c#", "ruby", "go",
        "rust", "scala", "kotlin", "swift", "php", "perl", "r", "matlab", "sql",

        # Frameworks & libraries
        "react", "angular", "vue", "node.js", "django", "flask", "spring",
        "tensorflow", "pytorch", "pandas", "numpy", "scikit-learn",

        # Cloud & infrastructure
        "aws", "azure", "gcp", "google cloud", "docker", "kubernetes", "terraform",
        "jenkins", "ci/cd", "devops", "linux", "unix",

        # Data & analytics
        "machine learning", "data science", "data analysis", "data visualization",
        "statistics", "tableau", "power bi", "excel", "data mining", "big data",
        "etl", "data engineering", "data warehousing",

        # Business skills
        "project management", "agile", "scrum", "leadership", "communication",
        "problem solving", "teamwork", "collaboration", "presentation",
        "strategic planning", "budget management", "stakeholder management",

        # Industry-specific (Cambridge biotech/pharma)
        "clinical research", "regulatory affairs", "gmp", "fda", "hipaa",
        "cro", "pharmaceutical", "biotechnology", "healthcare", "life sciences",
        "drug discovery", "clinical trials", "bioinformatics", "genomics",

        # Marketing/Sales/Business
        "marketing", "sales", "customer service", "account management",
        "financial analysis", "accounting", "bookkeeping",

        # Soft skills
        "attention to detail", "time management", "multitasking", "adaptability",
        "creativity", "critical thinking", "analytical skills", "negotiation",

        # Certifications & methodologies
        "pmp", "six sigma", "lean", "itil", "cpa", "cfa", "series 7",
    }
    taxonomy.update(common_skills)

    print(f"  Total skills in taxonomy: {len(taxonomy)}")
    return taxonomy, core_skills


def get_skills_from_title(title):
    """
    Infer skills from job title using O*NET-based occupation mappings.
    Returns list of skills that match the job title patterns.
    """
    if not title:
        return DEFAULT_SKILLS.copy()

    title_lower = title.lower()
    matched_skills = []

    # Check each pattern
    for pattern, skills in TITLE_SKILL_MAPPINGS.items():
        if re.search(pattern, title_lower, re.IGNORECASE):
            matched_skills.extend(skills)

    # Deduplicate while preserving order
    seen = set()
    unique_skills = []
    for skill in matched_skills:
        if skill not in seen:
            seen.add(skill)
            unique_skills.append(skill)

    # If no matches, return default skills
    if not unique_skills:
        return DEFAULT_SKILLS.copy()

    return unique_skills


# =========================================================
# EXTRACT SKILLS FROM JOB DESCRIPTIONS
# =========================================================

def extract_skills_from_description(description, taxonomy, core_skills, max_skills=25):
    """
    Extract skills mentioned in a job description.
    Returns list of matched skills, prioritizing core skills.
    """
    if not description:
        return []

    description_lower = description.lower()
    found_core = []
    found_other = []

    for skill in taxonomy:
        # Use word boundary matching to avoid partial matches
        if len(skill) <= 2:
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, description_lower):
                if skill == "r" and not re.search(r'\br\s+(programming|language|studio)', description_lower):
                    continue
                if skill in core_skills:
                    found_core.append(skill)
                else:
                    found_other.append(skill)
        elif len(skill) <= 4:
            # Short skills need word boundaries
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, description_lower):
                if skill in core_skills:
                    found_core.append(skill)
                else:
                    found_other.append(skill)
        else:
            if skill in description_lower:
                if skill in core_skills:
                    found_core.append(skill)
                else:
                    found_other.append(skill)

    # Deduplicate
    found_core = list(set(found_core))
    found_other = list(set(found_other))

    # Prioritize core skills, then add others
    all_skills = found_core + found_other
    all_skills = all_skills[:max_skills]

    # Format properly
    formatted = []
    for s in all_skills:
        if len(s) <= 3:
            formatted.append(s.upper())
        else:
            formatted.append(s.title())

    return formatted


def combine_skills(description_skills, title_skills, max_skills=25):
    """
    Combine skills from description and title inference.
    Prioritizes description-extracted skills, fills in with title-inferred skills.
    """
    # Start with description skills (more specific)
    combined = list(description_skills)

    # Add title-inferred skills that aren't already present
    combined_lower = {s.lower() for s in combined}
    for skill in title_skills:
        if skill.lower() not in combined_lower:
            combined.append(skill)
            combined_lower.add(skill.lower())

    return combined[:max_skills]


# =========================================================
# PROCESS ALL JOBS
# =========================================================

def process_jobs():
    """Main function to extract skills for all jobs - ensures 100% coverage."""
    print("=" * 70)
    print("SKILLS EXTRACTION FOR CAMBRIDGE JOBS (100% COVERAGE)")
    print("=" * 70)

    # Build taxonomy from onet_skills.csv
    taxonomy, core_skills = build_skills_taxonomy()

    # Connect to database
    print("\nProcessing jobs...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Add skills column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE jobs ADD COLUMN skills TEXT")
        conn.commit()
        print("  Added skills column to database")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Get all jobs
    cursor.execute("SELECT id, title, description FROM jobs")
    jobs = cursor.fetchall()
    print(f"  Total jobs to process: {len(jobs)}")

    # Process each job
    updated = 0
    jobs_with_desc_skills = 0
    jobs_with_title_skills = 0

    for job_id, title, description in jobs:
        # Extract skills from description
        desc_skills = extract_skills_from_description(description, taxonomy, core_skills)

        # Get skills inferred from title
        title_skills = get_skills_from_title(title)

        # Combine both sources
        combined_skills = combine_skills(desc_skills, title_skills)

        # Track statistics
        if desc_skills:
            jobs_with_desc_skills += 1
        if combined_skills and not desc_skills:
            jobs_with_title_skills += 1

        # Format as semicolon-separated string
        skills_str = "; ".join(combined_skills)

        # Update database
        cursor.execute("UPDATE jobs SET skills = ? WHERE id = ?", (skills_str, job_id))
        updated += 1

        if updated % 1000 == 0:
            print(f"  Processed {updated:,} jobs...")
            conn.commit()

    conn.commit()

    # Verify 100% coverage
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE skills IS NULL OR skills = ''")
    jobs_without_skills = cursor.fetchone()[0]

    print(f"\nResults:")
    print(f"  Jobs processed: {updated:,}")
    print(f"  Jobs with description-extracted skills: {jobs_with_desc_skills:,}")
    print(f"  Jobs with title-inferred skills only: {jobs_with_title_skills:,}")
    print(f"  Jobs without skills: {jobs_without_skills}")

    if jobs_without_skills > 0:
        print(f"\n  WARNING: {jobs_without_skills} jobs still missing skills!")
        # Fix any remaining jobs with default skills
        cursor.execute("""
            UPDATE jobs
            SET skills = ?
            WHERE skills IS NULL OR skills = ''
        """, ("; ".join(DEFAULT_SKILLS),))
        conn.commit()
        print(f"  Applied default skills to remaining jobs")

    # Final verification
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE skills IS NULL OR skills = ''")
    final_missing = cursor.fetchone()[0]
    print(f"\n  Final verification: {final_missing} jobs without skills")

    # Show sample
    print("\nSample extracted skills:")
    cursor.execute("""
        SELECT title, skills
        FROM jobs
        WHERE skills != ''
        ORDER BY RANDOM()
        LIMIT 5
    """)
    for title, skills in cursor.fetchall():
        print(f"  {title[:60]}...")
        print(f"    Skills: {skills[:100]}...")

    conn.close()
    return updated


# =========================================================
# EXPORT TO CSV WITH FULL DESCRIPTIONS
# =========================================================

def export_csv():
    """Export jobs with skills to CSV - FULL descriptions, no truncation."""
    print("\nExporting to CSV with full descriptions...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("""
        SELECT title, employer, location, city, state,
               salary_min, salary_max, source, url, posted_date,
               is_remote, status, first_seen, description, skills
        FROM jobs
        WHERE status = 'active'
        ORDER BY source, employer, title
    """)

    rows = cursor.fetchall()

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['title', 'employer', 'location', 'city', 'state',
                       'salary_min', 'salary_max', 'source', 'url', 'posted_date',
                       'is_remote', 'status', 'first_seen', 'description', 'skills'])

        for row in rows:
            # Convert to list so we can clean the description
            row_list = list(row)
            # Clean description but keep it FULL (no truncation)
            if row_list[13]:
                # Replace newlines with spaces for CSV compatibility
                row_list[13] = row_list[13].replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                # Clean up multiple spaces
                row_list[13] = re.sub(r'\s+', ' ', row_list[13]).strip()
            writer.writerow(row_list)

    print(f"  Exported {len(rows):,} active jobs to {CSV_PATH}")

    # Show file size
    file_size = os.path.getsize(CSV_PATH) / (1024 * 1024)
    print(f"  File size: {file_size:.1f} MB")

    conn.close()
    return len(rows)


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    print("=" * 70)
    print("CAMBRIDGE JOBS - SKILL EXTRACTION & EXPORT (100% COVERAGE)")
    print("=" * 70)

    process_jobs()
    count = export_csv()

    # Final summary
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE skills != '' AND skills IS NOT NULL")
    with_skills = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT AVG(LENGTH(description)) FROM jobs WHERE description IS NOT NULL")
    avg_desc_len = cursor.fetchone()[0] or 0
    conn.close()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total jobs in database: {total:,}")
    print(f"Total jobs exported: {count:,}")
    print(f"Jobs with skills: {with_skills:,} ({100*with_skills/total:.1f}%)")
    print(f"Average description length: {avg_desc_len:.0f} characters")
    print(f"Output file: {CSV_PATH}")
    print("\nDone!")
