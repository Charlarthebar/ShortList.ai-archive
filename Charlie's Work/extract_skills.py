#!/usr/bin/env python3
"""
Skills Extractor for Cambridge Jobs
====================================
Extracts skills for each job using two methods:
1. Job-specific skills - Keywords found in the job description
2. O*NET occupation skills - Baseline skills from matched occupations

Data Sources:
- O*NET Database v30.1 (Skills.txt, Technology Skills.txt, Occupation Data.txt)
- Job descriptions from cambridge_jobs.db

Output:
- Updates the 'skills' column in cambridge_jobs.db
- Exports updated cambridge_jobs.csv
"""

import sqlite3
import csv
import re
from collections import defaultdict
from difflib import SequenceMatcher

# =========================================================
# CONFIGURATION
# =========================================================

ONET_DIR = "onet_data/db_30_1_text"
DB_PATH = "cambridge_jobs.db"
CSV_PATH = "cambridge_jobs.csv"

# Minimum similarity score for job title matching (0-1)
TITLE_MATCH_THRESHOLD = 0.5

# =========================================================
# STEP 1: LOAD O*NET DATA
# =========================================================

def load_onet_occupations():
    """Load O*NET occupation titles and codes."""
    print("Loading O*NET occupations...")
    occupations = {}
    with open(f"{ONET_DIR}/Occupation Data.txt", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            code = row["O*NET-SOC Code"]
            title = row["Title"]
            occupations[code] = title
    print(f"  Loaded {len(occupations)} occupations")
    return occupations


def load_onet_skills():
    """
    Load O*NET skills data.
    Returns dict: {occupation_code: [(skill_name, importance_score), ...]}
    """
    print("Loading O*NET skills...")
    skills_by_occupation = defaultdict(list)

    with open(f"{ONET_DIR}/Skills.txt", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Only use "IM" (Importance) scale, not "LV" (Level)
            if row["Scale ID"] == "IM":
                code = row["O*NET-SOC Code"]
                skill_name = row["Element Name"]
                try:
                    importance = float(row["Data Value"])
                except:
                    importance = 0
                skills_by_occupation[code].append((skill_name, importance))

    # Sort by importance (descending) for each occupation
    for code in skills_by_occupation:
        skills_by_occupation[code].sort(key=lambda x: x[1], reverse=True)

    print(f"  Loaded skills for {len(skills_by_occupation)} occupations")
    return skills_by_occupation


def load_onet_tech_skills():
    """
    Load O*NET technology skills (software, tools, etc.)
    Returns dict: {occupation_code: [skill_name, ...]}
    """
    print("Loading O*NET technology skills...")
    tech_skills_by_occupation = defaultdict(set)
    all_tech_skills = set()

    with open(f"{ONET_DIR}/Technology Skills.txt", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            code = row["O*NET-SOC Code"]
            example = row["Example"]
            commodity = row["Commodity Title"]

            tech_skills_by_occupation[code].add(example)
            all_tech_skills.add(example.lower())
            all_tech_skills.add(commodity.lower())

    print(f"  Loaded tech skills for {len(tech_skills_by_occupation)} occupations")
    print(f"  Total unique tech skills: {len(all_tech_skills)}")
    return tech_skills_by_occupation, all_tech_skills


# =========================================================
# STEP 2: BUILD SKILLS TAXONOMY FOR EXTRACTION
# =========================================================

def build_skills_taxonomy(onet_skills, tech_skills_set):
    """
    Build a comprehensive skills taxonomy for keyword extraction.
    Combines O*NET skills with common industry skills.
    """
    print("Building skills taxonomy...")

    # Start with O*NET core skills
    taxonomy = set()
    for occupation_skills in onet_skills.values():
        for skill_name, _ in occupation_skills:
            taxonomy.add(skill_name.lower())

    # Add tech skills
    taxonomy.update(tech_skills_set)

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

        # Industry-specific
        "clinical research", "regulatory affairs", "gmp", "fda", "hipaa",
        "cro", "pharmaceutical", "biotechnology", "healthcare",
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
    return taxonomy


# =========================================================
# STEP 3: EXTRACT SKILLS FROM JOB DESCRIPTIONS
# =========================================================

def extract_skills_from_description(description, taxonomy):
    """
    Extract skills mentioned in a job description.
    Returns list of matched skills.
    """
    if not description:
        return []

    description_lower = description.lower()
    found_skills = []

    for skill in taxonomy:
        # Use word boundary matching to avoid partial matches
        # e.g., "r" should match "R programming" but not "requirements"
        if len(skill) <= 2:
            # For very short skills (like "R", "C"), require specific patterns
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, description_lower):
                # Additional check: avoid common false positives
                if skill == "r" and not re.search(r'\br\s+(programming|language|studio)', description_lower):
                    continue
                found_skills.append(skill)
        else:
            if skill in description_lower:
                found_skills.append(skill)

    # Deduplicate and capitalize properly
    found_skills = list(set(found_skills))
    found_skills = [s.title() if len(s) > 3 else s.upper() for s in found_skills]

    return found_skills


# =========================================================
# STEP 4: MATCH JOB TITLES TO O*NET OCCUPATIONS
# =========================================================

def similarity(a, b):
    """Calculate string similarity ratio."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_best_onet_match(job_title, onet_occupations):
    """
    Find the best matching O*NET occupation for a job title.
    Returns (occupation_code, occupation_title, similarity_score) or None.
    """
    if not job_title:
        return None

    best_match = None
    best_score = 0

    # Clean up job title
    job_title_clean = re.sub(r'\([^)]*\)', '', job_title)  # Remove parentheses
    job_title_clean = re.sub(r'[,-].*$', '', job_title_clean)  # Remove after comma/dash
    job_title_clean = job_title_clean.strip()

    for code, title in onet_occupations.items():
        # Try multiple matching strategies
        score = max(
            similarity(job_title, title),
            similarity(job_title_clean, title),
            similarity(job_title.split()[0] if job_title.split() else "", title),
        )

        # Boost score if key words match
        job_words = set(job_title_clean.lower().split())
        title_words = set(title.lower().split())
        common_words = job_words & title_words
        if common_words:
            score += 0.1 * len(common_words)

        if score > best_score:
            best_score = score
            best_match = (code, title, score)

    if best_match and best_match[2] >= TITLE_MATCH_THRESHOLD:
        return best_match
    return None


def get_onet_skills_for_occupation(occupation_code, onet_skills, top_n=10):
    """Get top N skills for an O*NET occupation."""
    if occupation_code not in onet_skills:
        return []

    skills = onet_skills[occupation_code][:top_n]
    return [skill_name for skill_name, _ in skills]


# =========================================================
# STEP 5: PROCESS ALL JOBS
# =========================================================

def process_jobs():
    """Main function to extract skills for all jobs."""
    print("=" * 70)
    print("SKILLS EXTRACTION FOR CAMBRIDGE JOBS")
    print("=" * 70)

    # Load O*NET data
    onet_occupations = load_onet_occupations()
    onet_skills = load_onet_skills()
    onet_tech_skills, all_tech_skills = load_onet_tech_skills()

    # Build taxonomy
    taxonomy = build_skills_taxonomy(onet_skills, all_tech_skills)

    # Connect to database
    print("\nProcessing jobs...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all jobs
    cursor.execute("SELECT id, title, description FROM jobs")
    jobs = cursor.fetchall()
    print(f"  Total jobs to process: {len(jobs)}")

    # Process each job
    updated = 0
    matched_to_onet = 0

    for job_id, title, description in jobs:
        all_skills = set()

        # Method 1: Extract skills from job description
        desc_skills = extract_skills_from_description(description, taxonomy)
        all_skills.update(desc_skills)

        # Method 2: Get O*NET skills from matched occupation
        onet_match = find_best_onet_match(title, onet_occupations)
        if onet_match:
            occ_code, occ_title, score = onet_match
            onet_occupation_skills = get_onet_skills_for_occupation(occ_code, onet_skills)
            all_skills.update(onet_occupation_skills)
            matched_to_onet += 1

        # Combine and format skills
        skills_list = sorted(list(all_skills))
        skills_str = "; ".join(skills_list) if skills_list else ""

        # Update database
        cursor.execute("UPDATE jobs SET skills = ? WHERE id = ?", (skills_str, job_id))
        updated += 1

        if updated % 500 == 0:
            print(f"  Processed {updated} jobs...")
            conn.commit()

    conn.commit()

    print(f"\nResults:")
    print(f"  Jobs processed: {updated}")
    print(f"  Jobs matched to O*NET: {matched_to_onet} ({100*matched_to_onet/updated:.1f}%)")

    # Show sample
    print("\nSample extracted skills:")
    cursor.execute("SELECT title, skills FROM jobs WHERE skills != '' LIMIT 5")
    for title, skills in cursor.fetchall():
        print(f"  {title[:50]}: {skills[:80]}...")

    conn.close()
    return updated


# =========================================================
# STEP 6: EXPORT TO CSV
# =========================================================

def export_csv():
    """Export jobs with skills to CSV."""
    print("\nExporting to CSV...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("""
        SELECT title, employer, location, city, state,
               salary_min, salary_max, source, url, posted_date,
               is_remote, status, first_seen, description, skills
        FROM jobs
        ORDER BY source, employer, title
    """)

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['title', 'employer', 'location', 'city', 'state',
                       'salary_min', 'salary_max', 'source', 'url', 'posted_date',
                       'is_remote', 'status', 'first_seen', 'description', 'skills'])
        writer.writerows(cursor.fetchall())

    print(f"  Exported to {CSV_PATH}")
    conn.close()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    process_jobs()
    export_csv()
    print("\nDone!")
