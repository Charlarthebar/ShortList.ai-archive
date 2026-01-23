#!/usr/bin/env python3
"""
Resume skill extraction using OpenAI.

Extracts ONET skills from resume text and stores them in the database.
"""

import os
import json
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime

# Load environment variables
load_dotenv()

# Database connection - matches app.py config
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)


def get_all_onet_skills():
    """Get all unique ONET skills from the database."""
    conn = get_db()
    skills = []

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get unique skill names with their minimum ID (for linking)
        cur.execute("""
            SELECT MIN(id) as id, skill_name
            FROM onet_skills
            GROUP BY skill_name
            ORDER BY skill_name
        """)
        skills = [dict(row) for row in cur.fetchall()]

    conn.close()
    return skills


def extract_text_from_pdf(pdf_path):
    """Extract text content from a PDF file."""
    try:
        # Try pdfplumber first (better for complex PDFs)
        import pdfplumber
        text_parts = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        return '\n'.join(text_parts)
    except ImportError:
        pass

    try:
        # Fallback to PyPDF2
        import PyPDF2
        text_parts = []
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        return '\n'.join(text_parts)
    except ImportError:
        raise ImportError("Please install pdfplumber or PyPDF2: pip install pdfplumber PyPDF2")

    return ""


def extract_skills_with_ai(resume_text, onet_skills):
    """
    Use OpenAI to identify ONET skills from resume text.

    Args:
        resume_text: Extracted text from resume
        onet_skills: List of valid ONET skills to match against

    Returns:
        List of dicts with skill_id, skill_name, and confidence
    """
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment")

    client = OpenAI(api_key=api_key)

    # Create skill names list for the prompt
    skill_names = [s['skill_name'] for s in onet_skills]
    skill_name_to_id = {s['skill_name'].lower(): s['id'] for s in onet_skills}

    # Truncate resume if too long (keep first 6000 chars)
    resume_excerpt = resume_text[:6000] if len(resume_text) > 6000 else resume_text

    prompt = f"""Analyze this resume and identify which skills from the provided list the candidate demonstrates.

VALID SKILLS (only return skills from this exact list):
{json.dumps(skill_names, indent=2)}

RESUME TEXT:
{resume_excerpt}

Instructions:
1. Only return skills that are explicitly mentioned or clearly demonstrated in the resume
2. Assign a confidence score (0.0 to 1.0) based on how clearly the skill is demonstrated
3. Return a JSON array of objects with "skill_name" (exact match from list) and "confidence"
4. Do NOT invent skills not in the list
5. If unsure, use lower confidence scores

Return ONLY valid JSON array, no other text. Example format:
[{{"skill_name": "Python", "confidence": 0.95}}, {{"skill_name": "Communication", "confidence": 0.8}}]"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2000
        )

        result_text = response.choices[0].message.content.strip()

        # Clean up JSON if wrapped in markdown
        if result_text.startswith('```'):
            result_text = re.sub(r'^```json?\n?', '', result_text)
            result_text = re.sub(r'\n?```$', '', result_text)

        skills_found = json.loads(result_text)

        # Map to skill IDs and validate
        validated_skills = []
        for skill in skills_found:
            skill_name = skill.get('skill_name', '').strip()
            confidence = min(1.0, max(0.0, float(skill.get('confidence', 0.5))))

            skill_id = skill_name_to_id.get(skill_name.lower())
            if skill_id:
                validated_skills.append({
                    'skill_id': skill_id,
                    'skill_name': skill_name,
                    'confidence': confidence
                })

        return validated_skills

    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        print(f"Raw response: {result_text}")
        return []
    except Exception as e:
        print(f"OpenAI API error: {e}")
        raise


def save_candidate_skills(user_id, skills, source='resume'):
    """
    Save extracted skills to the candidate_skills table.

    Args:
        user_id: The user's ID
        skills: List of skill dicts with skill_id and confidence
        source: Source of skills ('resume', 'manual', etc.)
    """
    if not skills:
        return 0

    conn = get_db()
    saved_count = 0

    with conn.cursor() as cur:
        # First, optionally clear existing resume-extracted skills
        if source == 'resume':
            cur.execute("""
                DELETE FROM candidate_skills
                WHERE user_id = %s AND source = 'resume'
            """, (user_id,))

        # Insert new skills
        for skill in skills:
            try:
                cur.execute("""
                    INSERT INTO candidate_skills (user_id, skill_id, confidence, source, extracted_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, skill_id) DO UPDATE
                    SET confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source,
                        extracted_at = EXCLUDED.extracted_at
                """, (user_id, skill['skill_id'], skill['confidence'], source, datetime.utcnow()))
                saved_count += 1
            except Exception as e:
                print(f"Error saving skill {skill.get('skill_name')}: {e}")

        # Update seeker_profiles to mark skills as extracted
        cur.execute("""
            UPDATE seeker_profiles
            SET skills_extracted = TRUE, skills_extracted_at = %s
            WHERE user_id = %s
        """, (datetime.utcnow(), user_id))

    conn.commit()
    conn.close()
    return saved_count


def process_resume(user_id, resume_path):
    """
    Full pipeline: Extract text from PDF, identify skills, save to database.

    Args:
        user_id: The user's ID
        resume_path: Path to the PDF resume file

    Returns:
        Dict with extraction results
    """
    # Extract text from PDF
    print(f"Extracting text from {resume_path}...")
    resume_text = extract_text_from_pdf(resume_path)

    if not resume_text or len(resume_text.strip()) < 50:
        return {
            'success': False,
            'error': 'Could not extract sufficient text from PDF',
            'skills_count': 0
        }

    print(f"Extracted {len(resume_text)} characters from resume")

    # Get ONET skills for matching
    onet_skills = get_all_onet_skills()
    if not onet_skills:
        return {
            'success': False,
            'error': 'No ONET skills found in database. Run import script first.',
            'skills_count': 0
        }

    print(f"Matching against {len(onet_skills)} ONET skills...")

    # Extract skills using AI
    extracted_skills = extract_skills_with_ai(resume_text, onet_skills)
    print(f"Found {len(extracted_skills)} matching skills")

    # Save to database
    saved_count = save_candidate_skills(user_id, extracted_skills, source='resume')
    print(f"Saved {saved_count} skills to database")

    return {
        'success': True,
        'skills_count': saved_count,
        'skills': extracted_skills
    }


def get_user_skills(user_id):
    """Get all skills for a user."""
    conn = get_db()
    skills = []

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT cs.skill_id, os.skill_name, os.skill_type, cs.confidence, cs.source
            FROM candidate_skills cs
            JOIN onet_skills os ON cs.skill_id = os.id
            WHERE cs.user_id = %s
            ORDER BY cs.confidence DESC
        """, (user_id,))
        skills = [dict(row) for row in cur.fetchall()]

    conn.close()
    return skills


def get_job_required_skills(position_id):
    """Get required skills for a job."""
    conn = get_db()
    skills = []

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT jrs.skill_id, os.skill_name, os.skill_type
            FROM job_required_skills jrs
            JOIN onet_skills os ON jrs.skill_id = os.id
            WHERE jrs.position_id = %s
        """, (position_id,))
        skills = [dict(row) for row in cur.fetchall()]

    conn.close()
    return skills


def calculate_skills_match(user_id, position_id):
    """
    Calculate how well a user's skills match a job's requirements.

    Returns:
        Float 0-100 representing skill match percentage
    """
    user_skills = get_user_skills(user_id)
    job_skills = get_job_required_skills(position_id)

    if not job_skills:
        # No skills required = full match
        return 100.0

    if not user_skills:
        # No user skills = no match
        return 0.0

    user_skill_ids = {s['skill_id'] for s in user_skills}
    job_skill_ids = {s['skill_id'] for s in job_skills}

    matched = len(user_skill_ids & job_skill_ids)
    match_percentage = (matched / len(job_skill_ids)) * 100

    return round(match_percentage, 1)


# CLI for testing
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Extract skills from resume')
    parser.add_argument('--resume', type=str, help='Path to PDF resume')
    parser.add_argument('--user-id', type=int, help='User ID to associate skills with')
    parser.add_argument('--list-skills', action='store_true', help='List all ONET skills')

    args = parser.parse_args()

    if args.list_skills:
        skills = get_all_onet_skills()
        print(f"Found {len(skills)} ONET skills:")
        for s in skills[:20]:
            print(f"  - {s['skill_name']} ({s['skill_type']})")
        print("  ...")

    elif args.resume and args.user_id:
        result = process_resume(args.user_id, args.resume)
        print(f"\nResult: {json.dumps(result, indent=2)}")

    else:
        parser.print_help()
