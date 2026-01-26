#!/usr/bin/env python3
"""
Semantic matching system for job-candidate matching.
Uses OpenAI embeddings to find similar jobs based on resume/profile.
"""

import os
import json
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI
import PyPDF2
import io
import re
from typing import List, Dict, Optional, Tuple

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

# OpenAI embedding model - text-embedding-3-small is fast and cheap
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def get_openai_client():
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment")
    return OpenAI(api_key=api_key)


def get_embedding(client: OpenAI, text: str) -> List[float]:
    """Get embedding vector for text using OpenAI."""
    # Truncate to ~8000 tokens worth of text (roughly 32000 chars)
    text = text[:32000] if len(text) > 32000 else text

    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return response.data[0].embedding


def get_embeddings_batch(client: OpenAI, texts: List[str]) -> List[List[float]]:
    """Get embeddings for multiple texts in a single API call."""
    # Truncate each text
    texts = [t[:32000] if len(t) > 32000 else t for t in texts]

    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts
    )
    return [item.embedding for item in response.data]


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    a = np.array(a)
    b = np.array(b)
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def create_job_embedding_text(job: Dict) -> str:
    """Create text representation of a job for embedding."""
    parts = []

    # Title is most important
    if job.get('title'):
        parts.append(f"Job Title: {job['title']}")

    # Company context
    if job.get('company_name'):
        parts.append(f"Company: {job['company_name']}")

    # Role type
    if job.get('role_type'):
        parts.append(f"Role Type: {job['role_type']}")

    # Experience level
    if job.get('experience_level'):
        parts.append(f"Experience Level: {job['experience_level']}")

    # Description (most content)
    if job.get('description'):
        # Clean up the description - remove markdown formatting
        desc = job['description']
        desc = re.sub(r'\*\*([^*]+)\*\*', r'\1', desc)  # Remove **bold**
        desc = re.sub(r'[•\-]\s*', '', desc)  # Remove bullet points
        parts.append(f"Description: {desc}")

    # Salary range for context
    if job.get('salary_range'):
        parts.append(f"Salary: {job['salary_range']}")

    return "\n".join(parts)


def generate_job_embeddings(limit: int = None, batch_size: int = 100):
    """Generate embeddings for all jobs that don't have them yet."""
    client = get_openai_client()
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find jobs without embeddings
        query = """
            SELECT id, title, company_name, description, role_type,
                   experience_level, salary_range
            FROM watchable_positions
            WHERE description_embedding IS NULL
              AND description IS NOT NULL
        """
        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        jobs = cur.fetchall()

        print(f"Found {len(jobs)} jobs needing embeddings")

        total_embedded = 0
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]

            # Create text for each job
            texts = [create_job_embedding_text(job) for job in batch]

            try:
                # Get embeddings in batch
                embeddings = get_embeddings_batch(client, texts)

                # Store embeddings
                for job, embedding in zip(batch, embeddings):
                    cur.execute("""
                        UPDATE watchable_positions
                        SET description_embedding = %s
                        WHERE id = %s
                    """, (json.dumps(embedding), job['id']))

                conn.commit()
                total_embedded += len(batch)
                print(f"  Embedded {total_embedded}/{len(jobs)} jobs")

            except Exception as e:
                print(f"  Error embedding batch: {e}")
                conn.rollback()

    conn.close()
    return total_embedded


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text from a PDF file."""
    try:
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            text_parts = []
            for page in reader.pages:
                text_parts.append(page.extract_text() or '')
            return '\n'.join(text_parts)
    except Exception as e:
        print(f"Error extracting PDF text: {e}")
        return ""


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or '')
        return '\n'.join(text_parts)
    except Exception as e:
        print(f"Error extracting PDF text: {e}")
        return ""


def extract_profile_from_resume(client: OpenAI, resume_text: str) -> Dict:
    """Use LLM to extract structured profile from resume text."""

    prompt = f"""Analyze this resume and extract a structured profile. Return JSON with these fields:

{{
    "current_title": "their most recent job title",
    "years_experience": "total years of professional experience (number)",
    "experience_level": "intern/entry/mid/senior based on experience",
    "education": {{
        "highest_degree": "PhD/Masters/Bachelors/Associates/High School",
        "field": "field of study",
        "school": "school name if notable"
    }},
    "skills": ["list", "of", "technical", "and", "soft", "skills"],
    "industries": ["industries they have experience in"],
    "job_titles_held": ["previous job titles"],
    "certifications": ["any certifications"],
    "summary": "2-3 sentence summary of their background and strengths"
}}

Resume text:
{resume_text[:8000]}

Return ONLY valid JSON, no markdown or explanation."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1000
        )

        result = response.choices[0].message.content.strip()
        # Clean up potential markdown
        if result.startswith('```'):
            result = re.sub(r'^```(?:json)?\n?', '', result)
            result = re.sub(r'\n?```$', '', result)

        return json.loads(result)
    except Exception as e:
        print(f"Error extracting profile: {e}")
        return {}


def create_profile_embedding_text(profile: Dict, resume_text: str = None) -> str:
    """Create text representation of a candidate profile for embedding."""
    parts = []

    if profile.get('current_title'):
        parts.append(f"Current Role: {profile['current_title']}")

    if profile.get('summary'):
        parts.append(f"Summary: {profile['summary']}")

    if profile.get('skills'):
        parts.append(f"Skills: {', '.join(profile['skills'])}")

    if profile.get('industries'):
        parts.append(f"Industries: {', '.join(profile['industries'])}")

    if profile.get('job_titles_held'):
        parts.append(f"Previous Roles: {', '.join(profile['job_titles_held'])}")

    if profile.get('education'):
        edu = profile['education']
        edu_str = f"{edu.get('highest_degree', '')} in {edu.get('field', '')}"
        if edu.get('school'):
            edu_str += f" from {edu['school']}"
        parts.append(f"Education: {edu_str}")

    if profile.get('certifications'):
        parts.append(f"Certifications: {', '.join(profile['certifications'])}")

    # Add resume text for more context (truncated)
    if resume_text:
        parts.append(f"Full Background: {resume_text[:4000]}")

    return "\n".join(parts)


def process_resume_for_user(user_id: int, pdf_path: str = None, pdf_bytes: bytes = None):
    """Process a resume and store extracted profile + embedding for a user."""
    client = get_openai_client()
    conn = get_db()

    # Extract text from PDF
    if pdf_path:
        resume_text = extract_text_from_pdf(pdf_path)
    elif pdf_bytes:
        resume_text = extract_text_from_pdf_bytes(pdf_bytes)
    else:
        raise ValueError("Must provide either pdf_path or pdf_bytes")

    if not resume_text.strip():
        print("Could not extract text from PDF")
        return None

    # Extract structured profile
    profile = extract_profile_from_resume(client, resume_text)

    # Create embedding text and generate embedding
    embedding_text = create_profile_embedding_text(profile, resume_text)
    embedding = get_embedding(client, embedding_text)

    # Store in database
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE seeker_profiles
            SET resume_text = %s,
                extracted_profile = %s,
                resume_embedding = %s,
                skills_extracted = TRUE,
                skills_extracted_at = NOW()
            WHERE user_id = %s
        """, (
            resume_text,
            json.dumps(profile),
            json.dumps(embedding),
            user_id
        ))
        conn.commit()

    conn.close()
    return profile


def find_matching_jobs(
    user_id: int = None,
    resume_embedding: List[float] = None,
    filters: Dict = None,
    limit: int = 50
) -> List[Dict]:
    """Find jobs that match a candidate's profile using semantic similarity."""
    conn = get_db()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get user's embedding if not provided
        if resume_embedding is None and user_id:
            cur.execute("""
                SELECT resume_embedding, extracted_profile
                FROM seeker_profiles
                WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row and row['resume_embedding']:
                resume_embedding = row['resume_embedding']
            else:
                print("No embedding found for user")
                return []

        if resume_embedding is None:
            return []

        # Get all jobs with embeddings
        query = """
            SELECT id, title, company_name, location, description,
                   role_type, experience_level, salary_range, salary_min, salary_max,
                   work_arrangement, description_embedding
            FROM watchable_positions
            WHERE description_embedding IS NOT NULL
        """

        # Apply filters
        params = []
        if filters:
            if filters.get('role_types'):
                query += " AND role_type = ANY(%s)"
                params.append(filters['role_types'])

            if filters.get('experience_levels'):
                query += " AND experience_level = ANY(%s)"
                params.append(filters['experience_levels'])

            if filters.get('min_salary'):
                query += " AND salary_max >= %s"
                params.append(filters['min_salary'])

            if filters.get('work_arrangements'):
                query += " AND work_arrangement = ANY(%s)"
                params.append(filters['work_arrangements'])

        cur.execute(query, params)
        jobs = cur.fetchall()

    conn.close()

    # Calculate similarity scores
    results = []
    for job in jobs:
        if job['description_embedding']:
            job_embedding = job['description_embedding']
            if isinstance(job_embedding, str):
                job_embedding = json.loads(job_embedding)

            similarity = cosine_similarity(resume_embedding, job_embedding)

            results.append({
                'id': job['id'],
                'title': job['title'],
                'company_name': job['company_name'],
                'location': job['location'],
                'role_type': job['role_type'],
                'experience_level': job['experience_level'],
                'salary_range': job['salary_range'],
                'work_arrangement': job['work_arrangement'],
                'match_score': round(similarity * 100, 1),  # Convert to percentage
                'description': job['description'][:500] + '...' if len(job['description'] or '') > 500 else job['description']
            })

    # Sort by match score
    results.sort(key=lambda x: x['match_score'], reverse=True)

    return results[:limit]


def get_job_matches_for_text(
    text: str,
    filters: Dict = None,
    limit: int = 50
) -> List[Dict]:
    """Find matching jobs for arbitrary text (e.g., job description, query)."""
    client = get_openai_client()

    # Generate embedding for the text
    embedding = get_embedding(client, text)

    return find_matching_jobs(
        resume_embedding=embedding,
        filters=filters,
        limit=limit
    )


def explain_match(job: Dict, profile: Dict) -> str:
    """Generate a brief explanation of why a job matches a profile."""
    reasons = []

    # Check for skill matches
    if profile.get('skills') and job.get('description'):
        desc_lower = job['description'].lower()
        matching_skills = [s for s in profile['skills'] if s.lower() in desc_lower]
        if matching_skills:
            reasons.append(f"Skills match: {', '.join(matching_skills[:5])}")

    # Check for industry match
    if profile.get('industries'):
        for industry in profile['industries']:
            if industry.lower() in job.get('description', '').lower():
                reasons.append(f"Industry experience: {industry}")
                break

    # Check for title similarity
    if profile.get('job_titles_held'):
        job_title_lower = job.get('title', '').lower()
        for title in profile['job_titles_held']:
            if any(word in job_title_lower for word in title.lower().split()):
                reasons.append(f"Similar role to your experience as {title}")
                break

    # Experience level match
    if profile.get('experience_level') and job.get('experience_level'):
        if profile['experience_level'] == job['experience_level']:
            reasons.append(f"Experience level match: {job['experience_level']}")

    return " | ".join(reasons) if reasons else "Strong semantic match based on your overall profile"


# ============================================================================
# CLI INTERFACE
# ============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Semantic job matching system')
    parser.add_argument('--generate-embeddings', action='store_true',
                        help='Generate embeddings for all jobs')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of jobs to process')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Batch size for embedding generation')
    parser.add_argument('--test-resume', type=str,
                        help='Test matching with a resume PDF')
    parser.add_argument('--search', type=str,
                        help='Search for jobs matching a text query')

    args = parser.parse_args()

    if args.generate_embeddings:
        print("Generating job embeddings...")
        count = generate_job_embeddings(limit=args.limit, batch_size=args.batch_size)
        print(f"Generated embeddings for {count} jobs")

    elif args.test_resume:
        print(f"Testing with resume: {args.test_resume}")
        client = get_openai_client()

        # Extract text
        resume_text = extract_text_from_pdf(args.test_resume)
        print(f"Extracted {len(resume_text)} characters from resume")

        # Extract profile
        profile = extract_profile_from_resume(client, resume_text)
        print(f"\nExtracted profile:")
        print(json.dumps(profile, indent=2))

        # Create embedding and find matches
        embedding_text = create_profile_embedding_text(profile, resume_text)
        embedding = get_embedding(client, embedding_text)

        matches = find_matching_jobs(resume_embedding=embedding, limit=10)
        print(f"\nTop 10 matching jobs:")
        for i, match in enumerate(matches, 1):
            print(f"{i}. [{match['match_score']}%] {match['title']} at {match['company_name']}")
            print(f"   {match['salary_range'] or 'Salary not listed'} | {match['location']}")

    elif args.search:
        print(f"Searching for: {args.search}")
        matches = get_job_matches_for_text(args.search, limit=10)
        print(f"\nTop 10 matching jobs:")
        for i, match in enumerate(matches, 1):
            print(f"{i}. [{match['match_score']}%] {match['title']} at {match['company_name']}")
            print(f"   {match['salary_range'] or 'Salary not listed'} | {match['location']}")

    else:
        parser.print_help()
