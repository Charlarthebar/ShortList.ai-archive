"""
ShortList API - Minimal Backend
Clean, focused API for the ShortList platform.
"""

import os
import hashlib
import secrets
import json
import re
from datetime import datetime, timedelta
from functools import wraps

# Load .env file
from dotenv import load_dotenv
load_dotenv()

import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from werkzeug.utils import secure_filename

# Optional: OpenAI for embeddings
try:
    from openai import OpenAI as OpenAIClient
    OPENAI_AVAILABLE = bool(os.environ.get('OPENAI_API_KEY'))
except ImportError:
    OPENAI_AVAILABLE = False

# Optional: Numpy for cosine similarity
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

app = Flask(__name__)

# Explicit CORS handling - more reliable than flask_cors alone
@app.after_request
def add_cors_headers(response):
    origin = request.headers.get('Origin', '')
    allowed_origins = ['http://localhost:8000', 'http://127.0.0.1:8000']

    if origin in allowed_origins:
        response.headers['Access-Control-Allow-Origin'] = origin
    else:
        # Default to localhost:8000 for development
        response.headers['Access-Control-Allow-Origin'] = 'http://localhost:8000'

    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Expose-Headers'] = 'Content-Type, Authorization'
    return response

# Handle OPTIONS preflight requests explicitly
@app.route('/api/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    response = app.make_default_options_response()
    return response

# Also initialize flask_cors as backup
CORS(app, supports_credentials=True)

# Config
SECRET_KEY = os.environ.get('SECRET_KEY', secrets.token_hex(32))
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Database connection
def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            port=os.environ.get('DB_PORT', '5432'),
            dbname=os.environ.get('DB_NAME', 'jobs_comprehensive'),
            user=os.environ.get('DB_USER', 'noahhopkins'),
            password=os.environ.get('DB_PASSWORD', '')
        )
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

# Auth helpers
def hash_password(password):
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}${hashed.hex()}"

def verify_password(password, stored):
    try:
        salt, hashed = stored.split('$')
        check = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return check.hex() == hashed
    except:
        return False

def generate_token(user_id):
    return jwt.encode(
        {'user_id': user_id, 'exp': datetime.utcnow() + timedelta(days=30)},
        SECRET_KEY, algorithm='HS256'
    )

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token:
            return jsonify({'error': 'Authentication required'}), 401
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            g.user_id = payload['user_id']
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401
        return f(*args, **kwargs)
    return decorated


# ============================================================================
# AUTH ENDPOINTS
# ============================================================================

@app.route('/api/auth/signup', methods=['POST'])
def signup():
    """Create a new user account."""
    data = request.json
    email = data.get('email', '').lower().strip()
    password = data.get('password', '')
    full_name = data.get('full_name', '').strip()
    user_type = data.get('user_type', 'seeker')
    company_name = data.get('company', '').strip() if data.get('company') else None

    # Map 'employer' to 'company' for database constraint
    db_user_type = 'company' if user_type == 'employer' else user_type

    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    if len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    if not full_name:
        return jsonify({'error': 'Full name required'}), 400
    if user_type == 'employer' and not company_name:
        return jsonify({'error': 'Company name required for employer accounts'}), 400

    # Split full name into first and last
    name_parts = full_name.split(' ', 1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else ''

    conn = get_db()
    with conn.cursor() as cur:
        # Check if email exists
        cur.execute("SELECT id FROM platform_users WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({'error': 'Email already registered'}), 409

        # Create user
        cur.execute("""
            INSERT INTO platform_users (email, password_hash, user_type, first_name, last_name)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (email, hash_password(password), db_user_type, first_name, last_name))
        user_id = cur.fetchone()[0]

        # Create empty profile for seekers
        if user_type == 'seeker':
            cur.execute("INSERT INTO seeker_profiles (user_id) VALUES (%s)", (user_id,))

        # For employers, create/find company profile and link user
        company_profile_id = None
        if user_type == 'employer' and company_name:
            # Check if company profile already exists
            cur.execute("""
                SELECT id FROM company_profiles WHERE company_name = %s
            """, (company_name,))
            result = cur.fetchone()

            if result:
                company_profile_id = result[0]
            else:
                # Create new company profile
                cur.execute("""
                    INSERT INTO company_profiles (company_name, verified)
                    VALUES (%s, FALSE) RETURNING id
                """, (company_name,))
                company_profile_id = cur.fetchone()[0]

            # Link user to company as owner (first person from company)
            cur.execute("""
                INSERT INTO company_team_members (company_profile_id, user_id, role, accepted_at)
                VALUES (%s, %s, 'owner', NOW())
            """, (company_profile_id, user_id))

        conn.commit()

    response_data = {
        'token': generate_token(user_id),
        'user': {
            'id': user_id,
            'email': email,
            'user_type': user_type,  # Return original user_type for frontend
            'first_name': first_name,
            'last_name': last_name
        }
    }

    if company_name:
        response_data['user']['company'] = company_name
        response_data['user']['company_profile_id'] = company_profile_id

    return jsonify(response_data)


@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login with email and password."""
    data = request.json
    email = data.get('email', '').lower().strip()
    password = data.get('password', '')

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, email, password_hash, user_type FROM platform_users WHERE email = %s
        """, (email,))
        user = cur.fetchone()

        if not user or not verify_password(password, user['password_hash']):
            return jsonify({'error': 'Invalid email or password'}), 401

        # Map 'company' back to 'employer' for frontend
        user_type = 'employer' if user['user_type'] == 'company' else user['user_type']

        return jsonify({
            'token': generate_token(user['id']),
            'user': {'id': user['id'], 'email': user['email'], 'user_type': user_type}
        })


@app.route('/api/auth/me', methods=['GET'])
@require_auth
def get_me():
    """Get current user info and profile."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT u.id, u.email, u.user_type, u.first_name, u.last_name, u.resume_path,
                   p.experience_level, p.work_preference, p.profile_complete
            FROM platform_users u
            LEFT JOIN seeker_profiles p ON p.user_id = u.id
            WHERE u.id = %s
        """, (g.user_id,))
        user = cur.fetchone()

        if not user:
            return jsonify({'error': 'User not found'}), 404

        # Map 'company' back to 'employer' for frontend
        if user['user_type'] == 'company':
            user['user_type'] = 'employer'

            # Get company info for employer
            cur.execute("""
                SELECT cp.company_name, cp.id as company_profile_id
                FROM company_team_members ctm
                JOIN company_profiles cp ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.user_id,))
            company = cur.fetchone()
            if company:
                user['company'] = company['company_name']
                user['company_profile_id'] = company['company_profile_id']

        # Convert to dict and add has_resume flag for frontend
        user_dict = dict(user)
        user_dict['has_resume'] = bool(user_dict.get('resume_path'))

        return jsonify({'user': user_dict})


# ============================================================================
# PROFILE ENDPOINTS
# ============================================================================

@app.route('/api/profile', methods=['PUT'])
@require_auth
def update_profile():
    """Update user profile (experience level, work preference)."""
    data = request.json
    experience_level = data.get('experience_level')
    work_preference = data.get('work_preference')

    if not experience_level or not work_preference:
        return jsonify({'error': 'Experience level and work preference required'}), 400

    valid_levels = ['intern', 'entry', 'mid', 'senior']
    valid_prefs = ['remote', 'hybrid', 'onsite']

    if experience_level not in valid_levels:
        return jsonify({'error': f'Invalid experience level. Must be one of: {valid_levels}'}), 400
    if work_preference not in valid_prefs:
        return jsonify({'error': f'Invalid work preference. Must be one of: {valid_prefs}'}), 400

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE seeker_profiles
            SET experience_level = %s, work_preference = %s, profile_complete = TRUE
            WHERE user_id = %s
        """, (experience_level, work_preference, g.user_id))
        conn.commit()

    return jsonify({'success': True})


@app.route('/api/profile/preferences', methods=['PUT'])
@require_auth
def update_preferences():
    """Update user job preferences. Only updates fields that are explicitly provided."""
    data = request.json or {}

    # Build dynamic update query - only update fields that are in the request
    updates = []
    params = []

    # Check each field - only update if key is present in request
    if 'preferred_locations' in data:
        updates.append("preferred_locations = %s")
        locs = data['preferred_locations']
        params.append(locs if locs else None)

    if 'salary_min' in data:
        updates.append("salary_min = %s")
        params.append(data['salary_min'])

    if 'salary_max' in data:
        updates.append("salary_max = %s")
        params.append(data['salary_max'])

    if 'open_to_roles' in data:
        updates.append("open_to_roles = %s")
        roles = data['open_to_roles']
        params.append(roles if roles else None)

    if 'experience_level' in data:
        updates.append("experience_level = %s")
        params.append(data['experience_level'])

    if 'work_arrangement' in data:
        updates.append("work_preference = %s")
        params.append(data['work_arrangement'])

    if 'preferences_text' in data:
        preferences_text = (data['preferences_text'] or '').strip()
        updates.append("preferences_text = %s")
        params.append(preferences_text if preferences_text else None)

        # Generate embedding for preferences text if OpenAI is available
        if preferences_text and OPENAI_AVAILABLE:
            try:
                response = openai.embeddings.create(
                    model="text-embedding-3-small",
                    input=preferences_text
                )
                updates.append("preferences_embedding = %s")
                params.append(json.dumps(response.data[0].embedding))
            except Exception as e:
                print(f"Failed to generate embedding: {e}")

    if 'profile_complete' in data:
        updates.append("profile_complete = %s")
        params.append(data['profile_complete'])

    # Always update timestamp
    updates.append("updated_at = NOW()")

    if not updates:
        return jsonify({'success': True, 'message': 'No fields to update'})

    params.append(g.user_id)

    conn = get_db()
    with conn.cursor() as cur:
        query = f"UPDATE seeker_profiles SET {', '.join(updates)} WHERE user_id = %s"
        cur.execute(query, params)
        conn.commit()

    return jsonify({'success': True})


@app.route('/api/profile/preferences', methods=['GET'])
@require_auth
def get_preferences():
    """Get current user's job preferences."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT preferred_locations, salary_min, salary_max,
                   open_to_roles, experience_level, work_preference, preferences_text
            FROM seeker_profiles
            WHERE user_id = %s
        """, (g.user_id,))
        prefs = cur.fetchone()

        if not prefs:
            return jsonify({'preferences': {}})

        return jsonify({'preferences': dict(prefs)})


@app.route('/api/profile/upload-resume', methods=['POST'])
@require_auth
def upload_profile_resume():
    """
    Upload resume to user's profile and process it for semantic matching.
    Extracts profile info and generates embedding for job recommendations.
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['file']
        if not file.filename:
            return jsonify({'error': 'No file selected'}), 400

        # Validate file type
        if not file.filename.lower().endswith('.pdf'):
            return jsonify({'error': 'Only PDF files are accepted'}), 400

        # Read file contents
        pdf_bytes = file.read()

        # Save file with user-specific name
        filename = secure_filename(f"{g.user_id}_resume.pdf")
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        with open(filepath, 'wb') as f:
            f.write(pdf_bytes)

        conn = get_db()
        with conn.cursor() as cur:
            # Save resume path to user profile
            cur.execute("""
                UPDATE platform_users
                SET resume_path = %s
                WHERE id = %s
            """, (filename, g.user_id))
            conn.commit()

        # Process resume with semantic matching system
        print(f"Processing resume for user {g.user_id}...")
        result = process_resume_and_get_matches(g.user_id, pdf_bytes)
        print(f"Resume processing complete: {result.get('profile', {}).get('current_title', 'unknown')}")

        if 'error' in result:
            return jsonify({
                'success': False,
                'error': result['error'],
                'filename': filename
            }), 500

        return jsonify({
            'success': True,
            'filename': filename,
            'profile': result.get('profile', {})
        })
    except Exception as e:
        import traceback
        print(f"Error in upload_profile_resume: {e}")
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/api/profile/skills', methods=['GET'])
@require_auth
def get_user_skills():
    """Get skills extracted from user's resume."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT os.skill_name, cs.confidence
            FROM candidate_skills cs
            JOIN onet_skills os ON cs.skill_id = os.id
            WHERE cs.user_id = %s
            ORDER BY cs.confidence DESC
        """, (g.user_id,))
        skills = [dict(row) for row in cur.fetchall()]

    return jsonify({'skills': skills})


@app.route('/api/locations', methods=['GET'])
def get_locations():
    """Get location suggestions for autocomplete."""
    query = request.args.get('q', '').strip().lower()

    if len(query) < 2:
        return jsonify({'locations': []})

    conn = get_db()
    with conn.cursor() as cur:
        # Get distinct locations that match the query
        cur.execute("""
            SELECT DISTINCT location
            FROM watchable_positions
            WHERE location IS NOT NULL
              AND location ILIKE %s
              AND location NOT LIKE '%%;%%'
            ORDER BY location
            LIMIT 20
        """, (f'%{query}%',))

        locations = [row[0] for row in cur.fetchall()]

        # Also add some common US cities if they match
        common_cities = [
            'Boston, MA', 'Cambridge, MA', 'New York, NY', 'San Francisco, CA',
            'Los Angeles, CA', 'Chicago, IL', 'Seattle, WA', 'Austin, TX',
            'Denver, CO', 'Atlanta, GA', 'Miami, FL', 'Washington, DC',
            'Portland, OR', 'Philadelphia, PA', 'San Diego, CA', 'Phoenix, AZ',
            'Dallas, TX', 'Houston, TX', 'Minneapolis, MN', 'Detroit, MI'
        ]
        for city in common_cities:
            if query in city.lower() and city not in locations:
                locations.append(city)

        # Sort and dedupe
        locations = sorted(set(locations))[:20]

        return jsonify({'locations': locations})


# ============================================================================
# JOB CLASSIFICATION HELPERS
# ============================================================================

def classify_role_type(title):
    """
    Classify a job title into one of 12 role categories.
    Returns the role_type string or None if unclassified.
    """
    if not title:
        return None

    title_lower = title.lower()

    # Order matters - more specific patterns first
    patterns = [
        # Engineering Manager (before software_engineer to catch managers)
        ('engineering_manager', [
            r'engineering manager', r'eng manager', r'development manager',
            r'software manager', r'technical manager', r'head of engineering',
            r'director.*(engineering|software|development)', r'vp.*(engineering|software)'
        ]),
        # Software Engineer
        ('software_engineer', [
            r'software engineer', r'software developer', r'backend engineer',
            r'frontend engineer', r'full.?stack', r'web developer',
            r'mobile (developer|engineer)', r'ios (developer|engineer)',
            r'android (developer|engineer)', r'platform engineer',
            r'systems engineer', r'devops', r'sre', r'site reliability',
            r'infrastructure engineer', r'cloud engineer', r'qa engineer',
            r'test engineer', r'sdet', r'automation engineer',
            r'security engineer', r'application engineer', r'embedded'
        ]),
        # Data Scientist
        ('data_scientist', [
            r'data scientist', r'machine learning', r'ml engineer',
            r'ai engineer', r'research scientist', r'applied scientist',
            r'deep learning', r'nlp engineer', r'computer vision'
        ]),
        # Data Analyst
        ('data_analyst', [
            r'data analyst', r'business analyst', r'analytics',
            r'bi analyst', r'business intelligence', r'reporting analyst',
            r'insights analyst', r'data engineer'
        ]),
        # Product Manager
        ('product_manager', [
            r'product manager', r'product owner', r'program manager',
            r'project manager', r'technical program', r'tpm',
            r'product lead', r'head of product'
        ]),
        # Sales
        ('sales', [
            r'sales', r'account executive', r'business development',
            r'bdr', r'sdr', r'account manager', r'customer success',
            r'solutions consultant', r'solutions engineer', r'pre.?sales'
        ]),
        # Marketing
        ('marketing', [
            r'marketing', r'growth', r'content', r'brand',
            r'communications', r'pr ', r'public relations',
            r'social media', r'seo', r'sem', r'demand gen'
        ]),
        # Design
        ('design', [
            r'designer', r'ux', r'ui', r'user experience',
            r'user interface', r'product design', r'visual design',
            r'graphic design', r'creative', r'art director'
        ]),
        # Operations
        ('operations', [
            r'operations', r'supply chain', r'logistics', r'procurement',
            r'facilities', r'office manager', r'executive assistant',
            r'chief of staff', r'strategy', r'consulting'
        ]),
        # Finance
        ('finance', [
            r'finance', r'accountant', r'accounting', r'controller',
            r'cfo', r'financial analyst', r'fp&a', r'treasury',
            r'audit', r'tax', r'payroll'
        ]),
        # HR
        ('hr', [
            r'human resources', r'\bhr\b', r'recruiter', r'recruiting',
            r'talent', r'people ops', r'people operations',
            r'compensation', r'benefits', r'hrbp'
        ]),
        # Support
        ('support', [
            r'customer support', r'customer service', r'technical support',
            r'help desk', r'support engineer', r'support specialist',
            r'client services', r'implementation'
        ]),
    ]

    for role_type, role_patterns in patterns:
        for pattern in role_patterns:
            if re.search(pattern, title_lower):
                return role_type

    return None


def classify_experience_level(title):
    """
    Classify a job title into one of 4 experience levels.
    Returns: 'intern', 'entry', 'mid', 'senior', or None if unclear.
    """
    if not title:
        return None

    title_lower = title.lower()

    # Intern
    if re.search(r'\bintern\b|\binternship\b', title_lower):
        return 'intern'

    # Senior/Lead/Staff/Principal
    if re.search(r'\bsenior\b|\bsr\.?\b|\blead\b|\bprincipal\b|\bstaff\b|\bdirector\b|\bhead\b|\bvp\b|\bchief\b|\bmanager\b|\biii\b|\biv\b|\b[4-9]\b', title_lower):
        return 'senior'

    # Entry/Junior/Associate
    if re.search(r'\bjunior\b|\bjr\.?\b|\bentry\b|\bassociate\b|\b[i1]\b(?!\w)|\bnew grad\b', title_lower):
        return 'entry'

    # Mid-level (II or 2, or no explicit level)
    if re.search(r'\bii\b|\b2\b|\bmid\b', title_lower):
        return 'mid'

    # If no explicit level indicators, return None (could be any level)
    return None


# ============================================================================
# FIT QUESTIONS - Hardcoded questions by role type
# ============================================================================

FIT_QUESTIONS = {
    # Tech roles: software_engineer, data_scientist, data_analyst, engineering_manager
    'tech': [
        {
            'id': 'tech_requirements',
            'question_text': 'When requirements are unclear, you prefer to:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Pause and push for clarity before building'},
                {'value': 'B', 'label': 'Build a small prototype to force clarity'},
                {'value': 'C', 'label': 'Write a short spec with assumptions and get async sign-off'},
                {'value': 'D', 'label': 'Ask for a quick live sync and decide'}
            ]
        },
        {
            'id': 'tech_pr_culture',
            'question_text': 'Your ideal PR/review culture is:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Fast approvals, small diffs, minimal commentary'},
                {'value': 'B', 'label': 'Thorough reviews, deeper discussion, fewer merges'},
                {'value': 'C', 'label': 'Strong standards but "rubber meets road" pragmatism'},
                {'value': 'D', 'label': 'Pairing/mobbing to reduce review friction'}
            ]
        },
        {
            'id': 'tech_work_style',
            'question_text': "You're happiest when your work is:",
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Deep and technical, few meetings'},
                {'value': 'B', 'label': 'Cross-functional, high coordination'},
                {'value': 'C', 'label': 'End-to-end ownership of a feature/product slice'},
                {'value': 'D', 'label': 'Rapid-fire tasks with visible momentum'}
            ]
        },
        {
            'id': 'tech_oncall',
            'question_text': 'In on-call/production ownership, you prefer:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Invest heavily in prevention/automation'},
                {'value': 'B', 'label': 'Accept some firefighting to move fast'},
                {'value': 'C', 'label': 'Rotate ownership; share load broadly'},
                {'value': 'D', 'label': 'One clear owner per system for accountability'}
            ]
        },
        {
            'id': 'tech_team_env',
            'question_text': 'What kind of team environment makes you do your best work?',
            'question_type': 'free_response',
            'options': None
        },
        {
            'id': 'tech_frustrations',
            'question_text': 'What frustrates you most at work (process, ambiguity, slow decisions, etc.)?',
            'question_type': 'free_response',
            'options': None
        }
    ],
    # Sales roles
    'sales': [
        {
            'id': 'sales_motion',
            'question_text': 'Your most natural selling motion is:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'High-volume outbound with tight messaging'},
                {'value': 'B', 'label': 'Deep discovery + tailored solution'},
                {'value': 'C', 'label': 'Relationship building + long-cycle trust'},
                {'value': 'D', 'label': 'Insight selling (teach/reframe)'}
            ]
        },
        {
            'id': 'sales_leadership',
            'question_text': 'In a company, you prefer sales leadership that is:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Metrics-heavy and structured'},
                {'value': 'B', 'label': 'Light process, high autonomy'},
                {'value': 'C', 'label': 'Coaching-oriented (call reviews/roleplays)'},
                {'value': 'D', 'label': 'Strategy-oriented (deal shaping, multi-threading)'}
            ]
        },
        {
            'id': 'sales_uncertainty',
            'question_text': 'When a deal is uncertain, you prefer to:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Push for a firm next step/decision date'},
                {'value': 'B', 'label': 'Nurture with value until timing is right'},
                {'value': 'C', 'label': 'Expand stakeholders to increase momentum'},
                {'value': 'D', 'label': 'Shrink scope to a pilot to reduce risk'}
            ]
        },
        {
            'id': 'sales_motivation',
            'question_text': 'What motivates you most week-to-week:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Winning/competition'},
                {'value': 'B', 'label': 'Money and clear targets'},
                {'value': 'C', 'label': 'Helping customers succeed'},
                {'value': 'D', 'label': 'Mastery and improving your craft'}
            ]
        },
        {
            'id': 'sales_buyer',
            'question_text': 'What kind of buyer/customer do you enjoy working with most? Least?',
            'question_type': 'free_response',
            'options': None
        },
        {
            'id': 'sales_manager',
            'question_text': 'What does a "good manager" do that actually changes your performance?',
            'question_type': 'free_response',
            'options': None
        },
        {
            'id': 'sales_avoid',
            'question_text': 'What kind of product/company would you not want to sell for?',
            'question_type': 'free_response',
            'options': None
        }
    ],
    # Default questions for other roles
    'default': [
        {
            'id': 'default_work_style',
            'question_text': "You're happiest when your work is:",
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Independent with clear goals'},
                {'value': 'B', 'label': 'Collaborative with lots of interaction'},
                {'value': 'C', 'label': 'Creative with room to experiment'},
                {'value': 'D', 'label': 'Structured with defined processes'}
            ]
        },
        {
            'id': 'default_feedback',
            'question_text': 'You prefer feedback that is:',
            'question_type': 'multiple_choice',
            'options': [
                {'value': 'A', 'label': 'Direct and frequent'},
                {'value': 'B', 'label': 'Thoughtful and scheduled'},
                {'value': 'C', 'label': 'Informal and conversational'},
                {'value': 'D', 'label': 'Data-driven and specific'}
            ]
        },
        {
            'id': 'default_team_env',
            'question_text': 'What kind of team environment makes you do your best work?',
            'question_type': 'free_response',
            'options': None
        },
        {
            'id': 'default_frustrations',
            'question_text': 'What frustrates you most at work?',
            'question_type': 'free_response',
            'options': None
        }
    ]
}

# Map role_types to question sets
ROLE_TYPE_TO_QUESTION_SET = {
    'software_engineer': 'tech',
    'data_scientist': 'tech',
    'data_analyst': 'tech',
    'engineering_manager': 'tech',
    'sales': 'sales',
    # All other role types get 'default'
}


def get_questions_for_role_type(role_type):
    """Get the appropriate fit questions for a given role type."""
    question_set = ROLE_TYPE_TO_QUESTION_SET.get(role_type, 'default')
    return FIT_QUESTIONS.get(question_set, FIT_QUESTIONS['default'])


# ============================================================================
# ROLES ENDPOINTS
# ============================================================================

def parse_salary_range(salary_str):
    """Extract min/max salary from salary range string."""
    if not salary_str:
        return None, None
    # Find all numbers in the string
    numbers = re.findall(r'[\d,]+', salary_str.replace(',', ''))
    if len(numbers) >= 2:
        return int(numbers[0]), int(numbers[1])
    elif len(numbers) == 1:
        return int(numbers[0]), int(numbers[0])
    return None, None


def has_any_preferences(user_prefs):
    """Check if user has filled in any preferences."""
    if not user_prefs:
        return False
    return bool(
        user_prefs.get('preferred_locations') or
        user_prefs.get('salary_min') or
        user_prefs.get('salary_max') or
        user_prefs.get('open_to_roles') or
        user_prefs.get('experience_level') or
        user_prefs.get('work_preference')
    )


def evaluate_hard_filters(eligibility_data, job_requirements):
    """
    Evaluate hard pass/fail requirements for a candidate.

    Returns:
        {
            'passed': bool,
            'breakdown': {
                'work_authorization': bool,
                'location': bool,
                'start_date': bool,
                'seniority': bool
            },
            'failure_reason': str or None
        }
    """
    breakdown = {
        'work_authorization': True,
        'location': True,
        'start_date': True,
        'seniority': True
    }
    failure_reason = None

    # Work Authorization Check
    if job_requirements.get('requires_authorization'):
        authorized = eligibility_data.get('authorized_us')
        needs_sponsorship = eligibility_data.get('needs_sponsorship')

        # Fail if not authorized or needs sponsorship when not offered
        if not authorized:
            breakdown['work_authorization'] = False
            failure_reason = 'Not authorized to work in US'
        elif needs_sponsorship and not job_requirements.get('offers_sponsorship'):
            breakdown['work_authorization'] = False
            failure_reason = 'Needs visa sponsorship (not offered)'

    # Location/Hybrid Check
    if job_requirements.get('requires_hybrid'):
        hybrid_onsite = eligibility_data.get('hybrid_onsite')
        if hybrid_onsite == 'no' or hybrid_onsite == 'No, I need fully remote':
            breakdown['location'] = False
            failure_reason = failure_reason or 'Cannot meet hybrid/onsite requirement'

    # Start Date Check
    if job_requirements.get('latest_start_date'):
        candidate_start = eligibility_data.get('start_date')
        # Map start date options to weeks
        start_weeks = {
            'Immediately': 0,
            'Within 2 weeks': 2,
            'Within 1 month': 4,
            '1-3 months': 12,
            '3+ months': 16
        }
        candidate_weeks = start_weeks.get(candidate_start, 8)
        required_weeks = job_requirements.get('max_start_weeks', 8)

        if candidate_weeks > required_weeks:
            breakdown['start_date'] = False
            failure_reason = failure_reason or f'Start date too late ({candidate_start})'

    # Seniority Check
    if job_requirements.get('min_seniority') or job_requirements.get('max_seniority'):
        candidate_seniority = eligibility_data.get('seniority_band')
        seniority_levels = {'intern': 0, 'entry': 1, 'mid': 2, 'senior': 3, 'lead': 4, 'executive': 5}

        candidate_level = seniority_levels.get(candidate_seniority, 2)
        min_level = seniority_levels.get(job_requirements.get('min_seniority'), 0)
        max_level = seniority_levels.get(job_requirements.get('max_seniority'), 5)

        if candidate_level < min_level or candidate_level > max_level:
            breakdown['seniority'] = False
            failure_reason = failure_reason or f'Seniority level mismatch ({candidate_seniority})'

    passed = all(breakdown.values())

    return {
        'passed': passed,
        'breakdown': breakdown,
        'failure_reason': failure_reason if not passed else None
    }


def calculate_preference_score(role, user_prefs):
    """
    Calculate how well a role matches user preferences.
    Returns score 0-100 based on weighted preferences.

    Weights (when preference is set):
    - Location: 35% (most important - if user wants Boston, CA jobs should score lower)
    - Role type: 25% (second most important)
    - Salary: 15%
    - Experience: 15%
    - Work arrangement: 10% (remote/onsite/hybrid)

    Returns None if no preferences are set.
    """
    if not user_prefs or not has_any_preferences(user_prefs):
        return None

    weighted_scores = []
    total_weight = 0

    # Location match - HIGHEST WEIGHT (50%)
    if user_prefs.get('preferred_locations'):
        location_score = 0
        role_location = (role.get('location') or '').lower()

        # Extract state from job location (e.g., "Foster City, CA" -> "ca")
        job_state = None
        if ', ' in role_location:
            parts = role_location.split(', ')
            # Last part might be state abbreviation or full state name
            potential_state = parts[-1].strip()
            if len(potential_state) == 2:
                job_state = potential_state
            elif len(parts) >= 2:
                # Try second to last (e.g., "City, MA, USA")
                potential_state = parts[-2].strip() if len(parts[-2].strip()) == 2 else None
                job_state = potential_state

        # Check for exact or partial location match
        for pref_loc in user_prefs['preferred_locations']:
            pref_lower = pref_loc.lower()

            # Exact city match
            if pref_lower in role_location or role_location in pref_lower:
                location_score = 100
                break

            # Check for state abbreviation match (e.g., user selected "MA")
            if len(pref_lower) == 2 and f', {pref_lower}' in role_location:
                location_score = 100
                break

            # Extract state from user's preferred location
            pref_state = None
            if ', ' in pref_loc:
                pref_parts = pref_loc.split(', ')
                potential_pref_state = pref_parts[-1].strip().lower()
                if len(potential_pref_state) == 2:
                    pref_state = potential_pref_state

            # Same state but different city = 65% credit
            if location_score == 0 and job_state and pref_state and job_state == pref_state:
                location_score = 65
                # Don't break - keep looking for exact match

        # Remote jobs get partial credit if user didn't specifically select remote
        if location_score == 0 and 'remote' in role_location:
            location_score = 40  # Remote is ok but not as good as preferred location

        weighted_scores.append((location_score, 35))
        total_weight += 35

    # Role type match - SECOND HIGHEST (25%)
    if user_prefs.get('open_to_roles'):
        role_type_score = 0
        role_type = role.get('role_type')
        if role_type and role_type in user_prefs['open_to_roles']:
            role_type_score = 100
        elif not role_type:
            # No role type on job - check title for keywords
            title = (role.get('title') or '').lower()
            for pref_role in user_prefs['open_to_roles']:
                if pref_role.replace('_', ' ') in title:
                    role_type_score = 75
                    break
            # Give some credit if we can't determine role type
            if role_type_score == 0:
                role_type_score = 30

        weighted_scores.append((role_type_score, 25))
        total_weight += 25

    # Salary match (15%)
    if user_prefs.get('salary_min') or user_prefs.get('salary_max'):
        salary_score = 0
        role_min, role_max = parse_salary_range(role.get('salary_range'))
        user_min = user_prefs.get('salary_min') or 0
        user_max = user_prefs.get('salary_max') or 999999

        if role_min and role_max:
            # Check if ranges overlap
            if role_max >= user_min and role_min <= user_max:
                # Calculate overlap percentage
                overlap_min = max(role_min, user_min)
                overlap_max = min(role_max, user_max)
                user_range = user_max - user_min if user_max != 999999 else role_max - user_min
                if user_range > 0:
                    overlap = (overlap_max - overlap_min) / user_range
                    salary_score = min(100, int(overlap * 100) + 50)
                else:
                    salary_score = 100
        elif role.get('salary_range'):
            # Has salary but couldn't parse - give partial credit
            salary_score = 50

        weighted_scores.append((salary_score, 15))
        total_weight += 15

    # Experience level match (15%)
    if user_prefs.get('experience_level'):
        exp_score = 0
        title = (role.get('title') or '').lower()
        job_exp_level = role.get('experience_level')
        user_level = user_prefs['experience_level']

        if job_exp_level and job_exp_level == user_level:
            exp_score = 100
        else:
            level_keywords = {
                'intern': ['intern', 'internship'],
                'entry': ['entry', 'junior', 'associate', 'i ', ' i,', ' 1', 'new grad'],
                'mid': ['mid', 'ii ', ' ii,', ' 2', ' 3'],
                'senior': ['senior', 'sr', 'lead', 'principal', 'staff', 'iii', ' 4', ' 5']
            }

            # Check if job title contains keywords for user's preferred level
            for keyword in level_keywords.get(user_level, []):
                if keyword in title:
                    exp_score = 100
                    break

            # If user wants intern, non-intern jobs should score low
            if exp_score == 0 and user_level == 'intern':
                # Check if it's an entry-level job (partial credit)
                for keyword in level_keywords['entry']:
                    if keyword in title:
                        exp_score = 40  # Entry-level is somewhat close to intern
                        break
                # If still 0, it's likely mid/senior - no credit
            elif exp_score == 0 and user_level in ['entry', 'mid']:
                has_senior_keywords = any(kw in title for kw in level_keywords['senior'])
                if not has_senior_keywords:
                    exp_score = 50

        weighted_scores.append((exp_score, 15))
        total_weight += 15

    # Work arrangement match (10%)
    if user_prefs.get('work_preference'):
        work_score = 0
        role_arrangement = (role.get('work_arrangement') or '').lower()
        user_pref = user_prefs['work_preference'].lower()

        if user_pref in role_arrangement or role_arrangement in user_pref:
            work_score = 100
        elif 'hybrid' in role_arrangement and user_pref in ['remote', 'onsite', 'on-site']:
            # Hybrid is a partial match for both remote and onsite preferences
            work_score = 60
        elif role_arrangement:
            # Has arrangement but doesn't match
            work_score = 20
        else:
            # No arrangement specified on job - neutral
            work_score = 50

        weighted_scores.append((work_score, 10))
        total_weight += 10

    # Calculate weighted average
    if not weighted_scores or total_weight == 0:
        return None

    weighted_sum = sum(score * weight for score, weight in weighted_scores)
    return int(weighted_sum / total_weight)


def calculate_skills_score(role_id, user_skill_ids):
    """
    Calculate how well user's skills match job requirements.
    Returns 0-100 based on skill overlap.
    """
    if not user_skill_ids:
        return None  # No skills = no score

    conn = get_db()
    with conn.cursor() as cur:
        # Get job's required skills
        cur.execute("""
            SELECT skill_id FROM job_required_skills WHERE position_id = %s
        """, (role_id,))
        job_skill_ids = {row[0] for row in cur.fetchall()}

    if not job_skill_ids:
        return 100  # No skills required = full match

    # Calculate overlap
    matched = len(user_skill_ids & job_skill_ids)
    return int((matched / len(job_skill_ids)) * 100)


def calculate_match_score(role, user_prefs, user_skill_ids=None):
    """
    Calculate overall match score: 50% preferences + 50% skills.
    If user has no skills extracted, uses 100% preferences.
    Returns score 0-100 or None if no data.
    """
    preference_score = calculate_preference_score(role, user_prefs)
    skills_score = calculate_skills_score(role.get('id'), user_skill_ids) if user_skill_ids else None

    # If we have both scores, use 50/50 weighting
    if preference_score is not None and skills_score is not None:
        return int((preference_score * 0.5) + (skills_score * 0.5))

    # If only preferences, use that
    if preference_score is not None:
        return preference_score

    # If only skills, use that
    if skills_score is not None:
        return skills_score

    return None


# ============================================================================
# SEMANTIC MATCHING HELPERS
# ============================================================================

def cosine_similarity(a, b):
    """Calculate cosine similarity between two vectors."""
    if not NUMPY_AVAILABLE:
        # Fallback without numpy
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        return dot / (norm_a * norm_b) if norm_a and norm_b else 0
    a = np.array(a)
    b = np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def normalize_similarity(raw_similarity):
    """
    Convert raw cosine similarity to a more intuitive 0-100 scale.
    Calibrated based on observed similarity distributions for job-resume matching.

    Observed ranges for text-embedding-3-small:
    - Unrelated: 0.25-0.32
    - Somewhat related: 0.32-0.38
    - Related: 0.38-0.45
    - Very related: 0.45+
    """
    # Adjusted thresholds based on actual similarity distributions
    # These are calibrated for OpenAI text-embedding-3-small model
    min_sim = 0.28  # Below this is essentially random/unrelated
    max_sim = 0.50  # Above this is a very strong match

    # Clamp and normalize
    normalized = (raw_similarity - min_sim) / (max_sim - min_sim)
    normalized = max(0, min(1, normalized))

    # Apply slight curve to spread out middle values and reward stronger matches
    # Using power < 1 stretches lower values, making differences more visible
    score = normalized ** 0.85 * 100

    return round(score, 1)


def calculate_experience_match(user_level, job_level):
    """
    Calculate how well the user's experience level matches the job's requirements.
    Returns a multiplier (0.5 to 1.2) that adjusts the match score.
    """
    if not user_level or not job_level:
        return 1.0  # No penalty if unknown

    # Define experience level ordering
    levels = {'intern': 0, 'entry': 1, 'mid': 2, 'senior': 3}

    user_idx = levels.get(user_level.lower(), 1)
    job_idx = levels.get(job_level.lower(), 1)

    diff = job_idx - user_idx

    # Perfect match or one level difference is good
    if diff == 0:
        return 1.1  # Slight boost for exact match
    elif diff == 1:
        return 0.95  # Slightly underqualified - still good
    elif diff == -1:
        return 0.9  # Slightly overqualified - OK
    elif diff == 2:
        return 0.6  # Significantly underqualified
    elif diff == -2:
        return 0.7  # Significantly overqualified
    else:
        return 0.4  # Major mismatch


def generate_match_reason(user_profile, job, match_score, exp_multiplier):
    """
    Generate a brief, human-readable reason for why a job matches (or doesn't).
    """
    reasons = []

    # Experience level match
    user_exp = user_profile.get('experience_level') if user_profile else None
    job_exp = job.get('experience_level')

    if user_exp and job_exp:
        if user_exp == job_exp:
            reasons.append(f"Great fit for {job_exp}-level candidates")
        elif exp_multiplier >= 0.9:
            reasons.append(f"Good fit for your experience level")
        elif exp_multiplier >= 0.6:
            reasons.append(f"May require more experience than you currently have")
        else:
            reasons.append(f"Typically requires {job_exp}-level experience")

    # Skills match (if we have user skills)
    user_skills = user_profile.get('skills', []) if user_profile else []
    job_title_lower = job.get('title', '').lower()

    # Check for skill-title alignment
    tech_skills = ['python', 'javascript', 'java', 'sql', 'react', 'node', 'aws', 'machine learning', 'data']
    user_tech = [s.lower() for s in user_skills if any(t in s.lower() for t in tech_skills)]

    if user_tech and any(s in job_title_lower for s in ['engineer', 'developer', 'scientist', 'analyst']):
        if match_score >= 60:
            reasons.append("Your technical background aligns well")
        elif match_score >= 40:
            reasons.append("Some technical skill overlap")

    # Company/industry match
    user_industries = user_profile.get('industries', []) if user_profile else []
    if 'technology' in [i.lower() for i in user_industries]:
        if 'tech' in job.get('company_name', '').lower() or any(t in job_title_lower for t in ['software', 'data', 'ai', 'ml']):
            if not any('technical' in r.lower() or 'skill' in r.lower() for r in reasons):
                reasons.append("Matches your industry experience")

    # Default reasons based on score
    if not reasons:
        if match_score >= 70:
            reasons.append("Strong overall profile match")
        elif match_score >= 50:
            reasons.append("Moderate profile alignment")
        elif match_score >= 30:
            reasons.append("Some relevant background")
        else:
            reasons.append("Limited profile match")

    return reasons[0] if reasons else None


def get_semantic_matches(user_id=None, resume_embedding=None, filters=None, limit=50, user_profile=None):
    """
    Find jobs that semantically match a user's resume embedding.
    Returns list of jobs with nuanced match scores.
    """
    conn = get_db()
    extracted_profile = user_profile

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get user's embedding and profile if not provided
        if (resume_embedding is None or extracted_profile is None) and user_id:
            cur.execute("""
                SELECT resume_embedding, extracted_profile, experience_level
                FROM seeker_profiles
                WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row:
                if row.get('resume_embedding') and resume_embedding is None:
                    resume_embedding = row['resume_embedding']
                    if isinstance(resume_embedding, str):
                        resume_embedding = json.loads(resume_embedding)
                if row.get('extracted_profile') and extracted_profile is None:
                    extracted_profile = row['extracted_profile']
                    if isinstance(extracted_profile, str):
                        extracted_profile = json.loads(extracted_profile)
                # Also get user's stated experience level
                user_exp_level = row.get('experience_level')
                if extracted_profile and not extracted_profile.get('experience_level'):
                    extracted_profile['experience_level'] = user_exp_level

        if resume_embedding is None:
            return []

        # Get user's experience level from profile
        user_experience = None
        if extracted_profile:
            user_experience = extracted_profile.get('experience_level')

        # Get all jobs with embeddings
        query = """
            SELECT id, title, company_name, location, description,
                   role_type, experience_level, salary_range, salary_min, salary_max,
                   work_arrangement, description_embedding
            FROM watchable_positions
            WHERE description_embedding IS NOT NULL
        """

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

        cur.execute(query, params) if params else cur.execute(query)
        jobs = cur.fetchall()

    # Calculate similarity scores with experience level adjustment
    results = []
    for job in jobs:
        if job.get('description_embedding'):
            job_embedding = job['description_embedding']
            if isinstance(job_embedding, str):
                job_embedding = json.loads(job_embedding)

            # Calculate base semantic similarity
            raw_similarity = cosine_similarity(resume_embedding, job_embedding)
            base_score = normalize_similarity(raw_similarity)

            # Apply experience level adjustment
            exp_multiplier = calculate_experience_match(user_experience, job.get('experience_level'))
            adjusted_score = base_score * exp_multiplier

            # Cap at 98 to avoid perfect matches (nothing is perfect)
            final_score = min(98, max(5, adjusted_score))

            # Generate match reason
            match_reason = generate_match_reason(extracted_profile, job, final_score, exp_multiplier)

            results.append({
                'id': job['id'],
                'title': job['title'],
                'company_name': job['company_name'],
                'location': job['location'],
                'role_type': job['role_type'],
                'experience_level': job['experience_level'],
                'salary_range': job['salary_range'],
                'work_arrangement': job['work_arrangement'],
                'match_score': round(final_score, 0),
                'match_reason': match_reason,
                'description': (job['description'][:500] + '...') if job.get('description') and len(job['description']) > 500 else job.get('description')
            })

    # Sort by match score
    results.sort(key=lambda x: x['match_score'], reverse=True)

    return results[:limit]


def process_resume_and_get_matches(user_id, pdf_bytes):
    """
    Process a resume PDF and return matching jobs.
    Stores the extracted profile and embedding for future use.
    """
    if not OPENAI_AVAILABLE:
        return {'error': 'OpenAI not available for resume processing'}

    import PyPDF2
    import io

    # Extract text from PDF
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or '')
        resume_text = '\n'.join(text_parts)
    except Exception as e:
        return {'error': f'Could not read PDF: {str(e)}'}

    if not resume_text.strip():
        return {'error': 'Could not extract text from PDF'}

    client = OpenAIClient(api_key=os.environ.get('OPENAI_API_KEY'))

    # Extract structured profile using LLM
    profile_prompt = f"""Analyze this resume and extract a structured profile. Return JSON with these fields:

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
    "summary": "2-3 sentence summary of their background"
}}

Resume text:
{resume_text[:8000]}

Return ONLY valid JSON, no markdown."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": profile_prompt}],
            temperature=0.1,
            max_tokens=1000
        )

        result = response.choices[0].message.content.strip()
        if result.startswith('```'):
            result = re.sub(r'^```(?:json)?\n?', '', result)
            result = re.sub(r'\n?```$', '', result)

        profile = json.loads(result)
    except Exception as e:
        profile = {'error': str(e)}

    # Create embedding text
    embedding_parts = []
    if profile.get('current_title'):
        embedding_parts.append(f"Current Role: {profile['current_title']}")
    if profile.get('summary'):
        embedding_parts.append(f"Summary: {profile['summary']}")
    if profile.get('skills'):
        embedding_parts.append(f"Skills: {', '.join(profile['skills'])}")
    if profile.get('industries'):
        embedding_parts.append(f"Industries: {', '.join(profile['industries'])}")
    embedding_parts.append(f"Full Background: {resume_text[:4000]}")

    embedding_text = "\n".join(embedding_parts)

    # Generate embedding
    try:
        emb_response = client.embeddings.create(
            model="text-embedding-3-small",
            input=embedding_text[:32000]
        )
        embedding = emb_response.data[0].embedding
    except Exception as e:
        return {'error': f'Could not generate embedding: {str(e)}'}

    # Store in database
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE seeker_profiles
            SET resume_text = %s,
                extracted_profile = %s,
                resume_embedding = %s,
                skills_extracted = TRUE,
                skills_extracted_at = NOW()
            WHERE user_id = %s
        """, (resume_text, json.dumps(profile), json.dumps(embedding), user_id))
        conn.commit()

    # Get matching jobs
    matches = get_semantic_matches(resume_embedding=embedding, limit=20)

    return {
        'profile': profile,
        'matches': matches
    }


# ============================================================================
# SEMANTIC MATCHING ENDPOINTS
# ============================================================================

@app.route('/api/recommendations', methods=['GET'])
@require_auth
def get_recommendations():
    """
    Get semantically matched job recommendations for the authenticated user.
    Uses the user's resume embedding and profile to find the best matching jobs.
    Prioritizes jobs matching the user's experience level.
    """
    user_id = g.user_id
    limit = request.args.get('limit', 20, type=int)
    role_type = request.args.get('role_type')
    location = request.args.get('location')

    # Check if user has a resume embedding
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT resume_embedding, extracted_profile, skills_extracted, experience_level
            FROM seeker_profiles WHERE user_id = %s
        """, (user_id,))
        profile = cur.fetchone()

    if not profile:
        return jsonify({'error': 'Profile not found'}), 404

    if not profile.get('resume_embedding'):
        return jsonify({
            'error': 'No resume processed yet',
            'needs_resume': True,
            'message': 'Please upload your resume to get personalized recommendations'
        }), 400

    # Get user's experience level from profile or extracted data
    user_exp_level = profile.get('experience_level')
    extracted_profile = profile.get('extracted_profile')
    if isinstance(extracted_profile, str):
        extracted_profile = json.loads(extracted_profile)
    if extracted_profile and not user_exp_level:
        user_exp_level = extracted_profile.get('experience_level')

    # Build filters - prioritize experience-appropriate jobs
    filters = {}
    if role_type:
        filters['role_types'] = [role_type]

    # For interns, strongly prefer intern roles
    if user_exp_level == 'intern':
        filters['experience_levels'] = ['intern', 'entry']

    # Get semantic matches with user profile for better scoring
    matches = get_semantic_matches(
        user_id=user_id,
        resume_embedding=profile['resume_embedding'],
        filters=filters,
        limit=limit * 2,  # Get more to allow for filtering
        user_profile=extracted_profile
    )

    # If user is intern/entry level, boost intern/entry roles to top
    if user_exp_level in ['intern', 'entry']:
        intern_matches = [m for m in matches if m.get('experience_level') in ['intern', 'entry']]
        other_matches = [m for m in matches if m.get('experience_level') not in ['intern', 'entry']]
        matches = intern_matches + other_matches

    return jsonify({
        'recommendations': matches[:limit],
        'profile': extracted_profile,
        'total': len(matches[:limit])
    })


@app.route('/api/for-you', methods=['GET'])
@require_auth
def get_for_you_jobs():
    """
    Get personalized job recommendations with combined scoring:
    - 60% resume/semantic match + 40% preferences match
    - Preference weights: location 35%, role 25%, salary 15%, experience 15%, work 10%
    - Score ceiling based on preference match:
      - Poor match (<50%): capped at 75%
      - Moderate match (<70%): capped at 85%
      - Good match (<85%): capped at 92%
      - Excellent match (85%+): can reach 98%
    Returns jobs above min_score threshold, sorted by match percentage.
    """
    user_id = g.user_id
    limit = request.args.get('limit', 100, type=int)
    min_score = request.args.get('min_score', 70, type=int)

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get user's resume embedding and preferences
        cur.execute("""
            SELECT sp.resume_embedding, sp.extracted_profile, sp.experience_level,
                   sp.preferred_locations, sp.salary_min, sp.salary_max,
                   sp.open_to_roles, sp.work_preference
            FROM seeker_profiles sp
            WHERE sp.user_id = %s
        """, (user_id,))
        profile = cur.fetchone()

    if not profile:
        return jsonify({'error': 'Profile not found'}), 404

    if not profile.get('resume_embedding'):
        return jsonify({
            'error': 'No resume processed yet',
            'needs_resume': True,
            'message': 'Please upload your resume to get personalized recommendations'
        }), 400

    resume_embedding = profile['resume_embedding']
    if isinstance(resume_embedding, str):
        resume_embedding = json.loads(resume_embedding)

    extracted_profile = profile.get('extracted_profile')
    if isinstance(extracted_profile, str):
        extracted_profile = json.loads(extracted_profile)

    # Get user preferences for scoring
    # IMPORTANT: Only use explicitly set preferences, NOT extracted profile fallbacks
    # The extracted_profile experience is used for the semantic scoring multiplier,
    # but should NOT be used for preference matching unless user explicitly set it
    user_prefs = {
        'preferred_locations': profile.get('preferred_locations'),
        'salary_min': profile.get('salary_min'),
        'salary_max': profile.get('salary_max'),
        'open_to_roles': profile.get('open_to_roles'),
        'experience_level': profile.get('experience_level'),  # Only explicit user selection
        'work_preference': profile.get('work_preference')
    }

    # DEBUG: Log user preferences
    print(f"[FOR-YOU DEBUG] User {user_id} explicit preferences: {user_prefs}")

    # For semantic scoring multiplier, use extracted profile experience if not set
    user_experience = profile.get('experience_level') or (extracted_profile.get('experience_level') if extracted_profile else None)

    # Fetch all jobs with embeddings
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, title, company_name, location, description,
                   role_type, experience_level, salary_range, salary_min, salary_max,
                   work_arrangement, description_embedding
            FROM watchable_positions
            WHERE description_embedding IS NOT NULL
        """)
        jobs = cur.fetchall()

    # Calculate combined scores for each job
    results = []
    for job in jobs:
        if not job.get('description_embedding'):
            continue

        job_embedding = job['description_embedding']
        if isinstance(job_embedding, str):
            job_embedding = json.loads(job_embedding)

        # Calculate semantic similarity (65% weight when preferences are set)
        raw_similarity = cosine_similarity(resume_embedding, job_embedding)
        semantic_score = normalize_similarity(raw_similarity)

        # Apply experience level adjustment
        exp_multiplier = calculate_experience_match(user_experience, job.get('experience_level'))
        semantic_score = semantic_score * exp_multiplier

        # Calculate preference score (35% weight when preferences are set)
        pref_score = calculate_preference_score(job, user_prefs)

        # If user has preferences set, use combined scoring
        # If not, use semantic score only (don't penalize for missing preferences)
        has_preferences = any([
            user_prefs.get('preferred_locations'),
            user_prefs.get('open_to_roles'),
            user_prefs.get('salary_min'),
            user_prefs.get('experience_level'),
            user_prefs.get('work_preference')
        ])

        if has_preferences and pref_score is not None:
            # Combined score with preference-based ceiling
            # Base: 60% semantic/resume + 40% preferences
            base_score = (semantic_score * 0.60) + (pref_score * 0.40)

            # Apply a ceiling based on preference score
            # If preferences don't match well, cap the maximum score
            # This ensures 90%+ requires strong preference match
            if pref_score < 50:
                # Poor preference match - cap at 75%
                max_allowed = 75
            elif pref_score < 70:
                # Moderate preference match - cap at 85%
                max_allowed = 85
            elif pref_score < 85:
                # Good preference match - cap at 92%
                max_allowed = 92
            else:
                # Excellent preference match - can reach 98%
                max_allowed = 98

            combined_score = min(base_score, max_allowed)

            # DEBUG: Log scoring for jobs in non-Boston locations
            job_loc = (job.get('location') or '').lower()
            if 'boston' not in job_loc and 'ma' not in job_loc and combined_score >= 90:
                print(f"[SCORING DEBUG] High score for non-Boston job: {job['title'][:40]} @ {job_loc}")
                print(f"  semantic={semantic_score:.1f}, pref={pref_score}, base={base_score:.1f}, max_allowed={max_allowed}, final={combined_score:.1f}")
        else:
            # No preferences set - use semantic score directly
            combined_score = semantic_score
            print(f"[SCORING DEBUG] No preferences detected! has_preferences={has_preferences}, pref_score={pref_score}")

        combined_score = min(98, max(5, combined_score))  # Final bounds

        # Only include jobs with 70%+ match
        if combined_score >= min_score:
            # Generate match reason
            match_reason = generate_match_reason(extracted_profile, job, combined_score, exp_multiplier)

            results.append({
                'id': job['id'],
                'title': job['title'],
                'company_name': job['company_name'],
                'location': job['location'],
                'role_type': job['role_type'],
                'experience_level': job['experience_level'],
                'salary_range': job['salary_range'],
                'work_arrangement': job['work_arrangement'],
                'match_score': round(combined_score, 0),
                'semantic_score': round(semantic_score, 0),
                'preference_score': round(pref_score, 0) if pref_score is not None else None,
                'match_reason': match_reason,
                'description': (job['description'][:500] + '...') if job.get('description') and len(job['description']) > 500 else job.get('description')
            })

    # Sort by combined match score descending
    results.sort(key=lambda x: x['match_score'], reverse=True)

    return jsonify({
        'jobs': results[:limit],
        'total': len(results),
        'profile': extracted_profile,
        'min_score_used': min_score
    })


@app.route('/api/process-resume', methods=['POST'])
@require_auth
def process_resume():
    """
    Process an uploaded resume and return matching jobs.
    Extracts profile info, generates embedding, and returns top matches.
    """
    user_id = g.user_id

    # Check for file upload
    if 'resume' not in request.files:
        return jsonify({'error': 'No resume file provided'}), 400

    file = request.files['resume']
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400

    # Check file extension
    allowed_extensions = {'pdf'}
    ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
    if ext not in allowed_extensions:
        return jsonify({'error': 'Only PDF files are supported'}), 400

    # Read file contents
    pdf_bytes = file.read()

    # Process resume and get matches
    result = process_resume_and_get_matches(user_id, pdf_bytes)

    if 'error' in result:
        return jsonify({'error': result['error']}), 500

    # Also save the resume file
    filename = secure_filename(f"resume_{user_id}_{int(datetime.utcnow().timestamp())}.pdf")
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    with open(filepath, 'wb') as f:
        f.seek(0)
        file.seek(0)
        f.write(pdf_bytes)

    # Update user's resume path
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE platform_users SET resume_path = %s WHERE id = %s
        """, (filename, user_id))
        conn.commit()

    return jsonify({
        'success': True,
        'profile': result.get('profile'),
        'recommendations': result.get('matches', []),
        'message': 'Resume processed successfully'
    })


@app.route('/api/semantic-search', methods=['POST'])
def semantic_search():
    """
    Search for jobs using a text query with semantic matching.
    Useful for searching with natural language queries.
    """
    if not OPENAI_AVAILABLE:
        return jsonify({'error': 'Semantic search not available'}), 503

    data = request.json or {}
    query = data.get('query', '').strip()
    limit = data.get('limit', 20)
    role_type = data.get('role_type')

    if not query:
        return jsonify({'error': 'Search query required'}), 400

    # Generate embedding for the search query
    try:
        client = OpenAIClient()
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=query[:8000]
        )
        query_embedding = response.data[0].embedding
    except Exception as e:
        return jsonify({'error': f'Could not process query: {str(e)}'}), 500

    # Build filters
    filters = {}
    if role_type:
        filters['role_type'] = role_type

    # Get matches
    matches = get_semantic_matches(
        resume_embedding=query_embedding,
        filters=filters,
        limit=limit
    )

    return jsonify({
        'results': matches,
        'query': query,
        'total': len(matches)
    })


@app.route('/api/roles', methods=['GET'])
def get_roles():
    """
    Get roles in Boston/Cambridge area with match scores.
    Filters: role_type, search, location, salary_min, salary_max, experience_level
    """
    role_type = request.args.get('role_type')
    search = request.args.get('search', '').strip()
    location_filter = request.args.get('location', '').strip()
    salary_min_filter = request.args.get('salary_min', type=int)
    salary_max_filter = request.args.get('salary_max', type=int)
    exp_level_filter = request.args.get('experience_level', '').strip()
    work_arrangement_filter = request.args.get('work_arrangement', '').strip()

    # Get user preferences and skills if authenticated
    user_prefs = None
    user_skill_ids = None
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        try:
            token = auth_header.split(' ')[1]
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = payload.get('user_id')
            if user_id:
                conn = get_db()
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Get preferences
                    cur.execute("""
                        SELECT preferred_locations, salary_min, salary_max,
                               open_to_roles, experience_level, work_preference
                        FROM seeker_profiles WHERE user_id = %s
                    """, (user_id,))
                    user_prefs = cur.fetchone()

                    # Get user's extracted skills
                    cur.execute("""
                        SELECT skill_id FROM candidate_skills WHERE user_id = %s
                    """, (user_id,))
                    user_skill_ids = {row['skill_id'] for row in cur.fetchall()}
        except:
            pass  # Invalid token, continue without preferences

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT
                wp.id, wp.title, wp.company_name, wp.location, wp.department,
                wp.status, wp.salary_range, wp.role_type,
                (SELECT COUNT(*) FROM shortlist_applications sa WHERE sa.position_id = wp.id) as applicant_count
            FROM watchable_positions wp
            WHERE (wp.location ILIKE '%%boston%%' OR wp.location ILIKE '%%cambridge%%' OR wp.location ILIKE '%%massachusetts%%' OR wp.location ILIKE '%%, MA%%')
        """
        params = []

        if role_type:
            query += " AND wp.role_type = %s"
            params.append(role_type)

        if search:
            query += " AND (wp.title ILIKE %s OR wp.company_name ILIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])

        if location_filter:
            query += " AND wp.location ILIKE %s"
            params.append(f'%{location_filter}%')

        if exp_level_filter:
            # Filter by experience_level column
            query += " AND wp.experience_level = %s"
            params.append(exp_level_filter)

        if work_arrangement_filter:
            query += " AND wp.work_arrangement = %s"
            params.append(work_arrangement_filter)

        query += " ORDER BY wp.salary_range IS NOT NULL DESC, wp.status = 'open' DESC, wp.company_name ASC LIMIT 200"

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        roles = [dict(r) for r in cur.fetchall()]

        # Calculate match scores and filter by salary if needed
        filtered_roles = []
        for role in roles:
            # Filter by salary range if specified
            if salary_min_filter or salary_max_filter:
                role_min, role_max = parse_salary_range(role.get('salary_range'))
                if role_min is None and role_max is None:
                    continue  # Skip roles without salary info when filtering by salary
                if salary_min_filter and role_max and role_max < salary_min_filter:
                    continue
                if salary_max_filter and role_min and role_min > salary_max_filter:
                    continue

            # Calculate match score (50% preferences + 50% skills if user has skills)
            role['match_score'] = calculate_match_score(role, user_prefs, user_skill_ids)
            filtered_roles.append(role)

        # Sort by status (open first), salary presence, then company name
        # Note: Match scores are still calculated for use in role detail view
        filtered_roles.sort(key=lambda r: (
            r.get('status') == 'open',
            r.get('salary_range') is not None,
            r.get('company_name') or ''
        ), reverse=True)

        return jsonify({'roles': filtered_roles[:100]})


@app.route('/api/roles/<int:role_id>', methods=['GET'])
def get_role(role_id):
    """Get single role details with match score if authenticated."""
    # Get user preferences if authenticated
    user_prefs = None
    user_skill_ids = None
    user_embedding = None
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        try:
            token = auth_header.split(' ')[1]
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = payload.get('user_id')
            if user_id:
                conn = get_db()
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Get preferences and embedding
                    cur.execute("""
                        SELECT preferred_locations, salary_min, salary_max,
                               open_to_roles, experience_level, work_preference,
                               resume_embedding
                        FROM seeker_profiles WHERE user_id = %s
                    """, (user_id,))
                    profile = cur.fetchone()
                    if profile:
                        user_prefs = {
                            'preferred_locations': profile.get('preferred_locations'),
                            'salary_min': profile.get('salary_min'),
                            'salary_max': profile.get('salary_max'),
                            'open_to_roles': profile.get('open_to_roles'),
                            'experience_level': profile.get('experience_level'),
                            'work_preference': profile.get('work_preference')
                        }
                        user_embedding = profile.get('resume_embedding')

                    # Get user's extracted skills
                    cur.execute("""
                        SELECT skill_id FROM candidate_skills WHERE user_id = %s
                    """, (user_id,))
                    user_skill_ids = {row['skill_id'] for row in cur.fetchall()}
        except:
            pass  # Invalid token, continue without preferences

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                wp.id, wp.title, wp.company_name, wp.location, wp.department,
                wp.status, wp.salary_range, wp.salary_min, wp.salary_max,
                wp.description, wp.role_type, wp.experience_level,
                wp.description_embedding as embedding,
                (SELECT COUNT(*) FROM shortlist_applications sa WHERE sa.position_id = wp.id) as applicant_count
            FROM watchable_positions wp
            WHERE wp.id = %s
        """, (role_id,))
        role = cur.fetchone()

        if not role:
            return jsonify({'error': 'Role not found'}), 404

        role = dict(role)

        # Calculate match score if user is authenticated
        if user_prefs or user_embedding:
            # Use the same scoring logic as for-you page
            has_preferences = any([
                user_prefs.get('preferred_locations') if user_prefs else None,
                user_prefs.get('open_to_roles') if user_prefs else None,
                user_prefs.get('salary_min') if user_prefs else None,
                user_prefs.get('experience_level') if user_prefs else None,
                user_prefs.get('work_preference') if user_prefs else None
            ])

            pref_score = None
            if has_preferences and user_prefs:
                pref_score = calculate_preference_score(role, user_prefs)

            semantic_score = 0
            if user_embedding and role.get('embedding'):
                try:
                    import json
                    user_emb = json.loads(user_embedding) if isinstance(user_embedding, str) else user_embedding
                    role_emb = json.loads(role['embedding']) if isinstance(role['embedding'], str) else role['embedding']
                    semantic_score = cosine_similarity(user_emb, role_emb) * 100
                except:
                    pass

            # Combined scoring with preference-based ceiling
            if has_preferences and pref_score is not None:
                base_score = (semantic_score * 0.60) + (pref_score * 0.40)
                if pref_score < 50:
                    max_allowed = 75
                elif pref_score < 70:
                    max_allowed = 85
                elif pref_score < 85:
                    max_allowed = 92
                else:
                    max_allowed = 98
                role['match_score'] = round(min(base_score, max_allowed))
            elif semantic_score > 0:
                role['match_score'] = round(min(semantic_score, 85))
            else:
                role['match_score'] = None

        # Remove embedding from response (large)
        role.pop('embedding', None)

        return jsonify({'role': role})


# ============================================================================
# SHORTLIST ENDPOINTS
# ============================================================================

@app.route('/api/shortlist/prepare/<int:role_id>', methods=['GET'])
@require_auth
def prepare_application(role_id):
    """Prepare application data without creating an application. Returns fit questions and checks eligibility."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check profile is complete and get user's saved resume
        cur.execute("""
            SELECT sp.profile_complete, sp.experience_level, sp.work_preference, pu.resume_path
            FROM seeker_profiles sp
            JOIN platform_users pu ON pu.id = sp.user_id
            WHERE sp.user_id = %s
        """, (g.user_id,))
        profile = cur.fetchone()

        if not profile or not profile['profile_complete']:
            return jsonify({'error': 'Please complete your profile first', 'code': 'INCOMPLETE_PROFILE'}), 400

        # Check if already applied
        cur.execute("""
            SELECT id FROM shortlist_applications
            WHERE user_id = %s AND position_id = %s
        """, (g.user_id, role_id))
        if cur.fetchone():
            return jsonify({'error': 'Already on this shortlist'}), 409

        # Check role exists and get role_type for questions
        cur.execute("SELECT id, status, role_type FROM watchable_positions WHERE id = %s", (role_id,))
        role = cur.fetchone()
        if not role:
            return jsonify({'error': 'Role not found'}), 404

        # Get fit questions for this role type
        questions = get_questions_for_role_type(role.get('role_type'))

        # Get user's saved resume path (if any)
        user_resume = profile.get('resume_path')

        return jsonify({
            'success': True,
            'has_resume': bool(user_resume),
            'questions': questions,
            'role_type': role.get('role_type', 'other')
        })


@app.route('/api/shortlist/apply', methods=['POST'])
@require_auth
def apply_to_shortlist():
    """Join the shortlist for a role. Requires complete profile. Creates the application."""
    data = request.json
    role_id = data.get('role_id')

    if not role_id:
        return jsonify({'error': 'Role ID required'}), 400

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check profile is complete and get user's saved resume
        cur.execute("""
            SELECT sp.profile_complete, sp.experience_level, sp.work_preference, pu.resume_path
            FROM seeker_profiles sp
            JOIN platform_users pu ON pu.id = sp.user_id
            WHERE sp.user_id = %s
        """, (g.user_id,))
        profile = cur.fetchone()

        if not profile or not profile['profile_complete']:
            return jsonify({'error': 'Please complete your profile first', 'code': 'INCOMPLETE_PROFILE'}), 400

        # Check if already applied
        cur.execute("""
            SELECT id FROM shortlist_applications
            WHERE user_id = %s AND position_id = %s
        """, (g.user_id, role_id))
        if cur.fetchone():
            return jsonify({'error': 'Already on this shortlist'}), 409

        # Check role exists and get role_type for questions
        cur.execute("SELECT id, status, role_type FROM watchable_positions WHERE id = %s", (role_id,))
        role = cur.fetchone()
        if not role:
            return jsonify({'error': 'Role not found'}), 404

        # Get user's saved resume path (if any)
        user_resume = profile.get('resume_path')

        # Create application with status 'questions_pending' - will change to 'submitted' after fit questions
        cur.execute("""
            INSERT INTO shortlist_applications
            (user_id, position_id, experience_level, work_preference, resume_path, status, applied_at)
            VALUES (%s, %s, %s, %s, %s, 'questions_pending', NOW())
            RETURNING id
        """, (g.user_id, role_id, profile['experience_level'], profile['work_preference'], user_resume))

        app_id = cur.fetchone()['id']
        conn.commit()

        return jsonify({
            'success': True,
            'application_id': app_id,
            'has_resume': bool(user_resume),
            'role_type': role.get('role_type', 'other')
        })


@app.route('/api/shortlist/upload-resume/<int:application_id>', methods=['POST'])
@require_auth
def upload_resume(application_id):
    """Upload resume for a shortlist application."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400

    # Validate file type
    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files are accepted'}), 400

    conn = get_db()
    with conn.cursor() as cur:
        # Verify application belongs to user
        cur.execute("""
            SELECT id FROM shortlist_applications
            WHERE id = %s AND user_id = %s
        """, (application_id, g.user_id))
        if not cur.fetchone():
            return jsonify({'error': 'Application not found'}), 404

        # Save file with user-specific name (so it can be reused)
        filename = secure_filename(f"{g.user_id}_resume.pdf")
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)

        # Update application with resume path
        cur.execute("""
            UPDATE shortlist_applications
            SET resume_path = %s, status = 'submitted'
            WHERE id = %s
        """, (filepath, application_id))

        # Also save to user's profile for future applications
        cur.execute("""
            UPDATE platform_users
            SET resume_path = %s
            WHERE id = %s
        """, (filepath, g.user_id))

        conn.commit()

    return jsonify({'success': True, 'filename': filename})


@app.route('/api/shortlist/submit-fit-responses/<int:application_id>', methods=['POST'])
@require_auth
def submit_fit_responses(application_id):
    """Submit fit question responses for an application."""
    data = request.json
    responses = data.get('responses', [])

    if not responses:
        return jsonify({'error': 'No responses provided'}), 400

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Verify application belongs to user and is in questions_pending status
        cur.execute("""
            SELECT id, status, resume_path FROM shortlist_applications
            WHERE id = %s AND user_id = %s
        """, (application_id, g.user_id))
        app = cur.fetchone()

        if not app:
            return jsonify({'error': 'Application not found'}), 404

        if app['status'] != 'questions_pending':
            return jsonify({'error': 'Questions already submitted for this application'}), 400

        # Store each response
        for response in responses:
            question_id = response.get('question_id')
            response_value = response.get('response_value')  # A, B, C, D for MC
            response_text = response.get('response_text')  # For free response

            if not question_id:
                continue

            # Insert or update response
            cur.execute("""
                INSERT INTO application_fit_responses
                (application_id, question_id, response_value, response_text)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (application_id, question_id)
                DO UPDATE SET response_value = EXCLUDED.response_value,
                              response_text = EXCLUDED.response_text
            """, (application_id, question_id, response_value, response_text))

        # Update application status based on whether user has resume
        has_resume = bool(app.get('resume_path'))
        new_status = 'submitted' if has_resume else 'pending'

        cur.execute("""
            UPDATE shortlist_applications
            SET status = %s
            WHERE id = %s
        """, (new_status, application_id))

        conn.commit()

        return jsonify({
            'success': True,
            'needs_resume': not has_resume
        })


@app.route('/api/shortlist/submit-eligibility/<int:application_id>', methods=['POST'])
@require_auth
def submit_eligibility(application_id):
    """Submit eligibility data for an application (Step 1 of new flow)."""
    data = request.json

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Verify application belongs to user and get job requirements
        cur.execute("""
            SELECT sa.id, sa.position_id, wp.hard_requirements
            FROM shortlist_applications sa
            JOIN watchable_positions wp ON wp.id = sa.position_id
            WHERE sa.id = %s AND sa.user_id = %s
        """, (application_id, g.user_id))
        app = cur.fetchone()
        if not app:
            return jsonify({'error': 'Application not found'}), 404

        # Store eligibility data as JSON in the application
        eligibility_data = {
            'authorized_us': data.get('authorized_us'),
            'needs_sponsorship': data.get('needs_sponsorship'),
            'hybrid_onsite': data.get('hybrid_onsite'),
            'commute_tolerance': data.get('commute_tolerance'),
            'start_date': data.get('start_date'),
            'seniority_band': data.get('seniority_band'),
            'must_have_skills': data.get('must_have_skills', []),
            'portfolio_link': data.get('portfolio_link')
        }

        # Evaluate hard filters
        hard_filter_result = evaluate_hard_filters(eligibility_data, app.get('hard_requirements') or {})
        hard_filter_failed = not hard_filter_result['passed']

        import json
        cur.execute("""
            UPDATE shortlist_applications
            SET eligibility_data = %s, hard_filter_failed = %s
            WHERE id = %s
        """, (json.dumps(eligibility_data), hard_filter_failed, application_id))

        conn.commit()

        return jsonify({'success': True, 'hard_filter_failed': hard_filter_failed})


@app.route('/api/shortlist/my-applications', methods=['GET'])
@require_auth
def get_my_applications():
    """Get all shortlist applications for current user."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                sa.id, sa.status, sa.applied_at, sa.resume_path,
                COALESCE(sa.interview_status, 'pending') as interview_status,
                wp.id as role_id, wp.title, wp.company_name, wp.location,
                wp.status as role_status
            FROM shortlist_applications sa
            JOIN watchable_positions wp ON wp.id = sa.position_id
            WHERE sa.user_id = %s
            ORDER BY sa.applied_at DESC
        """, (g.user_id,))
        applications = cur.fetchall()

        return jsonify({'applications': applications})


# ============================================================================
# EMPLOYER ENDPOINTS (Minimal)
# ============================================================================

@app.route('/api/employer/roles', methods=['GET'])
@require_auth
def get_employer_roles():
    """Get roles for employer (for now, return all roles they can see)."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # For MVP, just show roles with applications
        cur.execute("""
            SELECT
                wp.id, wp.title, wp.company_name, wp.status,
                COUNT(sa.id) as applicant_count
            FROM watchable_positions wp
            LEFT JOIN shortlist_applications sa ON sa.position_id = wp.id
            GROUP BY wp.id
            HAVING COUNT(sa.id) > 0
            ORDER BY COUNT(sa.id) DESC
            LIMIT 50
        """)
        roles = cur.fetchall()

        return jsonify({'roles': roles})


@app.route('/api/employer/roles/<int:role_id>/applicants', methods=['GET'])
@require_auth
def get_role_applicants(role_id):
    """Get all applicants for a role, including their fit responses."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get the role's role_type to include question definitions
        cur.execute("SELECT role_type FROM watchable_positions WHERE id = %s", (role_id,))
        role = cur.fetchone()
        role_type = role.get('role_type') if role else None

        # Get questions for this role type (to include question text in responses)
        questions = get_questions_for_role_type(role_type)
        questions_by_id = {q['id']: q for q in questions}

        cur.execute("""
            SELECT
                sa.id as application_id, sa.status, sa.applied_at, sa.resume_path,
                sa.experience_level, sa.work_preference,
                u.email, TRIM(CONCAT(u.first_name, ' ', u.last_name)) as full_name
            FROM shortlist_applications sa
            JOIN platform_users u ON u.id = sa.user_id
            WHERE sa.position_id = %s
            ORDER BY sa.applied_at DESC
        """, (role_id,))
        applicants = [dict(row) for row in cur.fetchall()]

        # Get fit responses for all applicants
        app_ids = [a['application_id'] for a in applicants]
        if app_ids:
            cur.execute("""
                SELECT application_id, question_id, response_value, response_text
                FROM application_fit_responses
                WHERE application_id = ANY(%s)
            """, (app_ids,))
            all_responses = cur.fetchall()

            # Group responses by application
            responses_by_app = {}
            for r in all_responses:
                app_id = r['application_id']
                if app_id not in responses_by_app:
                    responses_by_app[app_id] = []

                # Enrich response with question details
                q = questions_by_id.get(r['question_id'], {})
                response_data = {
                    'question_id': r['question_id'],
                    'question_text': q.get('question_text', ''),
                    'question_type': q.get('question_type', ''),
                    'response_value': r['response_value'],
                    'response_text': r['response_text']
                }

                # For multiple choice, include the selected option label
                if q.get('options') and r['response_value']:
                    for opt in q['options']:
                        if opt['value'] == r['response_value']:
                            response_data['response_label'] = opt['label']
                            break

                responses_by_app[app_id].append(response_data)

            # Attach responses to applicants
            for applicant in applicants:
                applicant['fit_responses'] = responses_by_app.get(applicant['application_id'], [])
        else:
            for applicant in applicants:
                applicant['fit_responses'] = []

        return jsonify({'applicants': applicants})


def get_user_from_token_param():
    """Get user_id from query param token (for iframe/download links)."""
    token = request.args.get('token')
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return payload.get('user_id')
    except:
        return None


@app.route('/api/employer/download-resume/<int:application_id>', methods=['GET'])
def download_resume(application_id):
    """Download a candidate's resume."""
    # Check auth from header or query param
    user_id = None
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        try:
            token = auth_header.split(' ')[1]
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = payload.get('user_id')
        except:
            pass

    if not user_id:
        user_id = get_user_from_token_param()

    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT resume_path FROM shortlist_applications WHERE id = %s
        """, (application_id,))
        app = cur.fetchone()

        if not app or not app['resume_path']:
            return jsonify({'error': 'Resume not found'}), 404

        from flask import send_file
        # Construct full path - resume_path may be just filename or include uploads/
        resume_path = app['resume_path']
        if not os.path.isabs(resume_path):
            uploads_path = os.path.join(os.path.dirname(__file__), 'uploads', resume_path)
            if os.path.exists(uploads_path):
                resume_path = uploads_path
            else:
                resume_path = os.path.join(os.path.dirname(__file__), resume_path)

        if not os.path.exists(resume_path):
            return jsonify({'error': 'Resume file not found'}), 404

        return send_file(resume_path, as_attachment=True)


@app.route('/api/employer/view-resume/<int:application_id>', methods=['GET'])
def view_resume(application_id):
    """View a candidate's resume inline (for PDF embedding)."""
    # Check auth from header or query param
    user_id = None
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        try:
            token = auth_header.split(' ')[1]
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = payload.get('user_id')
        except:
            pass

    if not user_id:
        user_id = get_user_from_token_param()

    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT resume_path FROM shortlist_applications WHERE id = %s
        """, (application_id,))
        app = cur.fetchone()

        if not app or not app['resume_path']:
            return jsonify({'error': 'Resume not found'}), 404

        from flask import send_file
        # Construct full path - resume_path may be just filename or include uploads/
        resume_path = app['resume_path']
        if not os.path.isabs(resume_path):
            # Check if it's in uploads directory
            uploads_path = os.path.join(os.path.dirname(__file__), 'uploads', resume_path)
            if os.path.exists(uploads_path):
                resume_path = uploads_path
            else:
                # Try as relative to backend directory
                resume_path = os.path.join(os.path.dirname(__file__), resume_path)

        if not os.path.exists(resume_path):
            return jsonify({'error': 'Resume file not found'}), 404

        return send_file(
            resume_path,
            mimetype='application/pdf',
            as_attachment=False
        )


# ============================================================================
# PREMIUM EMPLOYER ENDPOINTS (Ranked Inbox & Candidate Detail)
# ============================================================================

@app.route('/api/employer/roles/<int:role_id>/applicants/ranked', methods=['GET'])
@require_auth
def get_ranked_applicants(role_id):
    """
    Get ranked applicants with full scoring breakdown.
    Supports filtering and pagination.
    """
    # Query params
    min_score = request.args.get('min_score', 70, type=int)
    include_hidden = request.args.get('include_hidden', 'false').lower() == 'true'
    seniority_filter = request.args.getlist('seniority')
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get job details
        cur.execute("""
            SELECT id, title, company_name, role_type, hard_requirements,
                   must_have_skills, nice_to_have_skills, experience_level
            FROM watchable_positions WHERE id = %s
        """, (role_id,))
        job = cur.fetchone()

        if not job:
            return jsonify({'error': 'Role not found'}), 404

        # Build query for applicants with scores and insights
        query = """
            SELECT
                sa.id as application_id,
                sa.status,
                sa.applied_at,
                sa.resume_path,
                sa.fit_score,
                sa.confidence_level,
                sa.hard_filter_failed,
                sa.interview_status,
                u.email,
                TRIM(CONCAT(u.first_name, ' ', u.last_name)) as full_name,
                sp.experience_level,
                sp.work_preference,
                ci.why_this_person,
                ci.matched_skill_chips,
                ci.strengths,
                ci.risks,
                cfs.hard_filter_breakdown,
                cfs.score_breakdown
            FROM shortlist_applications sa
            JOIN platform_users u ON u.id = sa.user_id
            LEFT JOIN seeker_profiles sp ON sp.user_id = sa.user_id
            LEFT JOIN candidate_insights ci ON ci.application_id = sa.id
            LEFT JOIN candidate_fit_scores cfs ON cfs.application_id = sa.id
            WHERE sa.position_id = %s AND sa.status != 'cancelled'
        """
        params = [role_id]

        if not include_hidden:
            query += " AND (sa.fit_score >= %s OR sa.fit_score IS NULL) AND sa.hard_filter_failed = FALSE"
            params.append(min_score)

        if seniority_filter:
            query += " AND sp.experience_level = ANY(%s)"
            params.append(seniority_filter)

        query += " ORDER BY sa.fit_score DESC NULLS LAST, sa.applied_at DESC"
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cur.execute(query, params)
        applicants = []
        for row in cur.fetchall():
            applicant = dict(row)
            # Parse JSON fields
            if applicant.get('matched_skill_chips'):
                try:
                    applicant['matched_skill_chips'] = json.loads(applicant['matched_skill_chips']) if isinstance(applicant['matched_skill_chips'], str) else applicant['matched_skill_chips']
                except:
                    applicant['matched_skill_chips'] = []
            if applicant.get('strengths'):
                try:
                    applicant['strengths'] = json.loads(applicant['strengths']) if isinstance(applicant['strengths'], str) else applicant['strengths']
                except:
                    applicant['strengths'] = []
            if applicant.get('risks'):
                try:
                    applicant['risks'] = json.loads(applicant['risks']) if isinstance(applicant['risks'], str) else applicant['risks']
                except:
                    applicant['risks'] = []
            if applicant.get('hard_filter_breakdown'):
                try:
                    applicant['hard_filter_breakdown'] = json.loads(applicant['hard_filter_breakdown']) if isinstance(applicant['hard_filter_breakdown'], str) else applicant['hard_filter_breakdown']
                except:
                    applicant['hard_filter_breakdown'] = {}
            applicants.append(applicant)

        # Get hidden count
        cur.execute("""
            SELECT COUNT(*) as hidden_count
            FROM shortlist_applications
            WHERE position_id = %s AND (fit_score < %s OR hard_filter_failed = TRUE)
            AND status != 'cancelled'
        """, (role_id, min_score))
        hidden_count = cur.fetchone()['hidden_count']

        # Get total count
        cur.execute("""
            SELECT COUNT(*) as total_count
            FROM shortlist_applications
            WHERE position_id = %s AND status != 'cancelled'
        """, (role_id,))
        total_count = cur.fetchone()['total_count']

    return jsonify({
        'job': dict(job) if job else None,
        'applicants': applicants,
        'hidden_count': hidden_count,
        'total_count': total_count,
        'filters_applied': {
            'min_score': min_score,
            'seniority': seniority_filter,
            'include_hidden': include_hidden
        }
    })


@app.route('/api/employer/applicants/<int:application_id>/detail', methods=['GET'])
@require_auth
def get_applicant_detail(application_id):
    """
    Get full candidate detail for side drawer view.
    Includes profile package, insights, and materials.
    """
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get application with all related data
        cur.execute("""
            SELECT
                sa.*,
                u.email,
                u.first_name,
                u.last_name,
                TRIM(CONCAT(u.first_name, ' ', u.last_name)) as full_name,
                sp.resume_text,
                sp.extracted_profile,
                sp.experience_level as profile_experience_level,
                sp.work_preference as profile_work_preference,
                wp.title as job_title,
                wp.company_name,
                wp.role_type
            FROM shortlist_applications sa
            JOIN platform_users u ON u.id = sa.user_id
            LEFT JOIN seeker_profiles sp ON sp.user_id = sa.user_id
            JOIN watchable_positions wp ON wp.id = sa.position_id
            WHERE sa.id = %s
        """, (application_id,))
        application = cur.fetchone()

        if not application:
            return jsonify({'error': 'Application not found'}), 404

        # Get fit score breakdown
        cur.execute("""
            SELECT * FROM candidate_fit_scores WHERE application_id = %s
        """, (application_id,))
        score_data = cur.fetchone()

        # Get AI insights
        cur.execute("""
            SELECT * FROM candidate_insights WHERE application_id = %s
        """, (application_id,))
        insights = cur.fetchone()

        # Get fit responses
        cur.execute("""
            SELECT question_id, response_value, response_text
            FROM application_fit_responses
            WHERE application_id = %s
        """, (application_id,))
        fit_responses = cur.fetchall()

    # Enrich fit responses with question text
    role_type = application.get('role_type')
    questions = get_questions_for_role_type(role_type)
    questions_by_id = {q['id']: q for q in questions}

    enriched_responses = []
    for r in fit_responses:
        q = questions_by_id.get(r['question_id'], {})
        enriched_responses.append({
            'question_id': r['question_id'],
            'question_text': q.get('question_text', ''),
            'question_type': q.get('question_type', ''),
            'response_value': r['response_value'],
            'response_text': r['response_text'],
            'response_label': next(
                (opt['label'] for opt in (q.get('options') or [])
                 if opt['value'] == r['response_value']),
                None
            )
        })

    # Parse JSON fields in insights
    if insights:
        insights = dict(insights)
        for field in ['strengths', 'risks', 'suggested_questions', 'matched_skill_chips', 'interview_highlights']:
            if insights.get(field):
                try:
                    insights[field] = json.loads(insights[field]) if isinstance(insights[field], str) else insights[field]
                except:
                    insights[field] = []

    # Parse JSON fields in score_data
    if score_data:
        score_data = dict(score_data)
        for field in ['hard_filter_breakdown', 'deductions', 'score_breakdown']:
            if score_data.get(field):
                try:
                    score_data[field] = json.loads(score_data[field]) if isinstance(score_data[field], str) else score_data[field]
                except:
                    score_data[field] = {}

    # Parse interview transcript
    interview_transcript = application.get('interview_transcript')
    if interview_transcript and isinstance(interview_transcript, str):
        try:
            interview_transcript = json.loads(interview_transcript)
        except:
            interview_transcript = []

    return jsonify({
        'application': {
            'application_id': application['id'],
            'status': application['status'],
            'applied_at': application['applied_at'].isoformat() if application.get('applied_at') else None,
            'resume_path': application.get('resume_path'),
            'fit_score': application.get('fit_score'),
            'confidence_level': application.get('confidence_level'),
            'hard_filter_failed': application.get('hard_filter_failed'),
            'interview_status': application.get('interview_status'),
            'interview_transcript': interview_transcript,
            'email': application['email'],
            'first_name': application.get('first_name'),
            'last_name': application.get('last_name'),
            'full_name': application['full_name'],
            'job_title': application.get('job_title'),
            'company_name': application.get('company_name'),
            'experience_level': application.get('profile_experience_level'),
            'work_preference': application.get('profile_work_preference')
        },
        'score_breakdown': score_data,
        'insights': insights,
        'fit_responses': enriched_responses,
        'materials': {
            'has_resume': bool(application.get('resume_path')),
            'has_interview': application.get('interview_status') == 'completed',
            'interview_completed_at': application['interview_completed_at'].isoformat() if application.get('interview_completed_at') else None
        }
    })


@app.route('/api/employer/applicants/<int:application_id>/regenerate-insights', methods=['POST'])
@require_auth
def regenerate_applicant_insights(application_id):
    """
    Regenerate AI insights for a candidate.
    Called after interview completion or on-demand.
    """
    from insights_generator import generate_and_store_insights

    conn = get_db()

    try:
        insights = generate_and_store_insights(conn, application_id)
        if insights:
            return jsonify({'success': True, 'insights': insights})
        else:
            return jsonify({'error': 'Failed to generate insights'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/employer/applicants/<int:application_id>/recalculate-score', methods=['POST'])
@require_auth
def recalculate_applicant_score(application_id):
    """
    Recalculate fit score for a candidate.
    Useful after data updates.
    """
    from scoring_engine import calculate_and_store_fit_score

    conn = get_db()

    try:
        result = calculate_and_store_fit_score(conn, application_id)
        if result:
            return jsonify({'success': True, 'score': result.overall_score, 'confidence': result.confidence})
        else:
            return jsonify({'error': 'Failed to calculate score'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# COMPANIES ENDPOINT
# ============================================================================

@app.route('/api/companies', methods=['GET'])
def get_companies():
    """Get list of companies for employer signup dropdown."""
    search = request.args.get('search', '').strip()

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if search:
            cur.execute("""
                SELECT DISTINCT company_name
                FROM watchable_positions
                WHERE company_name IS NOT NULL
                  AND company_name ILIKE %s
                ORDER BY company_name
                LIMIT 50
            """, (f'%{search}%',))
        else:
            cur.execute("""
                SELECT DISTINCT company_name
                FROM watchable_positions
                WHERE company_name IS NOT NULL
                ORDER BY company_name
                LIMIT 100
            """)

        companies = [row['company_name'] for row in cur.fetchall()]
        return jsonify({'companies': companies})


# ============================================================================
# ADMIN / CLASSIFICATION ENDPOINTS
# ============================================================================

@app.route('/api/admin/classify-jobs', methods=['POST'])
def classify_all_jobs():
    """
    Classify all jobs that don't have role_type or experience_level set.
    This can be run anytime to classify new jobs in bulk.
    """
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get jobs that need classification
        cur.execute("""
            SELECT id, title FROM watchable_positions
            WHERE role_type IS NULL OR experience_level IS NULL
        """)
        jobs = cur.fetchall()

        classified_role = 0
        classified_exp = 0

        for job in jobs:
            title = job['title']
            updates = []
            params = []

            # Classify role type if missing
            role_type = classify_role_type(title)
            if role_type:
                updates.append("role_type = %s")
                params.append(role_type)
                classified_role += 1

            # Classify experience level if missing
            exp_level = classify_experience_level(title)
            if exp_level:
                updates.append("experience_level = %s")
                params.append(exp_level)
                classified_exp += 1

            # Update if we have classifications
            if updates:
                params.append(job['id'])
                cur.execute(f"""
                    UPDATE watchable_positions
                    SET {', '.join(updates)}
                    WHERE id = %s
                """, params)

        conn.commit()

        # Get stats
        cur.execute("SELECT COUNT(*) as total FROM watchable_positions")
        total = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) as classified FROM watchable_positions WHERE role_type IS NOT NULL")
        role_classified = cur.fetchone()['classified']

        cur.execute("SELECT COUNT(*) as classified FROM watchable_positions WHERE experience_level IS NOT NULL")
        exp_classified = cur.fetchone()['classified']

        return jsonify({
            'success': True,
            'jobs_processed': len(jobs),
            'role_types_classified': classified_role,
            'experience_levels_classified': classified_exp,
            'stats': {
                'total_jobs': total,
                'jobs_with_role_type': role_classified,
                'jobs_with_experience_level': exp_classified
            }
        })


@app.route('/api/admin/classification-stats', methods=['GET'])
def get_classification_stats():
    """Get current classification statistics."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Total jobs
        cur.execute("SELECT COUNT(*) as count FROM watchable_positions")
        total = cur.fetchone()['count']

        # Role type breakdown
        cur.execute("""
            SELECT role_type, COUNT(*) as count
            FROM watchable_positions
            GROUP BY role_type
            ORDER BY count DESC
        """)
        role_types = cur.fetchall()

        # Experience level breakdown
        cur.execute("""
            SELECT experience_level, COUNT(*) as count
            FROM watchable_positions
            GROUP BY experience_level
            ORDER BY count DESC
        """)
        exp_levels = cur.fetchall()

        # Unclassified sample titles (for debugging)
        cur.execute("""
            SELECT DISTINCT title
            FROM watchable_positions
            WHERE role_type IS NULL
            LIMIT 20
        """)
        unclassified_titles = [row['title'] for row in cur.fetchall()]

        return jsonify({
            'total_jobs': total,
            'role_type_breakdown': role_types,
            'experience_level_breakdown': exp_levels,
            'unclassified_sample': unclassified_titles
        })


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.utcnow().isoformat()})


if __name__ == '__main__':
    app.run(debug=True, port=5002, host='0.0.0.0')
