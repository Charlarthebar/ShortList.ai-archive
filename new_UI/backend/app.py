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
    import openai
    OPENAI_AVAILABLE = bool(os.environ.get('OPENAI_API_KEY'))
    if OPENAI_AVAILABLE:
        openai.api_key = os.environ.get('OPENAI_API_KEY')
except ImportError:
    OPENAI_AVAILABLE = False

app = Flask(__name__)
CORS(app)

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
    """Update user job preferences."""
    data = request.json or {}

    preferred_locations = data.get('preferred_locations') or []  # List of cities
    salary_min = data.get('salary_min')  # Integer
    salary_max = data.get('salary_max')  # Integer
    open_to_roles = data.get('open_to_roles') or []  # List of role types
    experience_level = data.get('experience_level')  # String
    work_arrangement = data.get('work_arrangement')  # String: remote, hybrid, onsite
    preferences_text = (data.get('preferences_text') or '').strip()  # Free text

    # Generate embedding for preferences text if OpenAI is available
    preferences_embedding = None
    if preferences_text and OPENAI_AVAILABLE:
        try:
            response = openai.embeddings.create(
                model="text-embedding-3-small",
                input=preferences_text
            )
            preferences_embedding = json.dumps(response.data[0].embedding)
        except Exception as e:
            print(f"Failed to generate embedding: {e}")

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE seeker_profiles
            SET preferred_locations = %s,
                salary_min = %s,
                salary_max = %s,
                open_to_roles = %s,
                experience_level = %s,
                work_preference = %s,
                preferences_text = %s,
                preferences_embedding = %s,
                profile_complete = TRUE,
                updated_at = NOW()
            WHERE user_id = %s
        """, (
            preferred_locations if preferred_locations else None,
            salary_min,
            salary_max,
            open_to_roles if open_to_roles else None,
            experience_level,
            work_arrangement,
            preferences_text if preferences_text else None,
            preferences_embedding,
            g.user_id
        ))
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


def calculate_match_score(role, user_prefs):
    """
    Calculate how well a role matches user preferences.
    Returns score 0-100 based only on filled-in preferences.
    Returns None if no preferences are set.
    """
    if not user_prefs or not has_any_preferences(user_prefs):
        return None

    scores = []

    # Location match
    if user_prefs.get('preferred_locations'):
        location_score = 0
        role_location = (role.get('location') or '').lower()
        for pref_loc in user_prefs['preferred_locations']:
            if pref_loc.lower() in role_location or role_location in pref_loc.lower():
                location_score = 100
                break
        scores.append(location_score)

    # Salary match
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
                    salary_score = min(100, int(overlap * 100) + 50)  # At least 50 if there's overlap
                else:
                    salary_score = 100
        elif role.get('salary_range'):
            # Has salary but couldn't parse - give partial credit
            salary_score = 50
        scores.append(salary_score)

    # Role type match
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
        scores.append(role_type_score)

    # Experience level match
    if user_prefs.get('experience_level'):
        exp_score = 0
        title = (role.get('title') or '').lower()
        user_level = user_prefs['experience_level']

        # Map experience levels to title keywords
        level_keywords = {
            'intern': ['intern', 'internship'],
            'entry': ['entry', 'junior', 'associate', 'i ', ' i,', ' 1', 'new grad'],
            'mid': ['mid', 'ii ', ' ii,', ' 2', ' 3'],
            'senior': ['senior', 'sr', 'lead', 'principal', 'staff', 'iii', ' 4', ' 5']
        }

        # Check if job title matches user's experience level
        for keyword in level_keywords.get(user_level, []):
            if keyword in title:
                exp_score = 100
                break

        # If no explicit match but also no senior keywords for entry-level user, partial credit
        if exp_score == 0 and user_level in ['entry', 'mid']:
            has_senior_keywords = any(kw in title for kw in level_keywords['senior'])
            if not has_senior_keywords:
                exp_score = 50

        scores.append(exp_score)

    # Work arrangement match
    if user_prefs.get('work_preference'):
        work_score = 0
        role_arrangement = (role.get('work_arrangement') or '').lower()
        user_pref = user_prefs['work_preference'].lower()

        if user_pref in role_arrangement or role_arrangement in user_pref:
            work_score = 100
        elif role_arrangement:
            # Has arrangement but doesn't match
            work_score = 25
        else:
            # No arrangement specified on job - neutral
            work_score = 50

        scores.append(work_score)

    # Calculate simple average of all filled-in preference scores
    if not scores:
        return None

    return int(sum(scores) / len(scores))


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

    # Get user preferences if authenticated
    user_prefs = None
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        try:
            token = auth_header.split(' ')[1]
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = payload.get('user_id')
            if user_id:
                conn = get_db()
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT preferred_locations, salary_min, salary_max,
                               open_to_roles, experience_level, preferences_text
                        FROM seeker_profiles WHERE user_id = %s
                    """, (user_id,))
                    user_prefs = cur.fetchone()
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

            # Calculate match score
            role['match_score'] = calculate_match_score(role, user_prefs)
            filtered_roles.append(role)

        # Sort by match score (highest first), then by salary presence
        filtered_roles.sort(key=lambda r: (
            r.get('match_score') or 0,
            r.get('salary_range') is not None,
            r.get('status') == 'open'
        ), reverse=True)

        return jsonify({'roles': filtered_roles[:100]})


@app.route('/api/roles/<int:role_id>', methods=['GET'])
def get_role(role_id):
    """Get single role details."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                wp.id, wp.title, wp.company_name, wp.location, wp.department,
                wp.status, wp.salary_range, wp.description, wp.role_type,
                (SELECT COUNT(*) FROM shortlist_applications sa WHERE sa.position_id = wp.id) as applicant_count
            FROM watchable_positions wp
            WHERE wp.id = %s
        """, (role_id,))
        role = cur.fetchone()

        if not role:
            return jsonify({'error': 'Role not found'}), 404

        return jsonify({'role': role})


# ============================================================================
# SHORTLIST ENDPOINTS
# ============================================================================

@app.route('/api/shortlist/apply', methods=['POST'])
@require_auth
def apply_to_shortlist():
    """Join the shortlist for a role. Requires complete profile. Returns fit questions."""
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

        # Get fit questions for this role type
        questions = get_questions_for_role_type(role.get('role_type'))

        # Get user's saved resume path (if any)
        user_resume = profile.get('resume_path')

        # Create application with status 'questions_pending' - will be updated after fit questions submitted
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
            'questions': questions
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


@app.route('/api/shortlist/my-applications', methods=['GET'])
@require_auth
def get_my_applications():
    """Get all shortlist applications for current user."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                sa.id, sa.status, sa.applied_at, sa.resume_path,
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
        return send_file(app['resume_path'], as_attachment=True)


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
        return send_file(
            app['resume_path'],
            mimetype='application/pdf',
            as_attachment=False
        )


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
    app.run(debug=True, port=5002)
