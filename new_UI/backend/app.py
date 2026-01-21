"""
ShortList API - Minimal Backend
Clean, focused API for the ShortList platform.
"""

import os
import hashlib
import secrets
from datetime import datetime, timedelta
from functools import wraps

import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from werkzeug.utils import secure_filename

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
            SELECT u.id, u.email, u.user_type, u.first_name, u.last_name,
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

        return jsonify({'user': dict(user)})


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


# ============================================================================
# ROLES ENDPOINTS
# ============================================================================

@app.route('/api/roles', methods=['GET'])
def get_roles():
    """
    Get roles in Boston/Cambridge area.
    Filters: role_type, search
    """
    role_type = request.args.get('role_type')
    search = request.args.get('search', '').strip()

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT
                wp.id, wp.title, wp.company_name, wp.location, wp.department,
                wp.status, wp.salary_range, wp.role_type,
                (SELECT COUNT(*) FROM shortlist_applications sa WHERE sa.position_id = wp.id) as applicant_count
            FROM watchable_positions wp
            WHERE (wp.location ILIKE '%boston%' OR wp.location ILIKE '%cambridge%' OR wp.location ILIKE '%massachusetts%' OR wp.location ILIKE '%, MA%')
        """
        params = []

        if role_type:
            query += " AND wp.role_type = %s"
            params.append(role_type)

        if search:
            query += " AND (wp.title ILIKE %s OR wp.company_name ILIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])

        query += " ORDER BY wp.status = 'open' DESC, wp.company_name ASC LIMIT 100"

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        roles = cur.fetchall()

        return jsonify({'roles': roles})


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
    """Join the shortlist for a role. Requires complete profile."""
    data = request.json
    role_id = data.get('role_id')

    if not role_id:
        return jsonify({'error': 'Role ID required'}), 400

    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check profile is complete
        cur.execute("""
            SELECT profile_complete, experience_level, work_preference
            FROM seeker_profiles WHERE user_id = %s
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

        # Check role exists
        cur.execute("SELECT id, status FROM watchable_positions WHERE id = %s", (role_id,))
        role = cur.fetchone()
        if not role:
            return jsonify({'error': 'Role not found'}), 404

        # Create application
        cur.execute("""
            INSERT INTO shortlist_applications
            (user_id, position_id, experience_level, work_preference, status, applied_at)
            VALUES (%s, %s, %s, %s, 'pending', NOW())
            RETURNING id
        """, (g.user_id, role_id, profile['experience_level'], profile['work_preference']))
        app_id = cur.fetchone()['id']
        conn.commit()

        return jsonify({'success': True, 'application_id': app_id})


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

        # Save file
        filename = secure_filename(f"{g.user_id}_{application_id}_{file.filename}")
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)

        # Update application with resume path
        cur.execute("""
            UPDATE shortlist_applications
            SET resume_path = %s, status = 'submitted'
            WHERE id = %s
        """, (filepath, application_id))
        conn.commit()

    return jsonify({'success': True, 'filename': filename})


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
    """Get all applicants for a role."""
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                sa.id as application_id, sa.status, sa.applied_at, sa.resume_path,
                sa.experience_level, sa.work_preference,
                u.email
            FROM shortlist_applications sa
            JOIN platform_users u ON u.id = sa.user_id
            WHERE sa.position_id = %s
            ORDER BY sa.applied_at DESC
        """, (role_id,))
        applicants = cur.fetchall()

        return jsonify({'applicants': applicants})


@app.route('/api/employer/download-resume/<int:application_id>', methods=['GET'])
@require_auth
def download_resume(application_id):
    """Download a candidate's resume."""
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
# HEALTH CHECK
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.utcnow().isoformat()})


if __name__ == '__main__':
    app.run(debug=True, port=5002)
