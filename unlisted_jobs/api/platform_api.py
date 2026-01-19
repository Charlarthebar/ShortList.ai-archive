#!/usr/bin/env python3
"""
ShortList Platform API
======================

REST API for the ShortList role-watching platform.

Endpoints:
- /api/auth/* - Authentication (signup, login, logout)
- /api/users/* - User profile management
- /api/positions/* - Browse and search positions
- /api/watches/* - Watchlist management
- /api/companies/* - Company features (view watchers, invite)
- /api/notifications/* - Notification management

Author: ShortList.ai
Date: 2026-01-16
"""

import os
import sys
import logging
import re
import tempfile
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from functools import wraps
import json
import hashlib
import secrets

from flask import Flask, request, jsonify, g, send_from_directory
from flask_cors import CORS

# Optional PDF parsing
try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logging.warning("pdfplumber not installed - PDF parsing disabled")

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

# Import screening module
try:
    from screening import screen_application, rescreen_application, ScreeningResult
    SCREENING_AVAILABLE = True
except ImportError:
    SCREENING_AVAILABLE = False
    logging.warning("screening module not available")

# Import email service
try:
    from email_service import get_email_service
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False
    logging.warning("email service not available")

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# Database
config = Config()
db = DatabaseManager(config)


# ============================================================================
# HELPERS
# ============================================================================

def get_db():
    """Get database connection for request."""
    if 'db_conn' not in g:
        g.db_conn = db.get_connection()
    return g.db_conn


@app.teardown_appcontext
def close_db(exception):
    """Release database connection after request."""
    conn = g.pop('db_conn', None)
    if conn is not None:
        db.release_connection(conn)


def hash_password(password: str) -> str:
    """Hash password with salt."""
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}${hashed.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash."""
    try:
        salt, hashed = password_hash.split('$')
        check = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return check.hex() == hashed
    except:
        return False


def generate_token(user_id: int) -> str:
    """Generate simple auth token (in production use JWT)."""
    return f"{user_id}:{secrets.token_hex(32)}"


def generate_verification_token() -> str:
    """Generate a secure verification token."""
    return secrets.token_urlsafe(32)


def extract_email_domain(email: str) -> str:
    """Extract domain from email address."""
    return email.split('@')[-1].lower()


def is_corporate_domain(domain: str) -> bool:
    """Check if domain is likely a corporate domain (not personal email)."""
    personal_domains = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
        'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com', 'yandex.com',
        'gmx.com', 'live.com', 'msn.com', 'me.com', 'mac.com'
    }
    return domain not in personal_domains


def get_current_user():
    """Get current user from auth header."""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return None

    token = auth_header[7:]
    try:
        user_id = int(token.split(':')[0])
        conn = get_db()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, email, user_type, first_name, last_name FROM platform_users WHERE id = %s",
                (user_id,)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'email': row[1],
                    'user_type': row[2],
                    'first_name': row[3],
                    'last_name': row[4]
                }
    except:
        pass
    return None


def require_auth(f):
    """Decorator to require authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        g.current_user = user
        return f(*args, **kwargs)
    return decorated


def require_seeker(f):
    """Decorator to require job seeker user type."""
    @wraps(f)
    @require_auth
    def decorated(*args, **kwargs):
        if g.current_user['user_type'] != 'seeker':
            return jsonify({'error': 'Job seeker account required'}), 403
        return f(*args, **kwargs)
    return decorated


def require_company(f):
    """Decorator to require company user type."""
    @wraps(f)
    @require_auth
    def decorated(*args, **kwargs):
        if g.current_user['user_type'] != 'company':
            return jsonify({'error': 'Company account required'}), 403
        return f(*args, **kwargs)
    return decorated


# ============================================================================
# AUTH ENDPOINTS
# ============================================================================

@app.route('/api/auth/signup', methods=['POST'])
def signup():
    """Create new user account."""
    data = request.json
    email = data.get('email', '').lower().strip()
    password = data.get('password', '')
    user_type = data.get('user_type', 'seeker')
    first_name = data.get('first_name', '')
    last_name = data.get('last_name', '')

    # Company-specific fields
    company_name = data.get('company_name', '')
    company_website = data.get('company_website', '')
    company_industry = data.get('company_industry', '')
    company_size = data.get('company_size', '')
    company_description = data.get('company_description', '')

    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400

    if user_type not in ('seeker', 'company'):
        return jsonify({'error': 'Invalid user type'}), 400

    if user_type == 'company' and not company_name:
        return jsonify({'error': 'Company name required'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Check if email exists
            cursor.execute("SELECT id FROM platform_users WHERE email = %s", (email,))
            if cursor.fetchone():
                return jsonify({'error': 'Email already registered'}), 409

            # Create user
            password_hash = hash_password(password)
            cursor.execute("""
                INSERT INTO platform_users (email, password_hash, user_type, first_name, last_name)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (email, password_hash, user_type, first_name, last_name))
            user_id = cursor.fetchone()[0]

            company_profile_id = None
            verification_status = None

            # Create profile based on type
            if user_type == 'seeker':
                cursor.execute(
                    "INSERT INTO seeker_profiles (user_id) VALUES (%s)",
                    (user_id,)
                )
            else:
                # Create company profile
                cursor.execute("""
                    INSERT INTO company_profiles (
                        company_name, website, industry, company_size, description,
                        verified, verification_method
                    ) VALUES (%s, %s, %s, %s, %s, FALSE, NULL)
                    RETURNING id
                """, (company_name, company_website, company_industry, company_size, company_description))
                company_profile_id = cursor.fetchone()[0]

                # Add user as owner of company
                cursor.execute("""
                    INSERT INTO company_team_members (company_profile_id, user_id, role, accepted_at)
                    VALUES (%s, %s, 'owner', CURRENT_TIMESTAMP)
                """, (company_profile_id, user_id))

                # Check if email domain can be auto-verified
                email_domain = extract_email_domain(email)
                if is_corporate_domain(email_domain):
                    # Create verification request for domain verification
                    verification_token = generate_verification_token()
                    cursor.execute("""
                        INSERT INTO verification_tokens (
                            user_id, company_profile_id, token, token_type,
                            expected_domain, expires_at
                        ) VALUES (%s, %s, %s, 'company_domain', %s, CURRENT_TIMESTAMP + INTERVAL '7 days')
                    """, (user_id, company_profile_id, verification_token, email_domain))
                    verification_status = 'pending_domain_verification'
                else:
                    # Personal email - needs manual verification
                    cursor.execute("""
                        INSERT INTO company_verification_requests (
                            company_profile_id, requested_by, verification_type, status
                        ) VALUES (%s, %s, 'manual', 'pending')
                    """, (company_profile_id, user_id))
                    verification_status = 'pending_manual_verification'

        conn.commit()

        # Send verification email if applicable
        if user_type == 'company' and verification_status == 'pending_domain_verification' and EMAIL_AVAILABLE:
            try:
                email_service = get_email_service()
                email_service.send_verification_email(
                    to_email=email,
                    to_name=first_name or email.split('@')[0],
                    company_name=company_name,
                    verification_token=verification_token
                )
            except Exception as email_err:
                log.error(f"Failed to send verification email: {email_err}")
                # Don't fail signup if email fails

        token = generate_token(user_id)
        response = {
            'token': token,
            'user': {
                'id': user_id,
                'email': email,
                'user_type': user_type,
                'first_name': first_name,
                'last_name': last_name
            }
        }

        if user_type == 'company':
            response['company_profile_id'] = company_profile_id
            response['verification_status'] = verification_status
            if verification_status == 'pending_domain_verification':
                response['message'] = f"We've sent a verification email to {email}. Please check your inbox to verify your company."
            else:
                response['message'] = "Your company profile is pending manual verification. Our team will review it within 24-48 hours."

        return jsonify(response), 201

    except Exception as e:
        conn.rollback()
        log.error(f"Signup error: {e}")
        return jsonify({'error': 'Failed to create account'}), 500


@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login with email and password."""
    data = request.json
    email = data.get('email', '').lower().strip()
    password = data.get('password', '')

    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400

    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, password_hash, user_type, first_name, last_name
            FROM platform_users WHERE email = %s
        """, (email,))
        row = cursor.fetchone()

        if not row or not verify_password(password, row[1]):
            return jsonify({'error': 'Invalid email or password'}), 401

        user_id = row[0]

        # Update last login
        cursor.execute(
            "UPDATE platform_users SET last_login_at = CURRENT_TIMESTAMP WHERE id = %s",
            (user_id,)
        )
        conn.commit()

        token = generate_token(user_id)
        return jsonify({
            'token': token,
            'user': {
                'id': user_id,
                'email': email,
                'user_type': row[2],
                'first_name': row[3],
                'last_name': row[4]
            }
        })


@app.route('/api/auth/me', methods=['GET'])
@require_auth
def get_me():
    """Get current user info."""
    return jsonify({'user': g.current_user})


# ============================================================================
# COMPANY VERIFICATION ENDPOINTS
# ============================================================================

@app.route('/api/auth/verify-company', methods=['POST'])
def verify_company_domain():
    """Verify company ownership via email domain token."""
    data = request.json
    token = data.get('token', '')

    if not token:
        return jsonify({'error': 'Verification token required'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Find the verification token
            cursor.execute("""
                SELECT vt.id, vt.user_id, vt.company_profile_id, vt.expected_domain,
                       vt.expires_at, vt.used_at, cp.company_name
                FROM verification_tokens vt
                JOIN company_profiles cp ON vt.company_profile_id = cp.id
                WHERE vt.token = %s AND vt.token_type = 'company_domain'
            """, (token,))
            row = cursor.fetchone()

            if not row:
                return jsonify({'error': 'Invalid verification token'}), 400

            token_id, user_id, company_profile_id, expected_domain, expires_at, used_at, company_name = row

            if used_at:
                return jsonify({'error': 'Token has already been used'}), 400

            if expires_at < datetime.now():
                return jsonify({'error': 'Token has expired'}), 400

            # Mark token as used
            cursor.execute("""
                UPDATE verification_tokens
                SET used_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (token_id,))

            # Update company profile as verified
            cursor.execute("""
                UPDATE company_profiles
                SET verified = TRUE,
                    verified_at = CURRENT_TIMESTAMP,
                    verification_method = 'email_domain'
                WHERE id = %s
            """, (company_profile_id,))

        conn.commit()
        return jsonify({
            'success': True,
            'message': f'{company_name} has been verified!',
            'company_profile_id': company_profile_id
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Verification error: {e}")
        return jsonify({'error': 'Verification failed'}), 500


@app.route('/api/auth/resend-verification', methods=['POST'])
@require_company
def resend_verification():
    """Resend company verification email."""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Get user's company profile
            cursor.execute("""
                SELECT cp.id, cp.verified, cp.company_name
                FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s AND ctm.role = 'owner'
            """, (g.current_user['id'],))
            row = cursor.fetchone()

            if not row:
                return jsonify({'error': 'Company profile not found'}), 404

            company_profile_id, verified, company_name = row

            if verified:
                return jsonify({'error': 'Company is already verified'}), 400

            # Invalidate old tokens
            cursor.execute("""
                UPDATE verification_tokens
                SET used_at = CURRENT_TIMESTAMP
                WHERE company_profile_id = %s AND token_type = 'company_domain' AND used_at IS NULL
            """, (company_profile_id,))

            # Create new token
            email_domain = extract_email_domain(g.current_user['email'])
            verification_token = generate_verification_token()
            cursor.execute("""
                INSERT INTO verification_tokens (
                    user_id, company_profile_id, token, token_type,
                    expected_domain, expires_at
                ) VALUES (%s, %s, %s, 'company_domain', %s, CURRENT_TIMESTAMP + INTERVAL '7 days')
            """, (g.current_user['id'], company_profile_id, verification_token, email_domain))

        conn.commit()
        # TODO: Actually send the email here
        return jsonify({
            'success': True,
            'message': f'Verification email sent to {g.current_user["email"]}'
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Resend verification error: {e}")
        return jsonify({'error': 'Failed to resend verification'}), 500


@app.route('/api/company/verification-status', methods=['GET'])
@require_company
def get_verification_status():
    """Get company verification status."""
    conn = get_db()
    with conn.cursor() as cursor:
        # Get company profile and verification status
        cursor.execute("""
            SELECT cp.id, cp.company_name, cp.verified, cp.verified_at, cp.verification_method
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))
        row = cursor.fetchone()

        if not row:
            return jsonify({'error': 'Company profile not found'}), 404

        company_profile_id, company_name, verified, verified_at, verification_method = row

        status = {
            'company_profile_id': company_profile_id,
            'company_name': company_name,
            'verified': verified,
            'verified_at': verified_at.isoformat() if verified_at else None,
            'verification_method': verification_method
        }

        # Check for pending verification request
        if not verified:
            cursor.execute("""
                SELECT id, verification_type, status, created_at
                FROM company_verification_requests
                WHERE company_profile_id = %s AND status = 'pending'
                ORDER BY created_at DESC LIMIT 1
            """, (company_profile_id,))
            req_row = cursor.fetchone()
            if req_row:
                status['pending_request'] = {
                    'id': req_row[0],
                    'type': req_row[1],
                    'status': req_row[2],
                    'created_at': req_row[3].isoformat() if req_row[3] else None
                }

            # Check for pending domain verification token
            cursor.execute("""
                SELECT token, expires_at
                FROM verification_tokens
                WHERE company_profile_id = %s AND token_type = 'company_domain'
                  AND used_at IS NULL AND expires_at > CURRENT_TIMESTAMP
                ORDER BY created_at DESC LIMIT 1
            """, (company_profile_id,))
            token_row = cursor.fetchone()
            if token_row:
                status['pending_domain_verification'] = True
                status['domain_verification_expires'] = token_row[1].isoformat() if token_row[1] else None

        return jsonify(status)


# ============================================================================
# PASSWORD RESET ENDPOINTS
# ============================================================================

@app.route('/api/auth/forgot-password', methods=['POST'])
def forgot_password():
    """Request a password reset email."""
    data = request.json
    email = data.get('email', '').lower().strip()

    if not email:
        return jsonify({'error': 'Email is required'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Check if user exists
            cursor.execute("""
                SELECT id, first_name FROM platform_users WHERE email = %s
            """, (email,))
            row = cursor.fetchone()

            # Always return success to prevent email enumeration
            if not row:
                return jsonify({
                    'success': True,
                    'message': 'If an account exists with that email, you will receive a password reset link.'
                })

            user_id, first_name = row

            # Invalidate any existing reset tokens
            cursor.execute("""
                UPDATE verification_tokens
                SET used_at = CURRENT_TIMESTAMP
                WHERE user_id = %s AND token_type = 'password_reset' AND used_at IS NULL
            """, (user_id,))

            # Create new reset token (expires in 1 hour)
            reset_token = secrets.token_urlsafe(32)
            cursor.execute("""
                INSERT INTO verification_tokens (
                    user_id, token, token_type, expires_at
                ) VALUES (%s, %s, 'password_reset', CURRENT_TIMESTAMP + INTERVAL '1 hour')
            """, (user_id, reset_token))

        conn.commit()

        # Send reset email
        if EMAIL_AVAILABLE:
            try:
                email_service = get_email_service()
                email_service.send_password_reset_email(
                    to_email=email,
                    to_name=first_name or 'there',
                    reset_token=reset_token
                )
                log.info(f"Password reset email sent to {email}")
            except Exception as e:
                log.error(f"Failed to send password reset email: {e}")
        else:
            log.warning(f"Email service not available. Reset token for {email}: {reset_token}")

        return jsonify({
            'success': True,
            'message': 'If an account exists with that email, you will receive a password reset link.'
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Password reset request error: {e}")
        return jsonify({'error': 'Failed to process request'}), 500


@app.route('/api/auth/reset-password', methods=['POST'])
def reset_password():
    """Reset password using a token."""
    data = request.json
    token = data.get('token', '')
    new_password = data.get('password', '')

    if not token:
        return jsonify({'error': 'Reset token is required'}), 400

    if not new_password or len(new_password) < 8:
        return jsonify({'error': 'Password must be at least 8 characters'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Find valid token
            cursor.execute("""
                SELECT id, user_id, expires_at, used_at
                FROM verification_tokens
                WHERE token = %s AND token_type = 'password_reset'
            """, (token,))
            row = cursor.fetchone()

            if not row:
                return jsonify({'error': 'Invalid reset token'}), 400

            token_id, user_id, expires_at, used_at = row

            if used_at:
                return jsonify({'error': 'This reset link has already been used'}), 400

            if expires_at < datetime.now():
                return jsonify({'error': 'This reset link has expired. Please request a new one.'}), 400

            # Update password
            password_hash = hash_password(new_password)
            cursor.execute("""
                UPDATE platform_users
                SET password_hash = %s
                WHERE id = %s
            """, (password_hash, user_id))

            # Mark token as used
            cursor.execute("""
                UPDATE verification_tokens
                SET used_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (token_id,))

        conn.commit()
        log.info(f"Password reset successful for user {user_id}")
        return jsonify({
            'success': True,
            'message': 'Password has been reset successfully. You can now log in.'
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Password reset error: {e}")
        return jsonify({'error': 'Failed to reset password'}), 500


# ============================================================================
# ADMIN VERIFICATION ENDPOINTS
# ============================================================================

def require_admin(f):
    """Decorator to require admin user."""
    @wraps(f)
    @require_auth
    def decorated(*args, **kwargs):
        conn = get_db()
        with conn.cursor() as cursor:
            cursor.execute("SELECT is_admin FROM platform_users WHERE id = %s", (g.current_user['id'],))
            row = cursor.fetchone()
            if not row or not row[0]:
                return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated


@app.route('/api/admin/verification-requests', methods=['GET'])
@require_admin
def list_verification_requests():
    """List pending company verification requests."""
    status_filter = request.args.get('status', 'pending')
    limit = min(int(request.args.get('limit', 50)), 100)
    offset = int(request.args.get('offset', 0))

    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT cvr.id, cvr.company_profile_id, cvr.verification_type, cvr.domain,
                   cvr.document_url, cvr.document_type, cvr.status,
                   cvr.created_at, cp.company_name, cp.website,
                   pu.email as requester_email, pu.first_name, pu.last_name
            FROM company_verification_requests cvr
            JOIN company_profiles cp ON cvr.company_profile_id = cp.id
            JOIN platform_users pu ON cvr.requested_by = pu.id
            WHERE cvr.status = %s
            ORDER BY cvr.created_at ASC
            LIMIT %s OFFSET %s
        """, (status_filter, limit, offset))

        rows = cursor.fetchall()
        requests = []
        for row in rows:
            requests.append({
                'id': row[0],
                'company_profile_id': row[1],
                'verification_type': row[2],
                'domain': row[3],
                'document_url': row[4],
                'document_type': row[5],
                'status': row[6],
                'created_at': row[7].isoformat() if row[7] else None,
                'company_name': row[8],
                'website': row[9],
                'requester_email': row[10],
                'requester_name': f"{row[11]} {row[12]}".strip()
            })

        # Get total count
        cursor.execute("""
            SELECT COUNT(*) FROM company_verification_requests WHERE status = %s
        """, (status_filter,))
        total = cursor.fetchone()[0]

        return jsonify({
            'requests': requests,
            'total': total,
            'limit': limit,
            'offset': offset
        })


@app.route('/api/admin/verification-requests/<int:request_id>/approve', methods=['POST'])
@require_admin
def approve_verification_request(request_id):
    """Approve a company verification request."""
    data = request.json or {}
    notes = data.get('notes', '')

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Get the request
            cursor.execute("""
                SELECT cvr.id, cvr.company_profile_id, cvr.status, cp.company_name
                FROM company_verification_requests cvr
                JOIN company_profiles cp ON cvr.company_profile_id = cp.id
                WHERE cvr.id = %s
            """, (request_id,))
            row = cursor.fetchone()

            if not row:
                return jsonify({'error': 'Verification request not found'}), 404

            if row[2] != 'pending':
                return jsonify({'error': 'Request has already been processed'}), 400

            company_profile_id = row[1]
            company_name = row[3]

            # Update request status
            cursor.execute("""
                UPDATE company_verification_requests
                SET status = 'approved',
                    reviewed_by = %s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    review_notes = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (g.current_user['id'], notes, request_id))

            # Update company profile
            cursor.execute("""
                UPDATE company_profiles
                SET verified = TRUE,
                    verified_at = CURRENT_TIMESTAMP,
                    verification_method = 'manual'
                WHERE id = %s
            """, (company_profile_id,))

            # Get the owner's email for notification
            cursor.execute("""
                SELECT pu.email, pu.first_name
                FROM platform_users pu
                JOIN company_team_members ctm ON pu.id = ctm.user_id
                WHERE ctm.company_profile_id = %s AND ctm.role = 'owner'
                LIMIT 1
            """, (company_profile_id,))
            owner_row = cursor.fetchone()

        conn.commit()

        # Send approval notification email
        if owner_row and EMAIL_AVAILABLE:
            try:
                email_service = get_email_service()
                email_service.send_verification_approved_email(
                    to_email=owner_row[0],
                    to_name=owner_row[1],
                    company_name=company_name
                )
            except Exception as email_err:
                log.error(f"Failed to send approval email: {email_err}")

        return jsonify({
            'success': True,
            'message': f'{company_name} has been verified'
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Approval error: {e}")
        return jsonify({'error': 'Failed to approve request'}), 500


@app.route('/api/admin/verification-requests/<int:request_id>/reject', methods=['POST'])
@require_admin
def reject_verification_request(request_id):
    """Reject a company verification request."""
    data = request.json or {}
    reason = data.get('reason', 'Request rejected')
    notes = data.get('notes', '')

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Get the request
            cursor.execute("""
                SELECT id, status FROM company_verification_requests WHERE id = %s
            """, (request_id,))
            row = cursor.fetchone()

            if not row:
                return jsonify({'error': 'Verification request not found'}), 404

            if row[1] != 'pending':
                return jsonify({'error': 'Request has already been processed'}), 400

            # Update request status
            cursor.execute("""
                UPDATE company_verification_requests
                SET status = 'rejected',
                    reviewed_by = %s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    review_notes = %s,
                    rejection_reason = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (g.current_user['id'], notes, reason, request_id))

        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Verification request rejected'
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Rejection error: {e}")
        return jsonify({'error': 'Failed to reject request'}), 500


# ============================================================================
# USER PROFILE ENDPOINTS
# ============================================================================

@app.route('/api/users/profile', methods=['GET'])
@require_seeker
def get_seeker_profile():
    """Get current user's seeker profile."""
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT sp.*, pu.email, pu.first_name, pu.last_name
            FROM seeker_profiles sp
            JOIN platform_users pu ON sp.user_id = pu.id
            WHERE sp.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Profile not found'}), 404

        columns = [desc[0] for desc in cursor.description]
        profile = dict(zip(columns, row))

        return jsonify({'profile': profile})


@app.route('/api/users/profile', methods=['PUT'])
@require_seeker
def update_seeker_profile():
    """Update seeker profile."""
    data = request.json
    conn = get_db()

    # Build update query dynamically
    allowed_fields = [
        'current_title', 'current_company', 'years_experience',
        'search_status', 'education', 'skills', 'preferred_locations',
        'work_arrangement', 'salary_min', 'salary_max', 'open_to_roles',
        'resume_url', 'linkedin_url', 'portfolio_url'
    ]

    updates = []
    values = []
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            value = data[field]
            # Handle JSON fields
            if field in ('education',) and isinstance(value, (list, dict)):
                value = json.dumps(value)
            values.append(value)

    if not updates:
        return jsonify({'error': 'No fields to update'}), 400

    values.append(g.current_user['id'])

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                UPDATE seeker_profiles
                SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING *
            """, values)

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Profile not found'}), 404

            columns = [desc[0] for desc in cursor.description]
            profile = dict(zip(columns, row))

        conn.commit()
        return jsonify({'profile': profile})

    except Exception as e:
        conn.rollback()
        log.error(f"Profile update error: {e}")
        return jsonify({'error': 'Failed to update profile'}), 500


@app.route('/api/users/profile/name', methods=['PUT'])
@require_auth
def update_user_name():
    """Update user first/last name."""
    data = request.json
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE platform_users
                SET first_name = %s, last_name = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (data.get('first_name', ''), data.get('last_name', ''), g.current_user['id']))
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


# ============================================================================
# POSITIONS ENDPOINTS
# ============================================================================

@app.route('/api/positions', methods=['GET'])
def get_positions():
    """
    Get positions with optional filtering.

    Query params:
    - search: Search term for title/company
    - status: open, filled, all (default: open)
    - company_id: Filter by company
    - limit: Max results (default: 50)
    - offset: Pagination offset
    """
    search = request.args.get('search', '')
    status = request.args.get('status', 'open')
    company_id = request.args.get('company_id', type=int)
    limit = min(request.args.get('limit', 50, type=int), 100)
    offset = request.args.get('offset', 0, type=int)

    conn = get_db()
    with conn.cursor() as cursor:
        # Build query
        where_clauses = []
        params = []

        if status != 'all':
            where_clauses.append("wp.status = %s")
            params.append(status)

        if search:
            where_clauses.append("(wp.title ILIKE %s OR wp.company_name ILIKE %s)")
            params.extend([f'%{search}%', f'%{search}%'])

        if company_id:
            where_clauses.append("wp.company_id = %s")
            params.append(company_id)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM watchable_positions wp {where_sql}", params)
        total = cursor.fetchone()[0]

        # Get positions
        params.extend([limit, offset])
        cursor.execute(f"""
            SELECT
                wp.id, wp.company_name, wp.title, wp.department, wp.location,
                wp.salary_range, wp.employment_type, wp.work_arrangement,
                wp.description, wp.status, wp.watcher_count, wp.posted_at,
                c.id as company_id,
                wp.experience_level, wp.min_years_experience, wp.max_years_experience,
                wp.required_skills, wp.preferred_skills,
                wp.is_monitored, wp.data_source, wp.data_as_of_date
            FROM watchable_positions wp
            LEFT JOIN companies c ON wp.company_id = c.id
            {where_sql}
            ORDER BY wp.posted_at DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, params)

        positions = []
        for row in cursor.fetchall():
            positions.append({
                'id': row[0],
                'company_name': row[1],
                'title': row[2],
                'department': row[3],
                'location': row[4],
                'salary_range': row[5],
                'employment_type': row[6],
                'work_arrangement': row[7],
                'description': row[8],
                'status': row[9],
                'watcher_count': row[10],
                'posted_at': row[11].isoformat() if row[11] else None,
                'company_id': row[12],
                'experience_level': row[13],
                'min_years_experience': row[14],
                'max_years_experience': row[15],
                'required_skills': row[16] or [],
                'preferred_skills': row[17] or [],
                'is_monitored': row[18] or False,
                'data_source': row[19],
                'data_as_of_date': row[20].isoformat() if row[20] else None
            })

    return jsonify({
        'positions': positions,
        'total': total,
        'limit': limit,
        'offset': offset
    })


@app.route('/api/positions/<int:position_id>', methods=['GET'])
def get_position(position_id):
    """Get single position details."""
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                wp.*, c.name as company_display_name
            FROM watchable_positions wp
            LEFT JOIN companies c ON wp.company_id = c.id
            WHERE wp.id = %s
        """, (position_id,))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Position not found'}), 404

        columns = [desc[0] for desc in cursor.description]
        position = dict(zip(columns, row))

        # Check if current user is watching
        user = get_current_user()
        if user:
            cursor.execute(
                "SELECT id FROM job_watches WHERE user_id = %s AND position_id = %s",
                (user['id'], position_id)
            )
            position['is_watching'] = cursor.fetchone() is not None

    return jsonify({'position': position})


# ============================================================================
# WATCHLIST ENDPOINTS
# ============================================================================

@app.route('/api/watches', methods=['GET'])
@require_seeker
def get_watchlist():
    """Get user's watchlist."""
    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                jw.id as watch_id,
                jw.notes,
                jw.priority,
                jw.created_at as watching_since,
                wp.id as position_id,
                wp.company_name,
                wp.title,
                wp.location,
                wp.salary_range,
                wp.status,
                wp.watcher_count
            FROM job_watches jw
            JOIN watchable_positions wp ON jw.position_id = wp.id
            WHERE jw.user_id = %s
            ORDER BY jw.priority DESC, jw.created_at DESC
        """, (g.current_user['id'],))

        watches = []
        for row in cursor.fetchall():
            watches.append({
                'watch_id': row[0],
                'notes': row[1],
                'priority': row[2],
                'watching_since': row[3].isoformat() if row[3] else None,
                'position_id': row[4],
                'company_name': row[5],
                'title': row[6],
                'location': row[7],
                'salary_range': row[8],
                'status': row[9],
                'watcher_count': row[10]
            })

    return jsonify({'watches': watches, 'count': len(watches)})


@app.route('/api/watches', methods=['POST'])
@require_seeker
def add_watch():
    """Add position to watchlist."""
    data = request.json
    position_id = data.get('position_id')

    if not position_id:
        return jsonify({'error': 'position_id required'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Check position exists
            cursor.execute("SELECT id FROM watchable_positions WHERE id = %s", (position_id,))
            if not cursor.fetchone():
                return jsonify({'error': 'Position not found'}), 404

            # Add watch
            cursor.execute("""
                INSERT INTO job_watches (user_id, position_id, notes, priority)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, position_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                g.current_user['id'],
                position_id,
                data.get('notes', ''),
                data.get('priority', 0)
            ))
            watch_id = cursor.fetchone()[0]

        conn.commit()

        # Log activity
        log_activity(g.current_user['id'], 'watch', position_id=position_id)

        return jsonify({'watch_id': watch_id}), 201

    except Exception as e:
        conn.rollback()
        log.error(f"Add watch error: {e}")
        return jsonify({'error': 'Failed to add watch'}), 500


@app.route('/api/watches/<int:watch_id>', methods=['DELETE'])
@require_seeker
def remove_watch(watch_id):
    """Remove position from watchlist."""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM job_watches
                WHERE id = %s AND user_id = %s
                RETURNING position_id
            """, (watch_id, g.current_user['id']))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Watch not found'}), 404

        conn.commit()

        # Log activity
        log_activity(g.current_user['id'], 'unwatch', position_id=row[0])

        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/watches/position/<int:position_id>', methods=['DELETE'])
@require_seeker
def remove_watch_by_position(position_id):
    """Remove watch by position ID."""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM job_watches
                WHERE position_id = %s AND user_id = %s
                RETURNING id
            """, (position_id, g.current_user['id']))

            if not cursor.fetchone():
                return jsonify({'error': 'Watch not found'}), 404

        conn.commit()
        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


# ============================================================================
# COMPANY ENDPOINTS
# ============================================================================

@app.route('/api/companies/search', methods=['GET'])
def search_companies():
    """
    Search for companies by name for autocomplete.
    Returns matching companies from the master companies table.
    """
    query = request.args.get('q', '').strip()
    limit = min(int(request.args.get('limit', 10)), 25)

    if len(query) < 2:
        return jsonify({'companies': []})

    conn = get_db()
    with conn.cursor() as cursor:
        # Search by name with trigram similarity for fuzzy matching
        # Falls back to ILIKE if trigram extension not available
        cursor.execute("""
            SELECT id, name, domain, industry, size_category, is_public
            FROM companies
            WHERE name ILIKE %s OR normalized_name ILIKE %s
            ORDER BY
                CASE WHEN name ILIKE %s THEN 0 ELSE 1 END,
                name
            LIMIT %s
        """, (f'%{query}%', f'%{query}%', f'{query}%', limit))

        rows = cursor.fetchall()
        companies = []
        for row in rows:
            companies.append({
                'id': row[0],
                'name': row[1],
                'domain': row[2],
                'industry': row[3],
                'size': row[4],
                'is_public': row[5]
            })

    return jsonify({'companies': companies})


@app.route('/api/companies/lookup', methods=['GET'])
def lookup_company():
    """
    Lookup a company by ID or domain.
    Returns full company info for pre-filling forms.
    """
    company_id = request.args.get('id')
    domain = request.args.get('domain', '').lower().strip()

    if not company_id and not domain:
        return jsonify({'error': 'id or domain required'}), 400

    conn = get_db()
    with conn.cursor() as cursor:
        if company_id:
            cursor.execute("""
                SELECT id, name, domain, industry, size_category, is_public, ein
                FROM companies WHERE id = %s
            """, (company_id,))
        else:
            cursor.execute("""
                SELECT id, name, domain, industry, size_category, is_public, ein
                FROM companies WHERE domain = %s
            """, (domain,))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company not found'}), 404

        company = {
            'id': row[0],
            'name': row[1],
            'domain': row[2],
            'industry': row[3],
            'size': row[4],
            'is_public': row[5],
            'ein': row[6]
        }

    return jsonify({'company': company})


@app.route('/api/companies/by-domain', methods=['GET'])
def get_company_by_email_domain():
    """
    Find a company by email domain.
    Useful during signup to auto-suggest company.
    """
    email = request.args.get('email', '').lower().strip()

    if not email or '@' not in email:
        return jsonify({'error': 'Valid email required'}), 400

    domain = extract_email_domain(email)

    # Skip personal email domains
    if not is_corporate_domain(domain):
        return jsonify({'company': None, 'reason': 'personal_email'})

    conn = get_db()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, name, domain, industry, size_category, is_public
            FROM companies WHERE domain = %s
        """, (domain,))

        row = cursor.fetchone()
        if not row:
            return jsonify({'company': None, 'reason': 'not_found'})

        company = {
            'id': row[0],
            'name': row[1],
            'domain': row[2],
            'industry': row[3],
            'size': row[4],
            'is_public': row[5]
        }

    return jsonify({'company': company})


@app.route('/api/companies/profile', methods=['GET'])
@require_company
def get_company_profile():
    """Get company profile for current user."""
    conn = get_db()
    with conn.cursor() as cursor:
        # Get company profile via team membership
        cursor.execute("""
            SELECT cp.*
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company profile not found'}), 404

        columns = [desc[0] for desc in cursor.description]
        profile = dict(zip(columns, row))

    return jsonify({'profile': profile})


@app.route('/api/companies/profile', methods=['POST'])
@require_company
def create_company_profile():
    """Create company profile."""
    data = request.json
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            # Create company profile
            cursor.execute("""
                INSERT INTO company_profiles (
                    company_name, website, description, industry, company_size,
                    headquarters_location, work_arrangements, benefits, culture_description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.get('company_name'),
                data.get('website'),
                data.get('description'),
                data.get('industry'),
                data.get('company_size'),
                data.get('headquarters_location'),
                data.get('work_arrangements', []),
                data.get('benefits', []),
                data.get('culture_description')
            ))
            profile_id = cursor.fetchone()[0]

            # Add user as owner
            cursor.execute("""
                INSERT INTO company_team_members (company_profile_id, user_id, role, accepted_at)
                VALUES (%s, %s, 'owner', CURRENT_TIMESTAMP)
            """, (profile_id, g.current_user['id']))

        conn.commit()
        return jsonify({'profile_id': profile_id}), 201

    except Exception as e:
        conn.rollback()
        log.error(f"Create company profile error: {e}")
        return jsonify({'error': 'Failed to create profile'}), 500


@app.route('/api/companies/positions', methods=['GET'])
@require_company
def get_company_positions():
    """Get all positions for company (from ATS syncs and manually created)."""
    conn = get_db()
    status_filter = request.args.get('status', 'all')  # all, open, filled, closed, paused

    with conn.cursor() as cursor:
        # Get company profile and linked company_id
        cursor.execute("""
            SELECT cp.id, cp.company_id, cp.company_name
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company not found'}), 404

        company_profile_id, company_id, company_name = row

        # Build query to get positions from BOTH:
        # 1. Positions linked to company_profile_id (manually created)
        # 2. Positions linked to company_id (from ATS syncs)
        where_clauses = []
        params = []

        if company_id:
            where_clauses.append("(wp.company_profile_id = %s OR wp.company_id = %s)")
            params.extend([company_profile_id, company_id])
        else:
            where_clauses.append("wp.company_profile_id = %s")
            params.append(company_profile_id)

        if status_filter != 'all':
            where_clauses.append("wp.status = %s")
            params.append(status_filter)

        where_sql = "WHERE " + " AND ".join(where_clauses)

        # Get positions
        cursor.execute(f"""
            SELECT
                wp.id, wp.title, wp.department, wp.location, wp.salary_range,
                wp.status, wp.watcher_count, wp.view_count, wp.application_count,
                wp.posted_at, wp.work_arrangement, wp.description,
                wp.experience_level, wp.min_years_experience, wp.max_years_experience,
                wp.required_skills, wp.preferred_skills,
                wp.observed_job_id, wp.apply_url
            FROM watchable_positions wp
            {where_sql}
            ORDER BY wp.status = 'open' DESC, wp.updated_at DESC
        """, params)

        positions = []
        for row in cursor.fetchall():
            positions.append({
                'id': row[0],
                'title': row[1],
                'department': row[2],
                'location': row[3],
                'salary_range': row[4],
                'status': row[5],
                'watcher_count': row[6],
                'view_count': row[7],
                'application_count': row[8],
                'posted_at': row[9].isoformat() if row[9] else None,
                'work_arrangement': row[10],
                'description': row[11],
                'experience_level': row[12],
                'min_years_experience': row[13],
                'max_years_experience': row[14],
                'required_skills': row[15] or [],
                'preferred_skills': row[16] or [],
                'from_ats': row[17] is not None,  # True if synced from ATS
                'apply_url': row[18]
            })

    return jsonify({
        'positions': positions,
        'company_name': company_name,
        'total': len(positions)
    })


@app.route('/api/companies/positions', methods=['POST'])
@require_company
def create_position():
    """Create new position."""
    data = request.json
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            # Get company profile
            cursor.execute("""
                SELECT cp.id, cp.company_name, cp.company_id
                FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.current_user['id'],))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Company not found'}), 404

            company_profile_id, company_name, company_id = row

            # Create position with experience requirements
            cursor.execute("""
                INSERT INTO watchable_positions (
                    company_profile_id, company_id, company_name, title, department,
                    location, salary_range, employment_type, work_arrangement,
                    description, requirements, status, posted_at,
                    experience_level, min_years_experience, max_years_experience,
                    required_skills, preferred_skills
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP,
                          %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                company_profile_id,
                company_id,
                company_name,
                data.get('title'),
                data.get('department'),
                data.get('location'),
                data.get('salary_range'),
                data.get('employment_type', 'full-time'),
                data.get('work_arrangement', 'hybrid'),
                data.get('description'),
                data.get('requirements', []),
                data.get('status', 'open'),
                data.get('experience_level', 'any'),
                data.get('min_years_experience', 0),
                data.get('max_years_experience'),
                data.get('required_skills', []),
                data.get('preferred_skills', [])
            ))
            position_id = cursor.fetchone()[0]

        conn.commit()
        return jsonify({'position_id': position_id}), 201

    except Exception as e:
        conn.rollback()
        log.error(f"Create position error: {e}")
        return jsonify({'error': 'Failed to create position'}), 500


@app.route('/api/companies/positions/<int:position_id>', methods=['PATCH'])
@require_company
def update_position(position_id):
    """Update an existing position (works for both ATS-synced and manually created)."""
    data = request.json
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            # Get company profile and company_id
            cursor.execute("""
                SELECT cp.id, cp.company_id
                FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.current_user['id'],))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Company not found'}), 404

            company_profile_id, company_id = row

            # Verify position belongs to this company (either via profile or company_id)
            cursor.execute("""
                SELECT id, status FROM watchable_positions
                WHERE id = %s AND (company_profile_id = %s OR company_id = %s)
            """, (position_id, company_profile_id, company_id))

            pos_row = cursor.fetchone()
            if not pos_row:
                return jsonify({'error': 'Position not found or access denied'}), 404

            old_status = pos_row[1]

            # Build dynamic update based on provided fields
            allowed_fields = [
                'title', 'department', 'location', 'salary_range',
                'employment_type', 'work_arrangement', 'description',
                'status', 'experience_level', 'min_years_experience',
                'max_years_experience', 'required_skills', 'preferred_skills'
            ]

            updates = []
            values = []
            for field in allowed_fields:
                if field in data:
                    updates.append(f"{field} = %s")
                    values.append(data[field])

            if not updates:
                return jsonify({'error': 'No fields to update'}), 400

            # Handle status change to 'filled'
            new_status = data.get('status')
            if new_status == 'filled' and old_status != 'filled':
                updates.append("filled_at = CURRENT_TIMESTAMP")

            updates.append("updated_at = CURRENT_TIMESTAMP")
            values.append(position_id)

            cursor.execute(f"""
                UPDATE watchable_positions
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, status
            """, values)

            result = cursor.fetchone()

        conn.commit()
        return jsonify({
            'success': True,
            'position_id': result[0],
            'status': result[1]
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Update position error: {e}")
        return jsonify({'error': 'Failed to update position'}), 500


@app.route('/api/companies/positions/<int:position_id>', methods=['DELETE'])
@require_company
def delete_position(position_id):
    """Delete/close a position (marks as closed, doesn't actually delete)."""
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            # Get company profile and company_id
            cursor.execute("""
                SELECT cp.id, cp.company_id
                FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.current_user['id'],))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Company not found'}), 404

            company_profile_id, company_id = row

            # Mark position as closed (soft delete)
            cursor.execute("""
                UPDATE watchable_positions
                SET status = 'closed', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND (company_profile_id = %s OR company_id = %s)
                RETURNING id
            """, (position_id, company_profile_id, company_id))

            if not cursor.fetchone():
                return jsonify({'error': 'Position not found or access denied'}), 404

        conn.commit()
        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        log.error(f"Delete position error: {e}")
        return jsonify({'error': 'Failed to delete position'}), 500


@app.route('/api/companies/positions/<int:position_id>/watchers', methods=['GET'])
@require_company
def get_position_watchers(position_id):
    """Get candidates watching a position."""
    conn = get_db()
    with conn.cursor() as cursor:
        # Get company profile and company_id for ownership check
        cursor.execute("""
            SELECT cp.id, cp.company_id
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company not found'}), 404

        company_profile_id, company_id = row

        # Verify position belongs to this company (via profile OR company_id for ATS)
        cursor.execute("""
            SELECT wp.id FROM watchable_positions wp
            WHERE wp.id = %s AND (wp.company_profile_id = %s OR wp.company_id = %s)
        """, (position_id, company_profile_id, company_id))

        if not cursor.fetchone():
            return jsonify({'error': 'Position not found or access denied'}), 404

        # Get watchers
        cursor.execute("""
            SELECT
                pu.id as user_id,
                pu.first_name,
                pu.last_name,
                sp.current_title,
                sp.current_company,
                sp.years_experience,
                sp.search_status,
                sp.skills,
                jw.created_at as watching_since,
                COALESCE(spm.match_score, 0) as match_score
            FROM job_watches jw
            JOIN platform_users pu ON jw.user_id = pu.id
            LEFT JOIN seeker_profiles sp ON pu.id = sp.user_id
            LEFT JOIN seeker_position_matches spm ON sp.id = spm.seeker_profile_id AND spm.position_id = %s
            WHERE jw.position_id = %s
              AND (pu.privacy_settings->>'show_to_companies')::boolean = true
            ORDER BY spm.match_score DESC NULLS LAST, jw.created_at DESC
        """, (position_id, position_id))

        watchers = []
        for row in cursor.fetchall():
            watchers.append({
                'user_id': row[0],
                'first_name': row[1],
                'last_name': row[2],
                'current_title': row[3],
                'current_company': row[4],
                'years_experience': row[5],
                'search_status': row[6],
                'skills': row[7] or [],
                'watching_since': row[8].isoformat() if row[8] else None,
                'match_score': row[9]
            })

    return jsonify({'watchers': watchers, 'count': len(watchers)})


@app.route('/api/companies/candidates', methods=['GET'])
@require_company
def search_candidates():
    """Search all candidates (talent discovery)."""
    search = request.args.get('search', '')
    status = request.args.get('status', '')  # actively-looking, open-to-offers
    skills = request.args.getlist('skills')
    limit = min(request.args.get('limit', 50, type=int), 100)
    offset = request.args.get('offset', 0, type=int)

    conn = get_db()
    with conn.cursor() as cursor:
        where_clauses = ["(pu.privacy_settings->>'show_to_companies')::boolean = true"]
        params = []

        if search:
            where_clauses.append("""
                (pu.first_name ILIKE %s OR pu.last_name ILIKE %s OR
                 sp.current_title ILIKE %s OR sp.current_company ILIKE %s)
            """)
            params.extend([f'%{search}%'] * 4)

        if status:
            where_clauses.append("sp.search_status = %s")
            params.append(status)

        if skills:
            where_clauses.append("sp.skills && %s")
            params.append(skills)

        where_sql = "WHERE " + " AND ".join(where_clauses)
        params.extend([limit, offset])

        cursor.execute(f"""
            SELECT
                pu.id,
                pu.first_name,
                pu.last_name,
                sp.current_title,
                sp.current_company,
                sp.years_experience,
                sp.search_status,
                sp.skills,
                sp.preferred_locations
            FROM platform_users pu
            JOIN seeker_profiles sp ON pu.id = sp.user_id
            {where_sql}
            ORDER BY
                CASE sp.search_status
                    WHEN 'actively-looking' THEN 1
                    WHEN 'open-to-offers' THEN 2
                    ELSE 3
                END,
                sp.updated_at DESC
            LIMIT %s OFFSET %s
        """, params)

        candidates = []
        for row in cursor.fetchall():
            candidates.append({
                'id': row[0],
                'first_name': row[1],
                'last_name': row[2],
                'current_title': row[3],
                'current_company': row[4],
                'years_experience': row[5],
                'search_status': row[6],
                'skills': row[7] or [],
                'preferred_locations': row[8] or []
            })

    return jsonify({'candidates': candidates})


@app.route('/api/companies/invite', methods=['POST'])
@require_company
def invite_candidate():
    """Invite a candidate to apply."""
    data = request.json
    candidate_user_id = data.get('candidate_user_id')
    position_id = data.get('position_id')
    message = data.get('message', '')

    if not candidate_user_id:
        return jsonify({'error': 'candidate_user_id required'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Get company profile
            cursor.execute("""
                SELECT cp.id FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.current_user['id'],))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Company not found'}), 404

            company_profile_id = row[0]

            # Create invitation
            cursor.execute("""
                INSERT INTO candidate_invitations (
                    company_profile_id, invited_by_user_id, candidate_user_id,
                    position_id, message
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (company_profile_id, candidate_user_id, position_id)
                DO UPDATE SET message = EXCLUDED.message, status = 'pending', created_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                company_profile_id,
                g.current_user['id'],
                candidate_user_id,
                position_id,
                message
            ))
            invitation_id = cursor.fetchone()[0]

            # Create notification for candidate
            cursor.execute("""
                INSERT INTO notifications (
                    user_id, type, title, message, related_position_id,
                    related_invitation_id, action_url
                ) VALUES (%s, 'invite', %s, %s, %s, %s, %s)
            """, (
                candidate_user_id,
                'You have been invited to apply!',
                message or 'A company wants you to apply for a position.',
                position_id,
                invitation_id,
                f'/invitations/{invitation_id}'
            ))

        conn.commit()
        return jsonify({'invitation_id': invitation_id}), 201

    except Exception as e:
        conn.rollback()
        log.error(f"Invite error: {e}")
        return jsonify({'error': 'Failed to send invitation'}), 500


# ============================================================================
# NOTIFICATIONS ENDPOINTS
# ============================================================================

@app.route('/api/notifications', methods=['GET'])
@require_auth
def get_notifications():
    """Get user's notifications."""
    limit = min(request.args.get('limit', 50, type=int), 100)
    unread_only = request.args.get('unread_only', 'false').lower() == 'true'

    conn = get_db()
    with conn.cursor() as cursor:
        where_clause = "WHERE n.user_id = %s"
        params = [g.current_user['id']]

        if unread_only:
            where_clause += " AND n.read = FALSE"

        cursor.execute(f"""
            SELECT
                n.id, n.type, n.title, n.message, n.read, n.action_url,
                n.created_at, n.related_position_id,
                wp.title as position_title, wp.company_name
            FROM notifications n
            LEFT JOIN watchable_positions wp ON n.related_position_id = wp.id
            {where_clause}
            ORDER BY n.created_at DESC
            LIMIT %s
        """, params + [limit])

        notifications = []
        # Notification types that are for employers (company users)
        employer_notif_types = {'shortlist_ready', 'watcher', 'application'}
        for row in cursor.fetchall():
            notif_type = row[1]
            notifications.append({
                'id': row[0],
                'type': notif_type,
                'title': row[2],
                'message': row[3],
                'read': row[4],
                'action_url': row[5],
                'created_at': row[6].isoformat() if row[6] else None,
                'position_id': row[7],
                'position_title': row[8],
                'company_name': row[9],
                'forCompany': notif_type in employer_notif_types
            })

        # Get unread count
        cursor.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id = %s AND read = FALSE",
            (g.current_user['id'],)
        )
        unread_count = cursor.fetchone()[0]

    return jsonify({
        'notifications': notifications,
        'unread_count': unread_count
    })


@app.route('/api/notifications/<int:notification_id>/read', methods=['POST'])
@require_auth
def mark_notification_read(notification_id):
    """Mark notification as read."""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE notifications
                SET read = TRUE, read_at = CURRENT_TIMESTAMP
                WHERE id = %s AND user_id = %s
            """, (notification_id, g.current_user['id']))
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/notifications/read-all', methods=['POST'])
@require_auth
def mark_all_read():
    """Mark all notifications as read."""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE notifications
                SET read = TRUE, read_at = CURRENT_TIMESTAMP
                WHERE user_id = %s AND read = FALSE
            """, (g.current_user['id'],))
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ACTIVITY LOGGING
# ============================================================================

def log_activity(user_id: int, activity_type: str, **kwargs):
    """Log user activity."""
    try:
        conn = db.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO user_activity (user_id, activity_type, position_id, company_id, search_query, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                activity_type,
                kwargs.get('position_id'),
                kwargs.get('company_id'),
                kwargs.get('search_query'),
                json.dumps(kwargs.get('metadata', {}))
            ))
        conn.commit()
        db.release_connection(conn)
    except Exception as e:
        log.warning(f"Failed to log activity: {e}")


# ============================================================================
# RESUME PARSING
# ============================================================================

# Skills to look for in resumes - focused on SWE/Data/ML
SKILL_KEYWORDS = [
    # Programming Languages
    'JavaScript', 'TypeScript', 'Python', 'Java', 'Go', 'Rust', 'C++', 'C#', 'Scala',
    'Ruby', 'PHP', 'Swift', 'Kotlin', 'R', 'Julia', 'Haskell', 'Elixir',
    # Frontend
    'React', 'Vue.js', 'Angular', 'Next.js', 'Svelte', 'Redux', 'HTML', 'CSS', 'Tailwind',
    # Backend
    'Node.js', 'Django', 'Flask', 'FastAPI', 'Spring', 'Rails', '.NET', 'Express',
    # Data/ML
    'Machine Learning', 'Deep Learning', 'TensorFlow', 'PyTorch', 'Scikit-learn',
    'Pandas', 'NumPy', 'Data Science', 'NLP', 'Computer Vision', 'LLM', 'GPT',
    'Data Engineering', 'ETL', 'Spark', 'Hadoop', 'Airflow', 'dbt',
    # Cloud & Infrastructure
    'AWS', 'GCP', 'Azure', 'Docker', 'Kubernetes', 'Terraform', 'Linux',
    # Databases
    'PostgreSQL', 'MySQL', 'MongoDB', 'Redis', 'Elasticsearch', 'DynamoDB', 'Cassandra',
    # Other Tech
    'GraphQL', 'REST', 'API', 'CI/CD', 'Git', 'Kafka', 'RabbitMQ', 'gRPC',
    'Microservices', 'System Design', 'Distributed Systems',
    # Practices
    'Agile', 'Scrum', 'DevOps', 'TDD', 'Code Review',
    # Soft Skills (relevant)
    'Team Leadership', 'Technical Writing', 'Mentoring'
]

# Tech job title keywords for detecting relevant experience
TECH_TITLE_KEYWORDS = [
    'software', 'engineer', 'developer', 'programmer', 'swe', 'sde',
    'data', 'scientist', 'analyst', 'machine learning', 'ml', 'ai',
    'backend', 'frontend', 'full stack', 'fullstack', 'devops', 'sre',
    'architect', 'technical', 'platform', 'infrastructure', 'cloud',
    'research', 'applied scientist', 'quantitative', 'automation',
    'product', 'manager', 'designer', 'ux', 'ui', 'lead', 'director',
    'consultant', 'specialist', 'administrator', 'ops', 'security'
]


def clean_job_title(title: str) -> str:
    """Clean up extracted job title by removing dates, noise, etc."""
    if not title:
        return ''

    # Remove month names
    months = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b'
    title = re.sub(months, '', title, flags=re.IGNORECASE)

    # Remove years (4 digits)
    title = re.sub(r'\b(?:19|20)\d{2}\b', '', title)

    # Remove common separators and noise at the end
    title = re.sub(r'[\s\-–—|,]+$', '', title)

    # Remove common prefixes that aren't part of the title
    title = re.sub(r'^[\s\-–—|,•·]+', '', title)

    # Remove "Present" or "Current" if accidentally captured
    title = re.sub(r'\b(?:Present|Current)\b', '', title, flags=re.IGNORECASE)

    # Normalize whitespace
    title = re.sub(r'\s+', ' ', title).strip()

    return title


def extract_work_experiences(text: str) -> List[Dict[str, Any]]:
    """Extract work experiences from resume text."""
    experiences = []
    current_year = datetime.now().year

    # Common date patterns in resumes
    date_patterns = [
        # "Jan 2020 - Present", "January 2020 - Dec 2023"
        r'((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s*\d{4})\s*[-–—to]+\s*((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s*\d{4}|[Pp]resent|[Cc]urrent)',
        # "2020 - 2023", "2020 - Present"
        r'(\d{4})\s*[-–—to]+\s*(\d{4}|[Pp]resent|[Cc]urrent)',
        # "2020-Present"
        r'(\d{4})[-–]([Pp]resent|[Cc]urrent|\d{4})',
    ]

    # Find all date ranges
    date_matches = []
    for pattern in date_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            start_str = match.group(1)
            end_str = match.group(2)

            # Extract years
            start_year_match = re.search(r'\d{4}', start_str)
            start_year = int(start_year_match.group()) if start_year_match else None

            if end_str.lower() in ['present', 'current']:
                end_year = current_year
            else:
                end_year_match = re.search(r'\d{4}', end_str)
                end_year = int(end_year_match.group()) if end_year_match else None

            if start_year and end_year and 1980 <= start_year <= current_year and start_year <= end_year <= current_year + 1:
                date_matches.append({
                    'start_year': start_year,
                    'end_year': end_year,
                    'position': match.start(),
                    'text': match.group(0)
                })

    # For each date range, try to find the associated job title and company
    lines = text.split('\n')
    for date_info in date_matches:
        # Look at nearby text for job title and company
        pos = date_info['position']
        context_start = max(0, pos - 200)
        context_end = min(len(text), pos + 100)
        context = text[context_start:context_end]

        # Try to extract title - look for lines before the date
        title = ''
        # Get the text before the date on the same line or previous lines
        pre_date_text = text[context_start:pos]
        pre_date_lines = [l.strip() for l in pre_date_text.split('\n') if l.strip()]

        for line in reversed(pre_date_lines[-3:]):  # Check last 3 lines before date
            # Clean the line - remove dates, months, common separators
            cleaned = re.sub(
                r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b',
                '', line, flags=re.IGNORECASE
            )
            cleaned = re.sub(r'\b\d{4}\b', '', cleaned)  # Remove years
            cleaned = re.sub(r'[-–—|•·]', ' ', cleaned)  # Replace separators with space
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()  # Normalize whitespace

            # Check if this line contains a job title keyword
            for title_kw in TECH_TITLE_KEYWORDS:
                if title_kw.lower() in cleaned.lower():
                    # Extract the title portion - stop at company indicators
                    title_match = re.match(
                        r'^(.*?\b(?:' + '|'.join(TECH_TITLE_KEYWORDS) + r')\w*)\s*(?:at|@|,|$)',
                        cleaned, re.IGNORECASE
                    )
                    if title_match:
                        potential_title = title_match.group(1).strip()
                    else:
                        # Just take words around the keyword
                        potential_title = cleaned.split(',')[0].split(' at ')[0].split(' @ ')[0].strip()

                    # Clean up the title
                    potential_title = re.sub(r'^[^a-zA-Z]+', '', potential_title)  # Remove leading non-letters
                    potential_title = re.sub(r'[^a-zA-Z]+$', '', potential_title)  # Remove trailing non-letters

                    if 5 < len(potential_title) < 50 and potential_title[0].isupper():
                        title = clean_job_title(potential_title)
                        break
            if title:
                break

        # Try to extract company (often after "at" or on same/adjacent line)
        company = ''
        company_match = re.search(r'(?:at|@|,)\s*([A-Z][A-Za-z0-9\s&.,]+?)(?:\s*[-–|,\n]|$)', context)
        if company_match:
            company = company_match.group(1).strip()[:50]

        # Determine if this is a tech role
        is_tech = any(kw in title.lower() for kw in TECH_TITLE_KEYWORDS) if title else False

        experiences.append({
            'title': title,
            'company': company,
            'startYear': date_info['start_year'],
            'endYear': date_info['end_year'],
            'duration': date_info['end_year'] - date_info['start_year'],
            'isTechRole': is_tech
        })

    # Sort by start year (most recent first)
    experiences.sort(key=lambda x: x['startYear'], reverse=True)

    return experiences[:10]  # Limit to 10 most recent


def calculate_years_experience(experiences: List[Dict]) -> Dict[str, Any]:
    """Calculate total and tech-specific years of experience."""
    if not experiences:
        return {'total': 0, 'tech': 0, 'category': '0-2'}

    current_year = datetime.now().year

    # Find earliest start year for total experience
    earliest_year = min(exp['startYear'] for exp in experiences)
    total_years = current_year - earliest_year

    # Calculate tech-specific years (sum of durations for tech roles)
    tech_years = sum(exp['duration'] for exp in experiences if exp.get('isTechRole'))

    # Categorize
    if tech_years >= 10:
        category = '10+'
    elif tech_years >= 6:
        category = '6-10'
    elif tech_years >= 3:
        category = '3-5'
    else:
        category = '0-2'

    return {
        'total': total_years,
        'tech': tech_years,
        'category': category
    }


def parse_resume_text(text: str) -> Dict[str, Any]:
    """Extract structured data from resume text."""
    result = {
        'firstName': '',
        'lastName': '',
        'email': '',
        'phone': '',
        'currentTitle': '',
        'currentCompany': '',
        'skills': [],
        'experiences': [],
        'yearsExperience': {'total': 0, 'tech': 0, 'category': '0-2'},
        'rawText': text[:5000] if text else ''
    }

    if not text:
        return result

    # Extract work experiences first
    experiences = extract_work_experiences(text)
    result['experiences'] = experiences
    result['yearsExperience'] = calculate_years_experience(experiences)

    # Set current title/company from most recent experience if found
    if experiences:
        most_recent = experiences[0]
        if most_recent.get('title'):
            result['currentTitle'] = most_recent['title']
        if most_recent.get('company'):
            result['currentCompany'] = most_recent['company']

    # Extract email
    email_match = re.search(r'[\w.-]+@[\w.-]+\.\w+', text)
    if email_match:
        result['email'] = email_match.group(0)

    # Extract phone
    phone_match = re.search(r'(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', text)
    if phone_match:
        result['phone'] = phone_match.group(1)

    # Extract name from first non-empty lines
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    if lines:
        # First line is often the name
        first_line = lines[0]
        # Skip if it looks like a header/title
        if not any(word in first_line.lower() for word in ['resume', 'curriculum', 'vitae', 'cv']):
            name_parts = first_line.split()
            if 2 <= len(name_parts) <= 4 and all(len(p) < 20 for p in name_parts):
                # Check if it looks like a name (starts with capital, mostly letters)
                if all(p[0].isupper() and p.replace('-', '').replace("'", '').isalpha() for p in name_parts):
                    result['firstName'] = name_parts[0]
                    result['lastName'] = ' '.join(name_parts[1:])

    # If no title from experience, try pattern matching
    if not result['currentTitle']:
        title_patterns = [
            r'(?:^|\n)([A-Za-z\s]+(?:Engineer|Developer|Designer|Manager|Director|Lead|Architect|Analyst|Consultant|Specialist|Coordinator|Executive|Officer|Associate|VP|President|Scientist|Administrator))',
            r'(?:Title|Position|Role):\s*([^\n]+)',
            r'(?:^|\n)(?:Senior|Junior|Staff|Principal|Lead)\s+([A-Za-z\s]+)',
        ]
        for pattern in title_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                title = match.group(1).strip()
                if len(title) < 60:
                    result['currentTitle'] = title
                    break

    # If no company from experience, try pattern matching
    if not result['currentCompany']:
        company_patterns = [
            r'(?:at|@)\s+([A-Z][A-Za-z\s&.,]+?)(?:\s*[-–|,]|\n|$)',
            r'(?:Company|Employer):\s*([^\n]+)',
        ]
        for pattern in company_patterns:
            match = re.search(pattern, text)
            if match:
                company = match.group(1).strip()
                if len(company) < 60:
                    result['currentCompany'] = company
                    break

    # Extract skills
    text_lower = text.lower()
    found_skills = []
    for skill in SKILL_KEYWORDS:
        if skill.lower() in text_lower:
            found_skills.append(skill)
    result['skills'] = found_skills[:15]  # Limit to 15 skills

    return result


@app.route('/api/resume/parse', methods=['POST'])
def parse_resume():
    """Parse uploaded resume file and extract structured data."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400

    filename = file.filename.lower()

    # Handle plain text files
    if filename.endswith('.txt'):
        try:
            text = file.read().decode('utf-8')
            result = parse_resume_text(text)
            return jsonify({'success': True, 'data': result})
        except Exception as e:
            log.error(f"Text parsing error: {e}")
            return jsonify({'error': 'Failed to parse text file'}), 500

    # Handle PDF files
    if filename.endswith('.pdf'):
        if not PDF_SUPPORT:
            return jsonify({
                'error': 'PDF parsing not available. Please install pdfplumber: pip install pdfplumber'
            }), 501

        try:
            # Save to temp file for pdfplumber
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                file.save(tmp.name)
                tmp_path = tmp.name

            try:
                text_parts = []
                with pdfplumber.open(tmp_path) as pdf:
                    for page in pdf.pages[:10]:  # Limit to first 10 pages
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)

                full_text = '\n'.join(text_parts)
                result = parse_resume_text(full_text)
                return jsonify({'success': True, 'data': result})
            finally:
                # Clean up temp file
                os.unlink(tmp_path)

        except Exception as e:
            log.error(f"PDF parsing error: {e}")
            return jsonify({'error': f'Failed to parse PDF: {str(e)}'}), 500

    # Handle Word documents
    if filename.endswith(('.doc', '.docx')):
        return jsonify({
            'error': 'Word document parsing not yet supported. Please upload a PDF or TXT file.'
        }), 501

    return jsonify({'error': 'Unsupported file type. Please upload PDF or TXT.'}), 400


@app.route('/api/resume/upload', methods=['POST'])
@require_auth
def upload_resume():
    """
    Upload a resume file and store it.
    Returns the URL for use in shortlist applications.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400

    filename = file.filename.lower()

    # Validate file type
    allowed_extensions = ['.pdf', '.txt', '.doc', '.docx']
    if not any(filename.endswith(ext) for ext in allowed_extensions):
        return jsonify({'error': 'Unsupported file type. Please upload PDF, DOC, DOCX, or TXT.'}), 400

    # Validate file size (max 5MB)
    file.seek(0, 2)  # Seek to end
    size = file.tell()
    file.seek(0)  # Seek back to start
    if size > 5 * 1024 * 1024:
        return jsonify({'error': 'File size must be less than 5MB'}), 400

    user_id = g.current_user['id']

    # Create uploads directory if it doesn't exist
    uploads_dir = os.path.join(os.path.dirname(__file__), '..', 'uploads', 'resumes')
    os.makedirs(uploads_dir, exist_ok=True)

    # Generate unique filename
    ext = os.path.splitext(file.filename)[1]
    safe_filename = f"resume_{user_id}_{secrets.token_hex(8)}{ext}"
    file_path = os.path.join(uploads_dir, safe_filename)

    try:
        file.save(file_path)

        # Generate URL (in production this would be a CDN URL)
        resume_url = f"/uploads/resumes/{safe_filename}"

        # Update user's profile with resume URL
        conn = get_db()
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE seeker_profiles
                SET resume_url = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
            """, (resume_url, user_id))
        conn.commit()

        # Also parse the resume if it's a PDF
        parsed_data = None
        if filename.endswith('.pdf') and PDF_SUPPORT:
            try:
                text_parts = []
                with pdfplumber.open(file_path) as pdf:
                    for page in pdf.pages[:10]:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                full_text = '\n'.join(text_parts)
                parsed_data = parse_resume_text(full_text)
            except Exception as e:
                log.warning(f"Failed to parse uploaded PDF: {e}")

        return jsonify({
            'success': True,
            'resume_url': resume_url,
            'filename': file.filename,
            'parsed': parsed_data
        })

    except Exception as e:
        log.error(f"Resume upload error: {e}")
        return jsonify({'error': 'Failed to upload resume'}), 500


@app.route('/uploads/resumes/<filename>')
def serve_resume(filename):
    """Serve uploaded resume files."""
    uploads_dir = os.path.join(os.path.dirname(__file__), '..', 'uploads', 'resumes')
    return send_from_directory(uploads_dir, filename)


# ============================================================================
# SHORTLIST ENDPOINTS
# ============================================================================

# Valid work authorization options
WORK_AUTH_OPTIONS = [
    'us_citizen',
    'permanent_resident',
    'f1_opt',
    'f1_cpt',
    'h1b',
    'needs_sponsorship',
    'other'
]

# Valid experience levels
EXPERIENCE_LEVELS = ['intern', 'new_grad', 'entry', 'mid', 'senior', 'staff']


@app.route('/api/roles/<int:position_id>', methods=['GET'])
def get_role_page(position_id):
    """
    Get role page details for the "Join the Shortlist" flow.
    Returns role info + whether it's monitored + any existing application.
    """
    conn = get_db()
    with conn.cursor() as cursor:
        # Get position with monitoring info
        cursor.execute("""
            SELECT
                wp.id, wp.company_name, wp.title, wp.department, wp.location,
                wp.salary_range, wp.employment_type, wp.work_arrangement,
                wp.description, wp.status, wp.experience_level,
                wp.min_years_experience, wp.max_years_experience,
                wp.required_skills, wp.is_monitored, wp.data_source,
                c.id as company_id
            FROM watchable_positions wp
            LEFT JOIN companies c ON wp.company_id = c.id
            WHERE wp.id = %s
        """, (position_id,))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Role not found'}), 404

        role = {
            'id': row[0],
            'company_name': row[1],
            'title': row[2],
            'department': row[3],
            'location': row[4],
            'salary_range': row[5],
            'employment_type': row[6],
            'work_arrangement': row[7],
            'description': row[8],
            'status': row[9],
            'experience_level': row[10],
            'min_years_experience': row[11],
            'max_years_experience': row[12],
            'required_skills': row[13] or [],
            'is_monitored': row[14] or False,
            'data_source': row[15] or 'unknown',
            'company_id': row[16],
            # Can only promise notifications if monitored
            'can_notify_on_open': row[14] or False
        }

        # Get role configuration if exists
        cursor.execute("""
            SELECT require_work_auth, allowed_work_auth, require_experience_level,
                   allowed_experience_levels, min_grad_year, max_grad_year
            FROM role_configurations WHERE position_id = %s
        """, (position_id,))
        config_row = cursor.fetchone()
        if config_row:
            role['requirements'] = {
                'require_work_auth': config_row[0],
                'allowed_work_auth': config_row[1],
                'require_experience_level': config_row[2],
                'allowed_experience_levels': config_row[3],
                'min_grad_year': config_row[4],
                'max_grad_year': config_row[5]
            }

        # Check if current user already applied
        user = get_current_user()
        if user:
            cursor.execute(
                "SELECT id, status, created_at FROM shortlist_applications WHERE user_id = %s AND position_id = %s",
                (user['id'], position_id)
            )
            app_row = cursor.fetchone()
            if app_row:
                role['user_application'] = {
                    'id': app_row[0],
                    'status': app_row[1],
                    'applied_at': app_row[2].isoformat() if app_row[2] else None
                }

        # Get shortlist count (for social proof)
        cursor.execute(
            "SELECT COUNT(*) FROM shortlist_applications WHERE position_id = %s",
            (position_id,)
        )
        role['shortlist_count'] = cursor.fetchone()[0]

    return jsonify({'role': role})


@app.route('/api/shortlist/apply', methods=['POST'])
@require_auth
def join_shortlist():
    """
    Join the shortlist for a role.

    Required fields:
    - position_id: ID of the role
    - work_authorization: Work auth status
    - start_availability: When they can start (YYYY-MM-DD)
    - project_response: Answer to project question
    - fit_response: Answer to fit question

    One of:
    - resume_url: URL to uploaded resume
    - linkedin_url: LinkedIn profile URL

    Optional:
    - grad_year: Graduation year
    - experience_level: intern/new_grad/entry/mid/senior/staff
    - years_of_experience: Number of years
    - work_auth_details: Additional details for "other" work auth
    - availability_notes: Additional availability info
    """
    data = request.json
    user = g.current_user

    # Validate required fields
    required_fields = ['position_id', 'work_authorization', 'project_response', 'fit_response']
    missing = [f for f in required_fields if not data.get(f)]
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400

    # Validate work authorization
    work_auth = data['work_authorization']
    if work_auth not in WORK_AUTH_OPTIONS:
        return jsonify({'error': f'Invalid work_authorization. Must be one of: {", ".join(WORK_AUTH_OPTIONS)}'}), 400

    # Resume or LinkedIn (optional - they can add later)
    resume_url = data.get('resume_url')
    linkedin_url = data.get('linkedin_url')

    # Validate experience level if provided
    exp_level = data.get('experience_level')
    if exp_level and exp_level not in EXPERIENCE_LEVELS:
        return jsonify({'error': f'Invalid experience_level. Must be one of: {", ".join(EXPERIENCE_LEVELS)}'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Check position exists
            cursor.execute("SELECT id, status FROM watchable_positions WHERE id = %s", (data['position_id'],))
            pos_row = cursor.fetchone()
            if not pos_row:
                return jsonify({'error': 'Role not found'}), 404

            # Check if already applied
            cursor.execute(
                "SELECT id FROM shortlist_applications WHERE user_id = %s AND position_id = %s",
                (user['id'], data['position_id'])
            )
            if cursor.fetchone():
                return jsonify({'error': 'You have already joined this shortlist'}), 409

            # Insert application
            cursor.execute("""
                INSERT INTO shortlist_applications (
                    user_id, position_id, resume_url, linkedin_url,
                    work_authorization, work_auth_details, grad_year,
                    experience_level, years_of_experience, start_availability,
                    availability_notes, project_response, fit_response,
                    status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                RETURNING id
            """, (
                user['id'],
                data['position_id'],
                resume_url,
                linkedin_url,
                work_auth,
                data.get('work_auth_details'),
                data.get('grad_year'),
                exp_level,
                data.get('years_of_experience'),
                data['start_availability'],
                data.get('availability_notes'),
                data['project_response'],
                data['fit_response']
            ))
            application_id = cursor.fetchone()[0]

            # Fetch position and role config for screening
            cursor.execute("""
                SELECT title, company_name, location, description
                FROM watchable_positions WHERE id = %s
            """, (data['position_id'],))
            pos_data = cursor.fetchone()
            position = {
                'title': pos_data[0],
                'company_name': pos_data[1],
                'location': pos_data[2],
                'description': pos_data[3]
            }

            cursor.execute("""
                SELECT
                    require_work_auth, allowed_work_auth,
                    require_experience_level, allowed_experience_levels,
                    min_grad_year, max_grad_year,
                    required_skills, score_threshold
                FROM role_configurations
                WHERE position_id = %s
            """, (data['position_id'],))
            config_row = cursor.fetchone()
            role_config = None
            if config_row:
                role_config = {
                    'require_work_auth': config_row[0],
                    'allowed_work_auth': config_row[1],
                    'require_experience_level': config_row[2],
                    'allowed_experience_levels': config_row[3],
                    'min_grad_year': config_row[4],
                    'max_grad_year': config_row[5],
                    'required_skills': config_row[6],
                    'score_threshold': config_row[7],
                }

            # Run screening (Step A: must-haves, Step B: AI ranking)
            screening_result = None
            if SCREENING_AVAILABLE:
                try:
                    application_data = {
                        'id': application_id,
                        'work_authorization': work_auth,
                        'grad_year': data.get('grad_year'),
                        'experience_level': exp_level,
                        'start_availability': data['start_availability'],
                        'project_response': data['project_response'],
                        'fit_response': data['fit_response'],
                        'resume_url': resume_url,
                        'linkedin_url': linkedin_url
                    }

                    screening_result = screen_application(
                        application_data, role_config, position,
                        run_ai_ranking=True
                    )

                    # Update application with screening results
                    new_status = 'pending'
                    if not screening_result.passed:
                        new_status = 'rejected'
                    elif screening_result.ai_score is not None:
                        new_status = 'qualified'
                    else:
                        new_status = 'screened'

                    cursor.execute("""
                        UPDATE shortlist_applications
                        SET
                            screening_passed = %s,
                            screening_fail_reason = %s,
                            ai_score = %s,
                            ai_strengths = %s,
                            ai_concern = %s,
                            ai_scored_at = CASE WHEN %s IS NOT NULL THEN CURRENT_TIMESTAMP ELSE NULL END,
                            status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (
                        screening_result.passed,
                        screening_result.fail_reason,
                        screening_result.ai_score,
                        screening_result.ai_strengths if screening_result.ai_strengths else None,
                        screening_result.ai_concern,
                        screening_result.ai_score,
                        new_status,
                        application_id
                    ))
                except Exception as e:
                    log.error(f"Screening error (application will be reviewed manually): {e}")

        conn.commit()

        # Build response
        response = {
            'success': True,
            'application_id': application_id,
            'message': "You're on the shortlist. This is not a formal application. If this role opens, we'll notify you, and the employer can review your profile if you meet their requirements."
        }

        # Add screening info if available (don't show rejection reason to candidate)
        if screening_result:
            if not screening_result.passed:
                response['screening_status'] = 'not_eligible'
                response['message'] = "Thanks for your interest. Based on the role requirements, you may not be eligible for this position. We'll keep your information on file."
            elif screening_result.ai_score is not None:
                response['screening_status'] = 'qualified'
            else:
                response['screening_status'] = 'pending_review'

        return jsonify(response), 201

    except Exception as e:
        conn.rollback()
        log.error(f"Shortlist application error: {e}")
        return jsonify({'error': 'Failed to join shortlist'}), 500


@app.route('/api/shortlist/my-applications', methods=['GET'])
@require_auth
def get_my_applications():
    """Get all shortlist applications for current user."""
    user = g.current_user
    conn = get_db()

    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                sa.id, sa.position_id, sa.status, sa.screening_passed,
                sa.ai_score, sa.notified_role_opened, sa.created_at,
                wp.company_name, wp.title, wp.location, wp.status as role_status,
                wp.is_monitored
            FROM shortlist_applications sa
            JOIN watchable_positions wp ON sa.position_id = wp.id
            WHERE sa.user_id = %s
            ORDER BY sa.created_at DESC
        """, (user['id'],))

        applications = []
        for row in cursor.fetchall():
            applications.append({
                'id': row[0],
                'position_id': row[1],
                'status': row[2],
                'screening_passed': row[3],
                'ai_score': row[4],
                'notified_role_opened': row[5],
                'applied_at': row[6].isoformat() if row[6] else None,
                'company_name': row[7],
                'title': row[8],
                'location': row[9],
                'role_status': row[10],
                'is_monitored': row[11]
            })

    return jsonify({'applications': applications, 'count': len(applications)})


@app.route('/api/shortlist/<int:application_id>', methods=['DELETE'])
@require_auth
def withdraw_application(application_id):
    """Withdraw from a shortlist."""
    user = g.current_user
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM shortlist_applications
                WHERE id = %s AND user_id = %s
                RETURNING id
            """, (application_id, user['id']))

            if not cursor.fetchone():
                return jsonify({'error': 'Application not found'}), 404

        conn.commit()
        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        log.error(f"Withdraw application error: {e}")
        return jsonify({'error': 'Failed to withdraw'}), 500


# ============================================================================
# EMPLOYER SHORTLIST ENDPOINTS
# ============================================================================

@app.route('/api/employer/roles', methods=['GET'])
@require_company
def get_employer_roles():
    """
    Get all roles for employer with shortlist stats.
    Company-level dashboard view.
    """
    conn = get_db()
    with conn.cursor() as cursor:
        # Get company profile
        cursor.execute("""
            SELECT cp.id, cp.company_id, cp.company_name
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company not found'}), 404

        company_profile_id, company_id, company_name = row

        # Get all positions with shortlist stats
        cursor.execute("""
            SELECT
                wp.id, wp.title, wp.location, wp.status, wp.is_monitored,
                COUNT(sa.id) as total_applicants,
                COUNT(sa.id) FILTER (WHERE sa.screening_passed = TRUE) as passed_screening,
                COUNT(sa.id) FILTER (WHERE sa.screening_passed = TRUE AND sa.ai_score >= COALESCE(rc.score_threshold, 70)) as meets_threshold
            FROM watchable_positions wp
            LEFT JOIN shortlist_applications sa ON wp.id = sa.position_id
            LEFT JOIN role_configurations rc ON wp.id = rc.position_id
            WHERE wp.company_profile_id = %s OR wp.company_id = %s
            GROUP BY wp.id, wp.title, wp.location, wp.status, wp.is_monitored, rc.score_threshold
            ORDER BY wp.status = 'open' DESC, total_applicants DESC
        """, (company_profile_id, company_id))

        roles = []
        for row in cursor.fetchall():
            roles.append({
                'id': row[0],
                'title': row[1],
                'location': row[2],
                'status': row[3],
                'is_monitored': row[4],
                'total_applicants': row[5],
                'passed_screening': row[6],
                'meets_threshold': row[7]
            })

    return jsonify({
        'company_name': company_name,
        'roles': roles,
        'total_roles': len(roles)
    })


@app.route('/api/employer/roles/<int:position_id>/shortlist', methods=['GET'])
@require_company
def get_role_shortlist(position_id):
    """
    Get ranked shortlist for a specific role.
    Role-level dashboard view.
    """
    conn = get_db()

    # Query params for filtering
    show_all = request.args.get('show_all', 'false').lower() == 'true'
    min_score = request.args.get('min_score', type=int)

    with conn.cursor() as cursor:
        # Verify ownership
        cursor.execute("""
            SELECT cp.id, cp.company_id
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company not found'}), 404

        company_profile_id, company_id = row

        # Verify position belongs to company
        cursor.execute("""
            SELECT id, title, status FROM watchable_positions
            WHERE id = %s AND (company_profile_id = %s OR company_id = %s)
        """, (position_id, company_profile_id, company_id))

        pos_row = cursor.fetchone()
        if not pos_row:
            return jsonify({'error': 'Role not found or access denied'}), 404

        # Get role configuration
        cursor.execute("SELECT score_threshold, volume_cap FROM role_configurations WHERE position_id = %s", (position_id,))
        config_row = cursor.fetchone()
        score_threshold = config_row[0] if config_row else 70
        volume_cap = config_row[1] if config_row else None

        # Build query for shortlist
        where_clauses = ["sa.position_id = %s"]
        params = [position_id]

        if not show_all:
            where_clauses.append("sa.screening_passed = TRUE")
            effective_min_score = min_score if min_score is not None else score_threshold
            where_clauses.append("sa.ai_score >= %s")
            params.append(effective_min_score)

        where_sql = "WHERE " + " AND ".join(where_clauses)

        limit_sql = ""
        if volume_cap and not show_all:
            limit_sql = f"LIMIT {volume_cap}"

        cursor.execute(f"""
            SELECT
                sa.id, sa.user_id, pu.first_name, pu.last_name, pu.email,
                sa.resume_url, sa.linkedin_url, sa.work_authorization,
                sa.grad_year, sa.experience_level, sa.start_availability,
                sa.project_response, sa.fit_response,
                sa.screening_passed, sa.screening_fail_reason,
                sa.ai_score, sa.ai_strengths, sa.ai_concern,
                sa.status, sa.employer_notes, sa.reviewed_at, sa.created_at
            FROM shortlist_applications sa
            JOIN platform_users pu ON sa.user_id = pu.id
            {where_sql}
            ORDER BY sa.ai_score DESC NULLS LAST, sa.created_at ASC
            {limit_sql}
        """, params)

        candidates = []
        for row in cursor.fetchall():
            candidates.append({
                'application_id': row[0],
                'user_id': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'email': row[4],
                'resume_url': row[5],
                'linkedin_url': row[6],
                'work_authorization': row[7],
                'grad_year': row[8],
                'experience_level': row[9],
                'start_availability': row[10].isoformat() if row[10] else None,
                'project_response': row[11],
                'fit_response': row[12],
                'screening_passed': row[13],
                'screening_fail_reason': row[14],
                'ai_score': row[15],
                'ai_strengths': row[16],
                'ai_concern': row[17],
                'status': row[18],
                'employer_notes': row[19],
                'reviewed_at': row[20].isoformat() if row[20] else None,
                'applied_at': row[21].isoformat() if row[21] else None
            })

        # Get total counts for stats
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE screening_passed = TRUE) as passed,
                COUNT(*) FILTER (WHERE screening_passed = TRUE AND ai_score >= %s) as qualified
            FROM shortlist_applications WHERE position_id = %s
        """, (score_threshold, position_id))
        stats_row = cursor.fetchone()

    return jsonify({
        'role': {
            'id': pos_row[0],
            'title': pos_row[1],
            'status': pos_row[2]
        },
        'config': {
            'score_threshold': score_threshold,
            'volume_cap': volume_cap
        },
        'stats': {
            'total_applicants': stats_row[0],
            'passed_screening': stats_row[1],
            'meets_threshold': stats_row[2]
        },
        'candidates': candidates,
        'showing_count': len(candidates)
    })


@app.route('/api/employer/roles/<int:position_id>/config', methods=['PUT'])
@require_company
def update_role_config(position_id):
    """Update role configuration (must-haves, threshold, volume cap)."""
    data = request.json
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            # Verify ownership
            cursor.execute("""
                SELECT cp.id, cp.company_id
                FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.current_user['id'],))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Company not found'}), 404

            company_profile_id, company_id = row

            # Verify position
            cursor.execute("""
                SELECT id FROM watchable_positions
                WHERE id = %s AND (company_profile_id = %s OR company_id = %s)
            """, (position_id, company_profile_id, company_id))

            if not cursor.fetchone():
                return jsonify({'error': 'Role not found or access denied'}), 404

            # Upsert configuration
            cursor.execute("""
                INSERT INTO role_configurations (
                    position_id, require_work_auth, allowed_work_auth,
                    require_experience_level, allowed_experience_levels,
                    min_grad_year, max_grad_year, required_skills,
                    score_threshold, volume_cap
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (position_id) DO UPDATE SET
                    require_work_auth = EXCLUDED.require_work_auth,
                    allowed_work_auth = EXCLUDED.allowed_work_auth,
                    require_experience_level = EXCLUDED.require_experience_level,
                    allowed_experience_levels = EXCLUDED.allowed_experience_levels,
                    min_grad_year = EXCLUDED.min_grad_year,
                    max_grad_year = EXCLUDED.max_grad_year,
                    required_skills = EXCLUDED.required_skills,
                    score_threshold = EXCLUDED.score_threshold,
                    volume_cap = EXCLUDED.volume_cap,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                position_id,
                data.get('require_work_auth', False),
                data.get('allowed_work_auth', []),
                data.get('require_experience_level', False),
                data.get('allowed_experience_levels', []),
                data.get('min_grad_year'),
                data.get('max_grad_year'),
                data.get('required_skills', []),
                data.get('score_threshold', 70),
                data.get('volume_cap')
            ))

        conn.commit()
        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        log.error(f"Update role config error: {e}")
        return jsonify({'error': 'Failed to update configuration'}), 500


@app.route('/api/employer/applications/<int:application_id>/review', methods=['POST'])
@require_company
def mark_application_reviewed(application_id):
    """Mark an application as reviewed and optionally add notes."""
    data = request.json
    conn = get_db()

    try:
        with conn.cursor() as cursor:
            # Get company info for ownership check
            cursor.execute("""
                SELECT cp.id, cp.company_id
                FROM company_profiles cp
                JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
                WHERE ctm.user_id = %s
            """, (g.current_user['id'],))

            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Company not found'}), 404

            company_profile_id, company_id = row

            # Update application (verify it belongs to company's role)
            cursor.execute("""
                UPDATE shortlist_applications sa
                SET status = 'reviewed',
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = %s,
                    employer_notes = COALESCE(%s, employer_notes)
                FROM watchable_positions wp
                WHERE sa.id = %s
                  AND sa.position_id = wp.id
                  AND (wp.company_profile_id = %s OR wp.company_id = %s)
                RETURNING sa.id
            """, (
                g.current_user['id'],
                data.get('notes'),
                application_id,
                company_profile_id,
                company_id
            ))

            if not cursor.fetchone():
                return jsonify({'error': 'Application not found or access denied'}), 404

        conn.commit()
        return jsonify({'success': True})

    except Exception as e:
        conn.rollback()
        log.error(f"Review application error: {e}")
        return jsonify({'error': 'Failed to update'}), 500


@app.route('/api/employer/roles/<int:position_id>/export', methods=['GET'])
@require_company
def export_shortlist(position_id):
    """Export shortlist as CSV."""
    import csv
    import io

    conn = get_db()

    with conn.cursor() as cursor:
        # Verify ownership
        cursor.execute("""
            SELECT cp.id, cp.company_id
            FROM company_profiles cp
            JOIN company_team_members ctm ON cp.id = ctm.company_profile_id
            WHERE ctm.user_id = %s
        """, (g.current_user['id'],))

        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Company not found'}), 404

        company_profile_id, company_id = row

        # Get position info
        cursor.execute("""
            SELECT title FROM watchable_positions
            WHERE id = %s AND (company_profile_id = %s OR company_id = %s)
        """, (position_id, company_profile_id, company_id))

        pos_row = cursor.fetchone()
        if not pos_row:
            return jsonify({'error': 'Role not found or access denied'}), 404

        role_title = pos_row[0]

        # Get qualified candidates
        cursor.execute("""
            SELECT
                pu.first_name, pu.last_name, pu.email,
                sa.resume_url, sa.linkedin_url, sa.work_authorization,
                sa.grad_year, sa.experience_level, sa.start_availability,
                sa.ai_score, sa.ai_strengths, sa.ai_concern,
                sa.project_response, sa.fit_response, sa.created_at
            FROM shortlist_applications sa
            JOIN platform_users pu ON sa.user_id = pu.id
            LEFT JOIN role_configurations rc ON sa.position_id = rc.position_id
            WHERE sa.position_id = %s
              AND sa.screening_passed = TRUE
              AND sa.ai_score >= COALESCE(rc.score_threshold, 70)
            ORDER BY sa.ai_score DESC
        """, (position_id,))

        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow([
            'First Name', 'Last Name', 'Email', 'Resume URL', 'LinkedIn',
            'Work Auth', 'Grad Year', 'Experience Level', 'Start Availability',
            'AI Score', 'Strengths', 'Concern', 'Project Response', 'Fit Response',
            'Applied At'
        ])

        # Data rows
        for row in cursor.fetchall():
            writer.writerow([
                row[0], row[1], row[2], row[3], row[4],
                row[5], row[6], row[7],
                row[8].isoformat() if row[8] else '',
                row[9],
                '; '.join(row[10]) if row[10] else '',
                row[11],
                row[12][:200] if row[12] else '',  # Truncate long responses
                row[13][:200] if row[13] else '',
                row[14].isoformat() if row[14] else ''
            ])

    # Return CSV response
    from flask import Response
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=shortlist_{role_title.replace(" ", "_")}_{datetime.now().strftime("%Y%m%d")}.csv'}
    )


# ============================================================================
# SCREENING ENDPOINTS
# ============================================================================

@app.route('/api/employer/roles/<int:position_id>/rescreen', methods=['POST'])
@require_auth
def rescreen_role_applications(position_id):
    """
    Re-screen all applications for a role.

    Useful after updating role configurations (must-haves, threshold).
    Only re-screens applications that haven't been manually reviewed/contacted.
    """
    user = g.current_user
    if user.get('user_type') != 'employer':
        return jsonify({'error': 'Employer access required'}), 403

    if not SCREENING_AVAILABLE:
        return jsonify({'error': 'Screening module not available'}), 503

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Verify employer owns this position
            cursor.execute("""
                SELECT wp.id, wp.title, wp.company_name
                FROM watchable_positions wp
                LEFT JOIN company_profiles cp ON wp.company_profile_id = cp.id
                WHERE wp.id = %s
                  AND (cp.user_id = %s OR wp.company_id IN (
                      SELECT company_id FROM company_profiles WHERE user_id = %s
                  ))
            """, (position_id, user['id'], user['id']))

            pos_row = cursor.fetchone()
            if not pos_row:
                return jsonify({'error': 'Position not found or access denied'}), 404

            position = {
                'id': pos_row[0],
                'title': pos_row[1],
                'company_name': pos_row[2]
            }

            # Get role config
            cursor.execute("""
                SELECT
                    require_work_auth, allowed_work_auth,
                    require_experience_level, allowed_experience_levels,
                    min_grad_year, max_grad_year,
                    required_skills, score_threshold
                FROM role_configurations
                WHERE position_id = %s
            """, (position_id,))
            config_row = cursor.fetchone()
            role_config = None
            if config_row:
                role_config = {
                    'require_work_auth': config_row[0],
                    'allowed_work_auth': config_row[1],
                    'require_experience_level': config_row[2],
                    'allowed_experience_levels': config_row[3],
                    'min_grad_year': config_row[4],
                    'max_grad_year': config_row[5],
                    'required_skills': config_row[6],
                    'score_threshold': config_row[7],
                }

            # Get applications to rescreen (not manually reviewed/contacted)
            cursor.execute("""
                SELECT id FROM shortlist_applications
                WHERE position_id = %s
                  AND status NOT IN ('reviewed', 'contacted', 'archived')
            """, (position_id,))

            application_ids = [row[0] for row in cursor.fetchall()]

            if not application_ids:
                return jsonify({
                    'success': True,
                    'message': 'No applications to rescreen',
                    'rescreened': 0
                })

            # Rescreen each application
            rescreened = 0
            for app_id in application_ids:
                try:
                    rescreen_application(cursor, app_id)
                    rescreened += 1
                except Exception as e:
                    log.error(f"Error rescreening application {app_id}: {e}")

        conn.commit()

        return jsonify({
            'success': True,
            'message': f'Rescreened {rescreened} applications',
            'rescreened': rescreened
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Rescreen error: {e}")
        return jsonify({'error': 'Failed to rescreen applications'}), 500


@app.route('/api/employer/applications/<int:application_id>/rescreen', methods=['POST'])
@require_auth
def rescreen_single_application(application_id):
    """Re-screen a single application."""
    user = g.current_user
    if user.get('user_type') != 'employer':
        return jsonify({'error': 'Employer access required'}), 403

    if not SCREENING_AVAILABLE:
        return jsonify({'error': 'Screening module not available'}), 503

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # Verify employer owns the position for this application
            cursor.execute("""
                SELECT sa.id, sa.position_id
                FROM shortlist_applications sa
                JOIN watchable_positions wp ON sa.position_id = wp.id
                LEFT JOIN company_profiles cp ON wp.company_profile_id = cp.id
                WHERE sa.id = %s
                  AND (cp.user_id = %s OR wp.company_id IN (
                      SELECT company_id FROM company_profiles WHERE user_id = %s
                  ))
            """, (application_id, user['id'], user['id']))

            if not cursor.fetchone():
                return jsonify({'error': 'Application not found or access denied'}), 404

            # Rescreen
            result = rescreen_application(cursor, application_id)

        conn.commit()

        return jsonify({
            'success': True,
            'screening_result': result.to_dict()
        })

    except Exception as e:
        conn.rollback()
        log.error(f"Rescreen error: {e}")
        return jsonify({'error': 'Failed to rescreen application'}), 500


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'pdf_support': PDF_SUPPORT,
        'screening_available': SCREENING_AVAILABLE
    })


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    # Initialize database pool
    db.initialize_pool()

    # Run app (port 5001 to avoid macOS AirPlay on 5000)
    app.run(host='0.0.0.0', port=5001, debug=True)
