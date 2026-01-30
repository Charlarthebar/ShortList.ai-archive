"""
Weekly Digest Service for ShortList
Generates and sends weekly bench update emails to employers.
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

# SendGrid import (optional - graceful fallback if not installed)
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email, To, Content
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False
    print("SendGrid not installed. Email sending disabled.")

# Configuration
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
FROM_EMAIL = os.environ.get('DIGEST_FROM_EMAIL', 'alerts@shortlist.ai')
APP_URL = os.environ.get('APP_URL', 'http://localhost:5002')


def get_db():
    """Get database connection."""
    return psycopg2.connect(os.environ.get('DATABASE_URL'))


def get_companies_for_digest() -> List[Dict]:
    """
    Get all companies that should receive a digest this week.
    Returns companies with at least one role that has new high-scoring candidates.
    """
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get companies with digest-worthy updates
        cur.execute("""
            SELECT DISTINCT
                cp.id as company_id,
                cp.company_name,
                pu.email as recipient_email,
                pu.first_name,
                COALESCE(edp.digest_enabled, TRUE) as digest_enabled,
                COALESCE(edp.min_score_threshold, 80) as min_score_threshold
            FROM company_profiles cp
            JOIN company_team_members ctm ON ctm.company_profile_id = cp.id
            JOIN platform_users pu ON pu.id = ctm.user_id
            LEFT JOIN employer_digest_preferences edp ON edp.company_profile_id = cp.id AND edp.user_id = pu.id
            WHERE pu.email IS NOT NULL
              AND COALESCE(edp.digest_enabled, TRUE) = TRUE
        """)
        return [dict(row) for row in cur.fetchall()]


def get_digest_data_for_company(company_id: int, min_score: int = 80) -> Dict:
    """
    Get digest data for a specific company.
    Returns roles with new high-scoring candidates since last week.
    """
    conn = get_db()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get roles with new candidates above threshold in the last 7 days
        cur.execute("""
            SELECT
                wp.id as role_id,
                wp.title as role_title,
                COUNT(sa.id) as new_candidate_count
            FROM watchable_positions wp
            JOIN shortlist_applications sa ON sa.position_id = wp.id
            WHERE wp.company_profile_id = %s
              AND sa.fit_score >= %s
              AND sa.hard_filter_failed = FALSE
              AND sa.applied_at >= NOW() - INTERVAL '7 days'
              AND sa.status != 'cancelled'
            GROUP BY wp.id, wp.title
            HAVING COUNT(sa.id) > 0
            ORDER BY COUNT(sa.id) DESC
        """, (company_id, min_score))

        roles_with_updates = []
        for role in cur.fetchall():
            # Get top candidates for this role
            cur.execute("""
                SELECT
                    sa.id as application_id,
                    TRIM(CONCAT(pu.first_name, ' ', pu.last_name)) as full_name,
                    sa.fit_score,
                    sp.extracted_profile,
                    ci.why_this_person
                FROM shortlist_applications sa
                JOIN platform_users pu ON pu.id = sa.user_id
                LEFT JOIN seeker_profiles sp ON sp.user_id = sa.user_id
                LEFT JOIN candidate_insights ci ON ci.application_id = sa.id
                WHERE sa.position_id = %s
                  AND sa.fit_score >= %s
                  AND sa.hard_filter_failed = FALSE
                  AND sa.applied_at >= NOW() - INTERVAL '7 days'
                  AND sa.status != 'cancelled'
                ORDER BY sa.fit_score DESC
                LIMIT 5
            """, (role['role_id'], min_score))

            candidates = []
            for row in cur.fetchall():
                candidate = dict(row)
                # Extract current position from profile
                profile = candidate.get('extracted_profile')
                if profile:
                    if isinstance(profile, str):
                        try:
                            profile = json.loads(profile)
                        except:
                            profile = {}
                    work_history = profile.get('work_experience', [])
                    if work_history and len(work_history) > 0:
                        current = work_history[0]
                        candidate['current_position'] = current.get('title', '')
                        candidate['current_company'] = current.get('company', '')
                    else:
                        candidate['current_position'] = ''
                        candidate['current_company'] = ''
                else:
                    candidate['current_position'] = ''
                    candidate['current_company'] = ''

                del candidate['extracted_profile']
                candidates.append(candidate)

            roles_with_updates.append({
                'role_id': role['role_id'],
                'role_title': role['role_title'],
                'new_candidate_count': role['new_candidate_count'],
                'candidates': candidates
            })

        return {
            'roles': roles_with_updates,
            'has_updates': len(roles_with_updates) > 0
        }


def generate_digest_html(company_name: str, recipient_name: str, digest_data: Dict) -> str:
    """
    Generate HTML email content for the weekly digest.
    """
    roles = digest_data.get('roles', [])

    if not roles:
        return None  # Don't send empty digests

    # Build role sections
    role_sections = []
    for role in roles:
        candidates_html = []
        for c in role['candidates']:
            position_text = ''
            if c.get('current_position'):
                position_text = c['current_position']
                if c.get('current_company'):
                    position_text += f" at {c['current_company']}"

            ai_note = c.get('why_this_person', '')

            candidates_html.append(f"""
                <tr style="border-bottom: 1px solid #e5e7eb;">
                    <td style="padding: 12px 0;">
                        <strong style="color: #111827;">{c['full_name']}</strong>
                        <br>
                        <span style="color: #6b7280; font-size: 14px;">{position_text}</span>
                    </td>
                    <td style="padding: 12px 0; text-align: center;">
                        <span style="background: #dbeafe; color: #1e40af; padding: 4px 12px; border-radius: 12px; font-weight: 600;">
                            {c['fit_score']}%
                        </span>
                    </td>
                </tr>
                {f'<tr><td colspan="2" style="padding: 0 0 12px 0; color: #6b7280; font-size: 14px; font-style: italic;">"{ai_note}"</td></tr>' if ai_note else ''}
            """)

        bench_url = f"{APP_URL}/#/employer?role={role['role_id']}"

        role_sections.append(f"""
            <div style="margin-bottom: 32px; background: #f9fafb; border-radius: 12px; padding: 20px;">
                <h3 style="margin: 0 0 4px 0; color: #111827; font-size: 18px;">
                    {role['role_title']}
                </h3>
                <p style="margin: 0 0 16px 0; color: #2563eb; font-size: 14px;">
                    {role['new_candidate_count']} new candidate{'s' if role['new_candidate_count'] != 1 else ''} above 80%
                </p>
                <table style="width: 100%; border-collapse: collapse;">
                    {''.join(candidates_html)}
                </table>
                <div style="margin-top: 16px;">
                    <a href="{bench_url}" style="display: inline-block; background: #2563eb; color: white; padding: 10px 20px; border-radius: 8px; text-decoration: none; font-weight: 500;">
                        View Bench
                    </a>
                </div>
            </div>
        """)

    # Full email template
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.5; color: #374151; margin: 0; padding: 0; background: #f3f4f6;">
        <div style="max-width: 600px; margin: 0 auto; padding: 40px 20px;">
            <!-- Header -->
            <div style="text-align: center; margin-bottom: 32px;">
                <h1 style="color: #2563eb; font-size: 24px; margin: 0 0 8px 0;">ShortList</h1>
                <p style="color: #6b7280; margin: 0;">Your Bench Updates</p>
            </div>

            <!-- Main Content -->
            <div style="background: white; border-radius: 16px; padding: 32px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <p style="margin: 0 0 24px 0; font-size: 16px;">
                    Hi{' ' + recipient_name if recipient_name else ''},
                </p>
                <p style="margin: 0 0 24px 0; font-size: 16px;">
                    Here's what's new on your bench this week:
                </p>

                {''.join(role_sections)}
            </div>

            <!-- Footer -->
            <div style="text-align: center; margin-top: 32px; color: #9ca3af; font-size: 14px;">
                <p style="margin: 0 0 8px 0;">Sent by ShortList</p>
                <p style="margin: 0;">
                    <a href="{APP_URL}/#/employer" style="color: #6b7280;">Open Dashboard</a>
                </p>
            </div>
        </div>
    </body>
    </html>
    """

    return html


def send_digest_email(recipient_email: str, subject: str, html_content: str) -> bool:
    """
    Send the digest email using SendGrid.
    Returns True if successful, False otherwise.
    """
    if not SENDGRID_AVAILABLE or not SENDGRID_API_KEY:
        print(f"[Digest] Would send to {recipient_email}: {subject}")
        print(f"[Digest] SendGrid not configured. Skipping actual send.")
        return False

    try:
        message = Mail(
            from_email=Email(FROM_EMAIL, "ShortList"),
            to_emails=To(recipient_email),
            subject=subject,
            html_content=Content("text/html", html_content)
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        print(f"[Digest] Sent to {recipient_email}, status: {response.status_code}")
        return response.status_code in [200, 202]
    except Exception as e:
        print(f"[Digest] Error sending to {recipient_email}: {e}")
        return False


def log_digest_sent(company_id: int, recipient_email: str, roles_included: List[Dict], status: str = 'sent'):
    """
    Log the digest send to the database.
    """
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO email_digest_logs (company_profile_id, recipient_email, roles_included, status)
            VALUES (%s, %s, %s, %s)
        """, (company_id, recipient_email, json.dumps(roles_included), status))
        conn.commit()


def send_weekly_digests() -> Dict:
    """
    Main function to send weekly digests to all eligible employers.
    Returns summary of sends.
    """
    print(f"[Digest] Starting weekly digest run at {datetime.utcnow()}")

    companies = get_companies_for_digest()
    print(f"[Digest] Found {len(companies)} companies to check")

    sent_count = 0
    skipped_count = 0
    error_count = 0

    for company in companies:
        try:
            # Get digest data
            digest_data = get_digest_data_for_company(
                company['company_id'],
                company['min_score_threshold']
            )

            if not digest_data['has_updates']:
                skipped_count += 1
                continue

            # Generate email content
            html_content = generate_digest_html(
                company['company_name'],
                company['first_name'],
                digest_data
            )

            if not html_content:
                skipped_count += 1
                continue

            # Send email
            subject = f"Your ShortList Bench Updates - {len(digest_data['roles'])} role{'s' if len(digest_data['roles']) != 1 else ''} with new candidates"
            success = send_digest_email(company['recipient_email'], subject, html_content)

            # Log it
            roles_summary = [{'role_id': r['role_id'], 'role_title': r['role_title'], 'count': r['new_candidate_count']} for r in digest_data['roles']]
            log_digest_sent(
                company['company_id'],
                company['recipient_email'],
                roles_summary,
                'sent' if success else 'failed'
            )

            if success:
                sent_count += 1
            else:
                error_count += 1

        except Exception as e:
            print(f"[Digest] Error processing {company.get('company_name')}: {e}")
            error_count += 1

    summary = {
        'sent': sent_count,
        'skipped': skipped_count,
        'errors': error_count,
        'timestamp': datetime.utcnow().isoformat()
    }

    print(f"[Digest] Completed: {summary}")
    return summary


if __name__ == '__main__':
    # Manual run for testing
    send_weekly_digests()
