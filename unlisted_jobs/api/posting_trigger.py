#!/usr/bin/env python3
"""
Posting Trigger Service
========================

Handles notifications when a role opens:
1. Notifies employers: "This role just opened, you have X qualified candidates ready"
2. Notifies candidates: "This role is now open"

This service should be called when:
- A position status changes to 'open'
- The posting monitor detects a new opening

Author: ShortList.ai
Date: 2026-01-20
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Setup path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.email_service import EmailService, EmailMessage

log = logging.getLogger(__name__)


class PostingTriggerService:
    """
    Service to handle notifications when job postings open.
    """

    def __init__(self, db_connection):
        """
        Initialize with database connection.

        Args:
            db_connection: Active database connection
        """
        self.conn = db_connection
        self.email_service = EmailService()
        self.base_url = os.environ.get('FRONTEND_URL', 'http://localhost:3000')

    def trigger_opening(self, position_id: int) -> Dict[str, Any]:
        """
        Trigger notifications for a position that just opened.

        Args:
            position_id: The watchable_positions ID that opened

        Returns:
            Dict with notification stats
        """
        result = {
            'position_id': position_id,
            'employer_notified': False,
            'candidates_notified': 0,
            'errors': []
        }

        try:
            # Get position info
            position = self._get_position(position_id)
            if not position:
                result['errors'].append(f'Position {position_id} not found')
                return result

            # Check if position is monitored
            # Non-monitored (historical) data should not trigger notifications
            if not position.get('is_monitored'):
                log.info(f"Position {position_id} is not monitored - skipping notifications")
                result['skipped_reason'] = 'Position is not actively monitored'
                return result

            # Get shortlist stats for this position
            shortlist_info = self._get_shortlist_info(position_id)

            # Notify employer
            employer_notified = self._notify_employer(position, shortlist_info)
            result['employer_notified'] = employer_notified

            # Notify candidates
            candidates_notified = self._notify_candidates(position)
            result['candidates_notified'] = candidates_notified

            log.info(f"Posting trigger for position {position_id}: "
                     f"employer={employer_notified}, candidates={candidates_notified}")

        except Exception as e:
            log.error(f"Error in trigger_opening: {e}")
            result['errors'].append(str(e))

        return result

    def _get_position(self, position_id: int) -> Optional[Dict]:
        """Get position details."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    wp.id, wp.title, wp.company_name, wp.company_id,
                    wp.location, wp.department, wp.status,
                    wp.salary_min, wp.salary_max,
                    wp.is_monitored, wp.data_source, wp.data_as_of_date
                FROM watchable_positions wp
                WHERE wp.id = %s
            """, (position_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                'id': row[0],
                'title': row[1],
                'company_name': row[2],
                'company_id': row[3],
                'location': row[4],
                'department': row[5],
                'status': row[6],
                'salary_min': row[7],
                'salary_max': row[8],
                'is_monitored': row[9] if len(row) > 9 else False,
                'data_source': row[10] if len(row) > 10 else None,
                'data_as_of_date': row[11] if len(row) > 11 else None
            }

    def _get_shortlist_info(self, position_id: int) -> Dict:
        """Get shortlist statistics for a position."""
        with self.conn.cursor() as cursor:
            # Get role config for threshold
            cursor.execute("""
                SELECT config FROM role_configurations WHERE position_id = %s
            """, (position_id,))
            config_row = cursor.fetchone()
            threshold = 70
            if config_row and config_row[0]:
                threshold = config_row[0].get('score_threshold', 70)

            # Count applicants
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE screening_passed = TRUE) as passed,
                    COUNT(*) FILTER (WHERE screening_passed = TRUE AND ai_score >= %s) as qualified
                FROM shortlist_applications
                WHERE position_id = %s
            """, (threshold, position_id))
            row = cursor.fetchone()

            return {
                'total_applicants': row[0] or 0,
                'passed_screening': row[1] or 0,
                'qualified': row[2] or 0,
                'threshold': threshold
            }

    def _notify_employer(self, position: Dict, shortlist_info: Dict) -> bool:
        """
        Create notification for the employer when role opens.

        Message: "This role just opened, and you already have X qualified
                 shortlisted candidates ready to review."
        """
        if not position.get('company_id'):
            log.warning(f"Position {position['id']} has no company_id")
            return False

        # Find company admin users
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT u.id, u.email, u.first_name
                FROM users u
                JOIN company_profiles cp ON cp.company_id = u.company_id
                WHERE cp.company_id = %s
                  AND u.user_type = 'company'
            """, (position['company_id'],))
            employer_users = cursor.fetchall()

            if not employer_users:
                # Try alternate: users with company_profile matching position company
                cursor.execute("""
                    SELECT u.id, u.email, u.first_name
                    FROM users u
                    JOIN company_profiles cp ON u.id = cp.user_id
                    WHERE cp.company_name = %s
                      AND u.user_type = 'company'
                """, (position['company_name'],))
                employer_users = cursor.fetchall()

            if not employer_users:
                log.info(f"No employer users found for company {position['company_name']}")
                return False

            qualified = shortlist_info['qualified']
            title = f"🎉 {position['title']} is now open!"

            if qualified > 0:
                message = (f"Great news! Your {position['title']} role is now accepting applications. "
                          f"You already have {qualified} qualified candidates on the shortlist ready to review.")
            else:
                message = (f"Your {position['title']} role is now open and accepting applications. "
                          f"Candidates on your shortlist will be notified.")

            # Create in-app notification for each employer user
            for user_id, email, first_name in employer_users:
                cursor.execute("""
                    INSERT INTO notifications
                    (user_id, type, title, message, related_position_id, action_url)
                    VALUES (%s, 'role_opened', %s, %s, %s, %s)
                """, (
                    user_id,
                    title,
                    message,
                    position['id'],
                    f"/dashboard?tab=positions&position={position['id']}"
                ))

                # Send email notification
                self._send_employer_email(email, first_name, position, shortlist_info)

            self.conn.commit()
            return True

    def _send_employer_email(self, email: str, first_name: str,
                             position: Dict, shortlist_info: Dict):
        """Send email to employer about role opening."""
        qualified = shortlist_info['qualified']

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #1e293b; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: linear-gradient(135deg, #6366f1, #8b5cf6); color: white; padding: 30px; border-radius: 12px 12px 0 0; }}
                .content {{ background: #ffffff; padding: 30px; border: 1px solid #e2e8f0; border-top: none; border-radius: 0 0 12px 12px; }}
                .stat-box {{ background: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; padding: 20px; margin: 20px 0; text-align: center; }}
                .stat-num {{ font-size: 36px; font-weight: 700; color: #16a34a; }}
                .stat-label {{ color: #166534; font-size: 14px; }}
                .btn {{ display: inline-block; background: #6366f1; color: white; padding: 14px 28px; border-radius: 8px; text-decoration: none; font-weight: 600; }}
                .footer {{ text-align: center; padding: 20px; color: #64748b; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0; font-size: 24px;">🎉 Your Role is Now Open!</h1>
                </div>
                <div class="content">
                    <p>Hi {first_name or 'there'},</p>

                    <p><strong>{position['title']}</strong> at <strong>{position['company_name']}</strong> is now accepting applications.</p>

                    {"<div class='stat-box'><div class='stat-num'>" + str(qualified) + "</div><div class='stat-label'>Qualified candidates already on your shortlist</div></div>" if qualified > 0 else ""}

                    <p>{"Your pre-screened candidates are ready to review. They've already expressed interest and passed your screening criteria." if qualified > 0 else "Candidates who join your shortlist will be automatically screened based on your criteria."}</p>

                    <p style="text-align: center; margin-top: 30px;">
                        <a href="{self.base_url}/dashboard?tab=positions" class="btn">View Shortlist</a>
                    </p>
                </div>
                <div class="footer">
                    <p>ShortList - Hire faster with pre-qualified candidates</p>
                </div>
            </div>
        </body>
        </html>
        """

        try:
            msg = EmailMessage(
                to_email=email,
                to_name=first_name,
                subject=f"🎉 {position['title']} is now open - {qualified} qualified candidates ready" if qualified > 0 else f"🎉 {position['title']} is now open",
                html_content=html_content
            )
            self.email_service.send(msg)
        except Exception as e:
            log.error(f"Failed to send employer email: {e}")

    def _notify_candidates(self, position: Dict) -> int:
        """
        Notify all candidates who joined the shortlist for this position.

        Message: "This role is now open."
        """
        with self.conn.cursor() as cursor:
            # Get candidates who applied to this position's shortlist
            cursor.execute("""
                SELECT sa.user_id, u.email, u.first_name
                FROM shortlist_applications sa
                JOIN users u ON sa.user_id = u.id
                WHERE sa.position_id = %s
                  AND sa.screening_passed = TRUE
            """, (position['id'],))
            candidates = cursor.fetchall()

            if not candidates:
                return 0

            title = f"🔔 {position['title']} is now open!"
            message = (f"Good news! The {position['title']} role at {position['company_name']} "
                      f"that you joined the shortlist for is now accepting applications.")

            notified = 0
            for user_id, email, first_name in candidates:
                try:
                    # Create in-app notification
                    cursor.execute("""
                        INSERT INTO notifications
                        (user_id, type, title, message, related_position_id, action_url)
                        VALUES (%s, 'role_opened', %s, %s, %s, %s)
                    """, (
                        user_id,
                        title,
                        message,
                        position['id'],
                        f"/jobs/{position['id']}"
                    ))

                    # Send email
                    self._send_candidate_email(email, first_name, position)
                    notified += 1

                except Exception as e:
                    log.error(f"Failed to notify candidate {user_id}: {e}")

            self.conn.commit()
            return notified

    def _send_candidate_email(self, email: str, first_name: str, position: Dict):
        """Send email to candidate about role opening."""
        salary_str = ""
        if position.get('salary_min') and position.get('salary_max'):
            salary_str = f"${position['salary_min']:,.0f} - ${position['salary_max']:,.0f}"
        elif position.get('salary_min'):
            salary_str = f"${position['salary_min']:,.0f}+"

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #1e293b; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: linear-gradient(135deg, #10b981, #14b8a6); color: white; padding: 30px; border-radius: 12px 12px 0 0; }}
                .content {{ background: #ffffff; padding: 30px; border: 1px solid #e2e8f0; border-top: none; border-radius: 0 0 12px 12px; }}
                .role-card {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin: 20px 0; }}
                .role-title {{ font-size: 20px; font-weight: 600; color: #1e293b; margin: 0 0 8px 0; }}
                .role-company {{ color: #6366f1; font-weight: 500; }}
                .role-meta {{ color: #64748b; font-size: 14px; margin-top: 12px; }}
                .btn {{ display: inline-block; background: #10b981; color: white; padding: 14px 28px; border-radius: 8px; text-decoration: none; font-weight: 600; }}
                .footer {{ text-align: center; padding: 20px; color: #64748b; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0; font-size: 24px;">🔔 A Role You're Interested In is Now Open!</h1>
                </div>
                <div class="content">
                    <p>Hi {first_name or 'there'},</p>

                    <p>Great news! A position you joined the shortlist for is now accepting applications:</p>

                    <div class="role-card">
                        <h3 class="role-title">{position['title']}</h3>
                        <p class="role-company">{position['company_name']}</p>
                        <div class="role-meta">
                            📍 {position['location'] or 'Location not specified'}
                            {f"<br>💰 {salary_str}" if salary_str else ""}
                        </div>
                    </div>

                    <p>Since you're already on the shortlist, the employer can review your profile. Good luck!</p>

                    <p style="text-align: center; margin-top: 30px;">
                        <a href="{self.base_url}/jobs/{position['id']}" class="btn">View Role Details</a>
                    </p>
                </div>
                <div class="footer">
                    <p>You received this because you joined the shortlist for this role on ShortList.</p>
                </div>
            </div>
        </body>
        </html>
        """

        try:
            msg = EmailMessage(
                to_email=email,
                to_name=first_name,
                subject=f"🔔 {position['title']} at {position['company_name']} is now open!",
                html_content=html_content
            )
            self.email_service.send(msg)
        except Exception as e:
            log.error(f"Failed to send candidate email: {e}")


def trigger_posting_notifications(db_connection, position_id: int) -> Dict[str, Any]:
    """
    Convenience function to trigger posting notifications.

    Usage:
        from posting_trigger import trigger_posting_notifications
        result = trigger_posting_notifications(conn, position_id)
    """
    service = PostingTriggerService(db_connection)
    return service.trigger_opening(position_id)
