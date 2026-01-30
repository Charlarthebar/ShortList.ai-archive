#!/usr/bin/env python3
"""
Opening Notification Sender
============================

Sends notifications for new job openings to subscribed users.

Supports multiple notification channels:
- Console (for testing/debugging)
- Email (via SMTP or SendGrid)
- Webhook (for Slack, Discord, etc.)
- Database queue (for UI to poll)

Usage:
    # Console output only (testing)
    python send_notifications.py --channel console

    # Send emails
    python send_notifications.py --channel email

    # Send to webhook
    python send_notifications.py --channel webhook --webhook-url https://...

    # Process all pending notifications
    python send_notifications.py

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import argparse
import logging
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ['DB_USER'] = 'noahhopkins'

from database import DatabaseManager, Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


@dataclass
class NotificationPayload:
    """Notification data to send."""
    event_id: int
    company_name: str
    job_title: str
    location: str
    is_remote: bool
    posting_url: str
    salary_min: Optional[float]
    salary_max: Optional[float]
    detected_at: datetime


class NotificationSender:
    """
    Sends notifications for job opening events.
    """

    def __init__(self):
        self.config = Config()
        self.db = DatabaseManager(self.config)
        self.stats = {
            'pending': 0,
            'sent': 0,
            'failed': 0,
        }

    def close(self):
        self.db.close_all_connections()

    def get_pending_notifications(self, limit: int = 100) -> List[NotificationPayload]:
        """Get opening events that need notifications."""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        oe.id,
                        oe.metadata->>'company_name' as company_name,
                        oe.metadata->>'job_title' as job_title,
                        oe.metadata->>'location' as location,
                        (oe.metadata->>'is_remote')::boolean as is_remote,
                        oe.metadata->>'posting_url' as posting_url,
                        (oe.metadata->>'salary_min')::numeric as salary_min,
                        (oe.metadata->>'salary_max')::numeric as salary_max,
                        oe.detected_at
                    FROM opening_events oe
                    WHERE oe.notification_sent = false
                    ORDER BY oe.detected_at ASC
                    LIMIT %s
                """, (limit,))

                notifications = []
                for row in cursor.fetchall():
                    notifications.append(NotificationPayload(
                        event_id=row[0],
                        company_name=row[1] or 'Unknown',
                        job_title=row[2] or 'Unknown',
                        location=row[3] or 'Unknown',
                        is_remote=row[4] or False,
                        posting_url=row[5] or '',
                        salary_min=float(row[6]) if row[6] else None,
                        salary_max=float(row[7]) if row[7] else None,
                        detected_at=row[8]
                    ))

                return notifications

        finally:
            self.db.release_connection(conn)

    def mark_sent(self, event_ids: List[int]):
        """Mark events as notification sent."""
        if not event_ids:
            return

        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE opening_events
                    SET notification_sent = true,
                        notification_sent_at = CURRENT_TIMESTAMP
                    WHERE id = ANY(%s)
                """, (event_ids,))
            conn.commit()
        finally:
            self.db.release_connection(conn)

    def format_salary(self, payload: NotificationPayload) -> str:
        """Format salary range for display."""
        if payload.salary_min and payload.salary_max:
            return f"${payload.salary_min:,.0f} - ${payload.salary_max:,.0f}"
        elif payload.salary_min:
            return f"${payload.salary_min:,.0f}+"
        return "Salary not disclosed"

    # =========================================================================
    # NOTIFICATION CHANNELS
    # =========================================================================

    def send_console(self, notifications: List[NotificationPayload]) -> List[int]:
        """Print notifications to console (for testing)."""
        sent_ids = []

        log.info(f"\n{'='*70}")
        log.info(f"NEW JOB OPENINGS ({len(notifications)} total)")
        log.info('='*70)

        for n in notifications:
            remote_str = " (Remote)" if n.is_remote else ""
            salary_str = self.format_salary(n)

            log.info(f"\n📢 {n.company_name}")
            log.info(f"   {n.job_title}")
            log.info(f"   📍 {n.location}{remote_str}")
            log.info(f"   💰 {salary_str}")
            if n.posting_url:
                log.info(f"   🔗 {n.posting_url}")
            log.info(f"   ⏰ Detected: {n.detected_at.strftime('%Y-%m-%d %H:%M')}")

            sent_ids.append(n.event_id)

        log.info(f"\n{'='*70}")
        return sent_ids

    def send_email(self, notifications: List[NotificationPayload],
                   smtp_host: str = None, smtp_port: int = 587,
                   smtp_user: str = None, smtp_pass: str = None,
                   from_email: str = None, to_email: str = None) -> List[int]:
        """Send email notifications."""
        # Get credentials from env if not provided
        smtp_host = smtp_host or os.environ.get('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = smtp_port or int(os.environ.get('SMTP_PORT', 587))
        smtp_user = smtp_user or os.environ.get('SMTP_USER')
        smtp_pass = smtp_pass or os.environ.get('SMTP_PASS')
        from_email = from_email or os.environ.get('NOTIFICATION_FROM_EMAIL')
        to_email = to_email or os.environ.get('NOTIFICATION_TO_EMAIL')

        if not all([smtp_user, smtp_pass, from_email, to_email]):
            log.error("Email credentials not configured. Set SMTP_* and NOTIFICATION_* env vars.")
            return []

        sent_ids = []

        # Group by company for digest
        by_company = {}
        for n in notifications:
            if n.company_name not in by_company:
                by_company[n.company_name] = []
            by_company[n.company_name].append(n)

        # Build email content
        subject = f"🆕 {len(notifications)} New Job Openings Detected"

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .company {{ margin: 20px 0; padding: 15px; background: #f5f5f5; border-radius: 8px; }}
                .company-name {{ font-size: 18px; font-weight: bold; color: #333; }}
                .job {{ margin: 10px 0 10px 20px; }}
                .job-title {{ font-weight: bold; }}
                .job-details {{ color: #666; font-size: 14px; }}
                .salary {{ color: #2e7d32; }}
                .apply-link {{ color: #1976d2; }}
            </style>
        </head>
        <body>
            <h2>🆕 {len(notifications)} New Job Openings</h2>
            <p>We detected new openings at companies you're following:</p>
        """

        for company, jobs in by_company.items():
            html_content += f'<div class="company">'
            html_content += f'<div class="company-name">{company}</div>'

            for n in jobs:
                remote_str = " 🏠 Remote" if n.is_remote else ""
                salary_str = self.format_salary(n)

                html_content += f'<div class="job">'
                html_content += f'<div class="job-title">{n.job_title}</div>'
                html_content += f'<div class="job-details">'
                html_content += f'📍 {n.location}{remote_str}<br>'
                html_content += f'<span class="salary">💰 {salary_str}</span><br>'
                if n.posting_url:
                    html_content += f'<a class="apply-link" href="{n.posting_url}">View Posting →</a>'
                html_content += '</div></div>'

                sent_ids.append(n.event_id)

            html_content += '</div>'

        html_content += """
            <hr>
            <p style="color: #999; font-size: 12px;">
                You received this because you're subscribed to job alerts on ShortList.
            </p>
        </body>
        </html>
        """

        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = from_email
            msg['To'] = to_email
            msg.attach(MIMEText(html_content, 'html'))

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)

            log.info(f"Email sent to {to_email} with {len(notifications)} openings")

        except Exception as e:
            log.error(f"Failed to send email: {e}")
            return []

        return sent_ids

    def send_webhook(self, notifications: List[NotificationPayload],
                     webhook_url: str) -> List[int]:
        """Send notifications to a webhook (Slack, Discord, etc.)."""
        if not webhook_url:
            log.error("No webhook URL provided")
            return []

        sent_ids = []

        # Build payload
        blocks = []
        for n in notifications:
            remote_str = " (Remote)" if n.is_remote else ""
            salary_str = self.format_salary(n)

            block = {
                "company": n.company_name,
                "title": n.job_title,
                "location": f"{n.location}{remote_str}",
                "salary": salary_str,
                "url": n.posting_url,
                "detected_at": n.detected_at.isoformat()
            }
            blocks.append(block)
            sent_ids.append(n.event_id)

        payload = {
            "event": "new_openings",
            "count": len(notifications),
            "openings": blocks,
            "timestamp": datetime.now().isoformat()
        }

        try:
            response = requests.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            response.raise_for_status()
            log.info(f"Webhook sent with {len(notifications)} openings")
        except Exception as e:
            log.error(f"Failed to send webhook: {e}")
            return []

        return sent_ids

    # =========================================================================
    # MAIN PROCESS
    # =========================================================================

    def process(self, channel: str = 'console', limit: int = 100,
                mark_sent: bool = True, **kwargs) -> Dict[str, int]:
        """
        Process pending notifications.

        Args:
            channel: 'console', 'email', or 'webhook'
            limit: Max notifications to process
            mark_sent: Whether to mark as sent after processing
            **kwargs: Channel-specific options

        Returns:
            Stats dict
        """
        notifications = self.get_pending_notifications(limit)
        self.stats['pending'] = len(notifications)

        if not notifications:
            log.info("No pending notifications")
            return self.stats

        log.info(f"Processing {len(notifications)} pending notifications via {channel}")

        # Send via selected channel
        if channel == 'console':
            sent_ids = self.send_console(notifications)
        elif channel == 'email':
            sent_ids = self.send_email(notifications, **kwargs)
        elif channel == 'webhook':
            sent_ids = self.send_webhook(notifications, kwargs.get('webhook_url'))
        else:
            log.error(f"Unknown channel: {channel}")
            return self.stats

        self.stats['sent'] = len(sent_ids)
        self.stats['failed'] = len(notifications) - len(sent_ids)

        # Mark as sent
        if mark_sent and sent_ids:
            self.mark_sent(sent_ids)
            log.info(f"Marked {len(sent_ids)} notifications as sent")

        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Send job opening notifications')
    parser.add_argument('--channel', type=str, default='console',
                        choices=['console', 'email', 'webhook'],
                        help='Notification channel')
    parser.add_argument('--limit', type=int, default=100,
                        help='Max notifications to process')
    parser.add_argument('--no-mark-sent', action='store_true',
                        help='Do not mark notifications as sent')
    parser.add_argument('--webhook-url', type=str,
                        help='Webhook URL for webhook channel')
    parser.add_argument('--stats-only', action='store_true',
                        help='Only show pending notification stats')

    args = parser.parse_args()

    sender = NotificationSender()

    try:
        if args.stats_only:
            notifications = sender.get_pending_notifications(1000)
            log.info(f"Pending notifications: {len(notifications)}")
            return

        stats = sender.process(
            channel=args.channel,
            limit=args.limit,
            mark_sent=not args.no_mark_sent,
            webhook_url=args.webhook_url
        )

        log.info(f"\n=== Results ===")
        log.info(f"Pending: {stats['pending']}")
        log.info(f"Sent: {stats['sent']}")
        log.info(f"Failed: {stats['failed']}")

    finally:
        sender.close()


if __name__ == "__main__":
    main()
