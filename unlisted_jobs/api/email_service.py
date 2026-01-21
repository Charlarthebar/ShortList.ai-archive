#!/usr/bin/env python3
"""
Email Service for ShortList Platform
=====================================

Handles transactional email sending for:
- Company domain verification
- Password reset
- Role opening notifications
- Shortlist status updates

Supports multiple providers:
- SMTP (Gmail, etc.)
- SendGrid
- AWS SES (future)

Configuration via environment variables:
- EMAIL_PROVIDER: 'smtp', 'sendgrid', or 'console' (for testing)
- SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS (for SMTP)
- SENDGRID_API_KEY (for SendGrid)
- EMAIL_FROM_ADDRESS: Default sender address
- EMAIL_FROM_NAME: Default sender name

Author: ShortList.ai
Date: 2026-01-19
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

# Optional SendGrid support
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email, To, Content
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False

log = logging.getLogger(__name__)


@dataclass
class EmailMessage:
    """Email message data."""
    to_email: str
    to_name: Optional[str]
    subject: str
    html_content: str
    text_content: Optional[str] = None
    from_email: Optional[str] = None
    from_name: Optional[str] = None
    reply_to: Optional[str] = None


class EmailService:
    """
    Email sending service with multiple provider support.
    """

    def __init__(self):
        self.provider = os.environ.get('EMAIL_PROVIDER', 'console')
        self.from_email = os.environ.get('EMAIL_FROM_ADDRESS', 'noreply@shortlist.ai')
        self.from_name = os.environ.get('EMAIL_FROM_NAME', 'ShortList')
        self.base_url = os.environ.get('APP_BASE_URL', 'http://localhost:3000')

        # SMTP settings
        self.smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.environ.get('SMTP_PORT', 587))
        self.smtp_user = os.environ.get('SMTP_USER')
        self.smtp_pass = os.environ.get('SMTP_PASS')

        # SendGrid settings
        self.sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')

    def send(self, message: EmailMessage) -> bool:
        """
        Send an email using the configured provider.

        Args:
            message: EmailMessage with recipient and content

        Returns:
            True if sent successfully, False otherwise
        """
        # Apply defaults
        if not message.from_email:
            message.from_email = self.from_email
        if not message.from_name:
            message.from_name = self.from_name

        try:
            if self.provider == 'console':
                return self._send_console(message)
            elif self.provider == 'smtp':
                return self._send_smtp(message)
            elif self.provider == 'sendgrid':
                return self._send_sendgrid(message)
            else:
                log.error(f"Unknown email provider: {self.provider}")
                return False
        except Exception as e:
            log.error(f"Failed to send email: {e}")
            return False

    def _send_console(self, message: EmailMessage) -> bool:
        """Print email to console (for development/testing)."""
        log.info("=" * 60)
        log.info("📧 EMAIL (console mode)")
        log.info("=" * 60)
        log.info(f"To: {message.to_name} <{message.to_email}>")
        log.info(f"From: {message.from_name} <{message.from_email}>")
        log.info(f"Subject: {message.subject}")
        log.info("-" * 60)
        log.info(message.text_content or message.html_content[:500])
        log.info("=" * 60)
        return True

    def _send_smtp(self, message: EmailMessage) -> bool:
        """Send via SMTP."""
        if not all([self.smtp_user, self.smtp_pass]):
            log.error("SMTP credentials not configured")
            return False

        msg = MIMEMultipart('alternative')
        msg['Subject'] = message.subject
        msg['From'] = f"{message.from_name} <{message.from_email}>"
        msg['To'] = f"{message.to_name} <{message.to_email}>" if message.to_name else message.to_email

        if message.reply_to:
            msg['Reply-To'] = message.reply_to

        # Add text and HTML parts
        if message.text_content:
            msg.attach(MIMEText(message.text_content, 'plain'))
        msg.attach(MIMEText(message.html_content, 'html'))

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)

        log.info(f"Email sent via SMTP to {message.to_email}")
        return True

    def _send_sendgrid(self, message: EmailMessage) -> bool:
        """Send via SendGrid."""
        if not SENDGRID_AVAILABLE:
            log.error("SendGrid not installed. Run: pip install sendgrid")
            return False

        if not self.sendgrid_api_key:
            log.error("SENDGRID_API_KEY not configured")
            return False

        mail = Mail(
            from_email=Email(message.from_email, message.from_name),
            to_emails=To(message.to_email, message.to_name),
            subject=message.subject,
            html_content=Content("text/html", message.html_content)
        )

        if message.text_content:
            mail.add_content(Content("text/plain", message.text_content))

        sg = SendGridAPIClient(self.sendgrid_api_key)
        response = sg.send(mail)

        if response.status_code >= 200 and response.status_code < 300:
            log.info(f"Email sent via SendGrid to {message.to_email}")
            return True
        else:
            log.error(f"SendGrid error: {response.status_code}")
            return False

    # =========================================================================
    # EMAIL TEMPLATES
    # =========================================================================

    def send_verification_email(self, to_email: str, to_name: str,
                                 company_name: str, verification_token: str) -> bool:
        """Send company domain verification email."""
        verification_url = f"{self.base_url}/verify-company?token={verification_token}"

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 40px 20px; }}
                .logo {{ font-size: 24px; font-weight: bold; color: #6366f1; margin-bottom: 32px; }}
                .button {{ display: inline-block; padding: 14px 28px; background: #6366f1; color: white; text-decoration: none; border-radius: 8px; font-weight: 600; }}
                .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; font-size: 13px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="logo">◈ ShortList</div>

                <h2>Verify your company</h2>

                <p>Hi {to_name},</p>

                <p>Please click the button below to verify that you represent <strong>{company_name}</strong> on ShortList:</p>

                <p style="margin: 32px 0;">
                    <a href="{verification_url}" class="button">Verify Company</a>
                </p>

                <p>Or copy and paste this link into your browser:</p>
                <p style="word-break: break-all; color: #6366f1;">{verification_url}</p>

                <p>This link will expire in 7 days.</p>

                <div class="footer">
                    <p>If you didn't request this verification, you can safely ignore this email.</p>
                    <p>© 2026 ShortList.ai</p>
                </div>
            </div>
        </body>
        </html>
        """

        text_content = f"""
Verify your company on ShortList

Hi {to_name},

Please click the link below to verify that you represent {company_name} on ShortList:

{verification_url}

This link will expire in 7 days.

If you didn't request this verification, you can safely ignore this email.

© 2026 ShortList.ai
        """

        return self.send(EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject=f"Verify your company on ShortList",
            html_content=html_content,
            text_content=text_content
        ))

    def send_password_reset_email(self, to_email: str, to_name: str,
                                   reset_token: str) -> bool:
        """Send password reset email."""
        reset_url = f"{self.base_url}/reset-password?token={reset_token}"

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 40px 20px; }}
                .logo {{ font-size: 24px; font-weight: bold; color: #6366f1; margin-bottom: 32px; }}
                .button {{ display: inline-block; padding: 14px 28px; background: #6366f1; color: white; text-decoration: none; border-radius: 8px; font-weight: 600; }}
                .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; font-size: 13px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="logo">◈ ShortList</div>

                <h2>Reset your password</h2>

                <p>Hi {to_name or 'there'},</p>

                <p>We received a request to reset your password. Click the button below to create a new password:</p>

                <p style="margin: 32px 0;">
                    <a href="{reset_url}" class="button">Reset Password</a>
                </p>

                <p>Or copy and paste this link into your browser:</p>
                <p style="word-break: break-all; color: #6366f1;">{reset_url}</p>

                <p>This link will expire in 1 hour.</p>

                <div class="footer">
                    <p>If you didn't request a password reset, you can safely ignore this email.</p>
                    <p>© 2026 ShortList.ai</p>
                </div>
            </div>
        </body>
        </html>
        """

        text_content = f"""
Reset your password on ShortList

Hi {to_name or 'there'},

We received a request to reset your password. Click the link below to create a new password:

{reset_url}

This link will expire in 1 hour.

If you didn't request a password reset, you can safely ignore this email.

© 2026 ShortList.ai
        """

        return self.send(EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject="Reset your password on ShortList",
            html_content=html_content,
            text_content=text_content
        ))

    def send_role_opened_notification(self, to_email: str, to_name: str,
                                       position_title: str, company_name: str,
                                       position_id: int) -> bool:
        """Send notification when a role the user is on the shortlist for opens."""
        position_url = f"{self.base_url}/roles/{position_id}"

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 40px 20px; }}
                .logo {{ font-size: 24px; font-weight: bold; color: #6366f1; margin-bottom: 32px; }}
                .highlight {{ background: #f0fdf4; border: 1px solid #86efac; border-radius: 12px; padding: 24px; margin: 24px 0; }}
                .role-title {{ font-size: 20px; font-weight: 600; color: #0f172a; margin-bottom: 4px; }}
                .company {{ color: #6366f1; font-weight: 500; }}
                .button {{ display: inline-block; padding: 14px 28px; background: #10b981; color: white; text-decoration: none; border-radius: 8px; font-weight: 600; }}
                .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; font-size: 13px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="logo">◈ ShortList</div>

                <h2>🎉 Great news! A role you're interested in just opened</h2>

                <p>Hi {to_name or 'there'},</p>

                <p>A position you joined the shortlist for is now accepting applications:</p>

                <div class="highlight">
                    <div class="role-title">{position_title}</div>
                    <div class="company">{company_name}</div>
                </div>

                <p>Since you're already on the shortlist, your application will be among the first reviewed!</p>

                <p style="margin: 32px 0;">
                    <a href="{position_url}" class="button">View Role →</a>
                </p>

                <div class="footer">
                    <p>You're receiving this because you joined the shortlist for this role.</p>
                    <p>© 2026 ShortList.ai</p>
                </div>
            </div>
        </body>
        </html>
        """

        text_content = f"""
Great news! A role you're interested in just opened

Hi {to_name or 'there'},

A position you joined the shortlist for is now accepting applications:

{position_title} at {company_name}

Since you're already on the shortlist, your application will be among the first reviewed!

View the role: {position_url}

© 2026 ShortList.ai
        """

        return self.send(EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject=f"🎉 {position_title} at {company_name} is now open!",
            html_content=html_content,
            text_content=text_content
        ))

    def send_shortlist_ready_notification(self, to_email: str, to_name: str,
                                           position_title: str, candidate_count: int,
                                           position_id: int) -> bool:
        """Send notification to employer when shortlist is ready for review."""
        shortlist_url = f"{self.base_url}/employer/roles/{position_id}/shortlist"

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 40px 20px; }}
                .logo {{ font-size: 24px; font-weight: bold; color: #6366f1; margin-bottom: 32px; }}
                .highlight {{ background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 12px; padding: 24px; margin: 24px 0; text-align: center; }}
                .count {{ font-size: 48px; font-weight: 700; color: #6366f1; }}
                .label {{ color: #64748b; }}
                .button {{ display: inline-block; padding: 14px 28px; background: #6366f1; color: white; text-decoration: none; border-radius: 8px; font-weight: 600; }}
                .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; font-size: 13px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="logo">◈ ShortList</div>

                <h2>Your shortlist is ready for {position_title}</h2>

                <p>Hi {to_name or 'there'},</p>

                <p>Your role just opened, and you have qualified candidates ready to review:</p>

                <div class="highlight">
                    <div class="count">{candidate_count}</div>
                    <div class="label">qualified candidates</div>
                </div>

                <p>These candidates have been pre-screened based on your requirements and ranked by fit.</p>

                <p style="margin: 32px 0;">
                    <a href="{shortlist_url}" class="button">Review Shortlist →</a>
                </p>

                <div class="footer">
                    <p>© 2026 ShortList.ai</p>
                </div>
            </div>
        </body>
        </html>
        """

        return self.send(EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject=f"📋 {candidate_count} candidates ready for {position_title}",
            html_content=html_content
        ))

    def send_verification_approved_email(self, to_email: str, to_name: str,
                                          company_name: str) -> bool:
        """Send notification when company verification is approved."""
        dashboard_url = f"{self.base_url}/employer/dashboard"

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 40px 20px; }}
                .logo {{ font-size: 24px; font-weight: bold; color: #6366f1; margin-bottom: 32px; }}
                .success {{ background: #f0fdf4; border: 1px solid #86efac; border-radius: 12px; padding: 24px; margin: 24px 0; text-align: center; }}
                .badge {{ display: inline-flex; align-items: center; gap: 8px; background: #dcfce7; color: #166534; padding: 8px 16px; border-radius: 20px; font-weight: 600; }}
                .button {{ display: inline-block; padding: 14px 28px; background: #6366f1; color: white; text-decoration: none; border-radius: 8px; font-weight: 600; }}
                .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; font-size: 13px; color: #6b7280; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="logo">◈ ShortList</div>

                <h2>🎉 Your company is verified!</h2>

                <p>Hi {to_name or 'there'},</p>

                <p>Great news! <strong>{company_name}</strong> has been verified on ShortList.</p>

                <div class="success">
                    <span class="badge">✓ Verified Company</span>
                </div>

                <p>As a verified company, you can now:</p>
                <ul>
                    <li>Post positions and build shortlists</li>
                    <li>Review pre-screened candidates</li>
                    <li>Display a verified badge on your profile</li>
                </ul>

                <p style="margin: 32px 0;">
                    <a href="{dashboard_url}" class="button">Go to Dashboard →</a>
                </p>

                <div class="footer">
                    <p>© 2026 ShortList.ai</p>
                </div>
            </div>
        </body>
        </html>
        """

        return self.send(EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject=f"✓ {company_name} is now verified on ShortList",
            html_content=html_content
        ))


# Global instance for convenience
_email_service = None

def get_email_service() -> EmailService:
    """Get the global email service instance."""
    global _email_service
    if _email_service is None:
        _email_service = EmailService()
    return _email_service
