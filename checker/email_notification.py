import smtplib
import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formataddr
import ssl

# Environment variables
SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
EMAIL_USER = os.environ.get('EMAIL_USER', 'techsupport@ticketbash.com')
EMAIL_PASS = os.environ.get('EMAIL_PASS', 'nymn mowm iusp cuci')  # App password or real SMTP password
FROM_NAME = os.environ.get('FROM_NAME', 'techsupport')  # Optional display name

def send_email(recipients, subject, body_data, attachments=None):
    if attachments is None:
        attachments = []

    try:
        # Validate required environment variables
        if not EMAIL_USER or not EMAIL_PASS:
            raise ValueError("EMAIL_USER and EMAIL_PASS environment variables are required")

        # Create message
        if attachments:
            # Use mixed for attachments
            msg = MIMEMultipart("mixed")
        else:
            # Use alternative for text/html alternatives
            msg = MIMEMultipart("alternative")

        # Set headers
        if FROM_NAME:
            msg['From'] = formataddr((FROM_NAME, EMAIL_USER))
        else:
            msg['From'] = EMAIL_USER
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject

        # Add body content
        if attachments:
            # For mixed messages, create alternative part for body
            body_part = MIMEMultipart("alternative")
            if 'text' in body_data:
                body_part.attach(MIMEText(body_data['text'], 'plain', 'utf-8'))
            if 'html' in body_data:
                body_part.attach(MIMEText(body_data['html'], 'html', 'utf-8'))
            msg.attach(body_part)
        else:
            # For simple messages, attach directly
            if 'text' in body_data:
                msg.attach(MIMEText(body_data['text'], 'plain', 'utf-8'))
            if 'html' in body_data:
                msg.attach(MIMEText(body_data['html'], 'html', 'utf-8'))

        # Attach files
        for attachment in attachments:
            try:
                file_content = base64.b64decode(attachment['content_base64'])
                part = MIMEApplication(file_content)
                part.add_header('Content-Disposition', 'attachment', filename=attachment['filename'])
                msg.attach(part)
            except Exception as attach_error:
                print(f"[WARNING] Failed to attach file {attachment.get('filename', 'unknown')}: {attach_error}")

        # Create SSL context for secure connection
        context = ssl.create_default_context()

        # Send email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, recipients, msg.as_string())

        print(f"[INFO] Email sent successfully to {recipients}")
        return True

    except smtplib.SMTPAuthenticationError as e:
        print(f"[ERROR] SMTP Authentication failed: {e}")
        print("[INFO] Check your email credentials and app password")
        return False
    except smtplib.SMTPException as e:
        print(f"[ERROR] SMTP error occurred: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Failed to send email to {recipients}: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    recipients = ["krishna.dev@svam.com"]  # List of email addresses
    subject = "Test Email"
    body_data = {
        "text": "This is a plain text email.",
        "html": "<p>This is an <b>HTML</b> email.</p>"
    }
    # attachments = [
    #     {
    #         "filename": "document.pdf",
    #         "content_base64": "base64_encoded_content_here"
    #     }
    # ]
    send_email(recipients, subject, body_data, attachments=[])
