import boto3
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from botocore.exceptions import ClientError
from src.utils.get_param import get_parameter

def send_email(to_email, subject, content):
    """Send an email using SendGrid."""
    parameter_name = "/etl/sendgrid_creds"
    sendgrid_data_json = get_parameter(parameter_name)
    
    # Deserialize JSON string into Python dictionary
    try:
        sendgrid_data = json.loads(sendgrid_data_json)
    except json.JSONDecodeError:
        print("Error decoding JSON:", sendgrid_data_json)
        return

    # Get SendGrid API Key and sender email from parameter
    sendgrid_api_key = sendgrid_data.get('SENDGRID_API_KEY')
    from_email = sendgrid_data.get('SENDGRID_FROM_EMAIL')

    # Create a SendGrid mail object
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=content
    )

    try:
        # Send the email
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print("Email sent successfully!")
    except Exception as e:
        print("Error sending email:", e)

# Example usage
if __name__ == "__main__":
    send_email("shaune.gilbert@readyct.org", "Test Subject", "Test Content")

