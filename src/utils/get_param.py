import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up a specific profile from your AWS config
profile_name = os.environ.get('AWS_PROFILE_NAME')
if profile_name:
    boto3.setup_default_session(profile_name=profile_name)

# Function to fetch parameters from AWS Systems Manager Parameter Store
def get_parameter(parameter_name):
    region_name = "us-east-1"

    # Create a Systems Manager client
    client = boto3.client(service_name='ssm', region_name=region_name)

    try:
        # Get the parameter
        response = client.get_parameter(Name=parameter_name, WithDecryption=True)
        parameter = response['Parameter']['Value']
        # Debug: Print the parameter value
    except ClientError as e:
        # Handle exceptions
        raise e

    return parameter
