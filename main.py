import boto3
from botocore.exceptions import ClientError
from src.utils.get_param import get_parameter

parameter_name = "/etl/gmail_creds"

parameter = get_parameter(parameter_name)

print (parameter)