from googleapiclient.discovery import build
from google.oauth2 import service_account
import tempfile
import os
from dotenv import load_dotenv
import json


from get_param import get_parameter

load_dotenv()

### loading secret via param
gs_api_secret_name = "/etl/sheets_api"

service_account_secret = get_parameter(gs_api_secret_name)
#this loads the service account file to a json
service_account_secret_json = json.loads(service_account_secret)
# this is how I would load a specific value from the JSON
type = service_account_secret_json["type"]
print (type)

# Write the secret to a temporary file
with tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w') as f:
    f.write(service_account_secret)
    SERVICE_ACCOUNT_FILE = f.name  # This is now a path to a JSON file

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

creds = None
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('sheets', 'v4', credentials=creds)