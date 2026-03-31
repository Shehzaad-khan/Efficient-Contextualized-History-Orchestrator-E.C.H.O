from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from pathlib import Path

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
PROJECT_ROOT = Path(__file__).resolve().parents[1]
flow = InstalledAppFlow.from_client_secrets_file(str(PROJECT_ROOT / 'credentials.json'), SCOPES)
creds = flow.run_local_server(port=0)

# Save token for future runs
import json
with open(PROJECT_ROOT / 'token_gmail.json', 'w') as f:
    f.write(creds.to_json())

service = build('gmail', 'v1', credentials=creds)
profile = service.users().getProfile(userId='me').execute()
print('Authenticated as:', profile['emailAddress'])
print('Total messages:', profile['messagesTotal'])
