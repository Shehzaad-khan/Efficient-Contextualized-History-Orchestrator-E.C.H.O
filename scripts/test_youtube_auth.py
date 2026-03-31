print("Starting script...")

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from pathlib import Path

SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
PROJECT_ROOT = Path(__file__).resolve().parents[1]

print("Loading credentials...")
flow = InstalledAppFlow.from_client_secrets_file(str(PROJECT_ROOT / 'credentials.json'), SCOPES)

print("Starting auth flow...")
creds = flow.run_local_server(host='localhost', port=8080, open_browser=True)

print("Saving token...")
with open(PROJECT_ROOT / 'token_youtube.json', 'w') as f:
    f.write(creds.to_json())

print("Building YouTube client...")
youtube = build('youtube', 'v3', credentials=creds)

print("Making API request...")
request = youtube.videos().list(part='snippet', id='dQw4w9WgXcQ')
response = request.execute()

print('SUCCESS:', response['items'][0]['snippet']['title'])
