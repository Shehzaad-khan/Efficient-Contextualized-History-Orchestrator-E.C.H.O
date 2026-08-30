print("Starting script...")

from pathlib import Path

import pytest
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

from ste.security import write_encrypted_text

SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CREDENTIALS_PATH = PROJECT_ROOT / 'credentials.json'
pytestmark = pytest.mark.skipif(
    not CREDENTIALS_PATH.exists(),
    reason='Google OAuth credentials.json is not configured for this environment.',
)


def main() -> None:
    if not CREDENTIALS_PATH.exists():
        print(f"Skipping YouTube auth test: {CREDENTIALS_PATH} is missing.")
        return

    print("Loading credentials...")
    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)

    print("Starting auth flow...")
    creds = flow.run_local_server(host='localhost', port=8080, open_browser=True)

    print("Saving token...")
    write_encrypted_text(PROJECT_ROOT / 'token_youtube.enc', creds.to_json())

    print("Building YouTube client...")
    youtube = build('youtube', 'v3', credentials=creds)

    print("Making API request...")
    request = youtube.videos().list(part='snippet', id='dQw4w9WgXcQ')
    response = request.execute()

    print('SUCCESS:', response['items'][0]['snippet']['title'])


if __name__ == '__main__':
    main()
