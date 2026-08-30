from pathlib import Path

import pytest
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

from ste.security import write_encrypted_text

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CREDENTIALS_PATH = PROJECT_ROOT / 'credentials.json'
pytestmark = pytest.mark.skipif(
    not CREDENTIALS_PATH.exists(),
    reason='Google OAuth credentials.json is not configured for this environment.',
)


def main() -> None:
    if not CREDENTIALS_PATH.exists():
        print(f"Skipping Gmail auth test: {CREDENTIALS_PATH} is missing.")
        return

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
    creds = flow.run_local_server(port=0)

    # Save token for future runs
    write_encrypted_text(PROJECT_ROOT / 'token_gmail.enc', creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    profile = service.users().getProfile(userId='me').execute()
    print('Authenticated as:', profile['emailAddress'])
    print('Total messages:', profile['messagesTotal'])


if __name__ == '__main__':
    main()
