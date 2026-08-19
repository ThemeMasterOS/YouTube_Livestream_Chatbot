import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

def Authorize(client_secret_file='client_secret.json', token_file='token.json'):
    creds = None

    # 1. Load token file if present
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file)
        except Exception as e:
            print(f"Error loading {token_file}: {e}")

    # 2. Auto-refresh using refresh_token if expired
    if creds and creds.expired and creds.refresh_token:
        try:
            print(f"Access token in {token_file} expired. Automatically refreshing via refresh_token...")
            creds.refresh(Request())
            print(f"Token in {token_file} refreshed successfully!")
        except Exception as e:
            print(f"Failed to refresh token for {token_file}: {e}")
            creds = None

    # 3. Return valid credentials
    if creds and creds.valid:
        print(f"Successfully restored session from {token_file}!")
        return creds

    # 4. Raise error if token is missing/corrupt
    raise Exception(f"{token_file} is missing or invalid. Check Secret Files on Render.")
