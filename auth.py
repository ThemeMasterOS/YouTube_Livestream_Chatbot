import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

def Authorize(client_secret_file='client_secret.json'):
    creds = None

    # 1. Load token.json if present
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json')
        except Exception as e:
            print(f"Error loading token.json: {e}")

    # 2. Auto-refresh using refresh_token if the 1-hour access token expired
    if creds and creds.expired and creds.refresh_token:
        try:
            print("Access token expired. Automatically refreshing via refresh_token...")
            creds.refresh(Request())
            print("Token refreshed successfully!")
        except Exception as e:
            print(f"Failed to refresh token: {e}")
            creds = None

    # 3. Return valid credentials
    if creds and creds.valid:
        print("Successfully restored session from token.json!")
        return creds

    # 4. Clear error if token.json is missing or corrupt
    raise Exception("token.json is missing or invalid. Check Secret Files on Render.")
