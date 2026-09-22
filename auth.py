import json
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


def load_token_from_env():
    """Restores token.json from the YOUTUBE_TOKEN_JSON environment variable

    if it doesn't exist on disk.
    """
    if not os.path.exists(TOKEN_FILE):
        token_env = os.getenv("YOUTUBE_TOKEN_JSON")
        if token_env:
            try:
                token_data = json.loads(token_env)
                with open(TOKEN_FILE, "w") as f:
                    json.dump(token_data, f, indent=2)
                print(
                    f"Successfully created '{TOKEN_FILE}' from YOUTUBE_TOKEN_JSON."
                )
            except json.JSONDecodeError as e:
                print(
                    f"Error parsing YOUTUBE_TOKEN_JSON environment variable: {e}"
                )
        else:
            print(
                f"Warning: '{TOKEN_FILE}' missing and YOUTUBE_TOKEN_JSON is not set."
            )


def Authorize():
    """Loads credentials, automatically refreshes expired access tokens, and

    saves updated tokens back to disk.
    """
    # Step 1: Ensure token.json is written from ENV if missing
    load_token_from_env()

    creds = None

    # Step 2: Load credentials from token.json
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            print(f"Error loading credentials from {TOKEN_FILE}: {e}")

    # Step 3: Refresh access token if expired and refresh_token is available
    if creds and creds.expired and creds.refresh_token:
        try:
            print("Access token expired. Attempting automatic refresh...")
            creds.refresh(Request())
            print("Token refreshed successfully!")

            # Save the refreshed access token back to disk during runtime
            with open(TOKEN_FILE, "w") as token_file:
                token_file.write(creds.to_json())

        except Exception as e:
            print(f"Failed to refresh access token: {e}")

    return creds
