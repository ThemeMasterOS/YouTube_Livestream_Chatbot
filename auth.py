import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


def load_token_from_env():
  """Generates token.json on-the-fly from the YOUTUBE_TOKEN_JSON environment variable."""
  token_json = os.getenv("YOUTUBE_TOKEN_JSON")
  if token_json and not os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, "w") as f:
      f.write(token_json)


def Authorize(*args, **kwargs):
  """Loads credentials and automatically refreshes expired access tokens.

  *args and **kwargs allow it to accept any arguments bot.py passes into it.
  """
  load_token_from_env()

  creds = None

  if os.path.exists(TOKEN_FILE):
    try:
      creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    except Exception as e:
      print(f"Error loading credentials: {e}")

  if creds and creds.expired and creds.refresh_token:
    try:
      print("Access token expired. Attempting automatic refresh...")
      creds.refresh(Request())
      print("Token refreshed successfully!")

      with open(TOKEN_FILE, "w") as token_file:
        token_file.write(creds.to_json())

    except Exception as e:
      print(f"Failed to refresh access token: {e}")

  return creds
