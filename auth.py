import os
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request


def _load_creds_from_env(token_env):
    """Build Credentials from an env var holding the token.json contents (as JSON text)."""
    raw = os.getenv(token_env)
    if not raw:
        return None
    try:
        return Credentials.from_authorized_user_info(json.loads(raw))
    except Exception as e:
        print(f"Error loading token from env var {token_env}: {e}")
        return None


def Authorize(client_secret_file='client_secret.json', token_file='token.json', token_env=None):
    creds = None
    loaded_from_env = False

    # 1. Load token file if present
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file)
        except Exception as e:
            print(f"Error loading {token_file}: {e}")

    # 1b. Fallback: token file missing or unreadable -> use env var
    if creds is None and token_env:
        print(f"{token_file} not usable. Falling back to env var {token_env}...")
        creds = _load_creds_from_env(token_env)
        loaded_from_env = creds is not None

    # 2. Auto-refresh using refresh_token if expired
    if creds and creds.expired and creds.refresh_token:
        source = f"env var {token_env}" if loaded_from_env else token_file
        try:
            print(f"Access token from {source} expired. Automatically refreshing via refresh_token...")
            creds.refresh(Request())

            # Save updated token to disk (works for both sources; recreates the
            # file from the env var, so the refreshed token survives until restart)
            try:
                with open(token_file, "w") as token:
                    token.write(creds.to_json())
                print(f"Token refreshed and saved to {token_file}!")
            except Exception as e:
                print(f"Token refreshed, but could not write {token_file}: {e}")
        except Exception as e:
            print(f"Failed to refresh token from {source}: {e}")
            creds = None

    # 3. Return valid credentials
    if creds and creds.valid:
        source = f"env var {token_env}" if loaded_from_env else token_file
        print(f"Successfully restored session from {source}!")
        return creds

    # 4. Raise error if everything failed
    hint = f" or env var {token_env}" if token_env else ""
    raise Exception(f"{token_file}{hint} is missing or invalid. Check Secret Files / Environment on Render.")
