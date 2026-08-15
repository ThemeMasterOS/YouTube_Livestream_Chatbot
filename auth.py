import os
import json
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

load_dotenv()

TOKEN_FILE = 'token.json'

def Authorize(client_secrets_file):
    # 1. Check if we already have saved credentials (token.json)
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE)
            if creds and creds.valid:
                print(">>> Successfully restored session from token.json!")
                return creds
        except Exception as e:
            print(f">>> Invalid token.json, re-authenticating... Error: {e}")

    # 2. If no saved credentials, prepare OAuth Flow
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_file,
        scopes=[
            'openid',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/userinfo.profile',
            'https://www.googleapis.com/auth/youtube',
            'https://www.googleapis.com/auth/youtube.force-ssl',
            'https://www.googleapis.com/auth/youtube.readonly',
        ],
        redirect_uri='http://localhost:5500/'
    )

    auth_code = os.getenv('AUTH_CODE')

    # 3. If AUTH_CODE is missing from Render env, print link and stop
    if not auth_code:
        auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
        print("\n" + "="*60)
        print("1. OPEN THIS LINK ON YOUR PHONE TO LOG IN:")
        print(auth_url)
        print("="*60)
        print("2. AFTER AUTHORIZING, COPY THE FULL REDIRECT URL.")
        print("3. PASTE IT INTO RENDER ENVIRONMENT VARIABLES AS 'AUTH_CODE'.")
        print("="*60 + "\n")
        raise Exception("AUTH_CODE missing. Please generate a fresh link, authorize, and add AUTH_CODE to Render.")

    # Clean up URL/Code
    auth_code = auth_code.strip()
    if "code=" in auth_code:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(auth_code)
        auth_code = parse_qs(parsed.query).get('code', [auth_code])[0]

    # 4. Exchange code for credentials
    flow.fetch_token(code=auth_code)
    creds = flow.credentials

    # 5. Save the credentials locally so we never need AUTH_CODE again
    with open(TOKEN_FILE, 'w') as token_out:
        token_out.write(creds.to_json())
    print(">>> Token saved to token.json successfully!")

    return creds
    
