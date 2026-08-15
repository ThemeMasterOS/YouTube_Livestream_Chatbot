import os
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

def Authorize(file):
    flow = InstalledAppFlow.from_client_secrets_file(
        file,
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
    
    # Check if code is passed via Render Environment Variable
    auth_code = os.getenv('AUTH_CODE')
    
    if not auth_code:
        auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
        print("\n" + "="*60)
        print("1. OPEN THIS LINK ON YOUR PHONE:")
        print(auth_url)
        print("="*60)
        print("2. AFTER AUTHORIZING, COPY THE FULL REDIRECT URL OR CODE.")
        print("3. GO TO RENDER -> ENVIRONMENT VARIABLES -> ADD 'AUTH_CODE'.")
        print("="*60 + "\n")
        raise Exception("AUTH_CODE variable missing. Please add AUTH_CODE to Render Environment Variables and redeploy.")

    # Clean up code if full URL was pasted
    if "code=" in auth_code:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(auth_code)
        auth_code = parse_qs(parsed.query).get('code', [auth_code])[0]

    flow.fetch_token(code=auth_code)
    return flow
    
