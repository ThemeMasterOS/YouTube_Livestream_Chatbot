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
    
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
    
    print("\n" + "="*60)
    print("1. OPEN THIS URL IN YOUR BROWSER:")
    print(auth_url)
    print("="*60)
    print("2. AFTER AUTHORIZING, YOU WILL BE REDIRECTED TO A LOCALHOST URL.")
    print("3. COPY THE FULL REDIRECT URL (OR THE 'code=' VALUE) AND PASTE IT BELOW.")
    print("="*60 + "\n")
    
    code = input("Enter the authorization code or full redirect URL: ").strip()
    
    if "code=" in code:
        # Extract authorization code from the full redirect URL if pasted
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(code)
        code = parse_qs(parsed.query).get('code', [code])[0]

    flow.fetch_token(code=code)
    return flow
    
