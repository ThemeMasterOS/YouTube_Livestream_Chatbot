import os
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

def Authorize(file):
    flow = InstalledAppFlow.from_client_secrets_file(file, scopes={
        'openid',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile',
        'https://www.googleapis.com/auth/youtube',
        'https://www.googleapis.com/auth/youtube.force-ssl',
        'https://www.googleapis.com/auth/youtube.readonly',
    })
    
    # We use redirect_uri = 'http://localhost:5500/' matching your OAuth Client ID setup
    flow.redirect_uri = 'http://localhost:5500/'
    
    auth_url, _ = flow.authorization_url(prompt='consent')
    
    print("\n" + "="*50)
    print("PLEASE GO TO THIS URL ON YOUR PHONE/BROWSER TO AUTHENTICATE:")
    print(auth_url)
    print("="*50 + "\n")
    
    # Render logs will prompt for code if needed or process authorization flow
    return flow
    
