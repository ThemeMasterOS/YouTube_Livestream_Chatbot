"""
token_generator.py

Standalone helper to generate token.json (or backup_token.json /
backup2_token.json) without needing to run the full bot.

Run this once per credential set you want to set up:
    python token_generator.py

It will:
  1. Ask which credential set you're generating a token for.
  2. Look for the matching client_secret file in this folder.
  3. Open your browser so you can log in with Google and allow access.
  4. Save the resulting token to the matching token file.

Main token is required for the bot to run at all.
Backup 1 and Backup 2 are both optional — only set them up if you want
the extra fallback capacity for reading chat when quota runs low.
"""

import os
import sys

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("Missing dependency: google-auth-oauthlib")
    print("Run: pip install -r requirements.txt")
    sys.exit(1)

# Needs access to read + send live chat messages
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

CREDENTIAL_SETS = {
    "1": ("Main",     "client_secret.json",        "token.json"),
    "2": ("Backup 1", "backup_client_secret.json", "backup_token.json"),
    "3": ("Backup 2", "backup2_client_secret.json", "backup2_token.json"),
}


def generate_token(label, client_secret_file, token_file):
    if not os.path.exists(client_secret_file):
        print(f"\n❌ Could not find '{client_secret_file}' in this folder.")
        print(f"   Download it from Google Cloud Console (OAuth client ID, Desktop app type),")
        print(f"   rename it to '{client_secret_file}', and place it in this same folder.")
        return False

    print(f"\nGenerating token for: {label}")
    print(f"Using client secret: {client_secret_file}")
    print("A browser window will open — log in with the Google account for this")
    print("credential set and click Allow.\n")

    try:
        flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
        creds = flow.run_local_server(port=0)
    except Exception as e:
        print(f"\n❌ Authorization failed: {e}")
        return False

    try:
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    except Exception as e:
        print(f"\n❌ Got credentials, but couldn't write '{token_file}': {e}")
        return False

    print(f"\n✅ Success! '{token_file}' has been created.")
    return True


def main():
    print("=" * 50)
    print(" TOKEN GENERATOR")
    print("=" * 50)
    print("\nWhich token do you want to generate?")
    print("  [1] Main        (required — the bot needs this to run)")
    print("  [2] Backup 1    (optional fallback)")
    print("  [3] Backup 2    (optional fallback)")

    choice = input("\nEnter 1, 2, or 3: ").strip()

    if choice not in CREDENTIAL_SETS:
        print("Invalid choice. Please run the script again and enter 1, 2, or 3.")
        sys.exit(1)

    label, client_secret_file, token_file = CREDENTIAL_SETS[choice]

    if os.path.exists(token_file):
        overwrite = input(f"\n'{token_file}' already exists. Overwrite it? (y/n): ").strip().lower()
        if overwrite != "y":
            print("Cancelled. No changes made.")
            sys.exit(0)

    success = generate_token(label, client_secret_file, token_file)

    if success and choice == "1":
        print("\nYou're all set — you can now run the bot with: python bot.py")
    elif success:
        print(f"\n{label} is now configured as an extra fallback. Nothing else to do —")
        print("the bot will pick it up automatically next time it starts.")


if __name__ == "__main__":
    main()
