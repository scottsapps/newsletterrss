#!/usr/bin/env python3
"""One-time helper script to obtain a Gmail OAuth2 refresh token.

Run this ONCE on your local machine after creating OAuth credentials
in Google Cloud Console. It will open a browser for you to authorize
access, then print the three values you need to add as GitHub Secrets.

Usage:
    pip install google-auth-oauthlib
    python get_refresh_token.py
"""

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

print("=" * 60)
print("Gmail OAuth2 Refresh Token Helper")
print("=" * 60)
print()
print("Paste the values from your Google Cloud Console OAuth client.")
print("(APIs & Services → Credentials → your OAuth 2.0 Client ID)")
print()

client_id = input("Client ID:     ").strip()
client_secret = input("Client Secret: ").strip()

print()
print("A browser window will open. Sign in with scottsnewsletters1014@gmail.com")
print("and grant read + modify access. Come back here when done.")
print()

client_config = {
    "installed": {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }
}

flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
creds = flow.run_local_server(port=0)

print()
print("=" * 60)
print("SUCCESS! Add these three GitHub Secrets to your repo:")
print("  (Settings → Secrets and variables → Actions → New repository secret)")
print("=" * 60)
print()
print(f"  GMAIL_CLIENT_ID      →  {client_id}")
print(f"  GMAIL_CLIENT_SECRET  →  {client_secret}")
print(f"  GMAIL_REFRESH_TOKEN  →  {creds.refresh_token}")
print()
print("=" * 60)
