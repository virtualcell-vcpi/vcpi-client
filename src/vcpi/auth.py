import os
import keyring
import httpx

from .data import _clear_token_cache, SUPABASE_FUNCTIONS_URL, SUPABASE_KEY, TIMEOUT_METADATA


def _validate_token(token: str) -> bool:
    """Check if a token is valid by making a lightweight API call."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    try:
        resp = httpx.get(
            f"{SUPABASE_FUNCTIONS_URL}/list-datasets",
            headers=headers,
            timeout=TIMEOUT_METADATA,
        )
        return resp.status_code not in (401, 403)
    except httpx.HTTPError:
        return False


def login(token: str = None):
    """
    Validates and saves the user's TVC_TOKEN to the system keychain.
    """
    active_token = token

    if not active_token:
        existing = keyring.get_password("vcpi-client", "TVC_TOKEN")
        if existing and _validate_token(existing):
            print("Already logged in.")
            return
        print("Authentication required.")
        print("Get your token at: https://thevirtualcell.com/dashboard (open Settings)")
        active_token = input("Enter API Key: ").strip()
    
    if not active_token:
        print("Error: No token provided.")
        return

    if not _validate_token(active_token):
        print("\n" + "=" * 30)
        print("LOGIN FAILED")
        print("=" * 30)
        print("Token is invalid or revoked.")
        print("Generate a new one at: https://thevirtualcell.com/dashboard (open Settings)\n")
        return

    keyring.set_password("vcpi-client", "TVC_TOKEN", active_token)
    _clear_token_cache()
    print("\n" + "=" * 30)
    print("LOGIN SUCCESSFUL")
    print("=" * 30 + "\n")
