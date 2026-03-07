import os
import keyring

def login(token: str = None):
    """
    Saves the user's TVC_TOKEN to the system keychain.
    """
    active_token = token or os.environ.get("TVC_TOKEN")
    
    if not active_token:
        print("Authentication required.")
        print("Get your token at: https://thevirtualcell.com/settings")
        active_token = input("Enter TVC_TOKEN: ").strip()
    
    if active_token:
        keyring.set_password("vcpi-client", "TVC_TOKEN", active_token)
        print("\n" + "="*30)
        print("LOGIN SUCCESSFUL")
        print("="*30 + "\n")
    else:
        print("Error: No token provided.")
