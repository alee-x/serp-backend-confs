from airflow.hooks.base_hook import BaseHook

#https://gitlab.chi.swan.ac.uk/UKSerp/ApplianceHelperLibraries/-/blob/master/ApplianceHelperLibraries/BL-SAIL/ScriptGenerator.cs
def get_encryption_key_size(key):
    key = key.upper()
    encryption = None
    keyColSizeOverride = 0

    if key in ['KEY','KEY30']:
        encryption = "KEY"
        keyColSizeOverride = 30
    elif key == 'RALF': 
        encryption = "RALF"
        keyColSizeOverride = 16
    elif key == 'ALF': 
        encryption = "ALF"
        keyColSizeOverride = 10
    elif key == 'KEY15':
        encryption = "KEY"
        keyColSizeOverride = 15
    elif key == 'KEY50': 
        encryption = "KEY"
        keyColSizeOverride = 50
    elif key == 'HCP': 
        encryption = "HCP"
        keyColSizeOverride = 12  
    elif key == 'PROJKEY': 
        encryption = "PROJKEY"
        keyColSizeOverride = 15      
    else:
        encryption = ""
        keyColSizeOverride = 0
        raise ValueError("ERROR: Unexpected key {0}".format(key))

    return encryption, keyColSizeOverride


def create_password(pwdLength):
    
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits
    while True:
        password = ''.join(secrets.choice(alphabet) for i in range(pwdLength))  # specify password character length 
        # Enforce an acceptable structure to avoid an insecure password message
        if ( sum(c.islower() for c in password) >=1 and sum(c.isupper() for c in password) >=1 and sum(c.isdigit() for c in password) >=1 ):
            break
    return password





