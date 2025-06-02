from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def get_password(keyvaulturl,secretname):
    # Set the Key Vault URL
    key_vault_url = keyvaulturl

    # Authenticate using DefaultAzureCredential
    # This works with Managed Identity, Azure CLI authentication, or environment variables
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Get the secret from Key Vault
    secret_name = secretname
    try:
        retrieved_secret = client.get_secret(secret_name)
        superset_password = retrieved_secret.value
        print("Retrieved secret successfully!")
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        superset_password = None

    # Use the password in your application
    return superset_password


superset_password = get_password("https://xxxxxxxx.vault.azure.net/","superset-dev")

print(f"Function test: {superset_password}")
