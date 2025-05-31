import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Replace with your actual details
account = 'xxxx.australia-east.azure.snowflakecomputing.com'
user = 'ADF_xxxx'
private_key_path = r'C:\Users\xxxxxx\private_key.pem'
private_key_password = 'ADFTEST'  # Directly use the passphrase

# Print the password to verify it is being retrieved correctly (remove this in production)
print(f"Retrieved password: {private_key_password}")

# Load the private key
with open(private_key_path, 'rb') as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=private_key_password.encode(),
        backend=default_backend()
    )

# Convert the private key to the correct format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Create a Snowflake connection
conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes
)

# Test the connection by running a query
try:
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
    for row in cur:
        print(row)
finally:
    cur.close()
    conn.close()
