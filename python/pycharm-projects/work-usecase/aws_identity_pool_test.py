import boto3
from botocore.exceptions import ClientError

# Replace with your Cognito Identity Pool ID and region
IDENTITY_POOL_ID = 'ap-southeast-2:XXXXXXXX'
REGION = 'ap-southeast-2'

def test_cognito_identity_pool():
    try:
        # Initialize the Cognito Identity client
        client = boto3.client('cognito-identity', region_name=REGION)

        # Step 1: Get an identity ID from the identity pool
        identity_response = client.get_id(IdentityPoolId=IDENTITY_POOL_ID)
        identity_id = identity_response['IdentityId']
        print(f"Identity ID: {identity_id}")

        # Step 2: Use the identity ID to get temporary credentials
        credentials_response = client.get_credentials_for_identity(IdentityId=identity_id)
        credentials = credentials_response['Credentials']

        print("Temporary AWS Credentials:")
        print(f"Access Key ID: {credentials['AccessKeyId']}")
        print(f"Secret Access Key: {credentials['SecretKey']}")
        print(f"Session Token: {credentials['SessionToken']}")

    except ClientError as e:
        print("Error:", e)
        print("Make sure the IAM role associated with your Cognito Identity Pool has the necessary permissions.")

# Run the test function
test_cognito_identity_pool()
