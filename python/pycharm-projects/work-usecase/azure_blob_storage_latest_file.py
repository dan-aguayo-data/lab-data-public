# Replace with your actual connection string
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

# Replace with your actual connection string
connection_string = "XXXXX;"
# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
print("Successfully connected to BlobServiceClient")

# Initialize variables to keep track of the latest blob
latest_blob = None
latest_time = datetime.min.replace(tzinfo=timezone.utc)
latest_container_name = None

# List all containers in the storage account
containers = blob_service_client.list_containers()
print("Successfully listed containers")

for container in containers:
    container_name = container.name
    container_client = blob_service_client.get_container_client(container_name)
    print(f"Checking container: {container_name}")

    # List all blobs in the container
    blobs = container_client.list_blobs()

    for blob in blobs:
        if blob.last_modified > latest_time:
            latest_time = blob.last_modified
            latest_blob = blob
            latest_container_name = container_name

# Output the latest modified blob across all containers
if latest_blob:
    print(f"The latest modified blob is '{latest_blob.name}' in container '{latest_container_name}' with modification time {latest_time}")
else:
    print("No blobs found in any container.")
