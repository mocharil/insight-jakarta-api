from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration for Google Cloud Storage
SERVICE_ACCOUNT_CREDENTIALS_PATH = os.getenv('CREDENTIAL_GCS_FILE_PATH')
BUCKET_NAME = os.getenv('BUCKET_NAME')
PROJECT_ID=os.getenv('PROJECT_ID')

# Initialize GCS Client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_CREDENTIALS_PATH)
storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
bucket = storage_client.get_bucket(BUCKET_NAME)

def upload_to_gcs(source_file: str, destination_blob_name: str):
    """
    Uploads a file to Google Cloud Storage.

    Parameters:
    - source_file (str): Local file path to be uploaded.
    - destination_blob_name (str): Destination path in the GCS bucket.

    Returns:
    dict: Confirmation message with uploaded file path.
    """
    # Upload the file to GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file)

    print(f"File {source_file} uploaded to {BUCKET_NAME}/{destination_blob_name} successfully.")


def download_from_gcs(blob_name: str, destination_file: str):
    """
    Downloads a file from Google Cloud Storage.

    Parameters:
    - blob_name (str): Path of the file in the GCS bucket.
    - destination_file (str): Local path to save the downloaded file.

    Returns:
    dict: Confirmation message with the downloaded file path.
    """
    # Download the file from GCS
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file)

    print(f"File {blob_name} downloaded from {BUCKET_NAME} to {destination_file} successfully.")

    return destination_file
