import os
from google.cloud import storage
from log import logger
from google.api_core.exceptions import NotFound, Conflict, Forbidden
from google.api_core.exceptions import GoogleAPIError

# IDEAS SPEED / PERFORMANCE IMPROVEMENTS:
# --default-storage-class = nearline | standard | coldline | archive

def connect_to_storage_client():
    """
    Connects to Google Cloud Storage using the project ID from the environment.
    Returns:
        storage.Client: A Google Cloud Storage client instance.
    Raises:
        GoogleAPIError: If the GCP_PROJECT_ID environment variable is not set or if the client connection fails.
    """
    project_id = os.getenv("GCP_PROJECT_ID", 'taxifare-opt')
    if not project_id:
        raise GoogleAPIError("GCP_PROJECT_ID environment variable is not set.")

    try:
        storage_client = storage.Client(project=project_id)
        return storage_client
    except GoogleAPIError as e:
        logger.error(f"Failed to connect to Google Cloud Storage: {e}")
        raise


def bucket_exists(bucket_name):
    """
    Checks whether a bucket exists and returns the bucket object if found.
    Args:
        bucket_name (str): The name of the bucket to check.
    Returns:
        storage.Bucket: The bucket object if it exists; otherwise, None.
    Raises:
        GoogleAPIError: If there is an error accessing Google Cloud Storage.
    """

    try:
        storage_client = connect_to_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        if not bucket:
            raise NotFound(f"Bucket '{bucket_name}' not found.")
        logger.info(f"Bucket {bucket_name} already exists.")
        return bucket

    except NotFound:
        logger.info(f"Bucket {bucket_name} does not exist.")
        return None
    except GoogleAPIError as e:
        logger.error(f"Error occurred while accessing bucket '{bucket_name}': {e}")
        raise

def create_bucket(bucket_name, storage_class=None, bucket_location=None):
    """
    Creates a new bucket with the specified storage class and location.
    Args:
        bucket_name (str): The name of the bucket to create.
        storage_class (str, optional): The storage class for the bucket (e.g., "STANDARD", "NEARLINE").
            Defaults to "STANDARD" or value from BUCKET_STORAGE_CLASS environment variable.
        bucket_location (str, optional): The location for the bucket (e.g., "EUROPE-WEST1").
            Defaults to "EUROPE-WEST1" or value from BUCKET_LOCATION environment variable.
    Returns:
        storage.Bucket: The newly created bucket object.
    Raises:
        Conflict: If the bucket name is already taken by another project.
        Forbidden: If permissions are insufficient to create the bucket.
        GoogleAPIError: If there is an error connecting to Google Cloud Storage or creating the bucket.
    """
    bucket_location = bucket_location or os.getenv("BUCKET_LOCATION", "EUROPE-WEST1")
    storage_class = storage_class or os.getenv("BUCKET_STORAGE_CLASS", "STANDARD")

    if bucket := bucket_exists(bucket_name):
        return bucket

    try:
        storage_client = connect_to_storage_client()
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = storage_class

        new_bucket = storage_client.create_bucket(bucket, location=bucket_location)

        logger.info(
            f"Created bucket {new_bucket.name} in {new_bucket.location} with storage class {new_bucket.storage_class}"
        )
        return new_bucket

    except Conflict:
        logger.error(f"Bucket name '{bucket_name}' is already taken by another project.")
        raise
    except Forbidden:
        logger.error(f"Permission denied to create bucket '{bucket_name}'.")
        raise
    except GoogleAPIError as e:
        logger.error(f"Failed to connect to Google Cloud Storage: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while creating the bucket: {e}")
        raise


def list_buckets():
    """
    Lists all buckets in the current project.
    Returns:
        list: A list of bucket names in the current project.
    Raises:
        GoogleAPIError: If there is an error connecting to Google Cloud Storage.
    """

    try:
        storage_client = connect_to_storage_client()
        buckets = storage_client.list_buckets()
        bucket_names = [bucket.name for bucket in buckets]

        project_id = os.getenv("GCP_PROJECT_ID")
        logger.info(f"Buckets in project '{project_id}': {bucket_names}")
        return bucket_names

    except GoogleAPIError as e:
        logger.error(f"Failed to connect to Google Cloud Storage: {e}")
        raise


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to the specified bucket.
    Args:
        bucket_name (str): The name of the bucket to upload to.
        source_file_name (str): The local file path of the file to upload.
        destination_blob_name (str): The destination path within the bucket.
    Raises:
        NotFound: If the specified bucket does not exist.
        GoogleAPIError: If there is an error connecting to Google Cloud Storage.
        FileNotFoundError: If the source file does not exist.
        Exception: For any other unexpected errors during upload.
    """

    try:
        storage_client = connect_to_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        if not bucket:
            raise NotFound(f"Bucket '{bucket_name}' not found.")
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logger.info(f"File {source_file_name} uploaded to {destination_blob_name}.")

    except NotFound:
        logger.error(f"Bucket {bucket_name} does not exist.")
        raise
    except GoogleAPIError as e:
        logger.error(f"Google Cloud Storage API error during upload of '{source_file_name}' to '{bucket_name}': {e}")
        raise
    except FileNotFoundError as e:
        logger.error(f"Source file '{source_file_name}' not found. Cannot upload to '{destination_blob_name}' in bucket '{bucket_name}'.")
        raise
    except Exception as e:
        logger.error(f"Failed to upload file {source_file_name} to {bucket_name}: {e}")
        raise

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a file from the specified bucket.
    Args:
        bucket_name (str): The name of the bucket to download from.
        source_blob_name (str): The path of the file within the bucket.
        destination_file_name (str): The local path to save the downloaded file.
    Raises:
        NotFound: If the specified bucket or blob does not exist.
        GoogleAPIError: If there is an error connecting to Google Cloud Storage.
        Exception: For any other unexpected errors during download.
    """

    try:
        storage_client = connect_to_storage_client()

        bucket = storage_client.get_bucket(bucket_name)
        if not bucket:
            raise NotFound(f"Bucket '{bucket_name}' not found.")

        blob = bucket.blob(source_blob_name)
        if not blob.exists():
            raise NotFound(f"Blob '{source_blob_name}' not found in bucket '{bucket_name}'.")

        blob.download_to_filename(destination_file_name)
        logger.info(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

    except NotFound:
        logger.error(f"Bucket {bucket_name} or blob {source_blob_name} does not exist.")
        raise
    except GoogleAPIError as e:
        logger.error(f"Google Cloud Storage API error during download of '{source_blob_name}' from bucket '{bucket_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to download blob {source_blob_name}: {e}")
        raise


def upload_directory(local_dir, bucket_name, gcs_dir):
    """
    Uploads a local directory to a GCS bucket.
    """
    client = connect_to_storage_client()
    bucket = client.bucket(bucket_name)
    for root, _, files in os.walk(local_dir):
        for file in files:
            logger.info(f'root {root} file {file}')
            file_path = os.path.join(root, file)
            logger.info(f'file_path {file_path}')
            relative_path = os.path.relpath(file_path, local_dir)
            logger.info(f'relative_path {relative_path}')
            gcs_relative_path = '/'.join(gcs_dir.split('/')[3:])
            gcs_path = os.path.join(gcs_relative_path, relative_path)
            logger.info(f'gcs_path {gcs_path}')
            blob = bucket.blob(gcs_path)

            blob.upload_from_filename(file_path)
            logger.info(f"Uploaded file {file_path} to {gcs_path}")

def delete_existing_blobs(bucket_name, blob_path):
    logger.info(f"Deleting blobs located at {blob_path}")
    client = connect_to_storage_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=blob_path))
    if blobs:
        logger.info(f"Deleting {len(blobs)} blobs :")
        [logger.info(b) for b in blobs]

        for blob in blobs:
            blob.delete()
        logger.info(f"Existing blob deleted from {blob_path}")
    else:
        logger.info(f"No existing blob found at {blob_path}")
