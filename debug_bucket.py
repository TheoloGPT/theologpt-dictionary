import os
from google.cloud import storage

# Configuration
PROJECT_ID = "736350108107"
GCS_BUCKET_NAME = "theologpt"
GCS_INPUT_PREFIX = "주석/"

def list_bucket_contents(project_id: str, bucket_name: str, prefix: str = ""):
    """List all contents in the bucket under the given prefix."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    print(f"📁 Listing all contents in gs://{bucket_name}/{prefix}")
    print("=" * 60)
    
    # List all blobs (files and folders)
    blobs = bucket.list_blobs(prefix=prefix)
    
    files = []
    folders = set()
    
    for blob in blobs:
        print(f"  {blob.name}")
        if blob.name.endswith('/'):
            folders.add(blob.name)
        else:
            files.append(blob.name)
            # Extract folder path
            folder_path = '/'.join(blob.name.split('/')[:-1]) + '/'
            if folder_path != prefix and folder_path != "/":
                folders.add(folder_path)
    
    print("\n" + "=" * 60)
    print(f"📊 Summary:")
    print(f"Files found: {len(files)}")
    print(f"Unique folder paths detected: {len(folders)}")
    
    if folders:
        print(f"\n📁 Detected folders:")
        for folder in sorted(folders):
            print(f"  - {folder}")
    
    return list(folders), files

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    # First check the root of the bucket
    print("🔍 Checking root of bucket...")
    folders, files = list_bucket_contents(PROJECT_ID, GCS_BUCKET_NAME, "")
    
    # Then check specifically for the 주석 directory
    print(f"\n🔍 Checking specifically for '주석/' directory...")
    folders2, files2 = list_bucket_contents(PROJECT_ID, GCS_BUCKET_NAME, GCS_INPUT_PREFIX)
