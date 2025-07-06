import os
from google.cloud import storage

# Configuration
PROJECT_ID = "736350108107"
GCS_BUCKET_NAME = "theologpt"
GCS_INPUT_PREFIX = "주석/"

def list_subdirectories_in_gcs_debug(project_id: str, bucket_name: str, prefix: str):
    """
    Lists all subdirectories under a given GCS prefix.
    """
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    print(f"🔍 Looking for subdirectories under: '{prefix}'")
    
    # List all blobs in the bucket
    all_blobs = bucket.list_blobs()
    
    subdirectories = set()
    file_count = 0
    matching_files = 0
    
    for blob in all_blobs:
        file_count += 1
        
        # Check if this blob matches our prefix
        if blob.name.startswith(prefix):
            matching_files += 1
            print(f"  Matching file {matching_files}: {blob.name}")
            
            # Extract the directory path from the file path
            relative_path = blob.name[len(prefix):]  # Remove the prefix
            print(f"    Relative path: '{relative_path}'")
            
            if '/' in relative_path:
                # Get the first directory level
                first_dir = relative_path.split('/')[0]
                print(f"    First directory: '{first_dir}'")
                if first_dir:  # Make sure it's not empty
                    subdir_path = prefix + first_dir + '/'
                    print(f"    Adding subdirectory: {subdir_path}")
                    subdirectories.add(subdir_path)
            
            if matching_files >= 10:  # Limit output for debugging
                print("  ... (showing first 10 matching files only)")
                break
    
    print(f"\n📊 Total files checked: {file_count}")
    print(f"📊 Files matching prefix '{prefix}': {matching_files}")
    print(f"📊 Found {len(subdirectories)} subdirectories:")
    for subdir in sorted(subdirectories):
        print(f"  - {subdir}")
    
    return list(subdirectories)

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    subdirs = list_subdirectories_in_gcs_debug(PROJECT_ID, GCS_BUCKET_NAME, GCS_INPUT_PREFIX)
