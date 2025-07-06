import os
from google.cloud import storage

# Configuration
PROJECT_ID = "736350108107"
GCS_BUCKET_NAME = "theologpt"

def debug_blob_names():
    """Debug the actual blob names in the bucket."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    korean_prefix = "주석/01_창세기/"
    
    print(f"🔍 Looking for files starting with: '{korean_prefix}'")
    print(f"Prefix bytes: {korean_prefix.encode('utf-8')}")
    
    all_blobs = bucket.list_blobs()
    count = 0
    korean_files = []
    
    for blob in all_blobs:
        count += 1
        
        # Check if the blob name contains Korean characters
        if '주석' in blob.name:
            korean_files.append(blob.name)
            if len(korean_files) <= 10:
                print(f"  Korean file {len(korean_files)}: '{blob.name}'")
                print(f"    Bytes: {blob.name.encode('utf-8')}")
                print(f"    Starts with prefix? {blob.name.startswith(korean_prefix)}")
                
                # Try character by character comparison
                if len(blob.name) >= len(korean_prefix):
                    prefix_part = blob.name[:len(korean_prefix)]
                    print(f"    First {len(korean_prefix)} chars: '{prefix_part}'")
                    print(f"    Character comparison:")
                    for i, (a, b) in enumerate(zip(korean_prefix, prefix_part)):
                        print(f"      {i}: '{a}' vs '{b}' -> {a == b}")
    
    print(f"\n📊 Summary:")
    print(f"  Total files: {count}")
    print(f"  Files with '주석': {len(korean_files)}")
    
    if korean_files:
        print(f"\nFirst few Korean files:")
        for i, filename in enumerate(korean_files[:5]):
            print(f"  {i+1}. {filename}")

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    debug_blob_names()
