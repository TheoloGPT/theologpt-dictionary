import os
import urllib.parse
from google.cloud import storage

# Configuration
PROJECT_ID = "736350108107"
GCS_BUCKET_NAME = "theologpt"

def test_korean_characters():
    """Test different ways to handle Korean characters in GCS paths."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    # Test different encodings
    korean_prefix = "주석/01_창세기/"
    
    print(f"🔍 Testing Korean prefix: '{korean_prefix}'")
    print(f"UTF-8 bytes: {korean_prefix.encode('utf-8')}")
    print(f"URL encoded: {urllib.parse.quote(korean_prefix)}")
    
    # Try direct approach
    print("\n1. Direct approach:")
    blobs = bucket.list_blobs(prefix=korean_prefix)
    count = 0
    for blob in blobs:
        count += 1
        if count <= 5:
            print(f"  - {blob.name}")
        if count > 5:
            print(f"  ... and {count-5} more")
            break
    
    # Try URL encoded approach
    print("\n2. URL encoded approach:")
    encoded_prefix = urllib.parse.quote(korean_prefix)
    blobs = bucket.list_blobs(prefix=encoded_prefix)
    count = 0
    for blob in blobs:
        count += 1
        if count <= 5:
            print(f"  - {blob.name}")
        if count > 5:
            print(f"  ... and {count-5} more")
            break
    
    # List all files and find matches manually
    print("\n3. Manual matching approach:")
    all_blobs = bucket.list_blobs()
    matches = []
    total_count = 0
    
    for blob in all_blobs:
        total_count += 1
        if blob.name.startswith(korean_prefix):
            matches.append(blob.name)
            if len(matches) <= 5:
                print(f"  ✅ Match: {blob.name}")
    
    print(f"\n📊 Results:")
    print(f"  Total files in bucket: {total_count}")
    print(f"  Files matching Korean prefix: {len(matches)}")
    
    return matches

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    matches = test_korean_characters()
