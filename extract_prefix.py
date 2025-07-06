import os
from google.cloud import storage

# Configuration
PROJECT_ID = "736350108107"
GCS_BUCKET_NAME = "theologpt"

def extract_exact_prefix():
    """Extract the exact prefix from a known file."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    # We know this file exists: '주석/01_창세기/창세기_NICOT주석_01.pdf'
    # Let's find it and extract the exact directory path
    
    all_blobs = bucket.list_blobs()
    
    for blob in all_blobs:
        if 'NICOT주석_01.pdf' in blob.name:
            print(f"Found target file: {blob.name}")
            print(f"File bytes: {blob.name.encode('utf-8')}")
            
            # Extract directory path
            parts = blob.name.split('/')
            if len(parts) >= 3:
                extracted_prefix = '/'.join(parts[:2]) + '/'
                print(f"Extracted prefix: '{extracted_prefix}'")
                print(f"Prefix bytes: {extracted_prefix.encode('utf-8')}")
                
                # Now test this exact prefix
                print(f"\n🧪 Testing extracted prefix...")
                test_blobs = bucket.list_blobs(prefix=extracted_prefix)
                pdf_count = 0
                
                for test_blob in test_blobs:
                    if test_blob.name.lower().endswith('.pdf'):
                        pdf_count += 1
                        if pdf_count <= 3:
                            print(f"  PDF {pdf_count}: {test_blob.name}")
                
                print(f"Total PDFs found with extracted prefix: {pdf_count}")
                return extracted_prefix
            break
    
    return None

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    prefix = extract_exact_prefix()
    if prefix:
        print(f"\n✅ Successfully found working prefix: '{prefix}'")
    else:
        print("\n❌ Could not find working prefix.")
