import os
import unicodedata
from google.cloud import storage

# Configuration
PROJECT_ID = "736350108107"
GCS_BUCKET_NAME = "theologpt"

def test_normalized_korean():
    """Test with properly normalized Korean characters."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    # Original Korean prefix
    korean_prefix = "주석/01_창세기/"
    
    # Normalize to NFD (decomposed form)
    normalized_prefix = unicodedata.normalize('NFD', korean_prefix)
    
    print(f"🔍 Original prefix: '{korean_prefix}'")
    print(f"Original bytes: {korean_prefix.encode('utf-8')}")
    print(f"🔄 Normalized prefix: '{normalized_prefix}'")
    print(f"Normalized bytes: {normalized_prefix.encode('utf-8')}")
    
    # List files with normalized prefix
    blobs = bucket.list_blobs(prefix=normalized_prefix)
    pdf_files = []
    
    for blob in blobs:
        if blob.name.lower().endswith('.pdf'):
            pdf_files.append(blob.name)
    
    print(f"\n📄 Found {len(pdf_files)} PDF files:")
    for i, pdf_file in enumerate(pdf_files, 1):
        print(f"  {i}. {pdf_file}")
    
    return pdf_files

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    pdf_files = test_normalized_korean()
    if pdf_files:
        print(f"\n✅ Success! Found {len(pdf_files)} PDF files with normalized Korean characters.")
    else:
        print("\n❌ Still no PDF files found.")
