import os
import time
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
from google.api_core.client_options import ClientOptions

# --- Configuration ---
PROJECT_ID = "736350108107"
PROCESSOR_LOCATION = "us"
PROCESSOR_ID = "786384a4117ccb51"
GCS_BUCKET_NAME = "theologpt"

# Test with one subdirectory
TEST_SUBDIRECTORY = "주석/01_창세기/"
GCS_OUTPUT_PREFIX = "text_annotations/"

def list_pdf_files_in_directory(project_id: str, bucket_name: str, prefix: str):
    """Lists all PDF files in a specific GCS directory."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # List all blobs with the prefix
    blobs = bucket.list_blobs(prefix=prefix)
    
    pdf_files = []
    for blob in blobs:
        if blob.name.lower().endswith('.pdf'):
            pdf_uri = f"gs://{bucket_name}/{blob.name}"
            pdf_files.append(pdf_uri)
    
    return pdf_files

def test_single_directory():
    """Test processing a single directory."""
    print(f"🧪 Testing single directory: {TEST_SUBDIRECTORY}")
    
    # List PDF files
    pdf_files = list_pdf_files_in_directory(PROJECT_ID, GCS_BUCKET_NAME, TEST_SUBDIRECTORY)
    
    print(f"📄 Found {len(pdf_files)} PDF files:")
    for i, pdf_file in enumerate(pdf_files, 1):
        print(f"  {i}. {pdf_file}")
    
    if not pdf_files:
        print("❌ No PDF files found!")
        return False
    
    # Initialize Document AI client
    opts = ClientOptions(api_endpoint=f"{PROCESSOR_LOCATION}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # Get the full resource name of the processor
    processor_name = client.processor_path(PROJECT_ID, PROCESSOR_LOCATION, PROCESSOR_ID)

    # Create output URI
    output_uri = f"gs://{GCS_BUCKET_NAME}/{GCS_OUTPUT_PREFIX}test_01_창세기/"

    # Configure the input documents from individual files
    gcs_documents = []
    for pdf_file in pdf_files:
        gcs_documents.append(
            documentai.GcsDocument(
                gcs_uri=pdf_file,
                mime_type="application/pdf"
            )
        )

    input_config = documentai.BatchDocumentsInputConfig(
        gcs_documents=documentai.GcsDocuments(documents=gcs_documents)
    )

    # Configure the output location in GCS
    output_config = documentai.DocumentOutputConfig(
        gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(
            gcs_uri=output_uri
        )
    )

    # Create the batch processing request
    request = documentai.BatchProcessRequest(
        name=processor_name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    print(f"🚀 Starting batch processing...")
    print(f"📤 Output will be written to: {output_uri}")

    try:
        # Send the batch processing request (asynchronous operation)
        operation = client.batch_process_documents(request)

        print(f"⏳ Operation {operation.operation.name} started. Waiting for completion...")

        # Wait for the operation to complete
        operation.result()

        print(f"✅ Test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
    
    success = test_single_directory()
    if success:
        print("\n🎉 Test successful! The approach works.")
    else:
        print("\n💥 Test failed. Need to debug further.")
