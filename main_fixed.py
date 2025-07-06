import os
import time
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
from google.api_core.client_options import ClientOptions

# --- Configuration ---
# Your Google Cloud Project ID
PROJECT_ID = "736350108107"  # <--- IMPORTANT: Replace with your actual Project ID

# The region where your Document AI processor is located (e.g., 'us', 'eu', 'asia-northeast3')
# This should match the region you selected when creating the processor.
PROCESSOR_LOCATION = "us"  # <--- IMPORTANT: Replace with your processor's region

# The ID of your Document AI "Enterprise Document OCR" processor
# You can find this in the Document AI console after creating the processor.
PROCESSOR_ID = "786384a4117ccb51"  # <--- IMPORTANT: Replace with your actual Processor ID

# Google Cloud Storage bucket name
GCS_BUCKET_NAME = "theologpt"

# Input directory within the GCS bucket where your scanned PDFs are located
GCS_INPUT_PREFIX = "주석/" # Ensure this ends with a slash for a directory

# Output directory within the GCS bucket where the transcribed text will be saved
GCS_OUTPUT_PREFIX = "text_annotations/" # Ensure this ends with a slash for a directory

# Full GCS input and output URIs
GCS_INPUT_URI = f"gs://{GCS_BUCKET_NAME}/{GCS_INPUT_PREFIX}"
GCS_OUTPUT_URI = f"gs://{GCS_BUCKET_NAME}/{GCS_OUTPUT_PREFIX}"

# --- Helper Function to List Subdirectories ---
def list_subdirectories_in_gcs(project_id: str, bucket_name: str, prefix: str):
    """
    Lists all subdirectories under a given GCS prefix.
    
    Args:
        project_id (str): Your Google Cloud Project ID.
        bucket_name (str): The GCS bucket name.
        prefix (str): The prefix to search under.
    
    Returns:
        list: List of subdirectory paths.
    """
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Use delimiter='/' to get only immediate subdirectories
    blobs = bucket.list_blobs(prefix=prefix, delimiter='/')
    
    subdirectories = []
    for page in blobs.pages:
        subdirectories.extend(page.prefixes)
    
    return subdirectories

# --- Helper Function to Process Individual Subdirectories ---
def batch_transcribe_subdirectory(
    project_id: str,
    location: str,
    processor_id: str,
    subdirectory_uri: str,
    output_base_uri: str,
):
    """
    Process a single subdirectory for batch OCR.
    """
    try:
        # Initialize Document AI client
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        client = documentai.DocumentProcessorServiceClient(client_options=opts)

        # Get the full resource name of the processor
        processor_name = client.processor_path(project_id, location, processor_id)

        # Create output URI for this specific subdirectory
        subdir_name = subdirectory_uri.split('/')[-2]  # Get the subdirectory name
        output_uri = f"{output_base_uri}{subdir_name}/"

        # Configure the input documents from GCS
        input_config = documentai.BatchDocumentsInputConfig(
            gcs_prefix=documentai.GcsPrefix(gcs_uri_prefix=subdirectory_uri)
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

        print(f"Processing subdirectory: {subdirectory_uri}")
        print(f"Output will be written to: {output_uri}")

        # Send the batch processing request (asynchronous operation)
        operation = client.batch_process_documents(request)

        print(f"Operation {operation.operation.name} started. Waiting for completion...")

        # Wait for the operation to complete
        operation.result()

        print(f"✅ Completed processing: {subdir_name}")
        return True

    except Exception as e:
        print(f"❌ Error processing {subdirectory_uri}: {e}")
        return False

# --- Main Batch Processing Function ---
def batch_transcribe_gcs_pdfs(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_input_uri: str,
    gcs_output_uri: str,
):
    """
    Initiates a batch OCR process for scanned PDFs in GCS using Document AI.

    Args:
        project_id (str): Your Google Cloud Project ID.
        location (str): The region where your Document AI processor is located.
        processor_id (str): The ID of your Document AI processor.
        gcs_input_uri (str): The GCS URI prefix for your input documents
                              (e.g., "gs://your-bucket/your-folder/").
        gcs_output_uri (str): The GCS URI prefix for where the output will be
                              written (e.g., "gs://your-bucket/output-folder/").
    """
    try:
        print(f"🔍 Discovering subdirectories in: {gcs_input_uri}")
        
        # Extract bucket name and prefix from input URI
        bucket_name = gcs_input_uri.replace("gs://", "").split("/")[0]
        input_prefix = "/".join(gcs_input_uri.replace("gs://", "").split("/")[1:])
        
        # List all subdirectories
        subdirectories = list_subdirectories_in_gcs(project_id, bucket_name, input_prefix)
        
        if not subdirectories:
            print("❌ No subdirectories found in the input path.")
            return
        
        print(f"📁 Found {len(subdirectories)} subdirectories:")
        for subdir in subdirectories:
            print(f"  - {subdir}")
        
        print(f"\n🚀 Starting batch OCR processing...")
        print(f"📤 Base output location: {gcs_output_uri}")
        print("=" * 60)
        
        successful_count = 0
        failed_count = 0
        
        # Process each subdirectory
        for i, subdirectory in enumerate(subdirectories, 1):
            subdir_uri = f"gs://{bucket_name}/{subdirectory}"
            print(f"\n[{i}/{len(subdirectories)}] Processing: {subdirectory}")
            
            success = batch_transcribe_subdirectory(
                project_id, location, processor_id, subdir_uri, gcs_output_uri
            )
            
            if success:
                successful_count += 1
            else:
                failed_count += 1
        
        print("\n" + "=" * 60)
        print(f"📊 Processing Summary:")
        print(f"✅ Successfully processed: {successful_count} subdirectories")
        print(f"❌ Failed to process: {failed_count} subdirectories")
        print(f"📁 Total subdirectories: {len(subdirectories)}")
        
        if successful_count > 0:
            print(f"\n🎉 OCR results are available in: {gcs_output_uri}")
        
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        print("Please ensure your Google Cloud environment is set up correctly:")
        print("1. GOOGLE_APPLICATION_CREDENTIALS environment variable is set to your service account key path.")
        print("2. Document AI API and Cloud Storage API are enabled in your GCP project.")
        print("3. The service account has 'Document AI Editor' and 'Storage Object Admin' roles.")
        print("4. Processor ID and Region in the script are correct and match your created processor.")
        print("5. Input GCS path is correct and contains subdirectories with PDF files.")

# --- Run the script ---
if __name__ == "__main__":
    # Validate environment variable for authentication
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        print("Please set it to the path of your service account JSON key file.")
        exit(1)

    # Validate configuration (basic check)
    if (PROJECT_ID == "your-gcp-project-id" or
        PROCESSOR_LOCATION == "your-processor-region" or
        PROCESSOR_ID == "your-processor-id"):
        print("ERROR: Please update the PROJECT_ID, PROCESSOR_LOCATION, and PROCESSOR_ID variables in the script with your actual values.")
        exit(1)

    batch_transcribe_gcs_pdfs(
        PROJECT_ID,
        PROCESSOR_LOCATION,
        PROCESSOR_ID,
        GCS_INPUT_URI,
        GCS_OUTPUT_URI,
    )
