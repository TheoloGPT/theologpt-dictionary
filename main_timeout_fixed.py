import os
import time
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
from google.api_core.client_options import ClientOptions

# --- Bible Book Name Mapping ---
KOREAN_TO_ENGLISH_BOOKS = {
    "01_창세기": "01_Genesis",
    "02_출애굽기": "02_Exodus", 
    "03_레위기": "03_Leviticus",
    "04_민수기": "04_Numbers",
    "05_신명기": "05_Deuteronomy",
    "06_여호수아": "06_Joshua",
    "07_사사기": "07_Judges",
    "08_룻기": "08_Ruth",
    "09_사무엘상": "09_1Samuel",
    "10_사무엘하": "10_2Samuel",
    "11_열왕기상": "11_1Kings",
    "12_열왕기하": "12_2Kings",
    "13_역대상": "13_1Chronicles",
    "14_역대하": "14_2Chronicles",
    "15_에스라": "15_Ezra",
    "16_느헤미야": "16_Nehemiah",
    "17_에스더": "17_Esther",
    "18_욥기": "18_Job",
    "19_시편": "19_Psalms",
    "20_잠언": "20_Proverbs",
    "21_전도서": "21_Ecclesiastes",
    "22_아가": "22_SongOfSongs",
    "23_이사야": "23_Isaiah",
    "24_예레미야": "24_Jeremiah",
    "25_예레미야애가": "25_Lamentations",
    "26_에스겔": "26_Ezekiel",
    "27_다니엘": "27_Daniel",
    "28_호세아": "28_Hosea",
    "29_요엘": "29_Joel",
    "30_아모스": "30_Amos",
    "31_오바댜": "31_Obadiah",
    "32_요나": "32_Jonah",
    "33_미가": "33_Micah",
    "34_나훔": "34_Nahum",
    "35_하박국": "35_Habakkuk",
    "36_스바냐": "36_Zephaniah",
    "37_학개": "37_Haggai",
    "38_스가랴": "38_Zechariah",
    "39_말라기": "39_Malachi",
    "40_마태복음": "40_Matthew",
    "41_마가복음": "41_Mark",
    "42_누가복음": "42_Luke",
    "43_요한복음": "43_John",
    "44_사도행전": "44_Acts",
    "45_로마서": "45_Romans",
    "46_고린도전서": "46_1Corinthians",
    "47_고린도후서": "47_2Corinthians",
    "48_갈라디아서": "48_Galatians",
    "49_에베소서": "49_Ephesians",
    "50_빌립보서": "50_Philippians",
    "51_골로새서": "51_Colossians",
    "52_데살로니가전서": "52_1Thessalonians",
    "53_데살로니가후서": "53_2Thessalonians",
    "54_디모데전서": "54_1Timothy",
    "55_디모데후서": "55_2Timothy",
    "56_디도서": "56_Titus",
    "57_빌레몬서": "57_Philemon",
    "58_히브리서": "58_Hebrews",
    "59_야고보서": "59_James",
    "60_베드로전후,유다서": "60_1Peter_2Peter_Jude",
    "62_요한1,2,3": "62_1John_2John_3John",
    "66_요한계시록": "66_Revelation"
}

def get_english_book_name(korean_folder_name):
    """
    Convert Korean folder name to English Bible book name.
    
    Args:
        korean_folder_name (str): Korean folder name (e.g., "01_창세기")
    
    Returns:
        str: English Bible book name (e.g., "01_Genesis")
    """
    return KOREAN_TO_ENGLISH_BOOKS.get(korean_folder_name, korean_folder_name)

# --- Configuration ---
# Your Google Cloud Project ID
PROJECT_ID = "theologpt"  # <--- IMPORTANT: Replace with your actual Project ID

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
    
    # For the Korean directories, we need to extract the actual directory prefixes
    # from the blob names due to Unicode normalization issues
    if prefix == "주석/":
        print("🔍 Extracting actual Korean directory prefixes from blob names...")
        
        # List all blobs and extract unique directory prefixes
        all_blobs = bucket.list_blobs()
        subdirectories = set()
        blob_count = 0
        
        for blob in all_blobs:
            blob_count += 1
            # Debug: show first few blobs
            if blob_count <= 5:
                print(f"  Debug - Blob {blob_count}: {blob.name}")
            
            if '주' in blob.name:  # Look for blobs containing Korean characters
                # Debug: show Korean files found
                if len(subdirectories) < 5:
                    print(f"  Korean file found: {blob.name}")
                
                # Extract directory path (first two levels: 주석/XX_name/)
                parts = blob.name.split('/')
                if len(parts) >= 3 and parts[0] and parts[1]:  # Has at least 주석/directory/file
                    subdir_path = '/'.join(parts[:2]) + '/'
                    subdirectories.add(subdir_path)
                    if len(subdirectories) <= 3:
                        print(f"  Adding subdirectory: {subdir_path}")
        
        result = sorted(list(subdirectories))
        print(f"📁 Found {len(result)} subdirectories with actual Korean prefixes")
        print(f"📊 Total blobs checked: {blob_count}")
        return result
    
    # For other prefixes, use the general approach
    blobs = bucket.list_blobs(prefix=prefix)
    
    subdirectories = set()
    
    for blob in blobs:
        # Extract the directory path from the file path
        relative_path = blob.name[len(prefix):]  # Remove the prefix
        if '/' in relative_path:
            # Get the first directory level after the prefix
            first_dir = relative_path.split('/')[0]
            if first_dir:  # Make sure it's not empty
                subdir_path = prefix + first_dir + '/'
                subdirectories.add(subdir_path)
    
    return sorted(list(subdirectories))

# --- Helper Function to List PDF Files in a Subdirectory ---
def list_pdf_files_in_directory(project_id: str, bucket_name: str, prefix: str):
    """
    Lists all PDF files in a specific GCS directory.
    
    Args:
        project_id (str): Your Google Cloud Project ID.
        bucket_name (str): The GCS bucket name.
        prefix (str): The directory prefix to search in.
    
    Returns:
        list: List of PDF file URIs.
    """
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

# --- Helper Function to Process Individual Subdirectories ---
def batch_transcribe_subdirectory(
    project_id: str,
    location: str,
    processor_id: str,
    subdirectory_uri: str,
    output_base_uri: str,
):
    """
    Process a single subdirectory for batch OCR (file-by-file to avoid timeouts).
    """
    try:
        # Initialize Document AI client
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        client = documentai.DocumentProcessorServiceClient(client_options=opts)

        # Get the full resource name of the processor
        processor_name = client.processor_path(project_id, location, processor_id)

        # Create output URI for this subdirectory (Document AI will create its own structure)
        korean_subdir_name = subdirectory_uri.split('/')[-2]  # Get the subdirectory name
        english_subdir_name = get_english_book_name(korean_subdir_name)
        # Create a temporary subdirectory for this book
        output_uri = f"{output_base_uri}{english_subdir_name}/"

        # Extract bucket name and prefix from subdirectory URI
        bucket_name = subdirectory_uri.replace("gs://", "").split("/")[0]
        directory_prefix = "/".join(subdirectory_uri.replace("gs://", "").split("/")[1:])
        
        # List all PDF files in this directory
        pdf_files = list_pdf_files_in_directory(project_id, bucket_name, directory_prefix)
        
        if not pdf_files:
            print(f"  ⚠️  No PDF files found in {subdirectory_uri}")
            return False
        
        print(f"  📄 Found {len(pdf_files)} PDF files")

        # Process files individually to avoid timeouts
        success_count = 0
        failed_count = 0
        
        for i, pdf_file in enumerate(pdf_files, 1):
            filename = pdf_file.split('/')[-1]
            print(f"  📄 [{i:3d}/{len(pdf_files):3d}] Processing: {filename}")
            
            # Configure the input document (single file)
            gcs_documents = [
                documentai.GcsDocument(
                    gcs_uri=pdf_file,
                    mime_type="application/pdf"
                )
            ]

            input_config = documentai.BatchDocumentsInputConfig(
                gcs_documents=documentai.GcsDocuments(documents=gcs_documents)
            )

            # Configure the output location in GCS (individual file output)
            file_output_uri = f"{output_uri}file_{i:03d}/"
            output_config = documentai.DocumentOutputConfig(
                gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(
                    gcs_uri=file_output_uri
                )
            )

            # Create the batch processing request (for single file)
            request = documentai.BatchProcessRequest(
                name=processor_name,
                input_documents=input_config,
                document_output_config=output_config,
            )

            print(f"      🚀 Starting Document AI processing...")

            try:
                # Send the batch processing request (asynchronous operation)
                operation = client.batch_process_documents(request)

                print(f"      ⏳ Waiting for completion (timeout: 15 minutes)...")

                # Wait for the operation to complete with timeout (900 seconds = 15 minutes)
                operation.result(timeout=900)
                print(f"      ✅ Completed successfully")
                success_count += 1
                
            except Exception as error:
                error_msg = str(error)
                if len(error_msg) > 100:
                    error_msg = error_msg[:100] + "..."
                print(f"      ❌ Failed: {error_msg}")
                print(f"      📝 Continuing with next file...")
                failed_count += 1
                continue  # Continue with next file

        if success_count == 0:
            print(f"  ❌ All files failed for {korean_subdir_name}")
            return False

        print(f"  📊 File processing summary: {success_count} succeeded, {failed_count} failed")
        print(f"✅ Document AI processing completed: {korean_subdir_name} -> {english_subdir_name}")
        print(f"   📊 Successfully processed {success_count}/{len(pdf_files)} files")
        
        # Post-process: flatten directory structure and rename files
        print(f"  🔄 Post-processing: flattening outputs and adding prefixes...")
        bucket_name_output = output_base_uri.replace("gs://", "").split("/")[0]
        output_prefix = "/".join(output_base_uri.replace("gs://", "").split("/")[1:])
        
        flatten_and_rename_outputs(
            project_id=project_id,
            bucket_name=bucket_name_output,
            output_prefix=output_prefix,
            english_book_name=english_subdir_name
        )
        
        print(f"✅ Completed processing: {korean_subdir_name} -> {english_subdir_name}")
        return True

    except Exception as e:
        print(f"❌ Error processing {subdirectory_uri}: {e}")
        return False

# --- Post-Processing Function to Flatten and Rename Outputs ---
def flatten_and_rename_outputs(
    project_id: str,
    bucket_name: str,
    output_prefix: str,
    english_book_name: str
):
    """
    Move and rename output files from Document AI to flatten directory structure
    and add English book name prefixes.
    
    Args:
        project_id: Google Cloud project ID
        bucket_name: GCS bucket name
        output_prefix: Base output prefix  
        english_book_name: English name of the book to use as prefix
    """
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # Find all files in the book's output directory (including file subdirectories)
        book_output_prefix = f"{output_prefix}{english_book_name}/"
        
        print(f"  📁 Looking for output files in: gs://{bucket_name}/{book_output_prefix}")
        
        # List all blobs in the book's output directory and its file subdirectories
        blobs = list(bucket.list_blobs(prefix=book_output_prefix))
        
        if not blobs:
            print(f"  ⚠️  No output files found in {book_output_prefix}")
            return
        
        print(f"  📄 Found {len(blobs)} items to process")
        
        file_count = 0
        # Move and rename each file
        for blob in blobs:
            # Skip if it's a directory marker
            if blob.name.endswith('/'):
                continue
                
            # Extract the original filename
            original_filename = blob.name.split('/')[-1]
            
            # Skip empty filenames
            if not original_filename:
                continue
            
            # Create a more descriptive filename with file number
            file_parts = blob.name.split('/')
            if len(file_parts) >= 3:
                # Extract file number from directory name (e.g., file_001)
                file_dir = file_parts[-2]  # e.g., "file_001"
                if file_dir.startswith('file_'):
                    file_num = file_dir.split('_')[1]
                    # Create new filename with book prefix and file number
                    new_filename = f"{english_book_name}_file{file_num}_{original_filename}"
                else:
                    # Fallback to simple prefix
                    new_filename = f"{english_book_name}_{original_filename}"
            else:
                # Fallback to simple prefix
                new_filename = f"{english_book_name}_{original_filename}"
            
            new_blob_name = f"{output_prefix}{new_filename}"
            
            print(f"    📝 Moving: {blob.name} -> {new_blob_name}")
            
            # Copy to new location
            new_blob = bucket.blob(new_blob_name)
            new_blob.rewrite(blob)
            
            # Delete original
            blob.delete()
            file_count += 1
            
        print(f"  ✅ Flattened {file_count} files for {english_book_name}")
        
        # Clean up empty file directories
        print(f"  🧹 Cleaning up empty file directories...")
        remaining_blobs = list(bucket.list_blobs(prefix=book_output_prefix))
        for blob in remaining_blobs:
            if blob.name.endswith('/'):
                try:
                    blob.delete()
                    print(f"    🗑️  Removed empty directory: {blob.name}")
                except:
                    pass  # Ignore errors when removing directories
        
    except Exception as e:
        print(f"  ❌ Error flattening outputs for {english_book_name}: {e}")

# --- Function to Provide Final Summary of Processed Files ---
def print_final_summary(project_id: str, gcs_output_uri: str):
    """
    Print a final summary of all flattened output files.
    """
    try:
        bucket_name = gcs_output_uri.replace("gs://", "").split("/")[0]
        output_prefix = "/".join(gcs_output_uri.replace("gs://", "").split("/")[1:])
        
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # List all files in the output directory (flattened)
        blobs = list(bucket.list_blobs(prefix=output_prefix))
        
        # Filter out directories
        files = [blob for blob in blobs if not blob.name.endswith('/')]
        
        if files:
            print(f"\n📋 Final Output Summary:")
            print(f"📁 Output location: {gcs_output_uri}")
            print(f"📄 Total files created: {len(files)}")
            
            # Group by book prefix
            book_counts = {}
            for file_blob in files:
                filename = file_blob.name.split('/')[-1]
                if '_' in filename:
                    book_prefix = filename.split('_')[0]
                    book_counts[book_prefix] = book_counts.get(book_prefix, 0) + 1
            
            print(f"📚 Files by book:")
            for book, count in sorted(book_counts.items()):
                print(f"   {book}: {count} files")
        else:
            print(f"\n⚠️  No output files found in {gcs_output_uri}")
            
    except Exception as e:
        print(f"\n❌ Error generating final summary: {e}")

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
        
        print(f"\n🚀 Starting batch OCR processing (file-by-file to avoid timeouts)...")
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
            print_final_summary(project_id, gcs_output_uri)
        
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
