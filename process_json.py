import json

# Define the input and output filenames
input_filename = 'dictionary.json'
output_filename = 'processed_dictionary.json'

try:
    # Load the JSON data from the input file
    # The 'r' mode is for reading, and encoding='utf-8' is important for handling Korean characters.
    with open(input_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Initialize a list to hold the new, processed data
    processed_data = {}

    # Iterate through the original data with an index (starting from 0)
    for i, item in enumerate(data):
        # Create a new dictionary for each item.
        # The ID is the 0-based index + 1 to make it 1-based.
        # The .get() method safely retrieves the value, defaulting to "" if the key is missing.
        
        # Add the new dictionary to our list
        processed_data[item.get("strong_number")] = item

    # Save the processed data to the new output JSON file
    # The 'w' mode is for writing.
    # ensure_ascii=False preserves the Korean characters.
    # indent=2 makes the JSON file human-readable (pretty-prints).
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)

    print(f"Processing complete. The new file is '{output_filename}'")

except FileNotFoundError:
    print(f"Error: The file '{input_filename}' was not found.")
except json.JSONDecodeError:
    print(f"Error: The file '{input_filename}' is not a valid JSON file.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
