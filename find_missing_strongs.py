import json

def find_missing_entries():
    """
    Identifies Strong's numbers with empty definitions in processed_dictionary.json.
    """
    missing_numbers = []
    try:
        with open('processed_dictionary.json', 'r', encoding='utf-8') as processed_dictionary:
            with open('greek_dict_en.json', 'r', encoding='utf-8') as greek_dict_en:
                with open('hebrew_dict_en.json', 'r', encoding='utf-8') as hebrew_dict_en:
                    processed_dictionary = json.load(processed_dictionary)
                    greek_dict_en = json.load(greek_dict_en)
                    hebrew_dict_en = json.load(hebrew_dict_en)
                    for strong_number in greek_dict_en:
                        if strong_number not in processed_dictionary:
                            missing_numbers.append(strong_number)
                    for strong_number in hebrew_dict_en:
                        if strong_number not in processed_dictionary:
                            missing_numbers.append(strong_number)

    except FileNotFoundError:
        print("Error: processed_dictionary.json not found.")
        return
    except json.JSONDecodeError:
        print("Error: Could not decode processed_dictionary.json.")
        return

    if missing_numbers:
        print("Missing Strong's numbers in processed_dictionary.json:")
        # Sort for consistent output and print in a compact format
        print(", ".join(sorted(missing_numbers)))
    else:
        print("No missing entries found in processed_dictionary.json.")

if __name__ == "__main__":
    find_missing_entries()