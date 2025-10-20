# file: parse_strongs.py

import re
import json
import os

def parse_strong_file(filepath, language_prefix):
    """
    Parses a Strong's dictionary text file (Hebrew or Greek) based on a two-line entry structure.

    Args:
        filepath (str): The path to the dictionary file.
        language_prefix (str): 'H' for Hebrew, 'G' for Greek.

    Returns:
        list: A list of dictionaries, where each dictionary represents a Strong's entry.
              Returns an empty list if the file is not found or cannot be parsed.
    """
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        return []

    print(f"Parsing {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    entries = []
    # Split the file content by one or more blank lines to get entry blocks
    # Split the file content by one or more blank lines to get entry blocks
    entry_blocks = re.split(r'\n\s*\n', content.strip())

    for block in entry_blocks:
        if not block.strip():
            continue

        lines = block.strip().split('\n')
        
        two_line_chunks = []
        if len(lines) == 2:
            two_line_chunks.append(lines)
        # Handle cases where entries are not separated by newlines
        elif len(lines) > 2 and len(lines) % 2 == 0:
            for i in range(0, len(lines), 2):
                two_line_chunks.append(lines[i:i+2])
        else:
            print(f"Warning: Skipping malformed entry block (expected a multiple of 2 lines, found {len(lines)}):\n---\n{block}\n---")
            continue

        for chunk in two_line_chunks:
            line1, line2 = chunk[0].strip(), chunk[1].strip()

            try:
                # --- Parse the first line ---
                parts1 = line1.split()
                strong_num = parts1[0]
                pronunciation = parts1[-1].strip('{}')
                
                # The original word is typically the second item
                original_word = parts1[1]
                
                # Transliteration and any other info is between the original word and pronunciation
                translit_parts = parts1[2:-1]
                transliteration = " ".join(translit_parts)

                # --- Parse the second line ---
                etymology = ''
                strongs_def = ''
                kjv_def = ''
                scripture_ref = ''
                part_of_speech = ''
                english_gloss = ''

                if language_prefix == 'H':
                    # Hebrew parsing logic
                    parts2 = line2.split(';')
                    if len(parts2) >= 2:
                        # Handle cases with one or two semicolons
                        # The last part contains the definitions
                        def_part = parts2[-1]
                        # The part(s) before the last one contain etymology
                        etymology = ";".join(parts2[:-1]).strip()
                        
                        # Split the definition part by ':-'
                        def_split = def_part.split(':-', 1)
                        if len(def_split) == 2:
                            strongs_def = def_split[0].strip()
                            kjv_def = def_split[1].strip()
                        else:
                            strongs_def = def_split[0].strip()
                    else:
                        # If no semicolon, the whole line is the Strong's definition
                        strongs_def = line2
                
                elif language_prefix == 'G':
                    # Greek parsing logic
                    parts2 = line2.split(';', 1)
                    if len(parts2) == 2:
                        etymology = parts2[0].strip()
                        def_part = parts2[1].strip()
                    else:
                        def_part = parts2[0].strip()

                    # Split definition part by '<' to find usage
                    usage_split = def_part.split('<', 1)
                    if len(usage_split) == 2:
                        strongs_def = usage_split[0].strip()
                        usage_text = '<' + usage_split[1].strip()
                        
                        # Further parse the usage_text
                        # Pattern: <scripture>part_of_speech. english_gloss
                        match = re.match(r"<([^>]+)>\s*([^.]+)\.\s*(.*)", usage_text)
                        if match:
                            scripture_ref = match.group(1).strip()
                            part_of_speech = match.group(2).strip()
                            english_gloss = match.group(3).strip()
                        else:
                            # Fallback if the pattern doesn't match
                            english_gloss = usage_text
                    else:
                        strongs_def = usage_split[0].strip()

                # --- Assemble the entry dictionary ---
                entry_data = {
                    'strong_number': f"{language_prefix}{strong_num}",
                    'original_word': original_word,
                    'transliteration': transliteration,
                    'pronunciation': pronunciation,
                    'etymology': etymology,
                }
                if language_prefix == 'H':
                    entry_data['strongs_definition'] = strongs_def
                    entry_data['kjv_definition'] = kjv_def
                else: # 'G'
                    entry_data['strongs_definition'] = strongs_def
                    entry_data['scripture_reference'] = scripture_ref
                    entry_data['part_of_speech'] = part_of_speech
                    entry_data['english_gloss'] = english_gloss

                entries.append(entry_data)

            except IndexError as e:
                print(f"Warning: Could not parse entry (error: {e}). Entry content:\n---\n{block}\n---")
                continue

    print(f"Successfully parsed {len(entries)} entries from {os.path.basename(filepath)}.")
    return entries

def main():
    """
    Main function to parse both Hebrew and Greek files and save to JSON.
    """
    # Assuming the text files are in the same directory as the script
    hebrew_filepath = '히브리어스트롱사전.txt'
    greek_filepath = '헬라어스트롱사전.txt'
    output_filepath = 'strongs_dictionary.json'

    hebrew_data = parse_strong_file(hebrew_filepath, 'H')
    greek_data = parse_strong_file(greek_filepath, 'G')

    combined_data = hebrew_data + greek_data

    if not combined_data:
        print("No data was parsed. Exiting.")
        return

    print(f"\nTotal entries combined: {len(combined_data)}")

    # Save the combined data to a JSON file
    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2)
        print(f"Successfully saved combined dictionary to {output_filepath}")
    except IOError as e:
        print(f"Error writing to JSON file: {e}")

if __name__ == '__main__':
    main()