import xml.etree.ElementTree as ET
import json
import os

book_mappings = {
  'Gen': 'Genesis',
  'Exod': 'Exodus',
  'Lev': 'Leviticus',
  'Num': 'Numbers',
  'Deut': 'Deuteronomy',
  'Josh': 'Joshua',
  'Judg': 'Judges',
  'Ruth': 'Ruth',
  '1Sam': 'I Samuel',
  '2Sam': 'II Samuel',
  '1Kgs': 'I Kings',
  '2Kgs': 'II Kings',
  '1Chr': 'I Chronicles',
  '2Chr': 'II Chronicles',
  'Ezra': 'Ezra',
  'Neh': 'Nehemiah',
  'Esth': 'Esther',
  'Job': 'Job',
  'Ps': 'Psalms',
  'Prov': 'Proverbs',
  'Eccl': 'Ecclesiastes',
  'Song': 'Song of Solomon',
  'Isa': 'Isaiah',
  'Jer': 'Jeremiah',
  'Lam': 'Lamentations',
  'Ezek': 'Ezekiel',
  'Dan': 'Daniel',
  'Hos': 'Hosea',
  'Joel': 'Joel',
  'Amos': 'Amos',
  'Obad': 'Obadiah',
  'Jonah': 'Jonah',
  'Mic': 'Micah',
  'Nah': 'Nahum',
  'Hab': 'Habakkuk',
  'Zeph': 'Zephaniah',
  'Hag': 'Haggai',
  'Zech': 'Zechariah',
  'Mal': 'Malachi',
  'Matt': 'Matthew',
  'Mark': 'Mark',
  'Luke': 'Luke',
  'John': 'John',
  'Acts': 'Acts',
  'Rom': 'Romans',
  '1Cor': 'I Corinthians',
  '2Cor': 'II Corinthians',
  'Gal': 'Galatians',
  'Eph': 'Ephesians',
  'Phil': 'Philippians',
  'Col': 'Colossians',
  '1Thess': 'I Thessalonians',
  '2Thess': 'II Thessalonians',
  '1Tim': 'I Timothy',
  '2Tim': 'II Timothy',
  'Titus': 'Titus',
  'Phlm': 'Philemon',
  'Heb': 'Hebrews',
  'Jas': 'James',
  '1Pet': 'I Peter',
  '2Pet': 'II Peter',
  '1John': 'I John',
  '2John': 'II John',
  '3John': 'III John',
  'Jude': 'Jude',
  'Rev': 'Revelation',
}
def parse_bible_xml(xml_file_path, json_file_path):
    """
    Parses a Bible XML file and converts it into a specific JSON format.

    The JSON format is:
    {
      "BookName": [  // Array of chapters
        [ // Chapter: Array of verses
          [ // Verse: Array of words
            [ "wordString", "strongs", "morphology", "lemma", "pos" ]
          ]
        ]
      ]
    }
    """
    if not os.path.exists(xml_file_path):
        print(f"Error: XML file not found at '{xml_file_path}'")
        return

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return

    bible_data = {}

    for book in root.findall('book'):
        book_name = book_mappings[book.get('num')]
        if not book_name:
            continue

        chapters_list = []
        for chapter in book.findall('chapter'):
            verses_list = []
            for verse in chapter.findall('verse'):
                words_list = []
                # Handle words that are direct children of the verse tag
                for word_element in verse.findall('w'):
                    word_data = [
                        word_element.text or "",
                        word_element.get('strongs', ''),
                        word_element.get('morph', ''),
                        word_element.get('lemma', ''),
                        word_element.get('pos', '')
                    ]
                    words_list.append(word_data)
                
                # Some words might be inside other tags within a verse, this is a simple handler for one level of nesting.
                # Based on the provided XML, some 'w' tags are inside formatting tags like ⸂...⸃ which are not parsed as tags.
                # The text of the verse element itself can contain mixed content.
                # A more robust parser might be needed for complex cases, but for SBLGNT this should handle most cases.
                # The current ElementTree approach might concatenate text from nested tags.
                # For simplicity and based on the file structure, we only look for direct 'w' children.

                verses_list.append(words_list)
            chapters_list.append(verses_list)
        bible_data[book_name] = chapters_list

    try:
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(bible_data, json_file, ensure_ascii=False, indent=2)
        print(f"Successfully parsed XML and created '{json_file_path}'")
    except IOError as e:
        print(f"Error writing to JSON file: {e}")


if __name__ == '__main__':
    # Assuming the script is in the same directory as the XML file.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    xml_path = os.path.join(current_dir, 'new_testament.xml')
    json_path = os.path.join(current_dir, 'new_testament.json')
    
    parse_bible_xml(xml_path, json_path)