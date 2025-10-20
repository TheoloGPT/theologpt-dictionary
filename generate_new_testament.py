import json
from pysblgnt import morphgnt_rows

def generate_new_testament_json():
    """
    Generates a JSON file of the New Testament with a detailed structure.
    Structure: { Book: [ Chapter: [ Verse: [ [Word, Lemma, Morphology] ] ] ] }
    """
    new_testament = {}

    # Define the canonical English names for the New Testament books
    book_map = {
        40: "Matthew",
        41: "Mark",
        42: "Luke",
        43: "John",
        44: "Acts",
        45: "Romans",
        46: "I Corinthians",
        47: "II Corinthians",
        48: "Galatians",
        49: "Ephesians",
        50: "Philippians",
        51: "Colossians",
        52: "I Thessalonians",
        53: "II Thessalonians",
        54: "I Timothy",
        55: "II Timothy",
        56: "Titus",
        57: "Philemon",
        58: "Hebrews",
        59: "James",
        60: "I Peter",
        61: "II Peter",
        62: "I John",
        63: "II John",
        64: "III John",
        65: "Jude",
        66: "Revelation"
    }

    for book_num_key in book_map:
        book_num_key = book_num_key - 39
        for row in morphgnt_rows(book_num_key):
            # The 'bcv' field contains Book, Chapter, and Verse info.
            # Format: BBCCCVVV (e.g., 40001001 is Matthew 1:1)
            bcv = row["bcv"]
            book_num = int(bcv[0:2])
            chapter_num = int(bcv[2:4])
            verse_num = int(bcv[4:6])

            book_name = book_map.get(book_num + 39, f"Unknown Book {book_num}")

            # Each word is an array of [word, lemma, morphology]
            word_data = [
                row["text"],
                row["lemma"],
                row["robinson"]
            ]

            # Get or create the book list (an array of chapters)
            # Chapter and verse numbers are 1-based, so we subtract 1 for 0-based list indices
            if book_name not in new_testament:
                new_testament[book_name] = []
            
            book_list = new_testament[book_name]

            # Ensure the chapter list is long enough
            while len(book_list) < chapter_num:
                book_list.append([])
            
            chapter_list = book_list[chapter_num - 1]

            # Ensure the verse list is long enough
            while len(chapter_list) < verse_num:
                chapter_list.append([])
            
            verse_list = chapter_list[verse_num - 1]
            
            # Append the word data to the verse list
            verse_list.append(word_data)
    
    return new_testament

if __name__ == "__main__":
    print("Generating New Testament JSON file...")
    nt_data = generate_new_testament_json()
    
    # Write the generated data to a JSON file
    with open("new_testament.json", "w", encoding="utf-8") as f:
        json.dump(nt_data, f, ensure_ascii=False, indent=2)
        
    print("Successfully generated new_testament.json")