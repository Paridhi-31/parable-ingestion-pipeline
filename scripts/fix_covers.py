import sys, os
from datetime import datetime, timezone
from tqdm import tqdm
from collections import Counter

# Setup paths
sys.path.append(os.path.join(os.getcwd(), "src"))
from parable_ingestion.storage.mongo_handler import MongoHandler
from parable_ingestion.providers.google_books import GoogleBooksProvider

def run_year_repair():
    mongo = MongoHandler()
    gb = GoogleBooksProvider()
    stats = Counter()

    # BROAD QUERY: Find any book where publicationDate is missing or invalid
    query = {
        "$or": [
            {"publicationDate": {"$exists": False}},
            {"publicationDate": None},
            {"publicationDate": ""},
            {"publicationDate": "null"}
        ]
    }
    
    total_to_fix = mongo.db.books.count_documents(query)
    print(f"🚀 Found {total_to_fix} books to repair.")

    books = list(mongo.db.books.find(query))

    for book in tqdm(books, desc="Repairing Metadata"):
        title = book.get('title')
        author_name = book.get('author_name')
        
        # Resolve Author Name from document if missing
        if not author_name:
            author_doc = mongo.db.authors.find_one({"_id": book.get('author')})
            author_name = author_doc.get('name') if author_doc else "Unknown"

        g_id = book.get('gutenbergId') or book.get('sourceId')
        
        # Fetch clean 4-digit year via waterfall
        found_year = gb.fetch_publication_year(title, author_name, gutenberg_id=g_id)

        if found_year:
            # UPDATE: Set correct field, UNSET incorrect field
            mongo.db.books.update_one(
                {"_id": book['_id']},
                {
                    "$set": {
                        "publicationDate": str(found_year),
                        "updatedAt": datetime.now(timezone.utc)
                    },
                    "$unset": {
                        "publicationYear": "" # This deletes the old/wrong field
                    }
                }
            )
            stats["Repaired & Cleaned"] += 1
        else:
            stats["Could Not Resolve"] += 1

    print("\n" + "="*35)
    print("📊 SCHEMA CLEANUP SUMMARY")
    print("="*35)
    for k, v in stats.items():
        print(f"{k:25}: {v}")
    print("="*35)

if __name__ == "__main__":
    run_year_repair()