import sys, os, re, time, random, requests
from datetime import datetime, timezone
from pymongo import UpdateOne
from tqdm import tqdm
from loguru import logger

# Setup paths
sys.path.append(os.path.join(os.getcwd(), "src"))
from parable_ingestion.storage.mongo_handler import MongoHandler
from parable_ingestion.providers.goodreads import GoodreadsProvider

def slugify(text):
    return re.sub(r'[\s/]+', '-', str(text).lower().strip()).replace('--', '-')

def fetch_long_wikipedia_description(genre_name):
    """Fetches a longer description from Wikipedia using the full text API."""
    headers = {'User-Agent': 'ParableIngestionBot/1.0 (contact@example.com)'}
    try:
        search_url = f"https://en.wikipedia.org/w/api.php?action=opensearch&search={genre_name}&limit=1&format=json"
        res = requests.get(search_url, headers=headers, timeout=5)
        
        if res.status_code == 200:
            search_res = res.json()
            if len(search_res) > 1 and len(search_res[1]) > 0:
                page_title = search_res[1][0]
                
                wiki_api = (
                    f"https://en.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=0"
                    f"&explaintext=1&titles={page_title}&format=json"
                )
                wiki_res = requests.get(wiki_api, headers=headers, timeout=5).json()
                pages = wiki_res.get("query", {}).get("pages", {})
                
                for page_id in pages:
                    content = pages[page_id].get("extract", "")
                    return content[:2500] if content else f"A collection of books in the {genre_name} genre."
                
    except Exception as e:
        # Variable name fixed to avoid Pylance error
        logger.warning(f"Wikipedia lookup failed for {genre_name}: {e}")
    
    return f"A collection of books under the {genre_name} category."

def full_genre_rebuild():
    mongo = MongoHandler()
    gb = GoodreadsProvider()
    
    print("🚀 Resuming Genre Rebuild...")
    
    # 1. Map authors for name resolution
    authors_cursor = mongo.db.authors.find({}, {"name": 1})
    author_name_map = {str(a['_id']): a['name'] for a in authors_cursor}

    # 2. Pre-cache existing genres to avoid duplicates/Wikipedia re-calls
    existing_genres = mongo.db.genres.find({})
    genre_cache = {g['name']: g['_id'] for g in existing_genres}

    books = list(mongo.db.books.find({}))
    book_updates = []

    print(f"📚 Processing {len(books)} books...")

    for book in tqdm(books):
        # SKIP books that already have genres (Resume Logic)
        if book.get('genre') and len(book.get('genre')) > 0:
            continue

        title = book.get('title')
        author_id = str(book.get('author', ''))
        author_name = author_name_map.get(author_id, "Unknown")

        tags = gb.fetch_goodreads_genres(title, author_name)
        
        genre_ids = []
        if tags:
            for name in tags[:5]:
                name = name.strip().title()
                if not name or len(name) > 30: continue 

                if name not in genre_cache:
                    print(f"\n📖 New Genre: {name}. Fetching long description...")
                    desc = fetch_long_wikipedia_description(name)
                    
                    new_genre = {
                        "name": name,
                        "genre": name,
                        "slug": slugify(name),
                        "description": desc,
                        "createdAt": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc),
                        "__v": 0
                    }
                    res = mongo.db.genres.insert_one(new_genre)
                    genre_cache[name] = res.inserted_id
                
                genre_ids.append(genre_cache[name])

        book_updates.append(UpdateOne(
            {"_id": book["_id"]},
            {"$set": {"genre": list(set(genre_ids)), "updatedAt": datetime.now(timezone.utc)}}
        ))

        # Bulk write every 10 books
        if len(book_updates) >= 10:
            mongo.db.books.bulk_write(book_updates)
            book_updates = []

    if book_updates:
        mongo.db.books.bulk_write(book_updates)
    print("\n✅ Process Complete.")

if __name__ == "__main__":
    full_genre_rebuild()