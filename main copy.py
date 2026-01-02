import os
import sys
import shutil
import re
import time
import random
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from bson import ObjectId
from tqdm import tqdm
from pymongo import UpdateOne

# Import your custom modules
# Ensure these paths match your local directory structure
sys.path.append(os.path.join(os.getcwd(), "src"))

from parable_ingestion.providers.google_books import GoogleBooksProvider
from parable_ingestion.providers.goodreads import GoodreadsProvider
from parable_ingestion.providers.gutenberg import GutenbergProvider
from parable_ingestion.storage.s3_handler import S3Handler
from parable_ingestion.storage.mongo_handler import MongoHandler
from parable_ingestion.processors.data_transformer import Transformer

# --- CONFIGURATION & LOGGING ---
logger.remove()
logger.add(sys.stderr, level="SUCCESS")
logger.add("data/ingestion_details.log", level="DEBUG")

def slugify(text):
    return re.sub(r'[\s/]+', '-', str(text).lower().strip()).replace('--', '-')

def cleanup_temp(gutenberg_id):
    path = f"data/temp/{gutenberg_id}"
    if os.path.exists(path):
        shutil.rmtree(path)

# --- THE MAIN INGESTION UNIT ---
def run_ingestion(gutenberg_id):
    """
    Complete chronological pipeline for a single book.
    """
    # 1. Initialize Handlers (Thread-safe inside function)
    provider = GutenbergProvider()
    s3 = S3Handler()
    mongo = MongoHandler()
    transform = Transformer()
    gb_provider = GoogleBooksProvider()
    gr_provider = GoodreadsProvider()
    
    try:
        # --- PHASE 1: EXTRACTION (Gutenberg) ---
        raw_data = provider.fetch_book_data(gutenberg_id)
        if not raw_data:
            return

        book_title = raw_data['title']
        author_name = raw_data['author_name']

        # --- PHASE 2: ENRICHMENT (Waterfall Logic) ---
        # 2a. Fetch 4-digit Year (Goodreads/Google)
        found_year = gb_provider.fetch_publication_year(book_title, author_name, gutenberg_id=gutenberg_id)
        raw_data['publicationDate'] = str(found_year) if found_year else None

        # 2b. Fetch High-Quality Genres (Goodreads)
        gr_genres = gr_provider.fetch_goodreads_genres(book_title, author_name)
        genre_list = gr_genres if gr_genres else raw_data['genres']

        # 2c. Resolve Modern Cover URL
        modern_cover_url = gr_provider.get_modern_cover_url(book_title, author_name, gutenberg_id)

        # --- PHASE 3: ASSET PROCESSING (Local) ---
        temp_dir = f"data/temp/{gutenberg_id}"
        local_epub = provider.download_asset(raw_data['epub_url'], f"{temp_dir}/epubs")
        local_cover = provider.download_asset(modern_cover_url, f"{temp_dir}/covers", is_cover=True)

        # Deep parse EPUB for page count and description fallback
        chapters, page_count, first_paragraph = provider.parse_epub_details(local_epub)
        
        # --- PHASE 4: DB ENTITY UPSERTS ---
        # Author: Wikipedia Bio + Profile Pic
        wiki_author = provider.fetch_author_extra_details(author_name)
        author_id = mongo.upsert_author(transform.prepare_author_payload(author_name, wiki_author))

        # Genres: Upsert with Wikipedia descriptions
        genre_ids = []
        for g_name in genre_list[:5]:
            g_desc = provider.fetch_genre_description(g_name)
            gid = mongo.upsert_genre({
                "name": g_name.title(),
                "description": g_desc,
                "slug": slugify(g_name)
            })
            genre_ids.append(gid)

        # --- PHASE 5: CLOUD & FINAL PERSISTENCE ---
        s3_urls = {
            "epub": s3.upload_file(local_epub, "books/ebook-files"),
            "cover": s3.upload_file(local_cover, "books/covers")
        }

        # Build final payload
        raw_data.update({
            "chapters": chapters,
            "pageCount": page_count,
            "description": raw_data.get('description') or first_paragraph
        })
        
        book_payload = transform.prepare_book_payload(raw_data, author_id, genre_ids, s3_urls)
        book_id = mongo.insert_book(book_payload)
        mongo.link_book_to_author(author_id, book_id)

        # --- PHASE 6: SOCIAL PROOF ---
        stats = gb_provider.fetch_social_stats(book_title, author_name)
        mongo.update_book_social_stats(book_id, stats)
        
        reviews = gr_provider.fetch_reviews(book_title, author_name)
        mongo.seed_social_proof(book_id, stats, reviews)

        logger.success(f"SUCCESS: '{book_title}' (ID: {gutenberg_id}) is live.")
        with open("data/processed_successfully.txt", "a") as f:
            f.write(f"{gutenberg_id}\n")

    except Exception as e:
        logger.error(f"Failed ID {gutenberg_id}: {e}")
    finally:
        cleanup_temp(gutenberg_id)

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    # 1. Setup Tracking
    processed_ids = set()
    if os.path.exists("data/processed_successfully.txt"):
        with open("data/processed_successfully.txt", "r") as f:
            processed_ids = {line.strip() for line in f}

    # 2. Define Range (Example 1001-10005)
    gutenberg_ids = [str(i) for i in range(1001, 1005) if str(i) not in processed_ids]
    
    if not gutenberg_ids:
        logger.success("All books in range already processed.")
        sys.exit()

    # 3. Multi-threaded Execution
    MAX_WORKERS = 5
    logger.info(f"🚀 Starting Ingestion for {len(gutenberg_ids)} books...")
    
    pbar = tqdm(total=len(gutenberg_ids), desc="Ingesting", unit="book")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(run_ingestion, gid): gid for gid in gutenberg_ids}
        
        for future in as_completed(futures):
            pbar.update(1)
            
    pbar.close()
    logger.success("🏁 Bulk Ingestion Task Complete.")