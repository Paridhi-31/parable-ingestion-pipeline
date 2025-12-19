import os
import sys
import shutil
from pathlib import Path
from datetime import datetime
from loguru import logger
from bson import ObjectId
from tqdm import tqdm

# Manual Path Setup
sys.path.append(os.path.join(os.getcwd(), "src"))

from parable_ingestion.providers.google_books import GoogleBooksProvider
from parable_ingestion.providers.goodreads import GoodreadsProvider
from parable_ingestion.providers.gutenberg import GutenbergProvider
from parable_ingestion.storage.s3_handler import S3Handler
from parable_ingestion.storage.mongo_handler import MongoHandler
from parable_ingestion.processors.data_transformer import Transformer
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. At the very top of your main script (after imports)
logger.remove() # Remove default handler
# Add a new handler that only shows SUCCESS and above to the console
logger.add(sys.stderr, level="SUCCESS") 
# Optional: Save all detailed logs to a file for later review
logger.add("data/ingestion_details.log", level="DEBUG")


# And update cleanup_temp to accept the id
def cleanup_temp(gutenberg_id):
    path = f"data/temp/{gutenberg_id}"
    if os.path.exists(path):
        shutil.rmtree(path)
        logger.debug(f"Temporary files for ID {gutenberg_id} cleaned up.")

def run_ingestion(gutenberg_id):
    # Initialize all handlers and providers
    provider = GutenbergProvider()
    s3 = S3Handler()
    mongo = MongoHandler()
    transform = Transformer()
    gb_provider = GoogleBooksProvider()
    gr_provider = GoodreadsProvider()
    temp_base = f"data/temp/{gutenberg_id}"

    try:
        # --- PHASE 1: EXTRACTION ---
        logger.debug(f"Step 1: Extracting metadata for ID {gutenberg_id}...")
        raw_data = provider.fetch_book_data(gutenberg_id)
        if not raw_data:
            return

        book_title = raw_data['title']
        author_name = raw_data['author_name']

        # --- PHASE 2: LOCAL PROCESSING ---
        logger.debug("Step 2: Downloading and parsing assets...")
        local_temp_base = f"data/temp/{gutenberg_id}"
        local_epub = provider.download_asset(raw_data['epub_url'], f"{local_temp_base}/epubs")
        local_cover = provider.download_asset(raw_data['cover_url'], f"{local_temp_base}/covers", is_cover=True)

        # Deep parse the EPUB for chapters and actual page counts
        chapters, page_count, first_paragraph = provider.parse_epub_details(local_epub)
        raw_data.update({
            'chapters': chapters,
            'pageCount': page_count
        })
        
        # Use book excerpt as description if Wikipedia/Metadata is missing
        if not raw_data.get('description') or len(raw_data['description']) < 50:
            raw_data['description'] = first_paragraph
    
        # --- PHASE 3: DATABASE ENRICHMENT (AUTHORS & GENRES) ---
        logger.debug(f"Step 3: Upserting Author ({author_name}) and Genres...")
        wiki_data = provider.fetch_author_extra_details(author_name)
        author_payload = transform.prepare_author_payload(author_name, wiki_data)
        author_id = mongo.upsert_author(author_payload)

        genre_ids = []
        for g_name in raw_data['genres']:
            g_desc = provider.fetch_genre_description(g_name)
            genre_payload = transform.prepare_genre_payload(g_name)
            genre_payload['description'] = g_desc
            gid = mongo.upsert_genre(genre_payload)
            genre_ids.append(gid)

        # --- PHASE 4: CLOUD STORAGE ---
        logger.debug("Step 4: Uploading assets to S3...")
        s3_urls = {
            "epub": s3.upload_file(local_epub, "books/ebook-files"),
            "cover": s3.upload_file(local_cover, "books/covers")
        }

        # --- PHASE 5: BOOK PERSISTENCE ---
        logger.debug("Step 5: Saving Book to MongoDB...")
        book_payload = transform.prepare_book_payload(raw_data, author_id, genre_ids, s3_urls)
        book_id = mongo.insert_book(book_payload) 
        
        # Create Mongoose-style relationship link
        mongo.link_book_to_author(author_id, book_id)

        # --- PHASE 6: SOCIAL PROOF & REVIEWS ---
        logger.debug(f"Step 6: Fetching ratings and seeding reviews for '{book_title}'...")
        
        # Fetch ratings from Google Books
        stats = gb_provider.fetch_social_stats(book_title, author_name)
        mongo.update_book_social_stats(book_id, stats)
        mongo.seed_social_proof(book_id, stats)
        
        # Fetch snippets from Goodreads
        gr_reviews = gr_provider.fetch_reviews(book_title, author_name)
        system_uid = mongo.get_or_create_system_user()
        
        for review_text in gr_reviews:
            review_doc = {
                "user": system_uid,
                "book": ObjectId(book_id), # Ensure BSON format for Mongoose
                "rating": stats.get('averageRating', 4.0) or 4.0,
                "comment": review_text,
                "status": "approved",
                "isSpoiler": False
            }
            mongo.insert_review(review_doc)

        logger.success(f"SUCCESS: '{book_title}' is now fully processed and live.")
        with open("data/processed_successfully.txt", "a") as f:
            f.write(f"{gutenberg_id}\n")

    except Exception as e:
        logger.error(f"Ingestion failed for ID {gutenberg_id}: {e}")
    finally:
        cleanup_temp(gutenberg_id)



if __name__ == "__main__":

    # Load already processed IDs to avoid duplicates
    processed_ids = set()
    if os.path.exists("data/processed_successfully.txt"):
        with open("data/processed_successfully.txt", "r") as f:
            processed_ids = {line.strip() for line in f}

    start_id = 1
    end_id = 1000
    #start_id = 74000
    #end_id = 75000
    gutenberg_ids = [str(i) for i in range(start_id, end_id + 1)]
    #gutenberg_ids = [str(i) for i in range(75000, 74000, -1)]

    if not gutenberg_ids:
        logger.success("All books in this range are already processed!")
        sys.exit()

    # --- ADD THIS LINE HERE ---
    # This "warms up" the connection so it's ready for the threads
    db_handler = MongoHandler() 
    # --------------------------

    MAX_WORKERS = 5 
    
    logger.info(f"🚀 Starting Bulk Ingestion: {start_id} to {end_id}")
    
    # 1. Initialize the Progress Bar
    # total: how many tasks to complete
    # desc: text shown next to the bar
    # unit: label for each iteration
    pbar = tqdm(total=len(gutenberg_ids), desc="Ingesting Books", unit="book")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(run_ingestion, gid): gid for gid in gutenberg_ids}
        
        for future in as_completed(future_to_id):
            gid = future_to_id[future]
            try:
                future.result()
            except Exception as e:
                # Use logger.error, but note that tqdm might push logs up
                logger.error(f"Critical failure on ID {gid}: {e}")
            finally:
                # 2. Update the progress bar after each book finishes
                pbar.update(1)

    # 3. Close the bar when done
    pbar.close()
    logger.success("🏁 Bulk Ingestion Task Complete.")