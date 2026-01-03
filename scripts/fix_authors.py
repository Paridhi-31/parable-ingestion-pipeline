import os
import sys
from loguru import logger
from tqdm import tqdm
from pymongo import UpdateOne  # Import for bulk operations

# Setup path to find your existing modules
sys.path.append(os.path.join(os.getcwd(), "src"))
from parable_ingestion.storage.mongo_handler import MongoHandler

def fix_broken_placeholders_fast():
    mongo = MongoHandler()
    db = mongo.db
    
    broken_url = "https://your-s3-bucket.s3.amazonaws.com/placeholders/author-default.webp"
    
    # 1. Fetch only the fields we need to save memory
    authors_to_fix = list(db.authors.find(
        {"profilePicture": broken_url}, 
        {"_id": 1, "name": 1}
    ))
    
    if not authors_to_fix:
        logger.info("No broken placeholders found.")
        return

    logger.info(f"Found {len(authors_to_fix)} authors. Preparing bulk update...")

    updates = []
    for author in tqdm(authors_to_fix, desc="Generating URLs"):
        name = author.get('name', 'Author').replace(' ', '+')
        new_url = f"https://ui-avatars.com/api/?name={name}&background=random&color=fff&size=512"
        
        # Add a bulk update operation to the list
        updates.append(
            UpdateOne({"_id": author["_id"]}, {"$set": {"profilePicture": new_url}})
        )

    # 2. Execute all updates in chunks of 1000
    if updates:
        logger.info("Executing bulk write to MongoDB...")
        # PyMongo handles the chunking, but we can execute it in one call
        result = db.authors.bulk_write(updates)
        logger.success(f"Modified {result.modified_count} author profiles.")

if __name__ == "__main__":
    fix_broken_placeholders_fast()