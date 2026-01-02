import random
from pymongo import MongoClient, UpdateOne
import threading
from bson import ObjectId
from slugify import slugify
import os
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

class MongoHandler:
    _instance = None
    _client = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MongoHandler, cls).__new__(cls)
                
                load_dotenv()
                uri = os.getenv("MONGO_URI")
                
                if not uri:
                    logger.critical("MONGO_URI is missing from environment!")
                    raise EnvironmentError("MONGO_URI not found.")

                try:
                    cls._client = MongoClient(
                        uri, 
                        maxPoolSize=50,
                        serverSelectionTimeoutMS=5000
                    )
                    cls._client.admin.command('ping')
                    
                    # --- PRO FIX: ASSIGN COLLECTIONS HERE ---
                    # We attach them directly to the _instance so they persist
                    db = cls._client.get_database() 
                    cls._instance.db = db
                    cls._instance.books = db.books
                    cls._instance.authors = db.authors
                    cls._instance.genres = db.genres
                    
                    # --- ADD INDEXES FOR PERFORMANCE ---
                    cls._instance.books.create_index("slug", unique=True)
                    cls._instance.authors.create_index("slug", unique=True)
                    cls._instance.genres.create_index("slug", unique=True)
                    
                    logger.info("MongoDB Connected: Indexes Verified.")

                    logger.info("Successfully connected to MongoDB and initialized collections.")
                except Exception as e:
                    logger.critical(f"MongoDB Connection Failed: {e}")
                    cls._client = None
                    raise e
        return cls._instance

    def __init__(self):
        # We leave this empty or just pass. 
        # Because of __new__, self.db and self.books are already available.
        pass
    
    def upsert_author(self, author_data):
        """
        Returns the OID of the author. 
        Updates bio, profilePicture, and nationality if they exist.
        """
        # Ensure we have a slug for lookup
        if 'slug' not in author_data:
            author_data['slug'] = slugify(author_data['name'])
            
        # Remove 'books' from the set payload so it's only handled during creation
        # or handled separately via link_book_to_author
        author_data.pop('books', None) 

        author = self.authors.find_one_and_update(
            {"slug": author_data['slug']},
            {
                "$set": author_data,
                "$setOnInsert": {
                    "createdAt": datetime.utcnow(), 
                    "books": [] # Initialize empty array only on first creation
                }
            },
            upsert=True,
            return_document=True
        )
        logger.info(f"Author upserted: {author_data['name']}")

        return str(author['_id'])

    def upsert_genre(self, genre_payload):
            """Standardized method name to match main.py calls."""
            # Main.py sends {'genre': 'Name', 'slug': '...', 'description': '...'}
            genre_name = genre_payload.get('genre')
            slug = genre_payload.get('slug') or slugify(genre_name)
            
            # Add the 'name' field to the payload to satisfy the unique index
            genre_payload['name'] = genre_name

            genre = self.genres.find_one_and_update(
                {"slug": slug},
                {
                    "$set": genre_payload, # Updates description if changed
                    "$setOnInsert": {
                        "createdAt": datetime.utcnow(),
                        "__v": 0
                    }
                },
                upsert=True,
                return_document=True
            )
            return str(genre['_id'])

    def insert_book(self, book_data):
        """
        Upserts the book document to avoid duplicates and ensures BSON ObjectIds.
        """
        try:
            # 1. Convert string IDs to BSON ObjectIds
            book_data['author'] = ObjectId(book_data['author'])
            book_data['genre'] = [ObjectId(gid) for gid in book_data['genre']]
            
            # 2. Extract slug for the unique check
            slug = book_data.get('slug')

            is_pick = book_data.pop('editorPick', False)

            # 3. Use find_one_and_update for Upsert
            result = self.books.find_one_and_update(
                {"slug": slug},
                {
                    "$set": {
                        **book_data,
                        "updatedAt": datetime.utcnow()
                    },
                    "$setOnInsert": {
                        "editorPick": is_pick,
                        "createdAt": datetime.utcnow(),
                        "__v": 0
                    }
                },
                upsert=True,
                return_document=True
            )
            
            book_id = result['_id']
            logger.success(f"Book upserted in MongoDB: {book_data['title']}")
            return str(book_id)
            
        except Exception as e:
            logger.error(f"MongoDB Upsert Failed: {e}")
            raise

    def link_book_to_author(self, author_id, book_id):
        """
        Adds the Book's ObjectID to the Author's 'books' array.
        This is crucial for your Mongoose relationship logic.
        """
        self.authors.update_one(
            {"_id": ObjectId(author_id)},
            {"$addToSet": {"books": ObjectId(book_id)}}
        )
        logger.info(f"Linked book {book_id} to author {author_id}")

    def get_or_create_system_user(self):
        """Ensures a 'Parable Archivist' exists to own scraped reviews."""
        system_user = {
            "username": "Parable Archivist",
            "email": "system@parable.app",
            "roles": ["admin"],
            "profileImage": "https://parableapp.s3.amazonaws.com/system/avatar.png"
        }
        user = self.db.users.find_one_and_update(
            {"email": system_user["email"]},
            {"$setOnInsert": {**system_user, "password": "LOCKED", "createdAt": datetime.utcnow()}},
            upsert=True,
            return_document=True
        )
        return user['_id']

    def insert_review(self, review_data):
        """Inserts a review and ensures book/user are proper ObjectIds."""
        try:
            # Convert strings to ObjectIds for MERN/Mongoose compatibility
            if isinstance(review_data.get('book'), str):
                review_data['book'] = ObjectId(review_data['book'])
            if isinstance(review_data.get('user'), str):
                review_data['user'] = ObjectId(review_data['user'])
                
            review_data['createdAt'] = datetime.utcnow()
            review_data['updatedAt'] = datetime.utcnow()
            self.db.reviews.insert_one(review_data)
        except Exception as e:
            logger.error(f"Review insertion failed: {e}")

    def update_book_social_stats(self, book_id, stats):
        """Updates the Book document with final ratings from Google/Goodreads."""
        self.db.books.update_one(
            {"_id": ObjectId(book_id)},
            {"$set": {
                "averageRating": stats.get('averageRating', 0),
                "numReviews": stats.get('numReviews', 0),
                "updatedAt": datetime.utcnow()
            }}
        )
        logger.info(f"Updated social stats for book: {book_id}")

    def seed_social_proof(self, book_id, social_stats):
        """
        Seeds engagement based on Google Books metrics.
        social_stats: dictionary containing 'numReviews' and 'averageRating'
        """
        system_uid = self.get_or_create_system_user()
        book_oid = ObjectId(book_id)
        
        # Base multiplier for our simulation
        base_count = social_stats.get('numReviews', 10) or 10
        
        # 1. Seed 'Actions' (Likes)
        # We insert one actual Action document for the System User
        self.db.actions.insert_one({
            "user": system_uid,
            "targetType": "Book",
            "target": book_oid,
            "action": "like",
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow()
        })

        # 2. Seed 'ReadingProgress'
        self.db.readingprogresses.update_one(
            {"user": system_uid, "book": book_oid},
            {"$set": {
                "progressPercentage": random.randint(20, 80),
                "currentPage": 15,
                "completed": False,
                "lastAccessedAt": datetime.utcnow(),
                "createdAt": datetime.utcnow(),
                "updatedAt": datetime.utcnow()
            }},
            upsert=True
        )

        # 3. Update Aggregate Totals on the Book Document
        # We scale the 'numReviews' to get realistic Likes/Reads
        simulated_likes = int(base_count * 1.5) + random.randint(1, 10)
        simulated_reads = int(base_count * 5) + random.randint(10, 50)

        self.db.books.update_one(
            {"_id": book_oid},
            {"$set": {
                "likesCount": simulated_likes,
                "readsCount": simulated_reads,
                "trendingScore": (simulated_likes * 3) + (simulated_reads)
            }}
        )
        logger.info(f"Social proof seeded for {book_id} (Likes: {simulated_likes})")