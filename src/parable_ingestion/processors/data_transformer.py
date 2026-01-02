from slugify import slugify
from loguru import logger

class Transformer:
    @staticmethod
    def generate_slug(text: str) -> str:
        return slugify(text, lowercase=True)

    def prepare_author_payload(self, name, wiki_data, author_pic_url):
        bio = wiki_data.get("bio", "")
        if not bio:
            bio = f"{name} is an author featured on Project Gutenberg."

        return {
            "name": name,
            "slug": self.generate_slug(name),
            "bio": bio,
            "profilePicture": author_pic_url,
            "nationality": wiki_data.get("nationality", "Unknown"),
            "books": []  # Will be populated by Mongo IDs later
        }

    def prepare_genre_payload(self, genre_name, wiki_description=None):
        # Standard descriptions for common genres
        genre_map = {
            "Fiction": "Literary works created from the imagination.",
            "Science Fiction": "Exploration of futuristic concepts and technology.",
            "Horror": "Fiction intended to frighten, scare, or startle.",
            "Poetry": "Literary work in which special intensity is given to the expression of feelings."
        }
        slug = self.generate_slug(genre_name)
        # 1. Try wiki desc, 2. Try hardcoded map, 3. Fallback string
        description = wiki_description or genre_map.get(slug, f"A collection of books in the {genre_name} genre.")

        return {
            "name": genre_name,
            "genre": genre_name,
            "slug": slug,
            "description": description
        }

    def prepare_book_payload(self, raw_data, author_id, genre_ids, s3_urls):
        raw_isbn = raw_data.get('isbn')
        isbn = str(raw_isbn) if raw_isbn and str(raw_isbn).strip() else None
        return {
            "title": raw_data['title'],
            "slug": self.generate_slug(raw_data['title']),
            "description": raw_data.get('description', 'No description available.'),
            "author": author_id,
            "editorPick": raw_data.get('editorPick', False),
            "isbn": isbn,
            "genre": genre_ids,
            "coverImage": s3_urls.get('cover'),
            "ebookFileUrl": s3_urls.get('epub'),
            "publisher": raw_data.get('publisher', 'Project Gutenberg'),
            "language": raw_data.get('language', 'English'),
            "pageCount": raw_data.get('pageCount', 0),
            "chapters": raw_data.get('chapters', []),
            "ebookFileType": "epub",
            "language": raw_data.get('language', 'English'),
            "isPremium": False,
            "isPublished": True, # Usually true for Gutenberg
            "price": raw_data.get('price', 0),
            "publicationDate": None, # We can add logic to parse this later
            "hasAudiobook": False,
            "averageRating": raw_data.get('averageRating', 0),
            "numReviews": raw_data.get('numReviews', 0)
        }