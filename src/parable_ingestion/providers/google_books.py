import requests
from loguru import logger

class GoogleBooksProvider:
    def __init__(self):
        self.base_url = "https://www.googleapis.com/books/v1/volumes"

    def fetch_social_stats(self, title, author):
        """Fetches aggregate ratings and counts from Google Books."""
        try:
            query = f"intitle:{title}+inauthor:{author}"
            response = requests.get(self.base_url, params={"q": query}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                if not items:
                    return {"averageRating": 0, "numReviews": 0}

                info = items[0].get("volumeInfo", {})
                return {
                    "averageRating": info.get("averageRating", 0),
                    "numReviews": info.get("ratingsCount", 0)
                }
        except Exception as e:
            logger.error(f"Google Books API failed: {e}")
            return {"averageRating": 0, "numReviews": 0}