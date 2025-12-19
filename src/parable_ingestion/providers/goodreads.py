import requests
from bs4 import BeautifulSoup
from loguru import logger
import random

class GoodreadsProvider:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.98 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        ]

    def fetch_reviews(self, title, author):
        """Scrapes public review snippets from Goodreads."""
        reviews = []
        headers = {"User-Agent": random.choice(self.user_agents)}
        try:
            search_query = f"{title} {author}".replace(" ", "+")
            search_url = f"https://www.goodreads.com/search?q={search_query}"
            
            res = requests.get(search_url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            book_tag = soup.find('a', class_='bookTitle')
            if not book_tag:
                return []

            full_url = f"https://www.goodreads.com{book_tag['href']}"
            res = requests.get(full_url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Extract up to 3 review snippets
            review_elements = soup.find_all('section', class_='ReviewText')[:3]
            for elem in review_elements:
                reviews.append(elem.get_text(strip=True)[:1000])
        except Exception as e:
            logger.warning(f"Goodreads scraping failed for {title}: {e}")
            
        return reviews