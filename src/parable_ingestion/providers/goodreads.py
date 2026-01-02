from io import BytesIO
import re
from PIL import Image
import requests
from bs4 import BeautifulSoup
from loguru import logger
import random
import time

class GoodreadsProvider:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.98 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        ]

        self.browser_header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/"
        }

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

    def fetch_rating_fallback(self, title, author):
        """Scrapes rating directly from page if API fails."""
        try:
            query = f"{title} {author}".replace(" ", "+")
            search_url = f"https://www.goodreads.com/search?q={query}"
            res = requests.get(search_url, headers=self.browser_header, timeout=5)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Find the rating text (e.g., "4.25 avg rating")
            rating_text = soup.find('span', class_='minirating').get_text(strip=True)
            # Regex to extract the float and the count
            rating_match = re.search(r'(\d+\.\d+)', rating_text)
            count_match = re.search(r'([\d,]+)\s+ratings', rating_text)
            
            if rating_match:
                return {
                    "averageRating": float(rating_match.group(1)),
                    "numReviews": int(count_match.group(1).replace(',', '')) if count_match else 100
                }
        except:
            pass
        return {"averageRating": 0, "numReviews": 0}
    
    def fetch_goodreads_genres(self, title, author):
            """Scrapes genres directly from Goodreads search and book pages."""
            try:
                query = f"{title} {author}".replace(" ", "+")
                search_url = f"https://www.goodreads.com/search?q={query}"
                
                res = requests.get(search_url, headers=self.browser_header, timeout=5)
                if res.status_code != 200:
                    return []

                soup = BeautifulSoup(res.text, 'html.parser')
                book_tag = soup.find('a', class_='bookTitle')
                if not book_tag:
                    return []

                book_url = f"https://www.goodreads.com{book_tag['href']}"
                res = requests.get(book_url, headers=self.browser_header, timeout=5)
                soup = BeautifulSoup(res.text, 'html.parser')
                
                # Select links that contain the genre path
                genre_elements = soup.select('a[href*="/genres/"]')
                genres = []
                for el in genre_elements:
                    name = el.get_text(strip=True)
                    if name and name not in genres:
                        genres.append(name)
                
                blacklist = {'to-read', 'currently-reading', 'favorites', 'owned', 'kindle', 'books-i-own'}
                return [g for g in genres if g.lower() not in blacklist][:5]

            except Exception as e:
                logger.warning(f"Goodreads Scrape Error for {title}: {e}")
                return []


    def fetch_long_wikipedia_description(self, genre_name):
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
    
    def is_valid_image(self, content):
        try:
            img = Image.open(BytesIO(content))
            img.verify()
            return True
        except:
            return False
    
    def get_modern_cover_url(self, title, author, gutenberg_id):
        """Waterfall: Open Library -> Google Books -> Gutenberg Cache"""
        # A. Open Library
        try:
            res = requests.get(f"https://openlibrary.org/search.json?title={title}&author={author}", timeout=5).json()
            cover_id = res['docs'][0].get('cover_i') if res.get('docs') else None
            if cover_id:
                url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
                if self.is_valid_image(requests.get(url, timeout=5).content): return url
        except: pass

        # B. Google Books
        try:
            res = requests.get("https://www.googleapis.com/books/v1/volumes", params={"q": f"intitle:{title}+inauthor:{author}"}, timeout=5).json()
            links = res['items'][0]['volumeInfo'].get('imageLinks', {}) if res.get('items') else {}
            url = (links.get('large') or links.get('medium') or links.get('thumbnail')).replace("http://", "https://")
            if url: return url
        except: pass

        # C. Gutenberg Fallback
        return f"https://www.gutenberg.org/cache/epub/{gutenberg_id}/pg{gutenberg_id}.cover.medium.jpg"