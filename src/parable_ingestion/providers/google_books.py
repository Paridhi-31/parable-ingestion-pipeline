import re
import requests
from loguru import logger

class GoogleBooksProvider:
    def __init__(self):
        self.base_url = "https://www.googleapis.com/books/v1/volumes"

    def fetch_social_stats(self, title, author):
        """Fetches stats using a waterfall: Strict -> Fuzzy -> Title Only."""
        # Clean title for better API matching (remove "A Novel", "Vol 1", etc.)
        clean_t = re.split(r'[:;]', title)[0].strip()
        
        queries = [
            f"intitle:\"{clean_t}\"+inauthor:\"{author}\"", # 1. Strict
            f"{clean_t} {author}",                          # 2. Fuzzy
            f"intitle:\"{clean_t}\""                       # 3. Title Only (Last Resort)
        ]

        for q in queries:
            try:
                response = requests.get(self.base_url, params={"q": q}, timeout=5)
                if response.status_code == 200:
                    items = response.json().get("items", [])
                    for item in items:
                        info = item.get("volumeInfo", {})
                        # Verify it's actually the same book (basic check)
                        if 'averageRating' in info:
                            logger.debug(f"Social stats found for {title} via query: {q}")
                            return {
                                "averageRating": info.get("averageRating", 0),
                                "numReviews": info.get("ratingsCount", 0)
                            }
            except Exception as e:
                logger.error(f"Google Books attempt failed: {e}")
        
        return {"averageRating": 0, "numReviews": 0}
        
    

    def _clean_year(self, date_str):
            """Extracts exactly 4 digits from any date string format."""
            if not date_str:
                return None
            match = re.search(r'\b(\d{4})\b', str(date_str))
            return match.group(1) if match else None

    def fetch_publication_year(self, title, author, gutenberg_id=None):
        """
        Attempts to find a publication year across Open Library, Google, 
        and finally a direct scrape of Project Gutenberg.
        """
        year = None

        # 1. Try Google Books (Fastest/Most accurate for modern metadata)
        try:
            res = requests.get(self.base_url, params={"q": f"intitle:{title}+inauthor:{author}"}, timeout=5)
            if res.status_code == 200:
                items = res.json().get("items", [])
                if items:
                    v = items[0].get("volumeInfo", {})
                    year = self._clean_year(v.get("publishedDate"))
        except Exception: pass

        # 2. Try Open Library Fallback
        if not year:
            try:
                res = requests.get("https://openlibrary.org/search.json", 
                                   params={"title": title, "author": author}, timeout=5)
                if res.status_code == 200:
                    docs = res.json().get("docs", [])
                    if docs:
                        year = self._clean_year(docs[0].get("first_publish_year"))
            except Exception: pass

        # 3. GUARANTEED FALLBACK: Scrape Project Gutenberg
        if not year and gutenberg_id:
            try:
                cid = str(gutenberg_id).replace("pg", "").strip()
                g_res = requests.get(f"https://www.gutenberg.org/ebooks/{cid}", timeout=5)
                if g_res.status_code == 200:
                    match = re.search(r'Release Date</th>\s*<td>([^<]+)</td>', g_res.text)
                    if match:
                        year = self._clean_year(match.group(1))
            except Exception: pass
 
        return year
    
    def fetch_isbn(self, title, author):
            """Requirement 6: ISBN Waterfall."""
            # 1. Try Google Books (Most reliable for ISBN_13)
            try:
                res = requests.get(
                    self.base_url, 
                    params={"q": f"intitle:{title}+inauthor:{author}"}, 
                    timeout=5
                ).json()
                
                if 'items' in res:
                    volume_info = res['items'][0].get('volumeInfo', {})
                    ids = volume_info.get('industryIdentifiers', [])
                    # Prioritize ISBN_13 over ISBN_10
                    sorted_ids = sorted(ids, key=lambda x: x['type'], reverse=True)
                    for identifier in sorted_ids:
                        if identifier['type'] in ['ISBN_13', 'ISBN_10']:
                            return identifier['identifier']
                    
                    # Bonus: If industry identifiers are missing, check if description exists here
                    # to help with Requirement 2 later
                    if volume_info.get('description'):
                        self.last_fetched_description = volume_info['description']
            except Exception as e:
                logger.debug(f"ISBN Google Books search failed: {e}")

            # 2. Fallback: Open Library
            try:
                res = requests.get(
                    f"https://openlibrary.org/search.json?title={title}&author={author}", 
                    timeout=5
                ).json()
                if res.get('docs') and res['docs'][0].get('isbn'):
                    return res['docs'][0]['isbn'][0]
            except Exception:
                pass
            
            return None