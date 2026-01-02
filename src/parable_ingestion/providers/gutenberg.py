import uuid
import requests
from bs4 import BeautifulSoup
from loguru import logger
import os
import ebooklib
from ebooklib import epub
import re
import time
from PIL import Image
from io import BytesIO

class GutenbergProvider:
    def __init__(self):
        self.base_url = "https://www.gutenberg.org"
        self.headers = {
            'User-Agent': 'ParableIngestionBot/1.0 (contact@yourdomain.com)'
        }

    def _clean_author_name(self, name):
        """Removes parentheses and extra whitespace for better Wikipedia hits."""
        if not name: return ""
        # Removes "(Mildred Augustine)" -> "Mildred A. Wirt"
        name = re.sub(r'\(.*?\)', '', name)
        return name.replace('  ', ' ').strip().rstrip(',')

    def fetch_book_data(self, book_id):
        url = f"{self.base_url}/ebooks/{book_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        data = {
            "title": soup.find('h1').text.strip() if soup.find('h1') else "Unknown Title",
            "author_name": "Unknown Author",
            "genres": [],
            "language": "English",
            "description": "", 
            "epub_url": f"{self.base_url}/ebooks/{book_id}.epub.images",
            "cover_url": f"{self.base_url}/cache/epub/{book_id}/pg{book_id}.cover.medium.jpg",
            "publisher": "Project Gutenberg"
        }

        # Metadata parsing logic
        bibrec = soup.find('table', {'class': 'bibrec'})
        if bibrec:
            for row in bibrec.find_all('tr'):
                h, v = row.find('th'), row.find('td')
                if h and v:
                    label, val = h.text.strip(), v.text.strip()
                    if "Author" in label:
                        clean = re.split(r'\d', val)[0].strip().rstrip(',')
                        if ',' in clean:
                            parts = clean.split(',')
                            data["author_name"] = f"{parts[1].strip()} {parts[0].strip()}"
                        else:
                            data["author_name"] = clean
                    elif "Subject" in label or "Categories" in label:
                        subs = re.split(r'--|;', val)
                        for s in subs:
                            g = s.strip()
                            if g and not any(c.isdigit() for c in g) and g not in data["genres"]:
                                data["genres"].append(g)
                    elif "Language" in label:
                        data["language"] = val

        # Use CLEANED name for the description search
        cleaned = self._clean_author_name(data["author_name"])
        
        return data


    def fetch_author_extra_details(self, author_name):
        """Corrected Wikipedia author biography logic."""
        fallback = {"bio": "", "profilePicture": "", "nationality": "Unknown"}
        clean_name = self._clean_author_name(author_name)
        
        try:
            search_url = f"https://en.wikipedia.org/w/api.php?action=opensearch&search={clean_name}&limit=1&format=json"
            res = requests.get(search_url, headers=self.headers, timeout=5)
            if res.status_code == 200:
                results = res.json()
                if len(results) > 1 and len(results[1]) > 0:
                    page_title = results[1][0].replace(" ", "_")
                    wiki_api = f"https://en.wikipedia.org/api/rest_v1/page/summary/{page_title}"
                    wiki_res = requests.get(wiki_api, headers=self.headers, timeout=5)
                    if wiki_res.status_code == 200:
                        resp = wiki_res.json()
                        return {
                            "bio": resp.get("extract", ""),
                            "profilePicture": resp.get("thumbnail", {}).get("source", ""),
                            "nationality": resp.get("description", "Unknown")
                        }
        except Exception:
            pass
        return fallback


    def parse_epub_details(self, local_path):
        chapters, page_count, first_paragraph = [], 0, ""
        try:
            book = epub.read_epub(local_path)
            for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
                content = item.get_content().decode('utf-8', errors='ignore')
                soup = BeautifulSoup(content, 'html.parser')
                if not first_paragraph:
                    for p in soup.find_all('p'):
                        txt = p.get_text().strip()
                        if len(txt) > 150:
                            first_paragraph = txt[:400] + "..."
                            break
                t = soup.find(['h1', 'h2', 'h3'])
                if t: chapters.append({"title": t.text.strip()})
                page_count += len(soup.get_text()) // 1500
            return chapters or [{"title": "Main Content"}], max(page_count, 1), first_paragraph
        except Exception:
            return [{"title": "Full Book"}], 0, ""


    def download_asset(self, url, folder, is_cover=False):  # <-- Added is_cover here
            os.makedirs(folder, exist_ok=True)
            
            if is_cover:
                    # FIX: Generate a clean, random unique name for the cover 
                    # to avoid Windows "Invalid Argument" errors from URL symbols
                    filename = f"cover_{uuid.uuid4().hex[:10]}.webp"
            else:
                # Standard Gutenberg behavior for EPUB files
                filename = url.split('/')[-1]
        
            path = os.path.join(folder, filename)
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # We use stream=True for large EPUBs, but for covers we grab the whole content
                    with requests.get(url, stream=True, timeout=20, headers=self.headers) as r:
                        r.raise_for_status()
                        
                        if is_cover:
                            # PROCESS IMAGE IN MEMORY
                            # .content gets the raw bytes from the response
                            img = Image.open(BytesIO(r.content))
                            
                            # Convert to RGB (required for WebP if source is PNG/indexed)
                            if img.mode in ("RGBA", "P"):
                                img = img.convert("RGB")
                            
                            # Resize to standard cover ratio (400x600)
                            img.thumbnail((400, 600))
                            
                            # Save to the local path as WebP
                            img.save(path, "WEBP", quality=80)
                        else:
                            # STANDARD DOWNLOAD FOR EPUBs
                            with open(path, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                                        
                    logger.info(f"Successfully processed: {filename}")
                    return path
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt+1} failed for {url}. Retrying...")
                        time.sleep(2)
                    else:
                        logger.error(f"Download failed: {e}")
                        raise