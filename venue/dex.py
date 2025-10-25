import requests
from bs4 import BeautifulSoup
import json
import os
import time
import re
from typing import Dict, Optional, List, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from urllib.parse import quote_plus

# ----------------------------
# Your existing helpers/classes
# ----------------------------

class Timer:
    """A simple context manager for timing code blocks."""
    def __init__(self, description: str):
        self.description = description
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        print(f"\n[Timer] Starting: {self.description}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed_time = time.time() - self.start_time
        if elapsed_time < 60:
            print(f"[Timer] Completed: {self.description} (took {elapsed_time:.2f} seconds)")
        else:
            minutes = int(elapsed_time // 60)
            seconds = elapsed_time % 60
            print(f"[Timer] Completed: {self.description} (took {minutes} minutes {seconds:.2f} seconds)")

class RetryingSession:
    """Creates a requests session that automatically retries on failures."""
    def __init__(self, retries=3, backoff_factor=1.5, status_forcelist=(500, 502, 503, 504)):
        self.session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

class Paper:
    """A class to hold paper information."""
    def __init__(self, title: str, authors: List[str]):
        self.title = title
        self.authors = authors
        self.paper_id = None
        self.pdf_url = None
        self.arxiv_url = None
        self.abstract = None
        self.supplemental_url = None
    
    def to_dict(self) -> dict:
        return {
            'paper_id': self.paper_id,
            'title': self.title,
            'authors': self.authors,
            'pdf_url': self.pdf_url,
            'arxiv_url': self.arxiv_url,
            'abstract': self.abstract,
            'supplemental_url': self.supplemental_url
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Paper':
        paper = cls(data['title'], data['authors'])
        paper.paper_id = data.get('paper_id')
        paper.pdf_url = data.get('pdf_url')
        paper.arxiv_url = data.get('arxiv_url')
        paper.abstract = data.get('abstract')
        paper.supplemental_url = data.get('supplemental_url')
        return paper

class PaperDatabase:
    """Manages loading and saving papers to a JSON file."""
    def __init__(self, output_dir: str = "dataset", conference: str = "dex", year: int = 2025):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, f"{conference}{str(year)[-2:]}.json")
        self.papers: Dict[str, Paper] = {}
        self._load_existing_papers()

    def _load_existing_papers(self):
        """Load existing papers from the JSON file if it exists."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for paper_dict in data:
                        paper = Paper.from_dict(paper_dict)
                        self.papers[paper.title] = paper
                print(f"[INFO] Loaded {len(self.papers)} existing papers from database: {self.filename}")
            except Exception as e:
                print(f"[ERROR] Failed to load existing papers: {e}")
                if os.path.exists(self.filename):
                    backup_name = f"{self.filename}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    os.rename(self.filename, backup_name)
                    print(f"[INFO] Created backup of corrupted database: {backup_name}")

    def save_paper(self, paper: Paper):
        """Save or update a single paper in the database and write to file."""
        self.papers[paper.title] = paper
        self._save_to_file()

    def _save_to_file(self):
        """Save all papers to the JSON file."""
        papers_data = [paper.to_dict() for paper in self.papers.values()]
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)

    def has_paper(self, title: str) -> bool:
        """Check if a paper with full data already exists in the database."""
        if title not in self.papers:
            return False
        existing_paper = self.papers[title]
        return bool(existing_paper.abstract and existing_paper.pdf_url)

# ----------------------------
# arXiv-specific helpers
# ----------------------------

ARXIV_API_BASE = "http://export.arxiv.org/api/query?search_query=id:"

_ARXIV_ID_RE = re.compile(
    r"""(?ix)
        (?:https?://)?(?:www\.)?arxiv\.org/
        (?:
            abs|pdf
        )/
        (?P<id>[0-9]{4}\.[0-9]{4,5}(?:v[0-9]+)?|[a-z\-]+(?:\.[A-Z]{2})?/[0-9]{7}(?:v[0-9]+)?)
        (?:\.pdf)?$
    """
)

_SIMPLE_ID_RE = re.compile(
    r"""(?ix)
        ^(?P<id>
            [0-9]{4}\.[0-9]{4,5}(?:v[0-9]+)? |
            [a-z\-]+(?:\.[A-Z]{2})?/[0-9]{7}(?:v[0-9]+)?
        )$
    """
)

def extract_arxiv_id(s: str) -> Optional[str]:
    """
    Accepts a full arXiv URL (abs/pdf) or a bare arXiv ID and returns the canonical ID (possibly with version).
    """
    s = s.strip()
    m = _ARXIV_ID_RE.search(s)
    if m:
        return m.group("id")
    m = _SIMPLE_ID_RE.match(s)
    if m:
        return m.group("id")
    return None

def parse_arxiv_atom_entry(entry_xml: str) -> Tuple[str, List[str], str, str, str]:
    """
    Parse a single <entry> from arXiv's Atom XML and return:
    (title, authors[], abstract, abs_url, pdf_url)
    """
    soup = BeautifulSoup(entry_xml, "xml")

    entry = soup.find("entry")
    if not entry:
        raise ValueError("No <entry> found in arXiv API response.")

    # Title & abstract (summary)
    title = (entry.find("title").get_text(strip=True) if entry.find("title") else "").replace("\n", " ")
    abstract = (entry.find("summary").get_text(strip=True) if entry.find("summary") else "").replace("\n", " ")

    # Authors
    authors = [a.find("name").get_text(strip=True) for a in entry.find_all("author") if a.find("name")]

    # Links
    abs_url, pdf_url = None, None
    for link in entry.find_all("link"):
        rel = link.get("rel", "")
        href = link.get("href", "")
        title_attr = link.get("title", "")
        # canonical abstract page
        if rel == "alternate" and "arxiv.org/abs/" in href:
            abs_url = href
        # pdf link (arXiv uses title="pdf")
        if title_attr.lower() == "pdf" or (href.endswith(".pdf") and "arxiv.org/pdf/" in href):
            pdf_url = href

    # Fallbacks if links not present
    id_tag = entry.find("id")
    if (not abs_url) and id_tag and "arxiv.org/abs/" in id_tag.text:
        abs_url = id_tag.text.strip()

    return title, authors, abstract, abs_url or "", pdf_url or ""

def fetch_arxiv_metadata(session: RetryingSession, arxiv_id: str) -> Optional[Paper]:
    """
    Hit arXiv API for a single ID and return a populated Paper object.
    """
    try:
        api_url = ARXIV_API_BASE + quote_plus(arxiv_id)
        headers = {"User-Agent": "arxiv-metadata-fetcher/1.0 (+https://arxiv.org)"}
        resp = session.get(api_url, headers=headers, timeout=20)
        resp.raise_for_status()

        # The response is an Atom XML feed; get the first <entry>
        # For simplicity, we pass the XML of the first (and only) entry into a helper.
        # If no entry exists, return None.
        # NOTE: Using BeautifulSoup to split because feed contains multiple tags.
        feed = BeautifulSoup(resp.text, "xml")
        entry = feed.find("entry")
        if not entry:
            print(f"[WARNING] No entry returned by arXiv for ID '{arxiv_id}'")
            return None

        title, authors, abstract, abs_url, pdf_url = parse_arxiv_atom_entry(str(entry))

        paper = Paper(title=title, authors=authors)
        paper.arxiv_url = abs_url or f"https://arxiv.org/abs/{arxiv_id}"
        paper.pdf_url = pdf_url or f"https://arxiv.org/pdf/{arxiv_id}.pdf"
        paper.abstract = abstract
        return paper
    except Exception as e:
        print(f"[ERROR] Failed to fetch arXiv metadata for '{arxiv_id}': {e}")
        return None

def load_json_id_list(json_source: str) -> List[str]:
    """
    Load a list of strings (arXiv URLs or IDs) from a local file path OR a URL.
    """
    try:
        if json_source.lower().startswith(("http://", "https://")):
            with Timer(f"Downloading JSON list from URL: {json_source}"):
                resp = requests.get(json_source, timeout=30)
                resp.raise_for_status()
                data = resp.json()
        else:
            with Timer(f"Reading JSON list from file: {json_source}"):
                with open(json_source, "r", encoding="utf-8") as f:
                    data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Input JSON must be a list of strings (arXiv IDs or URLs).")
        return [str(x).strip() for x in data]
    except Exception as e:
        raise RuntimeError(f"Failed to load JSON list from '{json_source}': {e}")

def process_arxiv_list(json_source: str, year: int = 2025, rate_limit_s: float = 0.35):
    """
    Given a JSON list (file path or URL) of arXiv IDs/links, download paper
    name (title), authors, abstract, and URLs, then save in your DB format.
    """
    session = RetryingSession()
    db = PaperDatabase(conference="arxiv", year=year)

    raw_items = load_json_id_list(json_source)
    print(f"[INFO] Loaded {len(raw_items)} items from JSON.")

    # Normalize to arXiv IDs
    id_list: List[str] = []
    for x in raw_items:
        aid = extract_arxiv_id(x)
        if not aid:
            print(f"[WARNING] Skipping unrecognized item: {x}")
            continue
        id_list.append(aid)

    total = len(id_list)
    print(f"[INFO] Parsed {total} valid arXiv IDs.")

    processed = 0
    skipped = 0

    with Timer("Fetching arXiv metadata for all IDs"):
        for idx, aid in enumerate(id_list, start=1):
            with Timer(f"Processing {idx}/{total}: {aid}"):
                paper = fetch_arxiv_metadata(session, aid)
                if not paper:
                    print(f"[WARNING] No data for {aid}")
                    continue

                # Skip if already present with full data
                if db.has_paper(paper.title):
                    print(f"[{idx}/{total}] Skipping already processed paper: {paper.title[:80]}...")
                    skipped += 1
                else:
                    paper.paper_id = idx
                    db.save_paper(paper)
                    processed += 1
                    print(f"[{idx}/{total}] Saved: {paper.title[:80]}...")

                # Respect arXiv's rate limit recommendations
                time.sleep(rate_limit_s)

    print("\n--- arXiv Import Summary ---")
    print(f"Total arXiv IDs parsed: {total}")
    print(f"Papers newly processed and saved: {processed}")
    print(f"Papers skipped (already in database): {skipped}")
    print(f"Total papers in database: {len(db.papers)}")
    print(f"Data saved to: {db.filename}")

# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    """
    Example:
      1) Local file:  JSON_LIST = "arxiv_list.json"
      2) Remote URL:  JSON_LIST = "https://example.com/my_arxiv_list.json"

    The JSON file should look like:
      ["https://arxiv.org/abs/2501.01234v2", "2502.00001", "cs/0701234", "https://arxiv.org/pdf/2503.04567.pdf"]
    """
    JSON_LIST = "arxiv_dexterous_links.json"  # change to your file path or URL
    YEAR = 2025
    process_arxiv_list(JSON_LIST, year=YEAR, rate_limit_s=0.35)
