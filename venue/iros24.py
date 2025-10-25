import requests
from bs4 import BeautifulSoup
import urllib.parse
import xml.etree.ElementTree as ET
import json
from datetime import datetime
import os
from typing import Dict, Optional, Tuple
from difflib import SequenceMatcher
import time  # Added for delay
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class Paper:
    def __init__(self, paper_no: str, authors: str, title: str):
        self.paper_no = paper_no
        self.authors = authors
        self.title = title
        self.arxiv_pdf = None
        self.arxiv_abstract = None
    
    def to_dict(self) -> dict:
        return {
            'paper_no': self.paper_no,
            'authors': self.authors,
            'title': self.title,
            'arxiv_pdf': self.arxiv_pdf,
            'arxiv_abstract': self.arxiv_abstract
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Paper':
        paper = cls(data['paper_no'], data['authors'], data['title'])
        paper.arxiv_pdf = data.get('arxiv_pdf')
        paper.arxiv_abstract = data.get('arxiv_abstract')
        return paper

class PaperDatabase:
    def __init__(self, output_dir: str = "dataset"):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, "iros24.json")
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
                        self.papers[paper.paper_no] = paper
                print(f"[INFO] Loaded {len(self.papers)} existing papers from database")
            except Exception as e:
                print(f"[ERROR] Failed to load existing papers: {e}")
                # Create a backup of the potentially corrupted file
                if os.path.exists(self.filename):
                    backup_name = f"{self.filename}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    os.rename(self.filename, backup_name)
                    print(f"[INFO] Created backup of existing database: {backup_name}")

    def save_paper(self, paper: Paper):
        """Save or update a single paper in the database."""
        self.papers[paper.paper_no] = paper
        self._save_to_file()

    def _save_to_file(self):
        """Save all papers to the JSON file."""
        papers_data = [paper.to_dict() for paper in self.papers.values()]
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)

    def has_paper(self, paper_no: str) -> bool:
        """Check if a paper exists in the database."""
        return paper_no in self.papers

    def get_paper(self, paper_no: str) -> Optional[Paper]:
        """Get a paper from the database if it exists."""
        return self.papers.get(paper_no)

def normalize_title(title: str) -> str:
    """
    Normalizes title for comparison by removing special characters and converting to lowercase.
    """
    # Remove special characters and extra spaces
    normalized = ''.join(c.lower() for c in title if c.isalnum() or c.isspace())
    return ' '.join(normalized.split())

def calculate_title_similarity(title1: str, title2: str) -> float:
    """
    Calculates the similarity ratio between two titles using SequenceMatcher.
    Returns a value between 0 and 1, where 1 means exact match.
    """
    # Normalize both titles
    norm_title1 = normalize_title(title1)
    norm_title2 = normalize_title(title2)
    
    # Calculate similarity
    return SequenceMatcher(None, norm_title1, norm_title2).ratio()

def normalize_author_name(name: str) -> str:
    """
    Normalizes author names for comparison by removing special characters and converting to lowercase.
    """
    # Remove special characters and extra spaces
    normalized = ''.join(c.lower() for c in name if c.isalnum() or c.isspace())
    return ' '.join(normalized.split())

def get_last_names(authors_str: str) -> set[str]:
    """
    Extracts and normalizes last names from a comma-separated author string.
    """
    # Split authors and clean up
    authors = [author.strip() for author in authors_str.split(',')]
    last_names = set()
    
    for author in authors:
        # Handle different name formats
        parts = author.strip().split()
        if parts:
            # Take the last part as the last name
            last_name = normalize_author_name(parts[-1])
            last_names.add(last_name)
    
    return last_names

def search_arxiv_for_paper(paper_title: str, paper_authors: str, max_results: int = 5, min_title_similarity: float = 0.8) -> Tuple[Optional[str], Optional[str]]:
    """
    Searches the arXiv API by paper title and verifies both title similarity and author overlap.
    Returns tuple of (pdf_url, abstract) or (None, None) if not found or no match.

    Args:
        paper_title: Title of the paper
        paper_authors: Author string from IROS webpage
        max_results: Maximum number of results to check from arXiv
        min_title_similarity: Minimum required similarity ratio between titles (0-1)
    """
    query = f"ti:{paper_title}"
    query_url = f"http://export.arxiv.org/api/query?search_query={urllib.parse.quote(query)}&max_results={max_results}"

    # Set up retry logic to handle intermittent connection issues
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)

    try:
        response = session.get(query_url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ARXIV] Request failed: {e}")
        return None, None

    time.sleep(3)  # Add a 3-second delay between requests to avoid hitting rate limits

    try:
        # Get IROS paper author last names
        iros_last_names = get_last_names(paper_authors)
        print(f"[ARXIV] IROS authors last names: {iros_last_names}")

        root = ET.fromstring(response.text)
        entries = root.findall('{http://www.w3.org/2005/Atom}entry')
        
        for entry in entries:
            # Get arXiv title and calculate similarity
            arxiv_title = entry.find('{http://www.w3.org/2005/Atom}title').text
            title_similarity = calculate_title_similarity(paper_title, arxiv_title)
            print(f"[ARXIV] Title similarity: {title_similarity:.2%}")
            print(f"[ARXIV] arXiv title: {arxiv_title}")
            print(f"[ARXIV] IROS title: {paper_title}")
            
            if title_similarity < min_title_similarity:
                print("[ARXIV] Title similarity below threshold, skipping...")
                continue

            # Get arXiv authors
            arxiv_authors_elem = entry.find('{http://www.w3.org/2005/Atom}author')
            if arxiv_authors_elem is not None:
                arxiv_authors = [author.find('{http://www.w3.org/2005/Atom}name').text
                               for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
                arxiv_last_names = get_last_names(', '.join(arxiv_authors))
                print(f"[ARXIV] arXiv authors last names: {arxiv_last_names}")

                # Check for author overlap
                common_authors = iros_last_names.intersection(arxiv_last_names)
                if not common_authors:
                    print("[ARXIV] No matching authors found, skipping...")
                    continue
                
                print(f"[ARXIV] Found {len(common_authors)} matching authors: {common_authors}")
                
                # Get PDF link
                pdf_url = None
                links = entry.findall('{http://www.w3.org/2005/Atom}link')
                for link in links:
                    if link.attrib.get('title') == 'pdf':
                        pdf_url = link.attrib['href']
                        break
                
                # Get abstract
                abstract_elem = entry.find('{http://www.w3.org/2005/Atom}summary')
                abstract = abstract_elem.text.strip() if abstract_elem is not None else None
                
                return pdf_url, abstract
        
        return None, None
    
    except ET.ParseError as e:
        print(f"[ARXIV] Failed to parse XML response: {e}")
        return None, None

def download_and_extract_titles(url: str) -> list[Paper]:
    """
    Downloads and extracts paper information from the IROS accepted papers page.
    Returns a list of Paper objects.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch the page: {e}")

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'dataTable'})
    if not table:
        raise RuntimeError("Could not find the dataTable in the HTML.")

    papers = []
    rows = table.find('tbody').find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 3:
            continue
        paper = Paper(
            paper_no=cols[0].get_text(strip=True),
            authors=cols[1].get_text(strip=True),
            title=cols[2].get_text(strip=True)
        )
        papers.append(paper)

    return papers

def main():
    iros_url = "http://iros2024-abudhabi.org/accepted-papers"
    db = PaperDatabase()

    print("[INFO] Downloading and parsing the accepted-papers page...")
    try:
        papers = download_and_extract_titles(iros_url)
        print(f"[INFO] Found {len(papers)} accepted papers.")

        for idx, paper in enumerate(papers, start=1):
            print(f"\n---\n[{idx}/{len(papers)}] Paper No: {paper.paper_no}")
            print(f"Authors  : {paper.authors}")
            print(f"Title    : {paper.title}")

            # Check if paper already exists in database
            existing_paper = db.get_paper(paper.paper_no)
            if existing_paper and existing_paper.arxiv_pdf:
                print(f"[*] Paper already in database with arXiv PDF: {existing_paper.arxiv_pdf}")
                continue

            # Search for paper on arXiv
            pdf_url, abstract = search_arxiv_for_paper(paper.title, paper.authors)
            if pdf_url:
                print(f"[*] arXiv PDF Found: {pdf_url}")
                paper.arxiv_pdf = pdf_url
                if abstract:
                    print(f"[*] Abstract: {abstract[:200]}...")
                    paper.arxiv_abstract = abstract
            else:
                print("[*] No matching arXiv paper found.")

            # Save paper to database after processing
            db.save_paper(paper)
            print(f"[*] Saved paper {paper.paper_no} to database")

    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")

if __name__ == "__main__":
    main()