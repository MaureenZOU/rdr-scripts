import requests
from bs4 import BeautifulSoup
import re
import json
import os
import time
from urllib.parse import urlparse
from datetime import datetime
from typing import Dict, Optional, List
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class Timer:
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
    def __init__(self, output_dir: str = "dataset", year: int = 2025):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, f"cvpr{str(year)[-2:]}.json")
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
                print(f"[INFO] Loaded {len(self.papers)} existing papers from database")
            except Exception as e:
                print(f"[ERROR] Failed to load existing papers: {e}")
                if os.path.exists(self.filename):
                    backup_name = f"{self.filename}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    os.rename(self.filename, backup_name)
                    print(f"[INFO] Created backup of existing database: {backup_name}")

    def save_paper(self, paper: Paper):
        """Save or update a single paper in the database."""
        self.papers[paper.title] = paper
        self._save_to_file()

    def _save_to_file(self):
        """Save all papers to the JSON file."""
        papers_data = [paper.to_dict() for paper in self.papers.values()]
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)

    def has_paper(self, title: str) -> bool:
        """Check if a paper exists in the database."""
        return title in self.papers

def parse_paper_page(url: str, session: RetryingSession) -> dict:
    """
    Parse detailed information from a paper's dedicated page.
    """
    try:
        print(f"[*] Fetching paper page: {url}")
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        paper_info = {
            'paper_id': None,
            'title': None,
            'authors': [],
            'abstract': None,
            'pdf_url': None,
            'arxiv_url': None,
            'supplemental_url': None
        }
        
        # Get title from meta tags (more reliable)
        title_meta = soup.find('meta', {'name': 'citation_title'})
        if title_meta:
            paper_info['title'] = title_meta['content']
        else:
            title_div = soup.find('div', id='papertitle')
            if title_div:
                paper_info['title'] = title_div.text.strip()
        
        # Get authors from meta tags
        author_metas = soup.find_all('meta', {'name': 'citation_author'})
        if author_metas:
            paper_info['authors'] = [meta['content'] for meta in author_metas]
        else:
            authors_div = soup.find('div', id='authors')
            if authors_div:
                authors_text = authors_div.text.split(';')[0]
                paper_info['authors'] = [auth.strip() for auth in authors_text.split(',')]
        
        # Get abstract
        abstract_div = soup.find('div', id='abstract')
        if abstract_div:
            paper_info['abstract'] = abstract_div.text.strip()
        
        # Get PDF URL from meta tags
        pdf_meta = soup.find('meta', {'name': 'citation_pdf_url'})
        if pdf_meta:
            paper_info['pdf_url'] = pdf_meta['content']
        else:
            pdf_link = soup.find('a', href=re.compile(r'.*\.pdf$'))
            if pdf_link:
                href = pdf_link['href']
                if not href.startswith('http'):
                    paper_info['pdf_url'] = f"https://openaccess.thecvf.com{href}"
                else:
                    paper_info['pdf_url'] = href
        
        # Find links in the page
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href:
                if 'arxiv.org' in href:
                    paper_info['arxiv_url'] = href
                elif 'supplemental' in href.lower():
                    paper_info['supplemental_url'] = href
                    if not href.startswith('http'):
                        paper_info['supplemental_url'] = f"https://openaccess.thecvf.com{href}"
        
        return paper_info
        
    except Exception as e:
        print(f"[ERROR] Failed to parse paper page {url}: {str(e)}")
        return None

def fetch_paper_details(title_link: str, session: RetryingSession, paper_count: int) -> Paper:
    """
    Fetch detailed paper information by visiting the paper's dedicated page.
    """
    base_url = "https://openaccess.thecvf.com"
    full_url = f"{base_url}{title_link}"
    
    paper_info = parse_paper_page(full_url, session)
    if not paper_info:
        return None
        
    paper = Paper(paper_info['title'], paper_info['authors'])
    paper.paper_id = paper_count
    paper.abstract = paper_info['abstract']
    paper.pdf_url = paper_info['pdf_url']
    paper.arxiv_url = paper_info['arxiv_url']
    paper.supplemental_url = paper_info['supplemental_url']
    
    return paper

def process_paper_element(paper_count: int, paper_elem: BeautifulSoup, session: RetryingSession, db: PaperDatabase) -> Optional[Paper]:
    """Process a single paper element from the main page."""
    title_link = paper_elem.find('a')
    if not title_link:
        return None
        
    title = ' '.join(title_link.text.strip().split())
    
    # Check if paper already exists and has all data
    if db.has_paper(title):
        existing_paper = db.papers[title]
        if existing_paper.abstract and existing_paper.pdf_url:
            return existing_paper
    
    # Fetch detailed paper information
    paper = fetch_paper_details(title_link['href'], session, paper_count)
    if paper:
        db.save_paper(paper)
        return paper
    
    return None

def fetch_cvpr_content(url: str, session: RetryingSession) -> str:
    """Fetch HTML content from the CVPR website."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch content from {url}: {str(e)}")

def scrape_cvpr_papers(url: str, year: int):
    """Scrapes the CVPR 2024 papers and saves them to a JSON database."""
    session = RetryingSession()
    db = PaperDatabase(year=year)
    
    with Timer("Complete paper scraping process"):
        try:
            print("[INFO] Downloading and parsing the papers page...")
            with Timer("Fetching main CVPR page"):
                html_content = fetch_cvpr_content(url, session)
                soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all paper titles
            paper_elems = soup.find_all('dt', class_='ptitle')
            total_papers = len(paper_elems)
            print(f"[INFO] Found {total_papers} papers to process")
            
            papers_with_new_info = 0
            papers_skipped = 0
            
            for paper_count, paper_elem in enumerate(paper_elems, 1):
                with Timer(f"Processing paper {paper_count}/{total_papers}"):
                    paper = process_paper_element(paper_count, paper_elem, session, db)
                    
                    if paper is None:
                        print(f"[WARNING] Failed to process paper {paper_count}")
                        continue
                        
                    if db.has_paper(paper.title):
                        if paper.abstract and paper.pdf_url:
                            papers_skipped += 1
                            print(f"[{paper_count}/{total_papers}] Paper already processed, skipping: {paper.title[:100]}...")
                            continue
                    
                    papers_with_new_info += 1
                    print(f"\n[{paper_count}/{total_papers}] Successfully processed: {paper.title[:100]}...")
            
            print(f"\n[Summary]")
            print(f"Total papers found: {total_papers}")
            print(f"Papers skipped (already had data): {papers_skipped}")
            print(f"Papers with new information: {papers_with_new_info}")
            
        except Exception as e:
            print(f"[ERROR] An error occurred: {e}")

if __name__ == "__main__":
    year = 2025
    url = f"https://openaccess.thecvf.com/CVPR{year}?day=all"
    scrape_cvpr_papers(url, year)