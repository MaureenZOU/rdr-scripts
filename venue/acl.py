import requests
from bs4 import BeautifulSoup
import json
import os
import time
from typing import Dict, Optional, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

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
    def __init__(self, output_dir: str = "dataset", conference: str = "acl", year: int = 2025):
        self.output_dir = output_dir
        # Changed filename to be specific to Long Papers
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

def parse_paper_element(paper_elem: BeautifulSoup, paper_count: int) -> Optional[Paper]:
    """
    Parses a single paper's information from its HTML element on the ACL Anthology page.
    """
    try:
        title_tag = paper_elem.select_one('span.d-block > strong > a')
        if not title_tag:
            return None
        title = title_tag.text.strip()

        author_tags = paper_elem.select('a[href^="/people/"]')
        authors = [author.text.strip() for author in author_tags]
        
        paper = Paper(title=title, authors=authors)
        paper.paper_id = paper_count

        pdf_tag = paper_elem.select_one('a.badge-primary[href$=".pdf"]')
        if pdf_tag:
            paper.pdf_url = pdf_tag['href']

        abstract_div = paper_elem.find_next_sibling('div', class_='abstract-collapse')
        if abstract_div:
            card_body = abstract_div.find('div', class_='card-body')
            if card_body:
                paper.abstract = card_body.text.strip()
        
        return paper
        
    except Exception as e:
        print(f"[ERROR] Failed to parse a paper element: {e}")
        return None

def scrape_acl_papers(url: str, year: int):
    """Scrapes ACL Long Papers from the anthology page and saves them to a JSON database."""
    session = RetryingSession()
    db = PaperDatabase(conference="acl", year=year)
    
    with Timer("Complete paper scraping process"):
        try:
            print(f"[INFO] Fetching ACL papers page from: {url}")
            with Timer("Fetching main ACL page"):
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = session.get(url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- MODIFICATION START ---
            # Define the ID for the Long Papers volume ONLY.
            target_div_ids = [
                f'{year}acl-long',
            ]
            print(f"[INFO] Targeting Long Papers DIV: {target_div_ids[0]}")

            paper_elems = []
            for div_id in target_div_ids:
                container = soup.find('div', id=div_id)
                if container:
                    papers_in_container = container.find_all('p', class_='d-sm-flex align-items-stretch')
                    valid_papers = [p for p in papers_in_container if p.select_one('a.badge-primary')]
                    paper_elems.extend(valid_papers)
                    print(f"[INFO] Found {len(valid_papers)} papers in '{div_id}'")
                else:
                    print(f"[WARNING] Could not find container DIV with id='{div_id}'")
            # --- MODIFICATION END ---

            total_papers = len(paper_elems)
            print(f"[INFO] Found {total_papers} total Long Papers to process")
            
            processed_count = 0
            skipped_count = 0
            
            for i, p_element in enumerate(paper_elems):
                title_tag = p_element.select_one('span.d-block > strong > a')
                if not title_tag:
                    continue
                
                temp_title = title_tag.text.strip()

                if db.has_paper(temp_title):
                    print(f"[{i+1}/{total_papers}] Skipping already processed paper: {temp_title[:80]}...")
                    skipped_count += 1
                    continue
                
                with Timer(f"Processing paper {i+1}/{total_papers}"):
                    paper = parse_paper_element(p_element, i + 1)
                    if paper:
                        db.save_paper(paper)
                        processed_count += 1
                        print(f"[{i+1}/{total_papers}] Successfully processed: {paper.title[:80]}...")
                    else:
                        print(f"[WARNING] Failed to process paper entry {i+1}")
            
            print("\n--- Scraping Summary ---")
            print(f"Total Long Papers found on page: {total_papers}")
            print(f"Papers newly processed and saved: {processed_count}")
            print(f"Papers skipped (already in database): {skipped_count}")
            print(f"Total papers in database: {len(db.papers)}")
            print(f"Data saved to: {db.filename}")
            
        except Exception as e:
            print(f"\n[CRITICAL ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    ACL_YEAR = 2021
    ACL_URL = f"https://aclanthology.org/events/acl-{ACL_YEAR}/#{ACL_YEAR}acl-long"
    
    scrape_acl_papers(url=ACL_URL, year=ACL_YEAR)