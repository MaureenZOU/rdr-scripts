import requests
from bs4 import BeautifulSoup
import json
import os
import time
from datetime import datetime
from typing import Dict, Optional

class Paper:
    def __init__(self, paper_id: str, authors: str, title: str):
        self.paper_id = paper_id
        self.authors = authors
        self.title = title
        self.paper_url = None
        self.pdf_link = None
        self.abstract = None
        self.doi_url = None
    
    def to_dict(self) -> dict:
        return {
            'paper_id': self.paper_id,
            'authors': self.authors,
            'title': self.title,
            'paper_url': self.paper_url,
            'pdf_link': self.pdf_link,
            'abstract': self.abstract,
            'doi_url': self.doi_url
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Paper':
        paper = cls(data['paper_id'], data['authors'], data['title'])
        paper.paper_url = data.get('paper_url')
        paper.pdf_link = data.get('pdf_link')
        paper.abstract = data.get('abstract')
        paper.doi_url = data.get('doi_url')
        return paper

class PaperDatabase:
    def __init__(self, output_dir: str = "dataset"):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, "eccv24.json")
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
                        self.papers[paper.paper_id] = paper
                print(f"[INFO] Loaded {len(self.papers)} existing papers from database")
            except Exception as e:
                print(f"[ERROR] Failed to load existing papers: {e}")
                if os.path.exists(self.filename):
                    backup_name = f"{self.filename}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    os.rename(self.filename, backup_name)
                    print(f"[INFO] Created backup of existing database: {backup_name}")

    def save_paper(self, paper: Paper):
        """Save or update a single paper in the database."""
        self.papers[paper.paper_id] = paper
        self._save_to_file()

    def _save_to_file(self):
        """Save all papers to the JSON file."""
        papers_data = [paper.to_dict() for paper in self.papers.values()]
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)

    def has_paper(self, paper_id: str) -> bool:
        """Check if a paper exists in the database."""
        return paper_id in self.papers

    def get_paper(self, paper_id: str) -> Optional[Paper]:
        """Get a paper from the database if it exists."""
        return self.papers.get(paper_id)

def get_eccv_papers_html():
    """Fetch the HTML content from ECCV 2024 papers page"""
    url = "https://www.ecva.net/papers.php"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"[ERROR] Error fetching ECCV papers page: {e}")
        return None

def get_doi_page_html(doi_url):
    """Fetch the HTML content from a DOI page"""
    try:
        time.sleep(1)  # Be respectful to the server
        response = requests.get(doi_url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"[ERROR] Error fetching DOI page {doi_url}: {e}")
        return None

def fetch_abstract_from_doi_page(doi_url):
    """Fetch and parse abstract from DOI page with enhanced extraction"""
    if not doi_url:
        return None
    
    html = get_doi_page_html(doi_url)
    if not html:
        return None
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Strategy 1: Look for main content/article body sections first
    abstract_section = soup.find('section', {'data-title': 'Abstract'})
    if abstract_section:
        abstract_content = abstract_section.find('div', {'class': 'c-article-section__content'})
        if abstract_content:
            return abstract_content.get_text(strip=True)
    
    # Strategy 2: Try paragraphs in main article body 
    article_body = soup.find('div', {'data-article-body': 'true'})
    if article_body:
        first_p = article_body.find('p')
        if first_p:
            return first_p.get_text(strip=True)
            
    # Strategy 3: Fall back to meta tags if needed
    meta_desc = soup.find("meta", attrs={"name": "dc.description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()

    return None

def scrape_eccv_papers():
    """Scrapes the ECCV 2024 papers and saves them to a JSON database."""
    db = PaperDatabase()
    
    print("[INFO] Downloading and parsing the ECCV papers page...")
    eccv_html = get_eccv_papers_html()
    if not eccv_html:
        print("[ERROR] Failed to fetch ECCV papers page")
        return
    
    soup = BeautifulSoup(eccv_html, "html.parser")
    dt_tags = soup.find_all("dt", class_="ptitle")
    total_papers = len(dt_tags)
    print(f"[INFO] Found {total_papers} papers to process")
    
    for idx, dt_tag in enumerate(dt_tags, 1):
        title_link_tag = dt_tag.find("a")
        if not title_link_tag:
            continue
        
        title = title_link_tag.get_text(strip=True)
        paper_url = title_link_tag.get("href")
        
        # Generate a paper ID from the title (simplified version)
        paper_id = f"ECCV24_{idx:04d}"
        
        # Check if paper already exists in database
        if db.has_paper(paper_id):
            existing_paper = db.get_paper(paper_id)
            if existing_paper.abstract:
                print(f"[{idx}/{total_papers}] Paper {paper_id} already processed, skipping...")
                continue
        
        dd_authors = dt_tag.find_next_sibling("dd")
        authors = dd_authors.get_text(strip=True) if dd_authors else ""
        
        dd_pdf_doi = dd_authors.find_next_sibling("dd") if dd_authors else None
        if dd_pdf_doi:
            pdf_link_tag = dd_pdf_doi.find("a", text="pdf")
            pdf_url = pdf_link_tag["href"] if pdf_link_tag else None
            
            doi_link_tag = dd_pdf_doi.find("a", text="DOI")
            doi_url = doi_link_tag["href"] if doi_link_tag else None
        else:
            pdf_url = None
            doi_url = None
        
        print(f"\n[{idx}/{total_papers}] Processing paper {paper_id}")
        print(f"Title    : {title}")
        print(f"Authors  : {authors}")
        
        # Create new paper object
        paper = Paper(paper_id, authors, title)
        paper.paper_url = paper_url
        paper.pdf_link = pdf_url
        paper.doi_url = doi_url
        
        # Fetch abstract
        if doi_url:
            print(f"[*] Fetching abstract from DOI: {doi_url}")
            abstract = fetch_abstract_from_doi_page(doi_url)
            if abstract:
                print(f"[*] Abstract: {abstract[:200]}...")
                paper.abstract = abstract
        
        # Save paper to database
        db.save_paper(paper)
        print(f"[*] Saved paper {paper_id} to database")

if __name__ == "__main__":
    scrape_eccv_papers()