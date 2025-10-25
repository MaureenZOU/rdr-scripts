import requests
from bs4 import BeautifulSoup
import urllib.parse
import json
import os
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
    
    def to_dict(self) -> dict:
        return {
            'paper_id': self.paper_id,
            'authors': self.authors,
            'title': self.title,
            'paper_url': self.paper_url,
            'pdf_link': self.pdf_link,
            'abstract': self.abstract
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Paper':
        paper = cls(data['paper_id'], data['authors'], data['title'])
        paper.paper_url = data.get('paper_url')
        paper.pdf_link = data.get('pdf_link')
        paper.abstract = data.get('abstract')
        return paper

class PaperDatabase:
    def __init__(self, output_dir: str = "dataset"):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, "rss24.json")
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
                # Create a backup of the potentially corrupted file
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

def scrape_individual_paper(session, paper_url):
    """
    Given a link to an individual paper page, scrape its PDF link and abstract.
    Returns (pdf_link, abstract).
    """
    try:
        r = session.get(paper_url)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {paper_url} due to error: {e}")
        return None, None
    
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Find the PDF link
    pdf_link = None
    paper_pdf_div = soup.find('div', class_='paper-pdf')
    if paper_pdf_div:
        pdf_a_tag = paper_pdf_div.find('a')
        if pdf_a_tag and pdf_a_tag.get('href'):
            pdf_link = urllib.parse.urljoin(BASE_URL, pdf_a_tag.get('href'))
    
    # Find the abstract
    abstract = None
    paragraphs = soup.find_all('p')
    for p in paragraphs:
        bold = p.find('b')
        if bold and "Abstract:" in bold.get_text():
            full_text = p.get_text(" ", strip=True)
            if full_text.lower().startswith("abstract:"):
                abstract = full_text[len("abstract:"):].strip()
            else:
                abstract = full_text
            break
    
    return pdf_link, abstract

def scrape_rss_papers():
    """
    Scrapes the RSS 2024 papers and saves them to a JSON database.
    """    
    db = PaperDatabase()
    session = requests.Session()
    
    print("[INFO] Downloading and parsing the papers page...")
    try:
        resp = session.get(PAPERS_URL)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='myTable')
        if not table:
            raise RuntimeError("Could not find the table with id=myTable")
        
        rows = table.find_all('tr')
        total_papers = len([r for r in rows if 'toprowHeader' not in r.get('class', [])])
        print(f"[INFO] Found {total_papers} papers to process")
        
        for idx, row in enumerate(rows, 1):
            if 'toprowHeader' in row.get('class', []):
                continue
                
            cols = row.find_all('td')
            if len(cols) < 3:
                continue
            
            paper_id = cols[0].get_text(strip=True)
            
            # Check if paper already exists in database
            if db.has_paper(paper_id):
                existing_paper = db.get_paper(paper_id)
                if existing_paper.pdf_link and existing_paper.abstract:
                    print(f"[{idx}/{total_papers}] Paper {paper_id} already processed, skipping...")
                    continue
            
            # Extract paper information
            title_col = cols[1]
            link_tag = title_col.find('a')
            if not link_tag:
                continue
            
            relative_paper_url = link_tag.get('href')
            paper_url = urllib.parse.urljoin(BASE_URL, relative_paper_url)
            title = link_tag.get_text(strip=True)
            authors = cols[2].get_text(strip=True)
            
            print(f"\n[{idx}/{total_papers}] Processing paper {paper_id}")
            print(f"Title    : {title}")
            print(f"Authors  : {authors}")
            
            # Create new paper object
            paper = Paper(paper_id, authors, title)
            paper.paper_url = paper_url
            
            # Fetch additional information
            pdf_link, abstract = scrape_individual_paper(session, paper_url)
            if pdf_link:
                print(f"[*] PDF Found: {pdf_link}")
                paper.pdf_link = pdf_link
            if abstract:
                print(f"[*] Abstract: {abstract[:200]}...")
                paper.abstract = abstract
            
            # Save paper to database
            db.save_paper(paper)
            print(f"[*] Saved paper {paper_id} to database")
            
    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")

if __name__ == "__main__":
    BASE_URL = "https://roboticsconference.org"
    PAPERS_URL = "https://roboticsconference.org/2024/program/papers/"
    scrape_rss_papers()