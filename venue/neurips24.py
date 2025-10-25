import openreview
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
        self.filename = os.path.join(output_dir, "neurips24.json")
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

def scrape_neurips_papers(username: str):
    """
    Scrapes the NeurIPS 2024 papers using OpenReview API and saves them to a JSON database.
    
    Args:
        username: OpenReview username for authentication
    """
    # Initialize the database
    db = PaperDatabase()
    
    print("[INFO] Connecting to OpenReview API...")
    try:
        # Initialize OpenReview client
        client = openreview.api.OpenReviewClient(
            baseurl='https://api2.openreview.net',
            username=username,
            password=input("Enter your OpenReview password: "),
        )
        
        # Get all submissions
        venue_id = 'NeurIPS.cc/2024/Conference'
        submissions = client.get_all_notes(invitation=f'{venue_id}/-/Submission')
        
        total_papers = len(submissions)
        print(f"[INFO] Found {total_papers} papers to process")
        
        # Process each paper
        for idx, paper in enumerate(submissions, 1):
            paper_id = paper.id
            
            # Skip if already processed
            if db.has_paper(paper_id):
                print(f"[{idx}/{total_papers}] Paper {paper_id} already processed, skipping...")
                continue
            
            # Handle nested 'value' fields in content
            title = paper.content.get('title', {}).get('value', '') if isinstance(paper.content.get('title'), dict) else paper.content.get('title', '')
            
            # Handle nested 'value' in authors field
            authors_content = paper.content.get('authors', {})
            if isinstance(authors_content, dict):
                authors = ', '.join(authors_content.get('value', []))
            else:
                authors = ', '.join(authors_content if isinstance(authors_content, list) else [])
                
            abstract = paper.content.get('abstract', {}).get('value', '') if isinstance(paper.content.get('abstract'), dict) else paper.content.get('abstract', '')
            pdf_url = f'https://openreview.net/pdf?id={paper_id}'
            paper_url = f'https://openreview.net/forum?id={paper_id}'
            
            print(f"\n[{idx}/{total_papers}] Processing paper {paper_id}")
            print(f"Title    : {title}")
            print(f"Authors  : {authors}")
            
            # Create new paper object
            paper_obj = Paper(paper_id, authors, title)
            paper_obj.paper_url = paper_url
            paper_obj.pdf_link = pdf_url
            paper_obj.abstract = abstract
            
            # Save paper to database
            db.save_paper(paper_obj)
            print(f"[*] Saved paper {paper_id} to database")
            
    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")

if __name__ == "__main__":
    # Replace with your OpenReview username
    USERNAME = "xueyan@cs.wisc.edu"
    scrape_neurips_papers(USERNAME)