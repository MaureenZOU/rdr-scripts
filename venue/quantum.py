# -*- coding: utf-8 -*-
"""OpenAlex Paper Fetcher with DOI‑based abstract scraper.

This script pulls recent papers from specified journals via the OpenAlex API and
stores them in a local JSON database.  If OpenAlex doesn’t provide an abstract
(or provides only the inverted index), we fall back to scraping the article’s
landing page (via its DOI) or, as a last resort, the Crossref API.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from typing import Dict, Optional, List, Tuple

import requests 
from bs4 import BeautifulSoup

# ────────────────────────────────────────── CONFIG ───────────────────────────────────────────
OPENALEX_EMAIL = "xueyanzoucs@gmail.com"
BASE_URL = "https://api.openalex.org/works"
HEADERS = {
    "User-Agent": f"mailto:{OPENALEX_EMAIL}",
    "Accept": "application/json",
}

# Journal names mapped to known OpenAlex source IDs (add as needed)
journal_mapping = {
    "PRX Quantum": "s4210195673", # MODIFIED: Added PRX Quantum
}

def get_openalex_paper_data(api_url: str) -> dict:
    """
    Fetches paper metadata from the OpenAlex API using a paper ID or link.

    Args:
        api_url (str): The full API URL for a specific paper.

    Returns:
        dict: The JSON response from the OpenAlex API, or None if request fails.
    """
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # raise HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from OpenAlex: {e}")
        return None

# ────────────────────────────── DOI → ABSTRACT SCRAPER HELPERS ───────────────────────────────

# ADDED: User-Agent for external API calls, from user's helper script
SCRAPER_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; abstract-grabber/1.0)"}

# ADDED: From user's provided helper script
def extract_doi(s: str) -> str | None:
    m = re.search(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", s, re.I)
    return m.group(1) if m else None

# ADDED: From user's provided helper script
# MODIFIED: to use 'html.parser' to match existing imports
def strip_html(s: str) -> str:
    return BeautifulSoup(s or "", "html.parser").get_text(" ", strip=True)

# ADDED: Helper to fetch from Semantic Scholar, based on user's script
# MODIFIED: To also fetch authors and return (abstract, authors) tuple
def get_from_semanticscholar(doi: str, timeout: int) -> Optional[Tuple[str, List[str]]]:
    url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}"
    r = requests.get(
        url,
        params={"fields": "title,abstract,authors"},
        timeout=timeout,
        headers=SCRAPER_HEADERS
    )
    r.raise_for_status()
    data = r.json()
    
    abs_txt = data.get("abstract")
    author_list_raw = data.get("authors", [])
    
    if not abs_txt or not author_list_raw:
        return None # Missing essential data

    # Use strip_html to clean abstract, just in case
    abstract = strip_html(abs_txt) 
    
    authors = [author.get("name", "").strip() for author in author_list_raw]
    authors = [name for name in authors if name] # Remove empty names

    if not abstract or not authors:
        return None # Ensure we have both after cleaning

    return abstract, authors

# MODIFIED: This function now uses Semantic Scholar per user request.
def fetch_doi_abstract(
    doi_or_url: str,
    timeout: int = 15,
) -> Optional[Tuple[str, List[str]]]:
    """
    Fetches the abstract and authors for a given DOI using the
    Semantic Scholar Graph API.

    Parameters
    ----------
    doi_or_url
        A DOI (starting with 10.) or a full https://… URL.
    timeout
        Seconds to wait for the HTTP response.

    Returns
    -------
    Optional[Tuple[str, List[str]]]
        A tuple of (abstract, authors) if data is found.
        Otherwise, returns None.
    """
    # 1. Extract DOI using the helper
    doi = extract_doi(doi_or_url)

    try:
        result = get_from_semanticscholar(doi, timeout=timeout)
    except Exception as e:
        return None
    try:
        abstract, authors = result
    except Exception as e:
        print('hihi')
        return None

    # 3. Quality check (from original function)
    if not abstract or not authors or len(abstract) < 200:
        print(f"  [-] Data for {doi} failed quality check (abstract < 200 chars or no authors).")
        return None 

    return abstract, authors
    
# ──────────────────────────────────── DATA MODEL ─────────────────────────────────────────────
class Paper:
    def __init__(self, paper_id: str, title: str, journal: str):
        self.journal = journal
        self.paper_id = paper_id
        self.title = title
        self.authors: Optional[List[str]] = None # Changed to List[str] for consistency
        self.publication_date: Optional[str] = None
        self.paper_link: Optional[str] = None
        self.doi: Optional[str] = None
        self.abstract: Optional[str] = None
        self.keywords: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "journal": self.journal,
            "paper_id": self.paper_id,
            "title": self.title,
            "authors": self.authors,
            "publication_date": self.publication_date,
            "paper_url": self.paper_link,
            "doi": self.doi,
            "abstract": self.abstract,
            "keywords": self.keywords,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Paper":
        paper = cls(data["paper_id"], data["title"], data["journal"])
        paper.publication_date = data.get("publication_date")
        paper.paper_link = data.get("paper_url") # Corrected key from 'paper_link' to 'paper_url'
        paper.doi = data.get("doi")
        paper.abstract = data.get("abstract")
        paper.keywords = data.get("keywords")
        paper.authors = data.get("authors")
        return paper


class PaperDatabase:
    """JSON‑backed store for *Paper* objects."""

    # MODIFIED: Added 'filename' parameter to the constructor for dynamic file naming.
    def __init__(self, output_dir: str = "dataset", filename: str = "science_data.json"):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, filename)
        self.papers: Dict[str, Paper] = {}
        self._load_existing_papers()

    def _load_existing_papers(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    for paper_dict in json.load(f):
                        paper = Paper.from_dict(paper_dict)
                        self.papers[paper.paper_id] = paper
            except Exception as e:
                print(f"[ERROR] Failed to load existing papers: {e}")
                backup = f"{self.filename}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.rename(self.filename, backup)
                print(f"[INFO] Created backup at {backup}")

    def _save_to_file(self):
        with open(self.filename, "w", encoding="utf-8") as f:
            json.dump([p.to_dict() for p in self.papers.values()], f, indent=2, ensure_ascii=False)

    def save_paper(self, paper: Paper):
        self.papers[paper.paper_id] = paper
        self._save_to_file()

    def has_paper(self, paper_id: str) -> bool:
        return paper_id in self.papers

    def get_papers_by_journal(self, journal_name: str):
        return [p for p in self.papers.values() if p.journal == journal_name]

# ──────────────────────────────── UTILITY FUNCTIONS ──────────────────────────────────────────

def reconstruct_abstract(inverted_index):
    if not inverted_index:
        return ""
    words = [(pos, word) for word, positions in inverted_index.items() for pos in positions]
    words.sort(key=lambda t: t[0])
    return " ".join(word for _, word in words)

def extract_keywords(keywords_list, concepts_list):
    keywords = []
    if keywords_list:
        keywords.extend(kw.get("display_name", "") for kw in keywords_list if kw.get("display_name"))
    if concepts_list:
        concepts = sorted(concepts_list, key=lambda x: x.get("score", 0), reverse=True)[:5]
        keywords.extend(c.get("display_name", "") for c in concepts if c.get("display_name"))
    seen = set()
    unique = [kw for kw in keywords if not (kw.lower() in seen or seen.add(kw.lower()))]
    return "; ".join(unique)

# ──────────────────────────── OPENALEX → DATABASE PIPELINE ───────────────────────────────────

# MODIFIED: Function now accepts both 'start_date' and 'end_date'.
def fetch_papers_for_journal(journal_name: str, journal_id: str, db: PaperDatabase, start_date: str, end_date: str):
    cursor = "*"
    page = 1
    new_papers = 0

    source_id = journal_id.replace("https://openalex.org/", "")
    # MODIFIED: Updated filter string to use OpenAlex's date range parameters.
    filter_str = (
        f"primary_location.source.id:{source_id},"
        f"from_publication_date:{start_date},"
        f"to_publication_date:{end_date}"
    )

    while cursor:
        params = {
            "filter": filter_str,
            "per_page": 200,
            "cursor": cursor,
            "select": "id,title,publication_date,doi,primary_location,abstract_inverted_index,concepts,keywords",
        }
        try:
            res = requests.get(BASE_URL, params=params, headers=HEADERS)
            res.raise_for_status()
            data = res.json()
        except Exception as exc:
            print(f"[ERROR] {exc}")
            break

        for paper_data in data.get("results", []):
            pid = paper_data.get("id", "")
            if not pid or db.has_paper(pid):
                continue
            
            p = Paper(pid, paper_data.get("title", ""), journal_name)
            p.publication_date = paper_data.get("publication_date")
            p.paper_link = pid # The OpenAlex work page is a good paper link.
            p.doi = paper_data.get("doi", "")
            
            # Use the DOI from the main paper data if available.
            if not p.doi:
                p.doi = paper_data.get("primary_location", {}).get("landing_page_url", "")
            
            api_info_url = pid.replace("https://openalex.org/", "https://api.openalex.org/works/")
            time.sleep(0.2)
            api_info = get_openalex_paper_data(api_info_url)

            if api_info is None or "type" not in api_info:
                continue
            
            if api_info['type'] != "article":
                print(f"Skipping non-article type: {api_info['type']}")
                continue            

            fetched_data = fetch_doi_abstract(p.doi)
            if fetched_data:
                p.abstract, p.authors = fetched_data
            else:
                # If scraping fails, try reconstructing from the inverted index as a fallback.
                if paper_data.get("abstract_inverted_index"):
                    p.abstract = reconstruct_abstract(paper_data["abstract_inverted_index"])
                else:
                    # If no abstract can be found, skip the paper.
                    print(f"  [-] No abstract found for: {p.title[:80]}…")
                    continue

            p.keywords = extract_keywords(paper_data.get("keywords", []), paper_data.get("concepts", []))
            db.save_paper(p)
            new_papers += 1
            print(f"  [+] Saved: {p.title[:80]}…")

        cursor = data.get("meta", {}).get("next_cursor")
        page += 1
        time.sleep(0.2)  # polite pause

    return new_papers

# ────────────────────────────────────── CLI ────────────────────────────────────────────────

def main():
    print("OpenAlex Paper Fetcher (with DOI abstract scraper)")
    
    # MODIFIED: Set the target year for scraping. You can edit this value.
    year = "2025"
    
    # MODIFIED: Define start and end dates based on the year.
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # MODIFIED: Loop logic updated to create one file per journal per year.
    for journal_name, journal_id in journal_mapping.items():
        
        # MODIFIED: Create a safe, dynamic filename, e.g., "prx_quantum_2021.json"
        output_filename = f"quantumPRX{year[2:]}.json"

        # MODIFIED: Database is now created *inside* the loop for each journal.
        db = PaperDatabase(filename=output_filename)
        
        print(f"\n[INFO] Loaded {len(db.papers)} existing papers from {output_filename}")
        print(f"Fetching {journal_name} (ID={journal_id}) from {start_date} to {end_date}…")
        
        # MODIFIED: Pass the start and end dates to the fetching function.
        added = fetch_papers_for_journal(
            journal_name, journal_id, db, start_date=start_date, end_date=end_date
        )
        
        print(f"[DONE] {added} new papers saved for {journal_name}\n")


if __name__ == "__main__":
    main()