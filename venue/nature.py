# -*- coding: utf-8 -*-
"""OpenAlex Paper Fetcher with DOI‑based abstract scraper.

This script pulls recent papers from specified journals via the OpenAlex API and
stores them in a local JSON database.  If OpenAlex doesn’t provide an abstract
(or provides only the inverted index), we fall back to scraping the article’s
landing page (via its DOI) or, as a last resort, the Crossref API.
"""

from __future__ import annotations

import html
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
    "Nature": "s137773608",
}

# ────────────────────────────── DOI → ABSTRACT SCRAPER HELPERS ───────────────────────────────
ABSTRACT_PATTERNS = [
    # 1) JSON‑LD blob – Springer Nature, IEEE, Wiley, …
    ("script[type='application/ld+json']", "json_ld"),
    # 2) Dublin Core / generic meta tag
    ("meta[name='dc.description'], meta[name='description']", "meta"),
    # 3) Visible abstract block (broad fallback)
    ("#Abs1-content, .Abstracts, .abstr, div[id*='abstract']", "html"),
]

def fetch_doi_abstract(doi_or_url: str, timeout: int = 15) -> Optional[Tuple[str, List[str]]]:
    """Return ``(abstract, authors)`` only when the landing page declares itself an
    *OriginalPaper* via ``<meta name="dc.type" content="OriginalPaper">``; otherwise
    return *None*.

    * Authors are pulled by locating the ``"contentInfo":{"authors": …}`` fragment
      directly in the raw HTML, which is more robust than attempting to parse the
      entire `window.dataLayer` object.
    """

    url = doi_or_url if doi_or_url.startswith("http") else f"https://doi.org/{doi_or_url}"

    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── 1 │ Confirm article type is *OriginalPaper* ──────────────────────────────
    dc_type = soup.find("meta", attrs={"name": "dc.type"})
    if not dc_type or dc_type.get("content", "").lower() != "originalpaper":
        return None

    # ── 2 │ Extract authors list ────────────────────────────────────────────────
    authors: List[str] = []
    # Search the *raw* HTML for the authors array inside the `contentInfo` object.
    m = re.search(r'"contentInfo"\s*:\s*{[^{}]*?"authors"\s*:\s*(\[[^\]]+\])', resp.text, re.S)
    if m:
        try:
            authors = json.loads(m.group(1))
        except Exception:
            authors = []  # Fallback to empty list if JSON parsing fails.

    # ── 3 │ Extract abstract using existing patterns ────────────────────────────
    abstract_text: Optional[str] = None

    for selector, kind in ABSTRACT_PATTERNS:
        element = soup.select_one(selector)
        if not element:
            continue

        if kind == "json_ld":
            try:
                data = json.loads(element.string or "{}")
                candidate = data.get("description") or (data.get("mainEntity") or {}).get("description")
                if candidate:
                    abstract_text = BeautifulSoup(candidate, "html.parser").get_text(" ", strip=True)
                    break
            except Exception:
                pass
        elif kind == "meta":
            content = element.get("content")
            if content:
                abstract_text = content.strip()
                break
        else:
            abstract_text = element.get_text(" ", strip=True)
            break

    # ── 4 │ Crossref fallback (unchanged) ───────────────────────────────────────
    if abstract_text is None and doi_or_url.startswith("10."):
        try:
            cr = requests.get(f"https://api.crossref.org/works/{doi_or_url}", timeout=timeout)
            cr.raise_for_status()
            abstract = cr.json()["message"].get("abstract")
            if abstract:
                abstract = re.sub(r"</?jats:[^>]+>", "", abstract)
                abstract_text = BeautifulSoup(abstract, "html.parser").get_text(" ", strip=True)
        except Exception:
            pass
        
    # ── 5 │ Final return ────────────────────────────────────────────────────────
    if abstract_text:
        return abstract_text, authors

    return None

# ──────────────────────────────────── DATA MODEL ─────────────────────────────────────────────
class Paper:
    def __init__(self, paper_id: str, title: str, journal: str):
        self.journal = journal
        self.paper_id = paper_id
        self.title = title
        self.authors: Optional[str] = None
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
        paper.paper_link = data.get("paper_link")
        paper.doi = data.get("doi")
        paper.abstract = data.get("abstract")
        paper.keywords = data.get("keywords")
        paper.authors = data.get("authors")
        return paper


class PaperDatabase:
    """JSON‑backed store for *Paper* objects."""

    def __init__(self, output_dir: str = "dataset", filename: str = "nature25.json"):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, filename)
        self.papers: Dict[str, Paper] = {}
        self._load_existing_papers()

    # … (unchanged helper methods _load_existing_papers, save_paper, etc.)
    #     For brevity we omit the bodies here; they are identical to your original.
    #     ↓↓↓ paste the original implementations ↓↓↓

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

def fetch_papers_for_journal(journal_name: str, journal_id: str, db: PaperDatabase, start_date="2025-01-01", end_date="2025-12-31"):
    cursor = "*"
    page = 1
    new_papers = 0

    source_id = journal_id.replace("https://openalex.org/", "")
    # filter_str = f"primary_location.source.id:{source_id},publication_date:>{start_date}"
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
            "select": "id,title,publication_date,primary_location,abstract_inverted_index,concepts,keywords",
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
            p.paper_link = pid.replace("https://openalex.org/", "https://api.openalex.org/works/")
            p.doi = paper_data.get("primary_location", {}).get("landing_page_url", "")
            
            fetched_data = fetch_doi_abstract(p.doi)
            if fetched_data:
                p.abstract, p.authors = fetched_data
            else:
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
    year = "2021"
    db = PaperDatabase(filename=f"nature{year[2:]}.json")
    print(f"[INFO] Loaded {len(db.papers)} existing papers")
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    for journal in journal_mapping:
        jid = journal_mapping[journal]
        print(f"\nFetching {journal} (ID={jid}) …")
        added = fetch_papers_for_journal(journal, jid, db, start_date=start_date, end_date=end_date)
        print(f"[DONE] {added} new papers saved for {journal}\n")


if __name__ == "__main__":
    main()