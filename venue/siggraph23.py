import json
import os
import time
from typing import Dict, Optional

import requests

TARGET_YEAR: int = 2023
YEAR_SUFFIX: str = str(TARGET_YEAR)[-2:]
ASSET_FILENAME: str = f"venue/assets/siggraph{TARGET_YEAR}.json"
SERPER_API_KEY: str = os.environ.get(
    "SERPER_API_KEY", "e1ca1e98a320ddd202767583654ec67e97e7fd12"
)


class Paper:
    def __init__(self, paper_id: str, authors: str, title: str):
        self.paper_id = paper_id
        self.authors = authors
        self.title = title
        self.paper_url: Optional[str] = None
        self.pdf_link: Optional[str] = None
        self.abstract: Optional[str] = None
        self.scholar_publication: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "paper_id": self.paper_id,
            "authors": self.authors,
            "title": self.title,
            "paper_url": self.paper_url,
            "pdf_link": self.pdf_link,
            "abstract": self.abstract,
            "scholar_publication": self.scholar_publication,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Paper":
        paper = cls(data["paper_id"], data["authors"], data["title"])
        paper.paper_url = data.get("paper_url")
        paper.pdf_link = data.get("pdf_link")
        paper.abstract = data.get("abstract")
        paper.scholar_publication = data.get("scholar_publication")
        return paper


class PaperDatabase:
    def __init__(self, output_dir: str = "dataset"):
        self.output_dir = output_dir
        self.filename = os.path.join(output_dir, f"siggraph{YEAR_SUFFIX}.json")
        self.papers: Dict[str, Paper] = {}
        self._load_existing_papers()

    def _load_existing_papers(self) -> None:
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        if os.path.exists(self.filename):
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for paper_dict in data:
                        paper = Paper.from_dict(paper_dict)
                        self.papers[paper.paper_id] = paper
                print(f"[INFO] Loaded {len(self.papers)} existing papers from database")
            except Exception as exc:
                print(f"[ERROR] Failed to load existing papers: {exc}")

    def save_paper(self, paper: Paper) -> None:
        self.papers[paper.paper_id] = paper
        self._save_to_file()

    def _save_to_file(self) -> None:
        papers_data = [paper.to_dict() for paper in self.papers.values()]
        with open(self.filename, "w", encoding="utf-8") as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)

    def has_paper(self, paper_id: str) -> bool:
        return paper_id in self.papers

    def get_paper(self, paper_id: str) -> Optional[Paper]:
        return self.papers.get(paper_id)


def search_google_scholar(
    title: str, api_key: str, timeout: int = 10
) -> Optional[dict]:
    url = "https://google.serper.dev/scholar"
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {"q": title}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ERROR] Serper request failed: {exc}")
        return None

    data = response.json()
    results = data.get("organic", [])
    result = next((r for r in results if isinstance(r, dict)), None)
    if not result:
        return None

    publication_summary = ""
    publication_info = result.get("publicationInfo")
    if isinstance(publication_info, dict):
        publication_summary = publication_info.get("summary", "")
    elif isinstance(publication_info, list):
        publication_summary = " - ".join(str(part) for part in publication_info if part)
    elif isinstance(publication_info, str):
        publication_summary = publication_info

    authors_text = ""
    publication_text = ""

    if publication_summary:
        parts = [
            part.strip() for part in publication_summary.split(" - ") if part.strip()
        ]
        if parts:
            authors_text = parts[0]
            if len(parts) > 1:
                publication_text = " - ".join(parts[1:])

    resources = result.get("resources") or []
    if isinstance(resources, dict):
        resources = [resources]
    elif not isinstance(resources, list):
        resources = []
    pdf_link = ""
    for resource in resources:
        link = resource.get("link", "")
        if link.lower().endswith(".pdf"):
            pdf_link = link
            break
        if not pdf_link and link:
            pdf_link = link

    return {
        "title": result.get("title", ""),
        "abstract": result.get("snippet", ""),
        "authors": authors_text,
        "paper_url": result.get("link", ""),
        "pdf_link": pdf_link,
        "publication": publication_text,
    }


def fetch_paper_info(paper_id: str, title: str, authors: str, paper_url: str) -> Paper:
    """Fetch paper information using Google Scholar only."""
    paper = Paper(paper_id, authors, title)
    paper.paper_url = paper_url

    print("[*] Fetching data from Google Scholar via Serper...")
    google_data = search_google_scholar(title, SERPER_API_KEY)

    if google_data:
        if google_data.get("abstract"):
            paper.abstract = google_data["abstract"].strip()
            print(f"[*] Google Scholar abstract: {paper.abstract[:100]}...")

        if google_data.get("authors") and not paper.authors:
            paper.authors = google_data["authors"].strip()

        if google_data.get("paper_url"):
            paper.paper_url = google_data["paper_url"].strip()
            print(f"[*] Google Scholar URL: {paper.paper_url}")

        if google_data.get("pdf_link"):
            paper.pdf_link = google_data["pdf_link"].strip()
            print(f"[*] Google Scholar PDF: {paper.pdf_link}")

        if google_data.get("publication"):
            paper.scholar_publication = google_data["publication"].strip()
            print(f"[*] Google Scholar publication: {paper.scholar_publication}")
    else:
        print(f"[WARNING] No Google Scholar data found for: {title}")

    if not paper.paper_url:
        paper.paper_url = paper_url

    return paper


def process_siggraph_papers() -> None:
    input_file = ASSET_FILENAME

    if not os.path.exists(input_file):
        print(f"[ERROR] Input file not found: {input_file}")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        siggraph_papers = json.load(f)

    db = PaperDatabase()
    total_papers = len(siggraph_papers)

    print(f"[INFO] Processing {total_papers} SIGGRAPH {TARGET_YEAR} papers")

    for idx, paper_data in enumerate(siggraph_papers, 1):
        paper_id = (
            paper_data.get("id")
            or paper_data.get("ssid")
            or paper_data.get("psid")
            or paper_data.get("title")
            or ""
        )
        title = paper_data.get("title", "")
        authors = (paper_data.get("author") or "").strip()
        paper_url = paper_data.get("url_paper", "")

        if not paper_id or not title:
            print(f"[{idx}/{total_papers}] Skipping invalid paper entry")
            continue

        if db.has_paper(paper_id):
            existing_paper = db.get_paper(paper_id)
            if existing_paper and existing_paper.abstract:
                print(
                    f"[{idx}/{total_papers}] Paper {paper_id} already processed, skipping..."
                )
                continue

        print(f"\n[{idx}/{total_papers}] Processing paper {paper_id}")
        print(f"Title: {title}")

        paper = fetch_paper_info(paper_id, title, authors, paper_url)
        db.save_paper(paper)
        print(f"[*] Saved paper {paper_id} to database")

        delay = 0.1
        time.sleep(delay)


if __name__ == "__main__":
    process_siggraph_papers()
