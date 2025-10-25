#!/usr/bin/env python3
"""
Parse ICRA 2021 paper metadata into a list of dicts with:
paper_id, authors, title, paper_url, pdf_link, abstract
"""

import json
import re
from collections import OrderedDict
from typing import List, Dict, Any, Optional
import requests


def fetch_source(url: str) -> List[Dict[str, Any]]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Expected a JSON array at the source URL.")
    return data


def dedupe_preserve_order(names: List[str]) -> List[str]:
    """Remove exact duplicates while keeping order."""
    seen = OrderedDict()
    for n in names:
        n = n.strip()
        if n:
            seen[n] = None
    return list(seen.keys())


def normalize_authors(author_field: Optional[str]) -> str:
    """
    Source uses semicolons and sometimes repeats authors.
    Normalize to a comma+space separated string with unique names.
    """
    if not author_field:
        return ""
    # Split on semicolons primarily; if commas appear inside names, we leave them.
    parts = [p.strip() for p in author_field.split(";")]
    parts = [p for p in parts if p]  # drop empties
    parts = dedupe_preserve_order(parts)
    return ", ".join(parts)


def build_pdf_link(item: Dict[str, Any]) -> str:
    """
    Prefer the provided 'pdf' field.
    If empty and the site is IEEE Xplore, try constructing the 'stamp' PDF link.
    Otherwise return empty string.
    """
    pdf = (item.get("pdf") or "").strip()
    if pdf:
        return pdf
    site = (item.get("site") or "").strip()
    paper_id = str(item.get("id") or "").strip()
    if "ieeexplore.ieee.org" in site and paper_id.isdigit():
        # IEEE's public PDF viewer link often works with the arnumber
        return f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={paper_id}"
    return ""


def transform_record(item: Dict[str, Any]) -> Dict[str, str]:
    return {
        "paper_id": str(item.get("id") or "").strip(),
        "authors": normalize_authors(item.get("author")),
        "title": (item.get("title") or "").strip(),
        "paper_url": (item.get("site") or "").strip(),
        "pdf_link": build_pdf_link(item),
        "abstract": (item.get("abstract") or "").strip(),
    }


def main():
    Year = 2025
    SOURCE_URL = f"https://raw.githubusercontent.com/papercopilot/paperlists/refs/heads/main/icra/icra{Year}.json"
    source = fetch_source(SOURCE_URL)
    result = [transform_record(it) for it in source]

    # Save to disk and also print a small preview
    out_path = f"dataset/icra{str(Year)[-2:]}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(result)} records to {out_path}")


if __name__ == "__main__":
    main()