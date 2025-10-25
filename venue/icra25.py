import re
import json
import requests
from bs4 import BeautifulSoup

def normalize_name(name: str) -> str:
    """
    Convert 'Last, First Middle' -> 'First Middle Last'.
    Leave as-is if there's no comma.
    """
    name = " ".join(name.split())
    if "," in name:
        last, first = name.split(",", 1)
        return f"{first.strip()} {last.strip()}"
    return name

def extract_abstract_text(div) -> str:
    """
    From the abstract <div id="Ab1234"> that typically contains:
      'Keywords: ... Abstract: ...'
    return only the abstract text.
    """
    txt = div.get_text(separator=" ", strip=True)
    m = re.search(r"abstract\s*:\s*(.*)$", txt, flags=re.IGNORECASE)
    return m.group(1).strip() if m else txt

def parse_icra_program(url: str):
    """
    Parse the ICRA program page and return a list of dicts:
      paper_id, authors, title, paper_url, pdf_link, abstract
    """
    resp = requests.get(url, timeout=30)
    # Respect declared encoding if present; otherwise fall back
    resp.encoding = resp.apparent_encoding or resp.encoding or "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    results = []

    # Each paper title anchor is inside: <span class="pTtl"><a ... onclick="viewAbstract('1234')">TITLE</a></span>
    for title_anchor in soup.select("span.pTtl > a"):
        title = title_anchor.get_text(strip=True).lstrip("\xa0")

        # Try to get the numeric paper id from the onclick handler
        paper_id = ""
        onclick = title_anchor.get("onclick", "") or ""
        m = re.search(r"viewAbstract\('(\d+)'\)", onclick)
        if m:
            paper_id = m.group(1)

        # Start from the title row, walk forward to collect authors until we hit the abstract div
        title_tr = title_anchor.find_parent("tr")
        authors = []
        abstract = ""

        cur = title_tr
        while True:
            cur = cur.find_next_sibling("tr")
            if cur is None:
                break

            # Abstract row contains a div id="Ab<id>"
            abstract_div = cur.find("div", id=re.compile(r"^Ab\d+$"))
            if abstract_div:
                # If we didn't find paper_id via onclick, derive it from the div id
                if not paper_id:
                    m2 = re.match(r"Ab(\d+)$", abstract_div.get("id", ""))
                    if m2:
                        paper_id = m2.group(1)
                abstract = extract_abstract_text(abstract_div)
                break

            # Author rows look like: <tr><td><a href="...AuthorIndex...">Last, First</a></td><td class="r">Affiliation</td></tr>
            tds = cur.find_all("td", recursive=False)
            if not tds:
                continue
            a = tds[0].find("a")
            if a and "AuthorIndex" in (a.get("href") or ""):
                authors.append(normalize_name(a.get_text(strip=True)))

            # Safety: stop if we accidentally hit the next paper header
            if "pHdr" in (cur.get("class") or []):
                break

        results.append({
            "paper_id": paper_id or "",
            "authors": ", ".join(authors),
            "title": title,
            "paper_url": "",   # not present on this page
            "pdf_link": "",    # not present on this page
            "abstract": abstract
        })

    return results

if __name__ == "__main__":
    # 🔗 Put the page you want to scrape here (e.g., the "Thursday" page you showed):
    url = "https://ras.papercept.net/conferences/conferences/ICRA25/program/ICRA25_ContentListWeb_3.html"
    data = parse_icra_program(url)
    output_path = "dataset/icra25.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(data)} records to {output_path}")