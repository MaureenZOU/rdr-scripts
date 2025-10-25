import openreview
import getpass # For password input
import json
import os
from datetime import datetime
from typing import Dict, Optional

# --- Configuration ---
BLIND_SUBMISSION_INVITATION = 'ICLR.cc/2021/Conference/-/Blind_Submission'
CONFERENCE_ID = 'ICLR.cc/2021/Conference'
BASE_API_URL = 'https://api.openreview.net'

# --- Paper and Database Classes (adapted from iclr25.py) ---
class Paper:
    def __init__(self, paper_id: str, authors: str, title: str):
        self.paper_id = paper_id
        self.authors = authors
        self.title = title
        self.paper_url: Optional[str] = None
        self.pdf_link: Optional[str] = None
        self.abstract: Optional[str] = None

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
    def __init__(self, output_dir: str = "dataset", filename: str = "iclr21.json"):
        self.output_dir = output_dir
        self.filename = os.path.join(self.output_dir, filename)
        self.papers: Dict[str, Paper] = {}
        self._load_existing_papers()

    def _load_existing_papers(self):
        """Load existing papers from the JSON file if it exists."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"[INFO] Created output directory: {self.output_dir}")

        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for paper_dict in data:
                        paper = Paper.from_dict(paper_dict)
                        self.papers[paper.paper_id] = paper
                print(f"[INFO] Loaded {len(self.papers)} existing papers from {self.filename}")
            except Exception as e:
                print(f"[ERROR] Failed to load existing papers from {self.filename}: {e}")
                if os.path.exists(self.filename):
                    backup_name = f"{self.filename}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    try:
                        os.rename(self.filename, backup_name)
                        print(f"[INFO] Created backup of existing database: {backup_name}")
                    except OSError as oe:
                        print(f"[ERROR] Could not create backup {backup_name}: {oe}")


    def save_paper(self, paper: Paper):
        """Save or update a single paper in the database."""
        self.papers[paper.paper_id] = paper
        self._save_to_file()

    def _save_to_file(self):
        """Save all papers to the JSON file."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"[INFO] Created output directory: {self.output_dir} before saving.")
        papers_data = [paper.to_dict() for paper in self.papers.values()]
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(papers_data, f, indent=2, ensure_ascii=False)
        print(f"[INFO] Saved {len(self.papers)} papers to {self.filename}")

    def has_paper(self, paper_id: str) -> bool:
        """Check if a paper exists in the database."""
        return paper_id in self.papers


# --- Helper Function to Fetch Reviews ---
def fetch_and_print_reviews(client, conference_id, submission_id, submission_number):
    """
    Fetches and prints reviews for a given submission.
    Includes its own error handling.
    """
    official_review_invitation = f"{conference_id}/Paper{submission_number}/-/Official_Review"
    print(f"  Fetching reviews with invitation: {official_review_invitation}")

    try:
        review_notes = client.get_notes(
            invitation=official_review_invitation,
            forum=submission_id
        )
        
        if review_notes:
            print(f"  Found {len(review_notes)} Review(s):")
            for j, review_note in enumerate(review_notes):
                print(f"\n  --- Review {j+1} for Submission {submission_number} ---")
                # import pdb; pdb.set_trace() # Uncomment to debug review_note
                print("Review Note:")
                print(review_note) # Print the raw review note object
        else:
            print(f"  No official reviews found for submission {submission_number} using invitation '{official_review_invitation}'.")

    except openreview.OpenReviewException as e:
        print(f"  Error fetching reviews for submission {submission_number}: {e}")
    except Exception as e:
        print(f"  Unexpected error fetching reviews for submission {submission_number}: {e}")

# --- Initialize OpenReview Client ---
# For anonymous access (publicly readable data only):
# client = openreview.Client(baseurl=BASE_API_URL)
# To authenticate (if you need to access non-public reviews):
USERNAME = "xueyan@cs.wisc.edu"
PASSWORD = getpass.getpass("Enter your OpenReview password: ")
client = openreview.Client(baseurl=BASE_API_URL, username=USERNAME, password=PASSWORD)

print(f"Client initialized. Anonymous: {not client.token}")

# --- Initialize Database ---
db = PaperDatabase()

# --- Fetch Submissions ---
# Initialize an empty list to hold all fetched submissions.
# We will populate this list by making paginated calls to the API.
submission_notes = [] 
all_submission_notes_accumulator = [] 
offset = 0
page_limit = 1000  # Set page limit for fetching notes, as per typical API behavior.
print(f"Fetching all submissions for: {BLIND_SUBMISSION_INVITATION} (paging with limit {page_limit})")
fetching_successful = True # Flag to track if all pages were fetched without error

while True:
    try:
        print(f"  Fetching submissions page: offset={offset}, limit={page_limit}...")
        current_batch = client.get_notes(
            invitation=BLIND_SUBMISSION_INVITATION,
            limit=page_limit,
            offset=offset,
            sort='number:asc' # Maintain sort order for pagination
        )
        
        if not current_batch:
            print("  No more submissions found on this page (or subsequent pages). Reached end.")
            break # Exit loop if no notes are returned, indicating end of data.
        
        all_submission_notes_accumulator.extend(current_batch)
        print(f"  Fetched {len(current_batch)} submissions in this batch. Total fetched so far: {len(all_submission_notes_accumulator)}")
        
        # If the number of notes fetched is less than the page limit, it means this is the last page.
        if len(current_batch) < page_limit:
            print("  This was the last page of submissions.")
            break 
        
        # Advance offset by the number of items actually received for the next iteration.
        offset += len(current_batch) 

    except openreview.OpenReviewException as e:
        print(f"Error fetching submissions batch at offset {offset}: {e}")
        print("This could be due to an incorrect invitation ID, network issues, or insufficient permissions for this page.")
        print("Proceeding with submissions fetched so far.")
        fetching_successful = False
        break # Exit loop on API error, will proceed with already fetched notes.
    except Exception as e:
        print(f"An unexpected error occurred while fetching submissions batch at offset {offset}: {e}")
        print("Proceeding with submissions fetched so far.")
        fetching_successful = False
        break # Exit loop on other errors.

submission_notes = all_submission_notes_accumulator

if fetching_successful:
    print(f"Successfully fetched a total of {len(submission_notes)} submissions.")
else:
    print(f"Fetching may have been interrupted. Processed {len(submission_notes)} submissions found before interruption.")


if not submission_notes:
    print("No submissions found or error occurred during fetching. Exiting.")
    exit()

# --- Process Each Submission and Fetch Its Reviews ---
for i, submission_note in enumerate(submission_notes):
    print(f"--- Processing Submission {submission_note.number} ({submission_note.id}) ---")
    # import pdb; pdb.set_trace() # Uncomment to debug submission_note
    # print("Submission Note:")
    # print(submission_note) # Print the raw submission note object

    paper_id = submission_note.id # Forum ID
    submission_number = submission_note.number

    if db.has_paper(paper_id):
        print(f"  Paper {paper_id} (Number: {submission_number}) already in database. Skipping.")
        continue

    # Try to get essential content, skip if key fields are missing
    try:
        title = submission_note.content['title']
        authors_list = submission_note.content['authors']
        abstract = submission_note.content['abstract']
    except KeyError as e:
        print(f"  Skipping paper {paper_id} due to missing essential content: {e}")
        continue
    
    authors = ', '.join(authors_list)
        
    # Decision check
    accepted = False
    decision_invitation = f"{CONFERENCE_ID}/Paper{submission_number}/-/Decision"
    try:
        decision_notes = client.get_notes(invitation=decision_invitation, forum=paper_id)
        if decision_notes:
            for decision_note in decision_notes:
                decision_content = decision_note.content.get('decision', '')
                if isinstance(decision_content, str) and 'accept' in decision_content.lower(): # Case-insensitive check
                    accepted = True
                    print(f"  Paper {paper_id} (Number: {submission_number}) is ACCEPTED. Decision: '{decision_content}'")
                    break 
            if not accepted:
                 print(f"  Paper {paper_id} (Number: {submission_number}) is NOT Accepted or decision not clear from {len(decision_notes)} decision notes.")
        else:
            print(f"  No decision notes found for paper {paper_id} (Number: {submission_number}) with invitation {decision_invitation}.")
            # Optionally, try Meta_Review if Decision is not found
            meta_review_invitation = f"{CONFERENCE_ID}/Paper{submission_number}/-/Meta_Review"
            # print(f"  Trying Meta_Review: {meta_review_invitation}")
            meta_review_notes = client.get_notes(invitation=meta_review_invitation, forum=paper_id)
            if meta_review_notes:
                for meta_review_note in meta_review_notes:
                    # Meta reviews might store decision in 'recommendation' or similar
                    recommendation = meta_review_note.content.get('recommendation', '')
                    if isinstance(recommendation, str) and 'accept' in recommendation.lower():
                        accepted = True
                        print(f"  Paper {paper_id} (Number: {submission_number}) is ACCEPTED via Meta_Review. Recommendation: '{recommendation}'")
                        break
                if not accepted and meta_review_notes:
                    print(f"  Decision not found in {len(meta_review_notes)} Meta_Review notes for paper {paper_id}.")
    except openreview.OpenReviewException as e:
        print(f"  Error fetching decision notes for paper {paper_id} (Number: {submission_number}): {e}")
    except Exception as e:
        print(f"  Unexpected error during decision check for paper {paper_id} (Number: {submission_number}): {e}")

    if not accepted:
        print(f"  Skipping paper {paper_id} (Number: {submission_number}) as it is not accepted or decision unclear.")
        continue
          
    paper_url = f'https://openreview.net/forum?id={paper_id}'

    pdf_content_link = submission_note.content.get('pdf')
    pdf_link = f"https://openreview.net{pdf_content_link}"

    paper_obj = Paper(paper_id=paper_id, authors=authors, title=title)
    paper_obj.paper_url = paper_url
    paper_obj.pdf_link = pdf_link
    paper_obj.abstract = abstract

    db.save_paper(paper_obj)
    print(f"  Saved paper {paper_id} (Number: {submission_number}) to database.")

print("--- Script Finished ---")
