import requests
from pymongo import MongoClient
import gridfs
from bson import ObjectId
import os
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client.ecfr
fs = gridfs.GridFS(db)  # GridFS instance

# API Base URL
BASE_URL = "https://www.ecfr.gov"

def fetch_agencies():
    """Fetch and return agencies from the eCFR API."""
    response = requests.get(f"{BASE_URL}/api/admin/v1/agencies.json")
    return response.json() if response.status_code == 200 else None

def fetch_titles_metadata():
    """Fetch and return eCFR title metadata."""
    response = requests.get(f"{BASE_URL}/api/versioner/v1/titles.json")
    return response.json() if response.status_code == 200 else None

def fetch_title_amendments(title):
    response = requests.get(f"{BASE_URL}/api/versioner/v1/versions/title-{title}.json")
    return response.json() if response.status_code == 200 else None

def fetch_all_revisions():
    response = requests.get(f"{BASE_URL}/api/admin/v1/corrections.json")
    return response.json() if response.status_code == 200 else None

def fetch_title_content(title, issue_date):
    """Fetch and return title XML"""
    response = requests.get(f"{BASE_URL}/api/versioner/v1/full/{issue_date}/title-{title}.xml")
    return response.text if response.status_code == 200 else None

def fetch_title_content_db(title_number:int):
    """Retrieve XML content for a specific title number."""
    title = db.titles.find_one({"number": title_number})
    print(title)

    if not title or "xml_content" not in title:
        print(f"No XML content found for Title {title_number}")
        return None

    gridfs_id = title["xml_content"]["gridfs_id"]
    xml_content = fs.get(ObjectId(gridfs_id)).read().decode("utf-8")
    
    return xml_content

