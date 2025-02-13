import logging
from pymongo import MongoClient
import dataFetcher
import gridfs
import xml.etree.ElementTree as ET
from bson import ObjectId
from collections import Counter
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurable options
download_titles = False
download_agencies = False
calculate_word_count = False
calculate_revisions_per_year = True

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.ecfr

titles_collection = db.titles
agencies_collection = db.agencies
fs = gridfs.GridFS(db)  # Use GridFS for large XML storage

# Clear existing data if downloading new data
if download_titles:
    titles_collection.delete_many({})
    db.fs.files.delete_many({})  # Clear GridFS files
    db.fs.chunks.delete_many({})  # Clear GridFS chunks
    logging.info("Cleared existing data from titles collection and GridFS.")

if download_agencies:
    agencies_collection.delete_many({})
    logging.info("Cleared existing data from agencies collection.")

def compute_word_count():
    """Compute and update word count for each title."""
    for title in titles_collection.find():
        gridfs_id = title.get("xml_content", {}).get("gridfs_id")
        if gridfs_id:
            try:
                xml_content = fs.get(ObjectId(gridfs_id)).read().decode("utf-8")
                word_count = len(
                    " ".join(
                        [p.text.strip() for p in ET.fromstring(xml_content).findall(".//P") if p.text]
                    ).split()
                )
                titles_collection.update_one(
                    {"_id": title["_id"]}, {"$set": {"word_count": word_count}}
                )
                logging.info(f"Updated word count for Title {title['number']}: {word_count}")
            except Exception as e:
                logging.error(f"Error processing Title {title['number']}: {e}")

# Fetch and store titles metadata
if download_titles:
    titles_metadata = dataFetcher.fetch_titles_metadata()
    if titles_metadata:
        for title in titles_metadata.get("titles", []):
            issue_date = title.get("latest_issue_date")
            if issue_date:
                try:
                    xml_content = dataFetcher.fetch_title_content(title["number"], issue_date)
                    if xml_content:
                        file_id = fs.put(xml_content.encode("utf-8"), filename=f"title_{title['number']}.xml")
                        title["xml_content"] = {"gridfs_id": str(file_id)}  # Store GridFS reference
                except Exception as e:
                    logging.error(f"Error fetching/storing XML for Title {title['number']}: {e}")

            titles_collection.insert_one(title)
        logging.info("Titles metadata successfully stored.")
    else:
        logging.error("Failed to fetch titles metadata.")

# Fetch and store agencies metadata
if download_agencies:
    agencies_metadata = dataFetcher.fetch_agencies()
    if agencies_metadata:
        agencies_collection.insert_many(agencies_metadata.get("agencies", []))
        logging.info("Agencies metadata successfully stored.")
    else:
        logging.error("Failed to fetch agencies metadata.")

# Compute word count if needed
if calculate_word_count:
    compute_word_count()

# Calculate revisions per year
if calculate_revisions_per_year:
    try:
        titles = list(titles_collection.find({}))
        title_revisions = []

        for t in titles:
            amendments = dataFetcher.fetch_title_amendments(t['number']).get('content_versions', [])
            changes_per_year = {
                str(year): count for year, count in Counter(
                    datetime.strptime(a["amendment_date"], "%Y-%m-%d").year for a in amendments
                ).items()
            }

            title_revisions.append({
                "title_number": t["number"],
                "title_name": t.get("name"),
                "changes_per_year": changes_per_year
            })

        if title_revisions:
            db.title_revisions.insert_many(title_revisions)
            logging.info("Title revisions data successfully stored.")
    except Exception as e:
        logging.error(f"Error calculating title revisions: {e}")
