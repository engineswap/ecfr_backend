from pymongo import MongoClient
import dataFetcher
import gridfs
import xml.etree.ElementTree as ET
import re
from bson import ObjectId
from collections import defaultdict, Counter
from datetime import datetime

# Configurable options
download_titles = False  # Set to True to re-download titles
download_agencies = False  # Set to True to re-download agencies
calculate_word_count = False  # Set to True to calculate word count
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
    print("Cleared existing data from titles collection and GridFS.")

if download_agencies:
    agencies_collection.delete_many({})
    print("Cleared existing data from agencies collection.")


def compute_word_count():
    """Compute and update word count for each title."""
    for title in titles_collection.find():
        gridfs_id = title.get("xml_content", {}).get("gridfs_id")
        if gridfs_id:
            xml_content = fs.get(ObjectId(gridfs_id)).read().decode("utf-8")
            word_count = len(
                " ".join(
                    [
                        p.text.strip()
                        for p in ET.fromstring(xml_content).findall(".//P")
                        if p.text
                    ]
                ).split()
            )
            titles_collection.update_one(
                {"_id": title["_id"]}, {"$set": {"word_count": word_count}}
            )
            print(f"Updated word count for Title {title['number']}: {word_count}")


# Fetch and store titles metadata
if download_titles:
    titles_metadata = dataFetcher.fetch_titles_metadata()
    if titles_metadata:
        for title in titles_metadata["titles"]:
            issue_date = title.get("latest_issue_date")
            if issue_date:
                print(
                    f"Fetching XML content for Title {title['number']} with issue date {issue_date}"
                )
                xml_content = dataFetcher.fetch_title_content(
                    title["number"], issue_date
                )
                if xml_content:
                    file_id = fs.put(
                        xml_content.encode("utf-8"),
                        filename=f"title_{title['number']}.xml",
                    )
                    title["xml_content"] = {
                        "gridfs_id": str(file_id)
                    }  # Store GridFS reference
                    print(
                        f"Stored XML for Title {title['number']} in GridFS with ID {file_id}"
                    )

            titles_collection.insert_one(title)  # Insert one by one
            print(f"Inserted Title {title['number']} into the database.")
    else:
        print("Failed to fetch titles metadata.")

# Fetch and store agencies metadata
if download_agencies:
    agencies_metadata = dataFetcher.fetch_agencies()
    if agencies_metadata:
        for agency in agencies_metadata["agencies"]:
            agencies_collection.insert_one(agency)  # Insert one by one
            print(f"Inserted Agency {agency['name']} into the database.")
    else:
        print("Failed to fetch agencies metadata.")

# Compute word count separately
if calculate_word_count:
    compute_word_count()


if calculate_revisions_per_year:
    # Get titles from MongoDB
    titles = list(db.titles.find({}))

    # List to store documents for bulk insert
    title_revisions = []

    # For each title, calculate changes per year
    for t in titles:
        amendments = dataFetcher.fetch_title_amendments(t['number'])['content_versions']
        changes_per_year = {str(year): count for year, count in Counter(
            datetime.strptime(a["amendment_date"], "%Y-%m-%d").year for a in amendments
        ).items()}  # Convert keys to strings

        # Create a document for MongoDB
        title_revision_doc = {
            "title_number": t["number"],
            "title_name": t["name"],
            "changes_per_year": changes_per_year
        }

        title_revisions.append(title_revision_doc)

    # Insert into MongoDB (use `insert_many` for bulk insert)
    if title_revisions:
        db.title_revisions.insert_many(title_revisions)

    print("Title revisions data has been saved to MongoDB.")
