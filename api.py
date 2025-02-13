from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import dataFetcher
from collections import Counter
from datetime import datetime
import xml.etree.ElementTree as ET
import re
from pymongo import MongoClient
from fastapi.encoders import jsonable_encoder
import random
import os
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow requests from any origin (change this for production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client.ecfr

@app.get("/ping")
def ping_route():
    return {"message": "pong!"}

@app.get("/regulation_churn")
def regulation_churn_route():
    data = list(db.title_revisions.find({}, {"_id": 0}))
    return jsonable_encoder(data)

@app.get("/words_by_title")
def words_by_title_route():
    """Fetch word counts for all titles."""
    return list(db.titles.find({}, {"number": 1, "name": 1, "word_count": 1, "_id": 0}))

banned_words = {
    "the", "of", "to", "a", "or", "and", "in", "for", "that", "be", "by", "is", "with",
    "this", "as", "an", "s", "any", "not", "b", "state", "2", "may", "on", "c", "1", "such",
    "under", "section", "if", "other", "are", "must", "shall", "will", "agency", "which",
    "federal", "information", "services", "part", "3", "program", "d", "from", "health", "i",
    "at", "paragraph", "ii"
}

@app.get("/common_words_by_title")
def common_words_by_title_route(title: int):
    """Get a counter of the most common words in a title."""
    xml_content = dataFetcher.fetch_title_content_db(title)
    root = ET.fromstring(xml_content)
    
    # Extract text from all <P> tags
    text_content = " ".join(p.text.strip() for p in root.findall(".//P") if p.text)
    
    # remove punctuation and convert to lowercase
    words = re.findall(r"\b\w+\b", text_content.lower())
    
    # Filter out banned words (or, not, and, i, 1)
    filtered_words = [
        word for word in words if word not in banned_words and not word.isdigit() and len(word) > 1
    ]
    
    # Limit processing to 250,000 words
    if len(filtered_words) > 250000:
        filtered_words = random.sample(filtered_words, 250000)
    
    word_counts = Counter(filtered_words)
    return word_counts.most_common(50)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

