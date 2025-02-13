- mongodb to store the data, mongo and not sql because its faster to ingest data and the data has a nested structure.

data persisted
- agencies
- titles

**Run instructions**
- first run `python populateDB.py`
- to start backend: `uvicorn api:app --host 0.0.0.0 --port 8000 --reload`
