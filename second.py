from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import json
import time
import os

client = MongoClient("mongodb+srv://rivalcoder01:PurmpaRXBSThyuQA@cluster0.f7zziod.mongodb.net/?retryWrites=true&w=majority")
collection = client["test"]["products"]

EXPORT_DIR = "mongo_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

def clean(doc):
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            doc[k] = str(v)
        elif isinstance(v, datetime):
            doc[k]=v.isoformat()
            # doc[k] = v.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        
    return doc

while True:
    docs = list(collection.find())
    fname = f"{EXPORT_DIR}/products.jsonl"
    with open(fname, "w") as f:
        for doc in docs:
            doc = clean(doc)
            f.write(json.dumps(doc) + "\n")
    print("[Mongo Exporter] Wrote updated products.jsonl")
    time.sleep(5)