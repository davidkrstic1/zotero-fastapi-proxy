from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

ZOTERO_USER_ID = os.getenv("ZOTERO_USER_ID")
ZOTERO_API_KEY = os.getenv("ZOTERO_API_KEY")
HEADERS = {"Zotero-API-Key": ZOTERO_API_KEY}

@app.get("/items")
def get_items():
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items"
    response = requests.get(url, headers=HEADERS)
    return response.json()

@app.get("/collections")
def get_collections():
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/collections"
    response = requests.get(url, headers=HEADERS)
    return response.json()

@app.get("/attachments/{attachment_key}/download")
def download_attachment(attachment_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{attachment_key}/file"
    response = requests.get(url, headers=HEADERS, stream=True)
    if response.status_code != 200:
        return {"error": "Download failed", "status": response.status_code}
    return StreamingResponse(response.raw, media_type="application/pdf")

@app.get("/items/{item_key}")
def get_item_details(item_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{item_key}"
    response = requests.get(url, headers=HEADERS)
    return response.json()

@app.get("/items/search")
def search_items_by_title(title: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items"
    response = requests.get(url, headers=HEADERS)
    results = [item for item in response.json() if title.lower() in item.get("data", {}).get("title", "").lower()]
    return results

@app.get("/items/{item_key}/tags")
def get_tags_for_item(item_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{item_key}/tags"
    response = requests.get(url, headers=HEADERS)
    return response.json()

@app.get("/items/{item_key}/notes")
def get_notes_for_item(item_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{item_key}/notes"
    response = requests.get(url, headers=HEADERS)
    return response.json()

@app.get("/items/{item_key}/attachments")
def get_attachments_for_item(item_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{item_key}/children"
    response = requests.get(url, headers=HEADERS)
    return [i for i in response.json() if i.get("data", {}).get("itemType") == "attachment"]


