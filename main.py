from fastapi import FastAPI, Response, status
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from fastapi.staticfiles import StaticFiles
import requests
import os
import json
import shutil
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

ZOTERO_USER_ID = os.getenv("ZOTERO_USER_ID")
ZOTERO_API_KEY = os.getenv("ZOTERO_API_KEY")
HEADERS = {"Zotero-API-Key": ZOTERO_API_KEY}

# Static folder mount
if not os.path.exists("static"):
    os.makedirs("static")

app.mount("/static", StaticFiles(directory="static"), name="static")


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
    # Prüfe zuerst die Metadaten
    meta_url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{attachment_key}"
    meta_response = requests.get(meta_url, headers=HEADERS)

    if meta_response.status_code != 200:
        return Response(
            content='{"error": "Attachment metadata not found"}',
            media_type="application/json",
            status_code=404
        )

    data = meta_response.json().get("data", {})
    if data.get("itemType") != "attachment" or data.get("contentType") != "application/pdf":
        return Response(
            content='{"error": "Item is not a valid PDF attachment"}',
            media_type="application/json",
            status_code=400
        )

    file_url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{attachment_key}/file"
    file_response = requests.get(file_url, headers=HEADERS, stream=True)

    if file_response.status_code != 200:
        return Response(
            content=json.dumps({
                "error": "PDF download failed",
                "status_code": file_response.status_code,
                "zotero_message": file_response.text
            }),
            media_type="application/json",
            status_code=file_response.status_code
        )

    # Speichere PDF lokal in static/
    filename = data.get("filename", f"{attachment_key}.pdf")
    local_path = os.path.join("static", filename)

    with open(local_path, "wb") as f:
        shutil.copyfileobj(file_response.raw, f)

    return {
        "pdf_url": f"https://zotero-fastapi-proxy.onrender.com/static/{filename}"
    }


@app.get("/items/{item_key}")
def get_item_details(item_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{item_key}"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            return {
                "error": "Zotero API returned non-200 status",
                "status_code": response.status_code,
                "body": response.text
            }
        return response.json()
    except Exception as e:
        return {"error": "Request failed", "details": str(e)}


@app.get("/items/search")
def search_items_by_title(title: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items?limit=100"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            return {
                "error": "Zotero API returned non-200 status",
                "status_code": response.status_code,
                "body": response.text
            }

        items = response.json()
        results = [
            item for item in items
            if title.lower() in item.get("data", {}).get("title", "").lower()
        ]
        return results
    except Exception as e:
        return {"error": "Search request failed", "details": str(e)}


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


@app.get("/item-key-by-title")
def get_key_by_title(title: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items?limit=100"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return {"error": "Zotero API failed", "status": response.status_code}

    items = response.json()
    for item in items:
        if title.lower() in item.get("data", {}).get("title", "").lower():
            return {"item_key": item.get("data", {}).get("key")}

    return {"error": "No match found"}


@app.get("/collections/{collection_key}/items")
def get_items_in_collection(collection_key: str):
    url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/collections/{collection_key}/items"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return {"error": "Zotero API returned an error", "status": response.status_code}
    return response.json()

