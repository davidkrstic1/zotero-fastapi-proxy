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
def get_items_in_collection(collection_key: str, limit: int = 50):
    items = []
    start = 0
    while True:
        url = (
            f"https://api.zotero.org/users/{ZOTERO_USER_ID}/collections/{collection_key}/items"
            f"?limit={limit}&start={start}"
        )
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            return {"error": "Zotero API returned an error", "status": response.status_code}

        batch = response.json()
        if not batch:
            break

        items.extend(batch)
        if len(batch) < limit:
            break

        start += limit

    return items


from fastapi.responses import HTMLResponse
import fitz  # PyMuPDF

@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def render_pdf_as_html(attachment_key: str):
    pdf_path = os.path.join("static", f"{attachment_key}.pdf")

    if not os.path.exists(pdf_path):
        return Response(content="PDF not found", status_code=404)

    try:
        doc = fitz.open(pdf_path)
        html = "<html><body><h2>Extrahierter PDF-Text</h2>"
        for i, page in enumerate(doc, start=1):
            html += f"<h3>Seite {i}</h3><p>{page.get_text()}</p><hr>"
        html += "</body></html>"
        return HTMLResponse(content=html)
    except Exception as e:
        return Response(content=f"Fehler beim Öffnen/Lesen der PDF: {str(e)}", status_code=500)

@app.get("/collections/{collection_key}/items-with-pdfs")
def get_items_with_pdfs(collection_key: str, limit: int = 50):
    collected_items = []
    start = 0

    while True:
        url = (
            f"https://api.zotero.org/users/{ZOTERO_USER_ID}/collections/{collection_key}/items"
            f"?limit={limit}&start={start}&include=children"
        )
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            return {"error": "Zotero API returned an error", "status": response.status_code}
        
        items = response.json()
        if not items:
            break

        for item in items:
            if item.get("data", {}).get("itemType") in ("note", "annotation", "attachment"):
                continue  # ignoriere irrelevante Typen
            key = item.get("data", {}).get("key")
            children_url = f"https://api.zotero.org/users/{ZOTERO_USER_ID}/items/{key}/children"
            children_resp = requests.get(children_url, headers=HEADERS)
            if children_resp.status_code == 200:
                children = children_resp.json()
                if any(child.get("data", {}).get("contentType") == "application/pdf" for child in children):
                    collected_items.append(item)

        if len(items) < limit:
            break
        start += limit

    return collected_items

