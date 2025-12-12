import os
import html
import requests
import fitz  # PyMuPDF
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from dotenv import load_dotenv

load_dotenv()

ZOTERO_API_KEY = os.getenv("ZOTERO_API_KEY")
ZOTERO_USER_ID = os.getenv("ZOTERO_USER_ID")

if not ZOTERO_API_KEY or not ZOTERO_USER_ID:
    raise RuntimeError("Missing ZOTERO_API_KEY or ZOTERO_USER_ID environment variables")

ZOTERO_API = "https://api.zotero.org"
HEADERS = {"Zotero-API-Key": ZOTERO_API_KEY}

app = FastAPI(title="Zotero FastAPI Proxy", version="1.0.6")


def zotero_get(url: str, params=None) -> requests.Response:
    r = requests.get(url, headers=HEADERS, params=params, timeout=60, allow_redirects=True)
    return r


@app.get("/collections")
def list_collections():
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/collections"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


@app.get("/items")
def list_items():
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


@app.get("/items/{item_key}")
def get_item_details(item_key: str):
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{item_key}"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


@app.get("/items/{item_key}/attachments")
def get_item_attachments(item_key: str):
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{item_key}/children"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    children = r.json()
    attachments = []
    for c in children:
        data = c.get("data", {})
        if data.get("itemType") == "attachment" and data.get("contentType") == "application/pdf":
            attachments.append(c)
    return attachments


@app.get("/attachments/{attachment_key}/download")
def get_pdf_download_stream(attachment_key: str):
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{attachment_key}/file"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=404, detail="PDF not found in Zotero Storage")

    content_type = r.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower():
        raise HTTPException(status_code=400, detail=f"Attachment is not a PDF (Content-Type: {content_type})")

    return StreamingResponse(iter([r.content]), media_type="application/pdf")


@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def get_pdf_text_as_html(attachment_key: str, max_pages: int = 50, max_chars: int = 400000):
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{attachment_key}/file"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=404, detail="PDF not found in Zotero Storage")

    content_type = r.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower():
        raise HTTPException(status_code=400, detail=f"Attachment is not a PDF (Content-Type: {content_type})")

    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open PDF: {str(e)}")

    texts = []
    pages = min(len(doc), max_pages)
    for i in range(pages):
        try:
            t = doc[i].get_text()
        except Exception:
            t = ""
        if t:
            texts.append(t)

    joined = "\n\n".join(texts)
    if len(joined) > max_chars:
        joined = joined[:max_chars] + "\n\n[...TRUNCATED...]"

    safe = html.escape(joined).replace("\n", "<br/>")
    page = f"<html><body><h2>Extracted PDF Text</h2><div style='white-space:normal;font-family:system-ui'>{safe}</div></body></html>"
    return HTMLResponse(content=page)
