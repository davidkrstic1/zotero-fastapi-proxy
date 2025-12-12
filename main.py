from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any, Optional, Tuple
import os
import requests
import re
import unicodedata
import fitz  # PyMuPDF
from html import escape

# =========================
# App
# =========================

APP_VERSION = "2.2.0"

app = FastAPI(
    title="Zotero FastAPI Proxy",
    version=APP_VERSION,
    description="High-level API for navigating and reading a Zotero library including PDF full text."
)

# =========================
# Zotero Config
# =========================

ZOTERO_API_KEY = os.getenv("ZOTERO_API_KEY")
ZOTERO_USER_ID = os.getenv("ZOTERO_USER_ID")

if not ZOTERO_API_KEY or not ZOTERO_USER_ID:
    raise RuntimeError("ZOTERO_API_KEY and ZOTERO_USER_ID must be set")

ZOTERO_BASE = f"https://api.zotero.org/users/{ZOTERO_USER_ID}"
HEADERS = {"Zotero-API-Key": ZOTERO_API_KEY}

# =========================
# Helpers
# =========================

def _get(url: str, params: dict = None) -> requests.Response:
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r

def _normalize(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()

def _year(item):
    d = (item.get("data", {}) or {}).get("date", "")
    m = re.search(r"\b(19|20)\d{2}\b", d)
    return m.group(0) if m else ""

def _creator_string(item):
    creators = (item.get("data", {}) or {}).get("creators", [])
    names = []
    for c in creators:
        if "lastName" in c:
            names.append(c["lastName"])
    return ", ".join(names)

def _tags(item):
    return [t.get("tag", "") for t in (item.get("data", {}) or {}).get("tags", [])]

def _pdf_attachment_keys(item_key: str) -> List[str]:
    r = _get(f"{ZOTERO_BASE}/items/{item_key}/children")
    return [
        c["data"]["key"]
        for c in r.json()
        if c.get("data", {}).get("itemType") == "attachment"
        and c.get("data", {}).get("contentType") == "application/pdf"
    ]

def _compact_item(item, pdf_keys, score, reason):
    data = item.get("data", {}) or {}
    return {
        "item_key": data.get("key"),
        "itemType": data.get("itemType"),
        "title": data.get("title"),
        "creators": _creator_string(item),
        "year": _year(item),
        "publicationTitle": data.get("publicationTitle"),
        "collections": data.get("collections", []),
        "tags": _tags(item),
        "has_pdf": bool(pdf_keys),
        "pdf_attachment_keys": pdf_keys,
        "score": score,
        "match_reason": reason,
    }

# =========================
# Health
# =========================

@app.get("/health")
def health():
    r = requests.get(f"{ZOTERO_BASE}/items?limit=1", headers=HEADERS, timeout=10)
    return {
        "ok": True,
        "app_version": APP_VERSION,
        "zotero_status": r.status_code,
    }

# =========================
# NEW: BIBLIOGRAPHIC RESOLVE
# =========================

@app.get("/resolve-biblio")
def resolve_biblio(
    title: Optional[str] = None,
    creator: Optional[str] = None,
    year: Optional[str] = None,
    collection_key: Optional[str] = None,
    limit: int = 10,
    max_scan: int = 3000,
):
    nt = _normalize(title)
    nc = _normalize(creator)
    ny = year

    start = 0
    chunk = 100
    scanned = 0
    scored = []

    while scanned < max_scan:
        if collection_key:
            r = _get(
                f"{ZOTERO_BASE}/collections/{collection_key}/items",
                params={"limit": chunk, "start": start, "itemType": "-attachment"},
            )
        else:
            r = _get(
                f"{ZOTERO_BASE}/items",
                params={"limit": chunk, "start": start, "itemType": "-attachment"},
            )

        batch = r.json()
        if not batch:
            break

        for it in batch:
            data = it.get("data", {}) or {}
            score = 0
            reasons = []

            it_title = _normalize(data.get("title"))
            it_creators = _normalize(_creator_string(it))

            if nt and nt in it_title:
                score += 10
                reasons.append("title_match")

            if nc and nc in it_creators:
                score += 6
                reasons.append("creator_match")

            if ny and ny == _year(it):
                score += 3
                reasons.append("year_match")

            if score > 0:
                scored.append((score, reasons, it))

        scanned += len(batch)
        start += chunk

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    for score, reasons, it in scored[:limit]:
        key = it["data"]["key"]
        pdfs = _pdf_attachment_keys(key)
        results.append(_compact_item(it, pdfs, score, ",".join(reasons)))

    return {
        "query": {
            "title": title,
            "creator": creator,
            "year": year,
            "collection_key": collection_key,
        },
        "scanned": scanned,
        "results": results,
    }

# =========================
# PDF HTML
# =========================

from fastapi.responses import HTMLResponse

@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def pdf_as_html(attachment_key: str):
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    parts = ["<html><head><meta charset='utf-8'></head><body>"]
    for page in doc:
        parts.append(f"<p>{escape(page.get_text())}</p>")
    parts.append("</body></html>")

    return HTMLResponse("".join(parts), media_type="text/html; charset=utf-8")
