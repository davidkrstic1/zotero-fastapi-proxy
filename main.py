from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any, Optional, Tuple
import os
import requests
import re
import fitz  # PyMuPDF
from html import escape

# =========================
# App
# =========================

APP_VERSION = "2.1.0"

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

def _to_str(x):
    return x if isinstance(x, str) and x.strip() else None

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

def _parse_year_from_query(q: str) -> str:
    if not q:
        return ""
    m = re.search(r"\b(19|20)\d{2}\b", q)
    return m.group(0) if m else ""

def _score_match_free(
    free: str,
    item: Dict[str, Any],
    prefer_year: Optional[str] = None,
    prefer_creator: Optional[str] = None,
) -> Tuple[int, str]:
    data = item.get("data", {}) or {}
    haystack = " ".join([
        data.get("title", ""),
        _creator_string(item),
        data.get("publicationTitle", ""),
        " ".join(_tags(item)),
    ]).lower()

    tokens = free.lower().split()
    hits = sum(1 for t in tokens if t in haystack)

    score = hits
    if prefer_year and prefer_year == _year(item):
        score += 2
    if prefer_creator and prefer_creator.lower() in _creator_string(item).lower():
        score += 2

    return score, f"token_hits:{hits}"

def _pdf_attachment_keys(item_key: str) -> List[str]:
    r = _get(f"{ZOTERO_BASE}/items/{item_key}/children")
    pdfs = []
    for c in r.json():
        d = c.get("data", {}) or {}
        if d.get("itemType") == "attachment" and d.get("contentType") == "application/pdf":
            pdfs.append(d.get("key"))
    return pdfs

def _compact_item(
    item: Dict[str, Any],
    has_pdf: bool,
    pdf_keys: List[str],
    match_reason: str,
    score: int,
) -> Dict[str, Any]:
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
        "has_pdf": has_pdf,
        "pdf_attachment_keys": pdf_keys,
        "match_reason": match_reason,
        "score": score,
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
# Listing
# =========================

@app.get("/items")
def list_items(limit: int = 100, start: int = 0):
    r = _get(f"{ZOTERO_BASE}/items", params={"limit": limit, "start": start, "itemType": "-attachment"})
    return r.json()

@app.get("/collections")
def list_collections():
    return _get(f"{ZOTERO_BASE}/collections").json()

# =========================
# SEARCH (broad, performant)
# =========================

@app.get("/search")
def search(
    q: Optional[str] = None,
    collection_key: Optional[str] = None,
    has_pdf: bool = True,
    limit: int = 20,
    max_scan: int = 2000,
    pdf_check_top_n: int = 300,
):
    q = _to_str(q)
    prefer_year = _parse_year_from_query(q or "")
    scanned = 0
    start = 0
    chunk = 100
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
            score, reason = _score_match_free(q, it, prefer_year)
            if score > 0:
                scored.append((score, reason, it))

        scanned += len(batch)
        start += chunk

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    checked = 0

    for score, reason, it in scored:
        if has_pdf and checked >= pdf_check_top_n:
            break

        key = it["data"]["key"]
        pdfs = _pdf_attachment_keys(key) if has_pdf else []
        checked += 1

        if has_pdf and not pdfs:
            continue

        results.append(_compact_item(it, bool(pdfs), pdfs, reason, score))
        if len(results) >= limit:
            break

    return {
        "query": q,
        "scanned": scanned,
        "pdf_checked": checked,
        "results": results,
    }

# =========================
# RESOLVE (precise)
# =========================

@app.get("/resolve")
def resolve(query: str, limit: int = 5):
    res = search(q=query, limit=limit * 5)
    candidates = res["results"][:limit]
    return {
        "query": query,
        "candidates": candidates,
    }

# =========================
# PDF HTML
# =========================

@app.get("/attachments/{attachment_key}/html")
def pdf_as_html(attachment_key: str):
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    html = "<html><body>"
    for page in doc:
        html += f"<p>{escape(page.get_text())}</p>"
    html += "</body></html>"

    return html

# =========================
# PDF SEARCH
# =========================

@app.get("/attachments/{attachment_key}/search")
def pdf_search(attachment_key: str, phrase: str):
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    hits = []
    for i, page in enumerate(doc):
        text = page.get_text()
        if phrase.lower() in text.lower():
            hits.append({
                "page": i + 1,
                "snippet": text[:1000]
            })

    return {
        "attachment_key": attachment_key,
        "phrase": phrase,
        "hits": hits[:10],
    }
