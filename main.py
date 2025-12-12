from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from typing import List, Dict, Any, Optional, Tuple
import os
import requests
import re
import fitz  # PyMuPDF
from html import escape

# =========================
# App
# =========================

APP_VERSION = "2.3.1"

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
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        raise HTTPException(status_code=status, detail=str(e))
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

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
        last = c.get("lastName")
        if last:
            names.append(last)
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
        data.get("title", "") or "",
        _creator_string(item) or "",
        data.get("publicationTitle", "") or "",
        " ".join(_tags(item)) or "",
    ]).lower()

    free = free or ""
    tokens = [t for t in free.lower().split() if t]
    hits = sum(1 for t in tokens if t in haystack)

    score = hits
    reasons = []
    if hits:
        reasons.append(f"token_hits:{hits}")
    if prefer_year and prefer_year == _year(item):
        score += 2
        reasons.append("year_bonus")
    if prefer_creator and prefer_creator.lower() in (_creator_string(item) or "").lower():
        score += 2
        reasons.append("creator_bonus")

    return score, ",".join(reasons) if reasons else "no_match"

def _pdf_attachment_keys(item_key: str) -> List[str]:
    r = _get(f"{ZOTERO_BASE}/items/{item_key}/children")
    pdfs = []
    for c in r.json():
        d = c.get("data", {}) or {}
        if d.get("itemType") == "attachment" and d.get("contentType") == "application/pdf":
            k = d.get("key")
            if k:
                pdfs.append(k)
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
        "score": score,
        "match_reason": match_reason,
    }

def _zotero_items_endpoint(collection_key: Optional[str]) -> str:
    if collection_key:
        return f"{ZOTERO_BASE}/collections/{collection_key}/items"
    return f"{ZOTERO_BASE}/items"

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
# SEARCH (broad, library-wide, server-assisted)
# =========================

@app.get("/search")
def search(
    q: Optional[str] = None,
    collection_key: Optional[str] = None,
    require_pdf: bool = True,
    limit: int = 20,
    max_fetch: int = 500,
    pdf_check_top_n: int = 100,
):
    """
    Broad search across Zotero items. Uses Zotero server-side q filtering first,
    then applies a lightweight local scoring (title, creators, publicationTitle, tags).
    """
    q = _to_str(q) or ""
    prefer_year = _parse_year_from_query(q)

    fetched = 0
    start = 0
    chunk = 100
    scored: List[Tuple[int, str, Dict[str, Any]]] = []

    endpoint = _zotero_items_endpoint(collection_key)

    # 1) Fetch candidates from Zotero with q (fast prefilter)
    while fetched < max_fetch:
        params = {
            "limit": min(chunk, max_fetch - fetched),
            "start": start,
            "itemType": "-attachment",
        }
        if q:
            params["q"] = q
            params["qmode"] = "everything"

        batch = _get(endpoint, params=params).json()
        if not batch:
            break

        for it in batch:
            score, reason = _score_match_free(q, it, prefer_year=prefer_year)
            if score > 0:
                scored.append((score, reason, it))

        fetched += len(batch)
        start += len(batch)

        # Wenn Zotero serverseitig q filtert, sind die Treffer oft schon „eng genug“.
        # Wir holen trotzdem bis max_fetch, damit „Autor only“ oder „Titel only“ robust bleibt.
        if q and len(batch) < chunk:
            break

    scored.sort(key=lambda x: x[0], reverse=True)

    # 2) Optional PDF check für Top-N Kandidaten, um Attachment Keys direkt zu liefern
    results: List[Dict[str, Any]] = []
    checked = 0

    for score, reason, it in scored:
        if require_pdf and checked >= pdf_check_top_n:
            break

        item_key = (it.get("data", {}) or {}).get("key")
        if not item_key:
            continue

        pdfs: List[str] = _pdf_attachment_keys(item_key) if require_pdf else []
        checked += 1

        if require_pdf and not pdfs:
            continue

        results.append(_compact_item(it, bool(pdfs), pdfs, reason, score))
        if len(results) >= limit:
            break

    return {
        "query": q,
        "collection_key": collection_key,
        "server_fetched": fetched,
        "pdf_checked": checked,
        "results": results,
    }

# =========================
# RESOLVE (structured bibliographic)
# =========================

@app.get("/resolve-biblio")
def resolve_biblio(
    title: Optional[str] = None,
    creator: Optional[str] = None,
    year: Optional[str] = None,
    collection_key: Optional[str] = None,
    limit: int = 10,
    max_fetch: int = 300,
    require_pdf: bool = True,
    pdf_check_top_n: int = 50,
):
    """
    Resolve bibliographic hints (title, creator, year) to concrete Zotero items.
    Uses /search internally for consistent behavior.
    """
    parts = []
    if title:
        parts.append(title)
    if creator:
        parts.append(creator)
    if year:
        parts.append(year)

    query = " ".join([p for p in parts if p]).strip()
    if not query:
        raise HTTPException(status_code=400, detail="Provide at least one of title, creator, year")

    res = search(
        q=query,
        collection_key=collection_key,
        require_pdf=require_pdf,
        limit=limit,
        max_fetch=max_fetch,
        pdf_check_top_n=pdf_check_top_n,
    )
    return {
        "query": {
            "title": title,
            "creator": creator,
            "year": year,
            "collection_key": collection_key,
        },
        "server_fetched": res.get("server_fetched"),
        "pdf_checked": res.get("pdf_checked"),
        "results": res.get("results"),
    }

# =========================
# PDF HTML
# =========================

@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def pdf_as_html(attachment_key: str):
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    parts = ["<html><head><meta charset='utf-8'></head><body>"]
    for page in doc:
        parts.append(f"<p>{escape(page.get_text())}</p>")
    parts.append("</body></html>")

    html = "".join(parts)
    return HTMLResponse(content=html, media_type="text/html; charset=utf-8")

# =========================
# PDF SEARCH
# =========================

@app.get("/attachments/{attachment_key}/search")
def pdf_search(
    attachment_key: str,
    phrase: str = Query(..., min_length=1),
    max_hits: int = 20,
    snippet_chars: int = 1200,
):
    """
    Search for a phrase inside a PDF attachment.
    Returns page numbers and a snippet from the page text.
    """
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    phrase_l = phrase.lower()
    hits = []

    for i, page in enumerate(doc):
        text = page.get_text() or ""
        if phrase_l in text.lower():
            hits.append({
                "page": i + 1,
                "snippet": text[:snippet_chars]
            })
            if len(hits) >= max_hits:
                break

    return {
        "attachment_key": attachment_key,
        "phrase": phrase,
        "hits": hits,
    }
