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

APP_VERSION = "2.3.0"

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

def _year(item: Dict[str, Any]) -> str:
    d = (item.get("data", {}) or {}).get("date", "") or ""
    m = re.search(r"\b(19|20)\d{2}\b", d)
    return m.group(0) if m else ""

def _creator_string(item: Dict[str, Any]) -> str:
    creators = (item.get("data", {}) or {}).get("creators", []) or []
    names = []
    for c in creators:
        ln = c.get("lastName")
        if ln:
            names.append(ln)
    return ", ".join(names)

def _tags(item: Dict[str, Any]) -> List[str]:
    return [t.get("tag", "") for t in ((item.get("data", {}) or {}).get("tags", []) or []) if t.get("tag")]

def _pdf_attachment_keys(item_key: str) -> List[str]:
    r = _get(f"{ZOTERO_BASE}/items/{item_key}/children")
    pdfs: List[str] = []
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

def _fix_mojibake(s: str) -> str:
    """
    Repairs common mojibake where UTF-8 bytes were decoded as latin-1/cp1252.
    Only attempts repair if typical markers are present.
    """
    if not s:
        return s

    markers = ("Ã", "Â", "â€", "�")
    if not any(m in s for m in markers):
        return s

    # Try latin1 -> utf8 repair (most common)
    try:
        repaired = s.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
        return repaired
    except Exception:
        pass

    # Fallback: cp1252 -> utf8 repair
    try:
        repaired = s.encode("cp1252", errors="strict").decode("utf-8", errors="strict")
        return repaired
    except Exception:
        return s

def _normalize_for_match(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _score_biblio_match(
    title: Optional[str],
    creator: Optional[str],
    year: Optional[str],
    item: Dict[str, Any],
) -> Tuple[int, str]:
    data = item.get("data", {}) or {}

    it_title = data.get("title") or ""
    it_creators = _creator_string(item)
    it_year = _year(item)

    t = _normalize_for_match(title)
    c = _normalize_for_match(creator)
    y = _normalize_for_match(year)

    score = 0
    reasons = []

    if t:
        # token-based, but stronger than plain substring:
        t_tokens = [x for x in re.split(r"\s+", t) if x]
        hay = _normalize_for_match(it_title)
        hits = sum(1 for tok in t_tokens if tok in hay)
        if hits > 0:
            score += 4 * hits
            reasons.append("title_match")

    if c:
        hayc = _normalize_for_match(it_creators)
        # allow either full creator string or individual surname(s)
        c_tokens = [x for x in re.split(r"[,\s]+", c) if x]
        chits = sum(1 for tok in c_tokens if tok and tok in hayc)
        if chits > 0:
            score += 3 * chits
            reasons.append("creator_match")

    if y:
        if y == _normalize_for_match(it_year):
            score += 2
            reasons.append("year_match")

    reason = ",".join(reasons) if reasons else "no_match"
    return score, reason

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
# RESOLVE (bibliographic)
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
    Fast bibliographic resolver:
    - Pulls up to max_fetch items from Zotero (server-side paging)
    - Scores by title / creator / year
    - Optionally filters to items that actually have at least one PDF attachment
    """
    title = _to_str(title)
    creator = _to_str(creator)
    year = _to_str(year)
    collection_key = _to_str(collection_key)

    if not any([title, creator, year]):
        raise HTTPException(status_code=400, detail="Provide at least one of: title, creator, year")

    fetched = 0
    start = 0
    chunk = 100
    scored: List[Tuple[int, str, Dict[str, Any]]] = []

    while fetched < max_fetch:
        params = {"limit": chunk, "start": start, "itemType": "-attachment"}
        if collection_key:
            r = _get(f"{ZOTERO_BASE}/collections/{collection_key}/items", params=params)
        else:
            r = _get(f"{ZOTERO_BASE}/items", params=params)

        batch = r.json()
        if not batch:
            break

        for it in batch:
            s, reason = _score_biblio_match(title, creator, year, it)
            if s > 0:
                scored.append((s, reason, it))

        fetched += len(batch)
        start += chunk

    scored.sort(key=lambda x: x[0], reverse=True)

    results: List[Dict[str, Any]] = []
    pdf_checked = 0

    for s, reason, it in scored:
        if len(results) >= limit:
            break

        key = (it.get("data", {}) or {}).get("key")
        if not key:
            continue

        pdfs: List[str] = []
        if require_pdf:
            if pdf_checked >= pdf_check_top_n and results:
                # we already found enough good matches; stop spending time on PDF checks
                break
            pdfs = _pdf_attachment_keys(key)
            pdf_checked += 1
            if not pdfs:
                continue

        results.append(_compact_item(it, bool(pdfs), pdfs, reason, s))

    return {
        "query": {
            "title": title,
            "creator": creator,
            "year": year,
            "collection_key": collection_key,
        },
        "server_fetched": fetched,
        "pdf_checked": pdf_checked,
        "results": results,
    }

# =========================
# PDF HTML
# =========================

@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def pdf_as_html(attachment_key: str):
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open PDF: {e}")

    parts = ["<html><head><meta charset='utf-8'></head><body>"]
    for page in doc:
        txt = page.get_text() or ""
        txt = _fix_mojibake(txt)
        parts.append(f"<p>{escape(txt)}</p>")
    parts.append("</body></html>")

    html = "".join(parts)
    return HTMLResponse(content=html, media_type="text/html; charset=utf-8")

# =========================
# PDF SEARCH
# =========================

@app.get("/attachments/{attachment_key}/search")
def pdf_search(
    attachment_key: str,
    phrase: str,
    max_hits: int = 10,
    snippet_chars: int = 1000,
):
    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    try:
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open PDF: {e}")

    phrase_l = (phrase or "").lower().strip()
    if not phrase_l:
        raise HTTPException(status_code=400, detail="phrase must be a non-empty string")

    hits: List[Dict[str, Any]] = []
    for i, page in enumerate(doc):
        text = page.get_text() or ""
        text_fixed = _fix_mojibake(text)
        if phrase_l in text_fixed.lower():
            hits.append({
                "page": i + 1,
                "snippet": text_fixed[:snippet_chars]
            })
            if len(hits) >= max_hits:
                break

    return {
        "attachment_key": attachment_key,
        "phrase": phrase,
        "hits": hits,
    }
