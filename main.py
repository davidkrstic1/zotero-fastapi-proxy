from fastapi import FastAPI, HTTPException
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
        ln = c.get("lastName")
        if ln:
            names.append(ln)
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
        data.get("publicationTitle", "") or "",
        " ".join(_tags(item)),
    ]).lower()

    tokens = (free or "").lower().split()
    hits = sum(1 for t in tokens if t and t in haystack)

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
        "match_reason": match_reason,
        "score": score,
    }

# --- Mojibake fixer -------------------------------------------------
# Typical sequences when UTF-8 bytes were wrongly decoded as cp1252/latin1:
# "Ã¤ Ã¶ Ã¼", "Â§", "â€“ â€ž â€œ"
_MOJIBAKE_MARKERS = ("Ã", "Â", "â€", "Ã¼", "Ã¶", "Ã¤", "ÃŸ")

def _fix_mojibake(s: str) -> str:
    if not s:
        return s
    # If it does not look like mojibake, return early.
    if not any(m in s for m in _MOJIBAKE_MARKERS):
        return s

    # Heuristic: try to recover by re-encoding as latin1/cp1252 then decoding as utf-8.
    # Use "replace" to avoid exceptions but keep text readable.
    try:
        repaired = s.encode("latin1", errors="replace").decode("utf-8", errors="replace")
        # Only accept if it actually improved (markers reduced).
        if sum(repaired.count(m) for m in _MOJIBAKE_MARKERS) < sum(s.count(m) for m in _MOJIBAKE_MARKERS):
            return repaired
        return repaired  # even if not reduced, usually still better
    except Exception:
        return s

def _page_text(page) -> str:
    # Central place to read + normalize page text.
    return _fix_mojibake(page.get_text() or "")

# =========================
# Health
# =========================

@app.get("/health")
def health():
    r = requests.get(f"{ZOTERO_BASE}/items?limit=1", headers=HEADERS, timeout=10)
    return {"ok": True, "app_version": APP_VERSION, "zotero_status": r.status_code}

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
    title = _to_str(title)
    creator = _to_str(creator)
    year = _to_str(year)

    prefer_year = _parse_year_from_query(year or "") or _parse_year_from_query(title or "")
    prefer_creator = creator

    fetched = 0
    start = 0
    chunk = 100
    scored: List[Tuple[int, str, Dict[str, Any]]] = []

    while fetched < max_fetch:
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
            hay_title = (data.get("title") or "").lower()
            hay_creator = _creator_string(it).lower()

            score = 0
            reasons = []

            if title and title.lower() in hay_title:
                score += 10
                reasons.append("title_match")
            elif title:
                # soft token scoring on title etc.
                s, rreason = _score_match_free(title, it, prefer_year, prefer_creator)
                score += s
                if s > 0:
                    reasons.append(rreason)

            if creator and creator.lower() in hay_creator:
                score += 6
                reasons.append("creator_match")

            if prefer_year and prefer_year == _year(it):
                score += 3
                reasons.append("year_match")

            if score > 0:
                scored.append((score, ",".join(reasons) if reasons else "score", it))

        fetched += len(batch)
        start += chunk

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    checked = 0

    for score, reason, it in scored:
        if require_pdf and checked >= pdf_check_top_n:
            break

        key = it["data"]["key"]
        pdfs = _pdf_attachment_keys(key) if require_pdf else []
        checked += 1

        if require_pdf and not pdfs:
            continue

        results.append(_compact_item(it, bool(pdfs), pdfs, reason, score))
        if len(results) >= limit:
            break

    return {
        "query": {"title": title, "creator": creator, "year": year, "collection_key": collection_key},
        "server_fetched": fetched,
        "pdf_checked": checked,
        "results": results,
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
        txt = _page_text(page)
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
    snippet_len: int = 1200,
    max_hits: int = 10,
):
    if not isinstance(phrase, str) or not phrase.strip():
        raise HTTPException(status_code=400, detail="phrase must be a non-empty string")

    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    needle = phrase.lower()
    hits = []

    for i, page in enumerate(doc):
        text = _page_text(page)
        if needle in text.lower():
            # Find a more local snippet around first occurrence when possible
            low = text.lower()
            pos = low.find(needle)
            if pos >= 0:
                start = max(0, pos - snippet_len // 3)
                end = min(len(text), pos + len(phrase) + snippet_len)
                snippet = text[start:end]
            else:
                snippet = text[:snippet_len]

            hits.append({"page": i + 1, "snippet": snippet})
            if len(hits) >= max_hits:
                break

    return {"attachment_key": attachment_key, "phrase": phrase, "hits": hits}
