from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from typing import List, Dict, Any, Optional, Tuple
import os
import requests
import re
import time
import fitz  # PyMuPDF
from html import escape

# =========================
# App
# =========================

APP_VERSION = "2.3.6"

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
    r = requests.get(url, headers=HEADERS, params=params, timeout=45)
    r.raise_for_status()
    return r

def _to_str(x) -> Optional[str]:
    return x.strip() if isinstance(x, str) and x.strip() else None

_MOJIBAKE_MARKERS = ["Ã", "Â", "â€", "â€™", "â€œ", "â€�", "â€“", "â€”", "ï¿½", "�"]

def _mojibake_score(s: str) -> int:
    if not s:
        return 0
    return sum(s.count(m) for m in _MOJIBAKE_MARKERS)

def _try_recode(s: str, src: str, dst: str) -> str:
    """
    Recode with tolerant error handling so one single non-encodable char does not
    prevent fixing the rest of the string.
    """
    try:
        b = s.encode(src, errors="ignore")
        return b.decode(dst, errors="ignore")
    except Exception:
        return s

def _clean_text(s: Optional[str]) -> str:
    if not isinstance(s, str) or not s:
        return ""

    original = s

    # Candidate 1: latin-1 -> utf-8
    c1 = _try_recode(original, "latin-1", "utf-8")

    # Candidate 2: cp1252 -> utf-8 (handles €-based mojibake sequences like â€ž)
    c2 = _try_recode(original, "cp1252", "utf-8")

    # Pick the best candidate by mojibake score (lower is better).
    best = original
    best_score = _mojibake_score(best)

    for cand in (c1, c2):
        sc = _mojibake_score(cand)
        if sc < best_score:
            best = cand
            best_score = sc

    out = best

    # Targeted replacements for residual sequences that sometimes survive recode
    out = out.replace("â€ž", "„").replace("â€œ", "“").replace("â€�", "”")
    out = out.replace("â€™", "’").replace("â€˜", "‘")
    out = out.replace("â€“", "–").replace("â€”", "—")
    out = out.replace("â€¦", "…")

    out = out.replace("Â©", "©").replace("Â§", "§").replace("Â°", "°").replace("Â·", "·")
    out = out.replace("\u00a0", " ")
    out = out.replace("Â ", " ")

    return out

def _year(item: Dict[str, Any]) -> str:
    d = (item.get("data", {}) or {}).get("date", "") or ""
    d = _clean_text(d)
    m = re.search(r"\b(19|20)\d{2}\b", d)
    return m.group(0) if m else ""

def _creator_string(item: Dict[str, Any]) -> str:
    creators = (item.get("data", {}) or {}).get("creators", []) or []
    names = []
    for c in creators:
        ln = c.get("lastName")
        if isinstance(ln, str) and ln.strip():
            names.append(_clean_text(ln.strip()))
    return ", ".join(names)

def _tags(item: Dict[str, Any]) -> List[str]:
    tags = (item.get("data", {}) or {}).get("tags", []) or []
    out = []
    for t in tags:
        tag = t.get("tag")
        if isinstance(tag, str) and tag.strip():
            out.append(_clean_text(tag.strip()))
    return out

def _score_match_biblio(
    title: Optional[str],
    creator: Optional[str],
    year: Optional[str],
    item: Dict[str, Any],
) -> Tuple[int, str]:
    data = item.get("data", {}) or {}
    it_title = _clean_text(data.get("title") or "").lower()
    it_creators = _clean_text(_creator_string(item)).lower()
    it_year = _year(item)

    score = 0
    reasons = []

    if title:
        t = _clean_text(title).lower()
        if t and t in it_title:
            score += 8
            reasons.append("title_match")
        else:
            title_tokens = [x for x in re.split(r"\s+", t) if x]
            hits = sum(1 for tok in title_tokens if tok in it_title)
            if hits:
                score += min(7, hits)
                reasons.append(f"title_token_hits:{hits}")

    if creator:
        c = _clean_text(creator).lower()
        if c and c in it_creators:
            score += 6
            reasons.append("creator_match")
        else:
            creator_tokens = [x for x in re.split(r"\s+", c) if x]
            hits = sum(1 for tok in creator_tokens if tok in it_creators)
            if hits:
                score += min(5, hits)
                reasons.append(f"creator_token_hits:{hits}")

    if year and year == it_year:
        score += 5
        reasons.append("year_match")

    if not reasons:
        reasons.append("no_strong_signal")

    return score, ",".join(reasons)

def _pdf_attachment_keys(item_key: str) -> List[str]:
    r = _get(f"{ZOTERO_BASE}/items/{item_key}/children")
    pdfs = []
    for c in r.json():
        d = c.get("data", {}) or {}
        if d.get("itemType") == "attachment" and d.get("contentType") == "application/pdf":
            k = d.get("key")
            if isinstance(k, str) and k:
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
        "title": _clean_text(data.get("title")),
        "creators": _clean_text(_creator_string(item)),
        "year": _year(item),
        "publicationTitle": _clean_text(data.get("publicationTitle")),
        "collections": data.get("collections", []),
        "tags": _tags(item),
        "has_pdf": has_pdf,
        "pdf_attachment_keys": pdf_keys,
        "score": score,
        "match_reason": match_reason,
    }

def _zotero_server_search_items(
    q: str,
    collection_key: Optional[str],
    limit: int,
    max_fetch: int,
) -> Tuple[List[Dict[str, Any]], int]:
    q = _to_str(q) or ""
    q = _clean_text(q)

    fetched = 0
    start = 0
    chunk = min(100, max_fetch)

    items: List[Dict[str, Any]] = []

    while fetched < max_fetch and len(items) < limit:
        params = {
            "q": q,
            "qmode": "everything",
            "limit": chunk,
            "start": start,
            "itemType": "-attachment",
        }
        if collection_key:
            r = _get(f"{ZOTERO_BASE}/collections/{collection_key}/items", params=params)
        else:
            r = _get(f"{ZOTERO_BASE}/items", params=params)

        batch = r.json()
        if not batch:
            break

        items.extend(batch)
        fetched += len(batch)
        start += chunk

        if len(batch) < chunk:
            break

    return items[:limit], fetched

# =========================
# Very small in-memory cache for resolve-biblio
# =========================

_RESOLVE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_RESOLVE_TTL_SECONDS = 15 * 60

def _cache_key(title, creator, year, collection_key, limit, max_fetch, require_pdf) -> str:
    return "|".join([
        title or "",
        creator or "",
        year or "",
        collection_key or "",
        str(limit),
        str(max_fetch),
        "pdf1" if require_pdf else "pdf0",
    ])

def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    entry = _RESOLVE_CACHE.get(key)
    if not entry:
        return None
    ts, payload = entry
    if now - ts > _RESOLVE_TTL_SECONDS:
        _RESOLVE_CACHE.pop(key, None)
        return None
    return payload

def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    _RESOLVE_CACHE[key] = (time.time(), payload)

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

@app.get("/collections")
def list_collections():
    return _get(f"{ZOTERO_BASE}/collections").json()

@app.get("/items")
def list_items(limit: int = 100, start: int = 0):
    r = _get(f"{ZOTERO_BASE}/items", params={"limit": limit, "start": start, "itemType": "-attachment"})
    return r.json()

# =========================
# Resolve (bibliographic, fast)
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
    collection_key = _to_str(collection_key)

    if year and not re.fullmatch(r"(19|20)\d{2}", year):
        raise HTTPException(status_code=400, detail="year must be a 4-digit year like 2023")

    cache_k = _cache_key(title, creator, year, collection_key, limit, max_fetch, require_pdf)
    cached = _cache_get(cache_k)
    if cached:
        return cached

    query_parts = [x for x in [title, creator, year] if x]
    query = " ".join(query_parts) if query_parts else ""

    candidates, fetched = _zotero_server_search_items(
        q=query,
        collection_key=collection_key,
        limit=max_fetch,
        max_fetch=max_fetch,
    )

    scored: List[Tuple[int, str, Dict[str, Any]]] = []
    for it in candidates:
        s, reason = _score_match_biblio(title, creator, year, it)
        if s > 0:
            scored.append((s, reason, it))

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    pdf_checked = 0

    for s, reason, it in scored:
        key = (it.get("data", {}) or {}).get("key")
        if not isinstance(key, str) or not key:
            continue

        pdfs: List[str] = []
        if require_pdf:
            if pdf_checked >= pdf_check_top_n:
                break
            pdfs = _pdf_attachment_keys(key)
            pdf_checked += 1
            if not pdfs:
                continue

        results.append(_compact_item(it, bool(pdfs), pdfs, reason, s))
        if len(results) >= limit:
            break

    payload = {
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

    _cache_set(cache_k, payload)
    return payload

# =========================
# PDF HTML
# =========================

@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def pdf_as_html(attachment_key: str):
    attachment_key = _to_str(attachment_key)
    if not attachment_key:
        raise HTTPException(status_code=400, detail="attachment_key required")

    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    parts = ["<html><head><meta charset='utf-8'></head><body>"]
    for page in doc:
        page_text = _clean_text(page.get_text())
        parts.append(f"<p>{escape(page_text)}</p>")
    parts.append("</body></html>")

    html = "".join(parts)
    return HTMLResponse(content=html, media_type="text/html; charset=utf-8")

# =========================
# PDF SEARCH
# =========================

@app.get("/attachments/{attachment_key}/search")
def pdf_search(attachment_key: str, phrase: str):
    attachment_key = _to_str(attachment_key)
    phrase = _to_str(phrase)

    if not attachment_key:
        raise HTTPException(status_code=400, detail="attachment_key required")
    if not phrase:
        raise HTTPException(status_code=400, detail="phrase required")

    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    hits = []
    needle = phrase.lower()

    for i, page in enumerate(doc):
        text = _clean_text(page.get_text())
        if needle in text.lower():
            hits.append({"page": i + 1, "snippet": text[:1000]})
        if len(hits) >= 10:
            break

    return {"attachment_key": attachment_key, "phrase": phrase, "hits": hits}
