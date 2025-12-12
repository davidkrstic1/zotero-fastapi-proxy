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
    r = requests.get(url, headers=HEADERS, params=params, timeout=45)
    r.raise_for_status()
    return r

def _to_str(x) -> Optional[str]:
    return x.strip() if isinstance(x, str) and x.strip() else None

def _year(item: Dict[str, Any]) -> str:
    d = (item.get("data", {}) or {}).get("date", "") or ""
    m = re.search(r"\b(19|20)\d{2}\b", d)
    return m.group(0) if m else ""

def _creator_string(item: Dict[str, Any]) -> str:
    creators = (item.get("data", {}) or {}).get("creators", []) or []
    names = []
    for c in creators:
        ln = c.get("lastName")
        if isinstance(ln, str) and ln.strip():
            names.append(ln.strip())
    return ", ".join(names)

def _tags(item: Dict[str, Any]) -> List[str]:
    tags = (item.get("data", {}) or {}).get("tags", []) or []
    out = []
    for t in tags:
        tag = t.get("tag")
        if isinstance(tag, str) and tag.strip():
            out.append(tag.strip())
    return out

def _parse_year_from_query(q: str) -> str:
    if not q:
        return ""
    m = re.search(r"\b(19|20)\d{2}\b", q)
    return m.group(0) if m else ""

def _score_match_biblio(
    title: Optional[str],
    creator: Optional[str],
    year: Optional[str],
    item: Dict[str, Any],
) -> Tuple[int, str]:
    data = item.get("data", {}) or {}
    it_title = (data.get("title") or "").lower()
    it_creators = _creator_string(item).lower()
    it_year = _year(item)

    score = 0
    reasons = []

    if title:
        t = title.lower()
        if t in it_title:
            score += 8
            reasons.append("title_match")
        else:
            title_tokens = [x for x in re.split(r"\s+", t) if x]
            hits = sum(1 for tok in title_tokens if tok in it_title)
            if hits:
                score += min(7, hits)
                reasons.append(f"title_token_hits:{hits}")

    if creator:
        c = creator.lower()
        if c in it_creators:
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

def _zotero_server_search_items(
    q: str,
    collection_key: Optional[str],
    limit: int,
    max_fetch: int,
) -> Tuple[List[Dict[str, Any]], int]:
    q = _to_str(q) or ""
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
# Conservative mojibake repair (optional)
# =========================

_MOJIBAKE_MARKERS = ("Ã", "Â", "â€", "â€“", "â€œ", "â€ž", "â€™")
_REPLACEMENT_CHAR = "\ufffd"  # "�"

def _mojibake_score(s: str) -> int:
    # Lower is better
    if not s:
        return 0
    return sum(s.count(m) for m in _MOJIBAKE_MARKERS) + 5 * s.count(_REPLACEMENT_CHAR)

def _try_repair(s: str, enc: str) -> Optional[str]:
    try:
        b = s.encode(enc, errors="strict")
        return b.decode("utf-8", errors="strict")
    except Exception:
        return None

def _maybe_fix_text(s: str) -> str:
    # Only attempt repair if it "looks like" mojibake
    if not s or not any(m in s for m in _MOJIBAKE_MARKERS):
        return s

    base = s
    base_score = _mojibake_score(base)

    # Two common reverse paths: cp1252 and latin1
    cand1 = _try_repair(base, "cp1252")
    cand2 = _try_repair(base, "latin1")

    best = base
    best_score = base_score

    for cand in [cand1, cand2]:
        if not cand:
            continue
        sc = _mojibake_score(cand)

        # Accept only if clearly better (strictly lower score)
        if sc < best_score:
            best = cand
            best_score = sc

    return best

# =========================
# Very small in-memory cache for resolve-biblio
# =========================

_RESOLVE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_RESOLVE_TTL_SECONDS = 15 * 60  # 15 minutes

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
    return {"ok": True, "app_version": APP_VERSION, "zotero_status": r.status_code}

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
        "query": {"title": title, "creator": creator, "year": year, "collection_key": collection_key},
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
def pdf_as_html(attachment_key: str, fix_encoding: bool = False):
    attachment_key = _to_str(attachment_key)
    if not attachment_key:
        raise HTTPException(status_code=400, detail="attachment_key required")

    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    parts = ["<html><head><meta charset='utf-8'></head><body>"]
    for page in doc:
        txt = page.get_text() or ""
        if fix_encoding:
            txt = _maybe_fix_text(txt)
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
    fix_encoding: bool = False,
    snippet_len: int = 1200,
    max_hits: int = 10,
):
    attachment_key = _to_str(attachment_key)
    phrase = _to_str(phrase)

    if not attachment_key:
        raise HTTPException(status_code=400, detail="attachment_key required")
    if not phrase:
        raise HTTPException(status_code=400, detail="phrase must be a non-empty string")

    r = _get(f"{ZOTERO_BASE}/items/{attachment_key}/file")
    doc = fitz.open(stream=r.content, filetype="pdf")

    needle = phrase.lower()
    hits = []

    for i, page in enumerate(doc):
        text = page.get_text() or ""
        if fix_encoding:
            text = _maybe_fix_text(text)

        if needle in text.lower():
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
