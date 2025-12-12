import os
import re
import time
import html as _html
from typing import Any, Dict, List, Optional, Tuple

import requests
import fitz  # PyMuPDF
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from dotenv import load_dotenv

load_dotenv()

ZOTERO_API_KEY = os.getenv("ZOTERO_API_KEY")
ZOTERO_USER_ID = os.getenv("ZOTERO_USER_ID")

if not ZOTERO_API_KEY or not ZOTERO_USER_ID:
    raise RuntimeError("Missing ZOTERO_API_KEY or ZOTERO_USER_ID environment variables")

ZOTERO_API = "https://api.zotero.org"
HEADERS = {"Zotero-API-Key": ZOTERO_API_KEY}

APP_VERSION = "2.0.1"

CACHE_MAX_ITEMS = int(os.getenv("CACHE_MAX_ITEMS", "32"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "3600"))

app = FastAPI(title="Zotero FastAPI Proxy", version=APP_VERSION)


# ---------------------------
# Small in-memory cache
# ---------------------------

class _Cache:
    def __init__(self, max_items: int, ttl: int) -> None:
        self.max_items = max_items
        self.ttl = ttl
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._order: List[str] = []

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        if key not in self._store:
            return None
        ts, val = self._store[key]
        if now - ts > self.ttl:
            self.delete(key)
            return None
        if key in self._order:
            self._order.remove(key)
        self._order.append(key)
        return val

    def set(self, key: str, val: Any) -> None:
        now = time.time()
        if key in self._store:
            self._store[key] = (now, val)
            if key in self._order:
                self._order.remove(key)
            self._order.append(key)
            return

        self._store[key] = (now, val)
        self._order.append(key)

        while len(self._order) > self.max_items:
            oldest = self._order.pop(0)
            if oldest in self._store:
                del self._store[oldest]

    def delete(self, key: str) -> None:
        if key in self._store:
            del self._store[key]
        if key in self._order:
            self._order.remove(key)

    def stats(self) -> Dict[str, Any]:
        return {"max_items": self.max_items, "ttl_seconds": self.ttl, "current_items": len(self._store)}


_pdf_text_cache = _Cache(CACHE_MAX_ITEMS, CACHE_TTL_SECONDS)


# ---------------------------
# Helpers
# ---------------------------

def zotero_get(url: str, params: Optional[dict] = None) -> requests.Response:
    return requests.get(url, headers=HEADERS, params=params, timeout=60, allow_redirects=True)


def _is_top_level_item(item: Dict[str, Any]) -> bool:
    data = item.get("data", {}) or {}
    return "parentItem" not in data and data.get("itemType") not in {"attachment", "note", "annotation"}


def _safe_str(x: Any) -> str:
    return (x or "").strip()


def _creator_string(item: Dict[str, Any]) -> str:
    creators = (item.get("data", {}) or {}).get("creators", []) or []
    parts: List[str] = []
    for c in creators:
        name = _safe_str(c.get("name"))
        if not name:
            fn = _safe_str(c.get("firstName"))
            ln = _safe_str(c.get("lastName"))
            name = _safe_str(f"{fn} {ln}")
        if name:
            parts.append(name)
    return ", ".join(parts)


def _year(item: Dict[str, Any]) -> str:
    date = (item.get("data", {}) or {}).get("date", "") or ""
    m = re.search(r"\b(19|20)\d{2}\b", date)
    return m.group(0) if m else ""


def _tags(item: Dict[str, Any]) -> List[str]:
    tags = (item.get("data", {}) or {}).get("tags", []) or []
    out: List[str] = []
    for t in tags:
        tag = t.get("tag")
        if tag:
            out.append(tag)
    return out


def _collections(item: Dict[str, Any]) -> List[str]:
    return (item.get("data", {}) or {}).get("collections", []) or []


def _compact_item(
    item: Dict[str, Any],
    has_pdf: bool = False,
    pdf_keys: Optional[List[str]] = None,
    note_snippet: str = "",
    match_reason: str = "",
    score: Optional[int] = None,
) -> Dict[str, Any]:
    data = item.get("data", {}) or {}
    return {
        "item_key": data.get("key") or item.get("key"),
        "itemType": data.get("itemType"),
        "title": data.get("title"),
        "creators": _creator_string(item),
        "year": _year(item),
        "publicationTitle": data.get("publicationTitle") or data.get("bookTitle") or data.get("encyclopediaTitle"),
        "collections": _collections(item),
        "tags": _tags(item),
        "has_pdf": has_pdf,
        "pdf_attachment_keys": pdf_keys or [],
        "note_snippet": note_snippet,
        "match_reason": match_reason,
        "score": int(score or 0),
    }


def _score_match_free(
    q: str,
    item: Dict[str, Any],
    note_text: str = "",
    prefer_year: Optional[str] = None,
    prefer_creator: Optional[str] = None,
) -> Tuple[int, str]:
    qn = (q or "").strip().lower()
    if not qn:
        return 0, ""

    data = item.get("data", {}) or {}
    title = (data.get("title") or "").lower()
    creators = _creator_string(item).lower()
    pub = (data.get("publicationTitle") or data.get("bookTitle") or data.get("encyclopediaTitle") or "").lower()
    tags = " ".join(_tags(item)).lower()
    year = _year(item).lower()
    notes = (note_text or "").lower()

    score = 0
    reasons: List[str] = []

    def hit(field: str, weight: int, label: str) -> None:
        nonlocal score
        if qn in field:
            score += weight
            reasons.append(label)

    hit(title, 14, "title")
    hit(creators, 9, "creators")
    hit(tags, 7, "tags")
    hit(pub, 5, "publication")
    hit(notes, 4, "notes")
    hit(year, 3, "year")

    tokens = [t for t in re.split(r"\W+", qn) if len(t) >= 3]
    hay = " ".join([title, creators, tags, pub, year, notes])
    token_hits = 0
    for t in tokens:
        if t in hay:
            token_hits += 1
    if token_hits:
        score += min(10, token_hits)
        reasons.append(f"token_hits:{token_hits}")

    if prefer_year and prefer_year.lower() == year:
        score += 4
        reasons.append("preferred_year")

    if prefer_creator and prefer_creator.lower() in creators:
        score += 5
        reasons.append("preferred_creator")

    return score, ",".join(reasons)


def _get_children(item_key: str) -> List[Dict[str, Any]]:
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{item_key}/children"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


def _notes_to_plain_text(note_html: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", note_html or "", flags=re.IGNORECASE)
    t = re.sub(r"</p\s*>", "\n", t, flags=re.IGNORECASE)
    t = re.sub(r"<[^>]+>", "", t)
    t = _html.unescape(t)
    return t.strip()


def _get_notes_for_item(item_key: str) -> List[Dict[str, Any]]:
    children = _get_children(item_key)
    notes: List[Dict[str, Any]] = []
    for c in children:
        data = c.get("data", {}) or {}
        if data.get("itemType") == "note":
            notes.append({"note_key": data.get("key") or c.get("key"), "note_html": data.get("note", "")})
    return notes


def _get_pdf_attachments(item_key: str) -> List[Dict[str, Any]]:
    children = _get_children(item_key)
    pdfs: List[Dict[str, Any]] = []
    for c in children:
        data = c.get("data", {}) or {}
        if data.get("itemType") == "attachment" and data.get("contentType") == "application/pdf":
            pdfs.append(c)
    return pdfs


def _pdf_attachment_keys(item_key: str) -> List[str]:
    pdfs = _get_pdf_attachments(item_key)
    keys: List[str] = []
    for p in pdfs:
        d = p.get("data", {}) or {}
        k = d.get("key") or p.get("key")
        if k:
            keys.append(k)
    return keys


def _choose_primary_pdf(item_key: str) -> Optional[str]:
    pdfs = _get_pdf_attachments(item_key)
    if not pdfs:
        return None

    best_key = None
    best_len = -1
    for p in pdfs:
        links = p.get("links", {}) or {}
        enclosure = links.get("enclosure", {}) or {}
        length = enclosure.get("length")
        try:
            ln = int(length) if length is not None else -1
        except Exception:
            ln = -1

        d = p.get("data", {}) or {}
        filename = (d.get("filename") or "").lower()

        bonus = 0
        if filename and filename != "pdf":
            bonus += 1

        if ln + bonus > best_len:
            best_len = ln + bonus
            best_key = d.get("key") or p.get("key")

    return best_key


def _download_pdf_bytes(attachment_key: str) -> bytes:
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{attachment_key}/file"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=404, detail="PDF not found in Zotero Storage")
    ct = r.headers.get("Content-Type", "") or ""
    if "pdf" not in ct.lower():
        raise HTTPException(status_code=400, detail=f"Attachment is not a PDF (Content-Type: {ct})")
    return r.content


def _pdf_to_text_by_pages(
    attachment_key: str,
    page_from: int,
    page_to: int,
    max_chars: int,
    use_cache: bool,
) -> str:
    cache_key = f"pdftext:{attachment_key}:{page_from}:{page_to}:{max_chars}"
    if use_cache:
        cached = _pdf_text_cache.get(cache_key)
        if cached is not None:
            return cached

    pdf_bytes = _download_pdf_bytes(attachment_key)
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open PDF: {str(e)}")

    n = len(doc)
    if n == 0:
        raise HTTPException(status_code=500, detail="PDF has zero pages")

    pf = max(1, page_from)
    pt = min(n, page_to)
    if pf > pt:
        raise HTTPException(status_code=400, detail="Invalid page range")

    texts: List[str] = []
    for i in range(pf - 1, pt):
        t = doc[i].get_text() or ""
        if t:
            texts.append(t)

    joined = "\n\n".join(texts)
    if len(joined) > max_chars:
        joined = joined[:max_chars] + "\n\n[...TRUNCATED...]"

    if use_cache:
        _pdf_text_cache.set(cache_key, joined)
    return joined


def _pdf_find_phrase(
    attachment_key: str,
    phrase: str,
    max_pages: int,
    max_hits: int,
    context_chars: int,
    use_cache: bool,
) -> Dict[str, Any]:
    phrase_clean = (phrase or "").strip()
    if not phrase_clean:
        return {"phrase": phrase, "hits": []}

    pdf_bytes = _download_pdf_bytes(attachment_key)
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open PDF: {str(e)}")

    total_pages = len(doc)
    pages_to_scan = min(total_pages, max_pages)

    coarse_cache_key = f"pdfcoarse:{attachment_key}:1:{pages_to_scan}"
    coarse_text = _pdf_text_cache.get(coarse_cache_key) if use_cache else None
    if coarse_text is None:
        texts: List[str] = []
        for i in range(pages_to_scan):
            texts.append(doc[i].get_text() or "")
        coarse_text = "\n\n".join(texts)
        if use_cache:
            _pdf_text_cache.set(coarse_cache_key, coarse_text)

    hits: List[Dict[str, Any]] = []
    for m in re.finditer(re.escape(phrase_clean), coarse_text, flags=re.IGNORECASE):
        if len(hits) >= max_hits:
            break
        start = max(0, m.start() - context_chars)
        end = min(len(coarse_text), m.end() + context_chars)
        context = coarse_text[start:end].strip()
        hits.append({"context": context, "page": None})

    if hits:
        enriched = 0
        for pidx in range(pages_to_scan):
            if enriched >= len(hits):
                break
            rects = doc[pidx].search_for(phrase_clean, quads=False)
            if rects:
                for _ in rects:
                    if enriched >= len(hits):
                        break
                    if hits[enriched]["page"] is None:
                        hits[enriched]["page"] = pidx + 1
                        enriched += 1

    return {"phrase": phrase_clean, "pages_scanned": pages_to_scan, "total_pages": total_pages, "hits": hits}


def _parse_year_from_query(q: str) -> Optional[str]:
    m = re.search(r"\b(19|20)\d{2}\b", q or "")
    return m.group(0) if m else None


def _to_str(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    return str(x)


# ---------------------------
# API
# ---------------------------

@app.get("/health")
def health() -> Dict[str, Any]:
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/collections"
    r = zotero_get(url)
    ok = r.status_code == 200
    return {
        "ok": ok,
        "app_version": APP_VERSION,
        "zotero_user_id": ZOTERO_USER_ID,
        "zotero_status_code": r.status_code,
        "cache": _pdf_text_cache.stats(),
    }


@app.get("/library/stats")
def library_stats(max_scan: int = 400) -> Dict[str, Any]:
    max_scan = max(50, min(3000, int(max_scan)))
    chunk = 100
    scanned = 0
    start = 0
    top_level = 0
    with_pdf = 0
    with_notes = 0

    while scanned < max_scan:
        batch = list_items(limit=chunk, start=start, top=True)
        if not batch:
            break
        for it in batch:
            top_level += 1
            key = (it.get("data", {}) or {}).get("key") or it.get("key")
            if key:
                if _pdf_attachment_keys(key):
                    with_pdf += 1
                if _get_notes_for_item(key):
                    with_notes += 1
        scanned += len(batch)
        start += chunk

    return {
        "scanned_top_level_items": top_level,
        "estimated_with_pdf_in_scanned": with_pdf,
        "estimated_with_notes_in_scanned": with_notes,
        "note": "This is a scan-based estimate for quick diagnostics, not a full library census.",
    }


@app.get("/collections")
def list_collections(include_deleted: bool = False) -> List[Dict[str, Any]]:
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/collections"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    cols = r.json()
    if not include_deleted:
        cols = [c for c in cols if not (c.get("data", {}) or {}).get("deleted", False)]
    return cols


@app.get("/collections/search")
def search_collections(q: str, include_deleted: bool = False) -> Dict[str, Any]:
    cols = list_collections(include_deleted=include_deleted)
    qn = (q or "").lower().strip()
    hits = []
    for c in cols:
        name = ((c.get("data", {}) or {}).get("name") or "").lower()
        if qn and qn in name:
            hits.append({"collection_key": (c.get("data", {}) or {}).get("key") or c.get("key"), "name": (c.get("data", {}) or {}).get("name")})
    return {"query": q, "results": hits}


@app.get("/collections/tree")
def collections_tree(include_deleted: bool = False) -> Dict[str, Any]:
    cols = list_collections(include_deleted=include_deleted)
    nodes: Dict[str, Dict[str, Any]] = {}
    roots: List[Dict[str, Any]] = []

    for c in cols:
        data = c.get("data", {}) or {}
        key = data.get("key") or c.get("key")
        nodes[key] = {"collection_key": key, "name": data.get("name"), "children": [], "parent": data.get("parentCollection")}

    for key, node in nodes.items():
        parent = node.get("parent")
        if parent and parent in nodes:
            nodes[parent]["children"].append(node)
        else:
            roots.append(node)

    return {"roots": roots}


@app.get("/collections/{collection_key}/items")
def list_items_in_collection(
    collection_key: str,
    limit: int = 50,
    start: int = 0,
    sort: str = "dateModified",
    direction: str = "desc",
    top: bool = True,
) -> List[Dict[str, Any]]:
    limit = max(1, min(100, int(limit)))
    start = max(0, int(start))
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/collections/{collection_key}/items"
    params = {"limit": limit, "start": start, "sort": sort, "direction": direction}
    r = zotero_get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    items = r.json()
    if top:
        items = [it for it in items if _is_top_level_item(it)]
    return items


@app.get("/items")
def list_items(
    limit: int = 50,
    start: int = 0,
    sort: str = "dateModified",
    direction: str = "desc",
    top: bool = True,
) -> List[Dict[str, Any]]:
    limit = max(1, min(100, int(limit)))
    start = max(0, int(start))
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items"
    params = {"limit": limit, "start": start, "sort": sort, "direction": direction}
    r = zotero_get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    items = r.json()
    if top:
        items = [it for it in items if _is_top_level_item(it)]
    return items


@app.get("/items/{item_key}")
def get_item_details(item_key: str) -> Dict[str, Any]:
    url = f"{ZOTERO_API}/users/{ZOTERO_USER_ID}/items/{item_key}"
    r = zotero_get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


@app.get("/items/{item_key}/children")
def get_item_children(item_key: str) -> List[Dict[str, Any]]:
    return _get_children(item_key)


@app.get("/items/{item_key}/notes")
def get_item_notes(item_key: str) -> Dict[str, Any]:
    return {"item_key": item_key, "notes": _get_notes_for_item(item_key)}


@app.get("/items/{item_key}/attachments")
def get_item_attachments(item_key: str) -> Dict[str, Any]:
    pdfs = _pdf_attachment_keys(item_key)
    children = _get_children(item_key)
    return {"item_key": item_key, "pdf_attachment_keys": pdfs, "children": children}


@app.get("/items/{item_key}/primary-pdf")
def get_primary_pdf(item_key: str) -> Dict[str, Any]:
    primary = _choose_primary_pdf(item_key)
    keys = _pdf_attachment_keys(item_key)
    return {"item_key": item_key, "primary_pdf_attachment_key": primary, "all_pdf_attachment_keys": keys}


@app.post("/items/batch")
def batch_items(payload: Dict[str, Any]) -> Dict[str, Any]:
    item_keys = payload.get("item_keys") or []
    if not isinstance(item_keys, list):
        raise HTTPException(status_code=400, detail="item_keys must be a list of strings")
    results = []
    for k in item_keys[:50]:
        try:
            it = get_item_details(str(k))
            pdfs = _pdf_attachment_keys(str(k))
            notes = _get_notes_for_item(str(k))
            note_plain = ""
            if notes:
                note_plain = _notes_to_plain_text(notes[0].get("note_html", ""))[:400]
            results.append(_compact_item(it, has_pdf=bool(pdfs), pdf_keys=pdfs, note_snippet=note_plain))
        except Exception:
            results.append({"item_key": str(k), "error": "Failed to fetch item"})
    return {"count": len(results), "results": results}


@app.get("/notes/search")
def search_notes(q: str, limit: int = 20, max_scan: int = 800) -> Dict[str, Any]:
    limit = max(1, min(50, int(limit)))
    max_scan = max(100, min(5000, int(max_scan)))

    chunk = 100
    scanned = 0
    start = 0
    hits = []
    qn = (q or "").lower().strip()

    while scanned < max_scan and len(hits) < limit:
        batch = list_items(limit=chunk, start=start, top=True)
        if not batch:
            break
        for it in batch:
            key = (it.get("data", {}) or {}).get("key") or it.get("key")
            if not key:
                continue
            notes = _get_notes_for_item(key)
            for n in notes:
                plain = _notes_to_plain_text(n.get("note_html", ""))
                if qn and qn in plain.lower():
                    idx = plain.lower().find(qn)
                    snippet_start = max(0, idx - 120)
                    snippet_end = min(len(plain), snippet_start + 320)
                    snippet = plain[snippet_start:snippet_end].strip()
                    hits.append(
                        {
                            "item_key": key,
                            "note_key": n.get("note_key"),
                            "title": (it.get("data", {}) or {}).get("title"),
                            "creators": _creator_string(it),
                            "snippet": snippet,
                        }
                    )
                    if len(hits) >= limit:
                        break
            if len(hits) >= limit:
                break
        scanned += len(batch)
        start += chunk

    return {"query": q, "scanned_top_level_items": scanned, "results": hits}


@app.get("/search")
def faceted_search(
    q: Optional[str] = None,
    title: Optional[str] = None,
    creator: Optional[str] = None,
    tag: Optional[str] = None,
    year: Optional[str] = None,
    collection_key: Optional[str] = None,
    itemType: Optional[str] = None,
    has_pdf: bool = True,
    has_notes: bool = False,
    limit: int = 20,
    max_scan: int = 1600,
) -> Dict[str, Any]:
    limit = max(1, min(50, int(limit)))
    max_scan = max(100, min(10000, int(max_scan)))

    q = _to_str(q)
    title = _to_str(title)
    creator = _to_str(creator)
    tag = _to_str(tag)
    year = _to_str(year)
    itemType = _to_str(itemType)
    collection_key = _to_str(collection_key)

    free = " ".join([x for x in [q, title, creator, tag, year] if isinstance(x, str) and x.strip()])
    prefer_year = year or _parse_year_from_query(free)
    prefer_creator = creator

    chunk = 100
    scanned = 0
    start = 0

    candidates: List[Tuple[int, str, Dict[str, Any], str, List[str]]] = []

    while scanned < max_scan:
        if collection_key:
            batch = list_items_in_collection(collection_key, limit=chunk, start=start, top=True)
        else:
            batch = list_items(limit=chunk, start=start, top=True)

        if not batch:
            break

        for it in batch:
            data = it.get("data", {}) or {}

            if itemType and (data.get("itemType") or "").lower() != itemType.lower():
                continue

            if tag:
                tags = [t.lower() for t in _tags(it)]
                if tag.lower() not in tags:
                    continue

            if title and title.lower() not in (data.get("title") or "").lower():
                continue

            if creator and creator.lower() not in _creator_string(it).lower():
                continue

            if year and year != _year(it):
                continue

            key = data.get("key") or it.get("key")
            if not key:
                continue

            note_plain = ""
            if has_notes:
                notes = _get_notes_for_item(key)
                if notes:
                    note_plain = _notes_to_plain_text(notes[0].get("note_html", ""))

            pdf_keys: List[str] = []
            if has_pdf:
                pdf_keys = _pdf_attachment_keys(key)
                if not pdf_keys:
                    continue

            if free:
                score, reason = _score_match_free(free, it, note_text=note_plain, prefer_year=prefer_year, prefer_creator=prefer_creator)
                if score <= 0:
                    continue
                candidates.append((score, reason, it, note_plain[:400], pdf_keys))
            else:
                candidates.append((1, "filters_only", it, note_plain[:400], pdf_keys))

        scanned += len(batch)
        start += chunk

    candidates.sort(key=lambda x: (x[0], (x[2].get("data", {}) or {}).get("title", "")), reverse=True)

    results = []
    for score, reason, it, note_snip, pdf_keys in candidates[: limit * 4]:
        results.append(_compact_item(it, has_pdf=bool(pdf_keys), pdf_keys=pdf_keys, note_snippet=note_snip, match_reason=reason, score=score))
        if len(results) >= limit:
            break

    return {"query": free.strip(), "scanned_top_level_items": scanned, "results": results}


@app.get("/resolve")
def resolve(
    query: str,
    limit: int = 5,
    collection_key: Optional[str] = None,
    has_pdf: bool = True,
    max_scan: int = 2000,
) -> Dict[str, Any]:
    limit = max(1, min(10, int(limit)))
    max_scan = max(100, min(10000, int(max_scan)))

    prefer_year = _parse_year_from_query(query)

    res = faceted_search(
        q=query,
        collection_key=collection_key,
        has_pdf=has_pdf,
        has_notes=False,
        limit=max(limit, 5),
        max_scan=max_scan,
    )

    candidates = (res.get("results", []) or [])[:limit]
    best_score = int(candidates[0].get("score", 0)) if candidates else 0

    def band(s: int) -> str:
        if s >= 20:
            return "high"
        if s >= 12:
            return "medium"
        if s >= 6:
            return "low"
        return "very_low"

    for c in candidates:
        c["confidence"] = band(int(c.get("score", 0) or 0))
        c["preferred_year_hint"] = prefer_year or ""

    return {"query": query, "best_score": best_score, "candidates": candidates}


# ---------------------------
# PDF endpoints
# ---------------------------

@app.get("/attachments/{attachment_key}/text", response_class=PlainTextResponse)
def get_pdf_text_plain(
    attachment_key: str,
    page_from: int = 1,
    page_to: int = 50,
    max_chars: int = 400000,
    use_cache: bool = True,
) -> PlainTextResponse:
    txt = _pdf_to_text_by_pages(attachment_key, int(page_from), int(page_to), int(max_chars), bool(use_cache))
    return PlainTextResponse(txt)


@app.get("/attachments/{attachment_key}/html", response_class=HTMLResponse)
def get_pdf_text_as_html(
    attachment_key: str,
    page_from: int = 1,
    page_to: int = 50,
    max_chars: int = 400000,
    use_cache: bool = True,
) -> HTMLResponse:
    txt = _pdf_to_text_by_pages(attachment_key, int(page_from), int(page_to), int(max_chars), bool(use_cache))
    safe = _html.escape(txt).replace("\n", "<br/>")
    page = (
        "<html><body>"
        "<h2>Extracted PDF Text</h2>"
        "<div style='white-space:normal;font-family:system-ui'>"
        f"{safe}"
        "</div></body></html>"
    )
    return HTMLResponse(content=page)


@app.get("/attachments/{attachment_key}/extract", response_class=PlainTextResponse)
def extract_pdf_range(
    attachment_key: str,
    page_from: int,
    page_to: int,
    max_chars: int = 250000,
    use_cache: bool = True,
) -> PlainTextResponse:
    txt = _pdf_to_text_by_pages(attachment_key, int(page_from), int(page_to), int(max_chars), bool(use_cache))
    return PlainTextResponse(txt)


@app.get("/attachments/{attachment_key}/search")
def search_in_pdf(
    attachment_key: str,
    phrase: str,
    max_pages: int = 120,
    max_hits: int = 20,
    context_chars: int = 160,
    use_cache: bool = True,
) -> Dict[str, Any]:
    return _pdf_find_phrase(attachment_key, phrase, int(max_pages), int(max_hits), int(context_chars), bool(use_cache))
