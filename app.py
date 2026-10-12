import asyncio
import os
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

app = FastAPI(title="FB Page Cleaner")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

SESSIONS: Dict[str, Dict] = {}
LOAD_JOBS: Dict[str, Dict[str, Any]] = {}
DELETE_JOBS: Dict[str, Dict[str, Any]] = {}

GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v26.0")
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"
FB_WEB = "https://www.facebook.com"

POST_FIELDS = (
    "id,message,created_time,permalink_url,status_type,full_picture,"
    "attachments{media_type,type,target{id},subattachments{target{id},media_type}}"
)
TRANSIENT_CODES = {2, 4, 17, 32, 341, 613, 80001, 80004}
WINDOW_SECONDS = 90 * 86400
SPLIT_THRESHOLD = 400          # jika 1 jendela >= ini, dipecah dua
MAX_EMPTY_WINDOWS = 16         # 16 x 90 hari ≈ 4 tahun kosong berturut-turut -> berhenti
MIN_TS = 1075593600            # 2004-02-01


class FBError(Exception):
    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        self.code = code


class DeleteRequest(BaseModel):
    session_id: str
    post_ids: List[str]
    post_pages: Dict[str, str] = {}
    page_tokens: Dict[str, str] = {}
    dry_run: bool = True
    delay_ms: int = 300
    concurrency: int = 3


class LoadRequest(BaseModel):
    session_id: str
    page_ids: List[str]


# ------------------------------------------------------------------ helpers
async def fb_get(client: httpx.AsyncClient, url: str, params: Optional[dict] = None) -> dict:
    last_msg = "Unknown error"
    last_code = None
    for attempt in range(6):
        try:
            r = await client.get(url, params=params)
            data = r.json()
        except Exception as exc:
            last_msg = str(exc) or exc.__class__.__name__
            await asyncio.sleep(1.5 * (attempt + 1))
            continue
        if isinstance(data, dict) and "error" in data:
            err = data["error"] or {}
            last_msg = err.get("message", "API error")
            last_code = err.get("code")
            if last_code in TRANSIENT_CODES or err.get("is_transient"):
                await asyncio.sleep(min(60, 3 * (attempt + 1) ** 2))
                continue
            raise FBError(last_msg, last_code)
        if not isinstance(data, dict):
            raise FBError("Response bukan JSON object")
        return data
    raise FBError(last_msg, last_code)


def set_limit(url: str, limit: int) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query))
    q["limit"] = str(limit)
    return urlunparse(p._replace(query=urlencode(q)))


async def fetch_all(client: httpx.AsyncClient, url: str, params: dict, page_size: int = 100) -> List[dict]:
    """Ambil semua halaman dari satu edge sampai habis."""
    items: List[dict] = []
    cur_url: Optional[str] = url
    cur_params: Optional[dict] = dict(params, limit=page_size)
    seen_urls: Set[str] = set()
    reduced = False

    while cur_url:
        if cur_params is None:
            if cur_url in seen_urls:
                break
            seen_urls.add(cur_url)
        try:
            data = await fb_get(client, cur_url, cur_params)
        except FBError as e:
            if (e.code == 1 or "reduce the amount" in e.message.lower()) and page_size > 5:
                page_size = max(5, page_size // 2)
                reduced = True
                if cur_params is not None:
                    cur_params["limit"] = page_size
                else:
                    cur_url = set_limit(cur_url, page_size)
                continue
            raise
        items.extend(x for x in data.get("data", []) if isinstance(x, dict))
        nxt = (data.get("paging") or {}).get("next")
        if nxt and reduced:
            nxt = set_limit(nxt, page_size)
        cur_url, cur_params = nxt, None
    return items


async def fetch_window(client, url, token, since: int, until: int) -> List[dict]:
    items = await fetch_all(client, url, {
        "fields": POST_FIELDS, "access_token": token, "since": since, "until": until,
    })
    if len(items) >= SPLIT_THRESHOLD and (until - since) > 3600:
        mid = (since + until) // 2
        a = await fetch_window(client, url, token, since, mid)
        b = await fetch_window(client, url, token, mid, until)
        return a + b
    return items


async def get_story_ids(client: httpx.AsyncClient, page_id: str, token: str) -> Set[str]:
    ids: Set[str] = set()
    for fields in ("post_id,media_id", "post_id"):
        try:
            items = await fetch_all(client, f"{BASE_URL}/{page_id}/stories",
                                    {"fields": fields, "access_token": token})
        except FBError:
            continue
        for it in items:
            for k in ("post_id", "media_id", "id"):
                v = it.get(k)
                if v:
                    ids.add(str(v))
                    ids.add(str(v).split("_")[-1])
        break
    return ids


def collect_targets(p: dict) -> Set[str]:
    out: Set[str] = set()
    for a in (p.get("attachments") or {}).get("data", []) or []:
        t = (a.get("target") or {}).get("id")
        if t:
            out.add(str(t))
        for s in (a.get("subattachments") or {}).get("data", []) or []:
            t2 = (s.get("target") or {}).get("id")
            if t2:
                out.add(str(t2))
    return out


def abs_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    return u if u.startswith("http") else FB_WEB + u


async def load_page_posts(client, page: dict, job: dict) -> List[dict]:
    page_id, name, token = str(page["id"]), page.get("name", ""), page["access_token"]

    def log(msg: str):
        job["log"].append(f"[{name}] {msg}")

    story_ids = await get_story_ids(client, page_id, token)
    log(f"Story aktif dikecualikan: {len(story_ids) // 2 if story_ids else 0}")

    collected: Dict[str, dict] = {}
    targets: Set[str] = set()
    skipped_story = 0

    def add_posts(raw: List[dict]):
        nonlocal skipped_story
        for p in raw:
            pid = p.get("id")
            if not pid:
                continue
            pid = str(pid)
            part = pid.split("_")[-1]
            tg = collect_targets(p)
            if pid in story_ids or part in story_ids or (tg & story_ids):
                skipped_story += 1
                continue
            targets.update(tg)
            p["type"] = "post"
            p["permalink_url"] = abs_url(p.get("permalink_url"))
            collected.setdefault(pid, p)

    url = f"{BASE_URL}/{page_id}/published_posts"

    # Pass A: pagination biasa
    try:
        raw = await fetch_all(client, url, {"fields": POST_FIELDS, "access_token": token})
        add_posts(raw)
        log(f"Pass A (paging biasa): {len(collected)} postingan")
    except FBError as e:
        job["warnings"].append(f"{name}: pass A gagal - {e.message}")
        log(f"Pass A gagal: {e.message}")

    # Pass B: jendela waktu (menembus batas kedalaman paging Facebook)
    until = int(time.time()) + 86400
    empty = 0
    n_win = 0
    try:
        while until > MIN_TS and empty < MAX_EMPTY_WINDOWS:
            since = until - WINDOW_SECONDS
            raw = await fetch_window(client, url, token, since, until)
            before = len(collected)
            add_posts(raw)
            empty = empty + 1 if not raw else 0
            n_win += 1
            if len(collected) > before:
                d = datetime.fromtimestamp(since).strftime("%Y-%m-%d")
                log(f"Jendela {d}: +{len(collected) - before} (total {len(collected)})")
            until = since
        log(f"Pass B (jendela waktu, {n_win} jendela): total {len(collected)} postingan")
    except FBError as e:
        job["warnings"].append(f"{name}: pass B gagal - {e.message}")
        log(f"Pass B gagal: {e.message}")

    known = {k.split("_")[-1] for k in collected} | targets

    # Video
    added_v = 0
    try:
        vids = await fetch_all(client, f"{BASE_URL}/{page_id}/videos", {
            "fields": "id,description,created_time,permalink_url,picture",
            "access_token": token,
        })
        for v in vids:
            vid = str(v.get("id", ""))
            if not vid or vid in known or vid in story_ids:
                continue
            known.add(vid)
            collected[vid] = {
                "id": vid, "message": v.get("description") or "(Video)",
                "created_time": v.get("created_time"),
                "permalink_url": abs_url(v.get("permalink_url")) or f"{FB_WEB}/watch/?v={vid}",
                "full_picture": v.get("picture"), "type": "video",
            }
            added_v += 1
    except FBError as e:
        job["warnings"].append(f"{name}: videos - {e.message}")
    log(f"Video tambahan (belum ada di feed): {added_v}")

    # Reels
    added_r = 0
    try:
        reels = await fetch_all(client, f"{BASE_URL}/{page_id}/video_reels", {
            "fields": "id,description,updated_time",
            "access_token": token,
        })
        for rl in reels:
            rid = str(rl.get("id", ""))
            if not rid or rid in known or rid in story_ids:
                continue
            known.add(rid)
            collected[rid] = {
                "id": rid, "message": rl.get("description") or "(Reel)",
                "created_time": rl.get("updated_time"),
                "permalink_url": f"{FB_WEB}/reel/{rid}",
                "full_picture": None, "type": "reel",
            }
            added_r += 1
    except FBError as e:
        job["warnings"].append(f"{name}: reels - {e.message}")
    log(f"Reels tambahan: {added_r}")

    if skipped_story:
        log(f"Item story terlewati: {skipped_story}")

    result = list(collected.values())
    result.sort(key=lambda x: x.get("created_time") or "", reverse=True)
    for p in result:
        p["_pageId"] = page_id
        p["_pageName"] = name
    return result


async def run_load_job(job_id: str, req: LoadRequest):
    job = LOAD_JOBS[job_id]
    sess = SESSIONS.get(req.session_id)
    if not sess:
        job.update(status="error", error="Session expired")
        return
    pages = {str(p["id"]): p for p in sess["pages"]}
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            for pid in req.page_ids:
                page = pages.get(str(pid))
                if not page or not page.get("access_token"):
                    job["log"].append(f"[{pid}] Page token tidak ditemukan")
                    job["pages_done"] += 1
                    continue
                job["current"] = page.get("name", pid)
                try:
                    posts = await load_page_posts(client, page, job)
                    job["posts"].extend(posts)
                    job["log"].append(f"[{page.get('name')}] SELESAI: {len(posts)} item")
                except Exception as exc:
                    job["log"].append(f"[{page.get('name')}] GAGAL: {exc}")
                job["pages_done"] += 1
                job["count"] = len(job["posts"])
        job["status"] = "completed"
    except Exception as exc:
        job.update(status="error", error=str(exc))


# ------------------------------------------------------------------ routes
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/login")
async def login(token: str = Form(...)):
    if not token or len(token) < 50:
        raise HTTPException(status_code=400, detail="Token tidak valid")
    pages: List[dict] = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            items = await fetch_all(client, f"{BASE_URL}/me/accounts", {
                "fields": "id,name,access_token,category,tasks", "access_token": token,
            })
            pages.extend(items)
    except FBError as e:
        raise HTTPException(status_code=400, detail=e.message)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    session_id = "sess_" + secrets.token_urlsafe(16)
    SESSIONS[session_id] = {"user_token": token, "pages": pages, "created": datetime.now().isoformat()}
    return {
        "session_id": session_id,
        "pages": [{"id": p["id"], "name": p["name"], "category": p.get("category", ""),
                   "has_token": bool(p.get("access_token"))} for p in pages],
    }


@app.post("/api/load/start")
async def load_start(req: LoadRequest):
    if req.session_id not in SESSIONS:
        raise HTTPException(status_code=404, detail="Session expired")
    if not req.page_ids:
        raise HTTPException(status_code=400, detail="Pilih minimal 1 Page")
    job_id = "load_" + secrets.token_urlsafe(8)
    LOAD_JOBS[job_id] = {
        "job_id": job_id, "status": "running", "total_pages": len(req.page_ids),
        "pages_done": 0, "current": "", "count": 0, "posts": [], "log": [],
        "warnings": [], "error": None,
    }
    asyncio.create_task(run_load_job(job_id, req))
    return {"job_id": job_id}


@app.get("/api/load/progress/{job_id}")
async def load_progress(job_id: str):
    job = LOAD_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Load job not found")
    done = job["status"] != "running"
    return {
        "status": job["status"], "total_pages": job["total_pages"],
        "pages_done": job["pages_done"], "current": job["current"],
        "count": job["count"], "log": job["log"][-40:],
        "warnings": job["warnings"], "error": job["error"],
        "posts": job["posts"] if done else [],
    }


async def run_delete_job(job_id: str, req: DeleteRequest):
    job = DELETE_JOBS[job_id]
    sess = SESSIONS.get(req.session_id)
    if not sess:
        job.update(status="error", error="Session expired")
        return

    token_by_page = {str(p["id"]): p.get("access_token") for p in sess["pages"]}
    concurrency = max(1, min(int(req.concurrency or 3), 5))
    delay = max(0, min(int(req.delay_ms or 0), 60000)) / 1000.0
    sem = asyncio.Semaphore(concurrency)
    job["concurrency"] = concurrency

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def process_one(post_id: str):
            page_id = req.post_pages.get(post_id) or (post_id.split("_")[0] if "_" in post_id else None)
            token = token_by_page.get(str(page_id)) or req.page_tokens.get(post_id)
            if not token:
                result = {"post_id": post_id, "success": False, "error": "Page token tidak ditemukan"}
            elif req.dry_run:
                result = {"post_id": post_id, "success": True, "dry_run": True, "message": "Simulasi"}
            else:
                async with sem:
                    try:
                        resp = await client.delete(f"{BASE_URL}/{post_id}", params={"access_token": token})
                        try:
                            data = resp.json()
                        except ValueError:
                            data = {"error": {"message": f"HTTP {resp.status_code}"}}
                        ok = resp.is_success and isinstance(data, dict) and bool(data.get("success"))
                        result = {"post_id": post_id, "success": ok,
                                  "error": None if ok else (data.get("error") or {}).get("message", "Gagal")}
                    except Exception as exc:
                        result = {"post_id": post_id, "success": False, "error": str(exc)}
                    if delay:
                        await asyncio.sleep(delay)
            job["results"].append(result)
            job["completed"] += 1
            job["success" if result.get("success") else "failed"] += 1

        try:
            await asyncio.gather(*(process_one(pid) for pid in req.post_ids))
            job["status"] = "completed"
        except Exception as exc:
            job.update(status="error", error=str(exc))


@app.post("/api/delete")
async def delete_posts(req: DeleteRequest):
    if req.session_id not in SESSIONS:
        raise HTTPException(status_code=404, detail="Session expired")
    job_id = "del_" + secrets.token_urlsafe(8)
    DELETE_JOBS[job_id] = {
        "job_id": job_id, "status": "running", "total": len(req.post_ids),
        "completed": 0, "success": 0, "failed": 0, "results": [],
        "concurrency": max(1, min(int(req.concurrency or 3), 5)),
        "dry_run": req.dry_run, "created": time.time(),
    }
    asyncio.create_task(run_delete_job(job_id, req))
    return {"job_id": job_id, "total": len(req.post_ids), "status": "running"}


@app.get("/api/delete/progress/{job_id}")
async def delete_progress(job_id: str):
    job = DELETE_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Delete job not found")
    return {
        "job_id": job["job_id"], "status": job["status"], "total": job["total"],
        "completed": job["completed"], "success": job["success"], "failed": job["failed"],
        "concurrency": job["concurrency"], "dry_run": job["dry_run"],
        "results": list(job["results"]) if job["status"] != "running" else [],
        "error": job.get("error"),
    }


@app.post("/api/logout")
async def logout(session_id: str = Form(...)):
    SESSIONS.pop(session_id, None)
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))