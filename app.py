import asyncio
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

import httpx
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

app = FastAPI(title="FB Page Cleaner")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

SESSIONS: Dict[str, Dict] = {}
GRAPH_VERSION = "v26.0"
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"


class DeleteRequest(BaseModel):
    session_id: str
    post_ids: List[str]
    page_tokens: Dict[str, str]
    dry_run: bool = True
    delay_ms: int = 800
    concurrency: int = 3


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/login")
async def login(token: str = Form(...)):
    if not token or len(token) < 50:
        raise HTTPException(status_code=400, detail="Token tidak valid")

    pages = []
    url = f"{BASE_URL}/me/accounts"
    params = {
        "fields": "id,name,access_token,category,tasks",
        "access_token": token,
        "limit": 100,
    }
    visited_urls = set()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            while url:
                if url in visited_urls:
                    raise HTTPException(status_code=502, detail="Pagination daftar Page berulang.")
                visited_urls.add(url)
                response = await client.get(url, params=params)
                params = None
                data = response.json()
                if "error" in data:
                    raise HTTPException(status_code=400, detail=data["error"].get("message", "Token error"))
                pages.extend(data.get("data", []))
                url = (data.get("paging") or {}).get("next")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    session_id = f"sess_{int(time.time() * 1000)}"
    SESSIONS[session_id] = {"user_token": token, "pages": pages, "created": datetime.now().isoformat()}
    return {
        "session_id": session_id,
        "pages": [{"id": p["id"], "name": p["name"], "category": p.get("category", ""), "has_token": bool(p.get("access_token"))} for p in pages],
    }


@app.get("/api/posts/{session_id}/{page_id}")
async def get_posts(session_id: str, page_id: str, limit: int = 50, since: Optional[str] = None, until: Optional[str] = None):
    if limit < 1 or limit > 10000:
        raise HTTPException(status_code=400, detail="Limit harus 1-10000")
    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Session expired")
    page = next((p for p in sess["pages"] if str(p.get("id")) == str(page_id)), None)
    if not page or not page.get("access_token"):
        raise HTTPException(status_code=400, detail="Page token tidak ditemukan")

    page_token = page["access_token"]
    fields = "id,message,created_time,permalink_url,status_type,full_picture,attachments{media_type,media,type,title,description,url,subattachments}"
    posts, seen, visited = [], set(), set()
    warning = None
    url = f"{BASE_URL}/{page_id}/posts"
    params = {"fields": fields, "limit": 100, "access_token": page_token}
    if since: params["since"] = since
    if until: params["until"] = until
    first = True

    async with httpx.AsyncClient(timeout=45.0) as client:
        while url and len(posts) < limit:
            if url in visited:
                warning = "Pagination berulang"
                break
            visited.add(url)
            try:
                r = await client.get(url, params=params if first else None)
            except Exception as e:
                warning = str(e)
                break
            first = False
            try:
                data = r.json()
            except Exception:
                warning = "Bukan JSON"
                break
            if not isinstance(data, dict) or "error" in data:
                warning = (data.get("error") or {}).get("message", "API error")
                break
            for p in data.get("data", []):
                if isinstance(p, dict) and p.get("id") and p["id"] not in seen:
                    seen.add(p["id"])
                    posts.append(p)
                    if len(posts) >= limit:
                        break
            url = (data.get("paging") or {}).get("next")
            params = None

    return {"page_id": page_id, "posts": posts, "count": len(posts), "requested_limit": limit, "limit_reached": len(posts) >= limit, "warning": warning}


@app.get("/api/videos/{session_id}/{page_id}")
async def get_videos(session_id: str, page_id: str, limit: int = 50):
    """Load videos / reels dari page."""
    if limit < 1 or limit > 10000:
        raise HTTPException(status_code=400, detail="Limit harus 1-10000")
    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Session expired")
    page = next((p for p in sess["pages"] if str(p.get("id")) == str(page_id)), None)
    if not page or not page.get("access_token"):
        raise HTTPException(status_code=400, detail="Page token tidak ditemukan")

    page_token = page["access_token"]
    videos = []
    seen = set()
    warning = None

    # Coba beberapa endpoint video
    endpoints = ["videos", "video_reels"]
    async with httpx.AsyncClient(timeout=45.0) as client:
        for endpoint in endpoints:
            url = f"{BASE_URL}/{page_id}/{endpoint}"
            params = {
                "fields": "id,title,description,created_time,permalink_url,picture,source,length",
                "limit": 100,
                "access_token": page_token,
            }
            visited = set()
            first = True
            while url and len(videos) < limit:
                if url in visited:
                    break
                visited.add(url)
                try:
                    r = await client.get(url, params=params if first else None)
                except Exception as e:
                    warning = str(e)
                    break
                first = False
                try:
                    data = r.json()
                except Exception:
                    warning = "Bukan JSON"
                    break
                if not isinstance(data, dict) or "error" in data:
                    # Endpoint mungkin tidak tersedia, lanjut ke berikutnya
                    break
                for v in data.get("data", []):
                    if isinstance(v, dict) and v.get("id") and v["id"] not in seen:
                        seen.add(v["id"])
                        # Normalisasi supaya mirip post
                        videos.append({
                            "id": v["id"],
                            "message": v.get("title") or v.get("description") or "(video)",
                            "created_time": v.get("created_time"),
                            "permalink_url": v.get("permalink_url"),
                            "full_picture": v.get("picture"),
                            "status_type": "video",
                        })
                        if len(videos) >= limit:
                            break
                url = (data.get("paging") or {}).get("next")
                params = None
            if len(videos) >= limit:
                break

    return {
        "page_id": page_id,
        "posts": videos,
        "count": len(videos),
        "requested_limit": limit,
        "warning": warning,
    }


# ===================== DELETE (tidak diubah) =====================
DELETE_JOBS: Dict[str, Dict[str, Any]] = {}

async def run_delete_job(job_id: str, req: DeleteRequest):
    job = DELETE_JOBS[job_id]
    sess = SESSIONS.get(req.session_id)
    if not sess:
        job.update({"status": "error", "error": "Session expired"})
        return

    page_token_by_id = {str(p["id"]): p.get("access_token") for p in sess["pages"]}
    concurrency = max(1, min(int(req.concurrency or 3), 5))
    delay_seconds = max(0, min(int(req.delay_ms or 0), 60000)) / 1000.0
    semaphore = asyncio.Semaphore(concurrency)
    job["concurrency"] = concurrency

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def process_one(post_id: str):
            page_id = post_id.split("_")[0] if "_" in post_id else None
            page_token = page_token_by_id.get(str(page_id)) or req.page_tokens.get(post_id)
            if not page_token:
                result = {"post_id": post_id, "success": False, "error": "Page token tidak ditemukan"}
            elif req.dry_run:
                result = {"post_id": post_id, "success": True, "dry_run": True, "message": "Simulasi"}
            else:
                async with semaphore:
                    try:
                        response = await client.delete(f"{BASE_URL}/{post_id}", params={"access_token": page_token})
                        try:
                            data = response.json()
                        except ValueError:
                            data = {"error": {"message": f"HTTP {response.status_code}"}}
                        success = response.is_success and isinstance(data, dict) and bool(data.get("success"))
                        result = {"post_id": post_id, "success": success, "error": (data.get("error") or {}).get("message") if not success else None}
                    except Exception as exc:
                        result = {"post_id": post_id, "success": False, "error": str(exc)}
                    if delay_seconds:
                        await asyncio.sleep(delay_seconds)
            job["results"].append(result)
            job["completed"] += 1
            if result.get("success"):
                job["success"] += 1
            else:
                job["failed"] += 1

        try:
            await asyncio.gather(*(process_one(pid) for pid in req.post_ids))
            job["status"] = "completed"
        except Exception as exc:
            job.update({"status": "error", "error": str(exc)})


@app.post("/api/delete")
async def delete_posts(req: DeleteRequest):
    if req.session_id not in SESSIONS:
        raise HTTPException(status_code=404, detail="Session expired")
    job_id = f"del_{int(time.time() * 1000)}_{len(DELETE_JOBS)}"
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


@app.get("/api/ads-posts/{session_id}")
async def get_ads_protected_posts(session_id: str):
    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Session expired")
    token = sess["user_token"]
    protected = set()
    ad_accounts = []
    async with httpx.AsyncClient(timeout=40.0) as client:
        try:
            r = await client.get(f"{BASE_URL}/me/adaccounts", params={"fields": "id,name", "access_token": token, "limit": 50})
            data = r.json()
            if "error" in data:
                return {"protected_posts": [], "warning": data["error"].get("message"), "ad_accounts": []}
            ad_accounts = data.get("data", [])
        except Exception as e:
            return {"protected_posts": [], "warning": str(e), "ad_accounts": []}

        for acc in ad_accounts:
            try:
                url = f"{BASE_URL}/{acc['id']}/ads"
                params = {
                    "fields": "id,creative{object_story_id,effective_object_story_id}",
                    "filtering": '[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]',
                    "limit": 200, "access_token": token,
                }
                visited = set()
                while url:
                    if url in visited: break
                    visited.add(url)
                    r = await client.get(url, params=params)
                    params = None
                    data = r.json()
                    if "error" in data: break
                    for ad in data.get("data", []):
                        creative = ad.get("creative") or {}
                        for key in ("object_story_id", "effective_object_story_id"):
                            sid = creative.get(key)
                            if sid and "_" in str(sid):
                                protected.add(str(sid))
                                protected.add(str(sid).split("_", 1)[-1])
                    url = (data.get("paging") or {}).get("next")
            except Exception:
                continue
    return {"protected_posts": list(protected), "count": len(protected), "ad_accounts": [{"id": a["id"], "name": a.get("name","")} for a in ad_accounts]}


@app.post("/api/logout")
async def logout(session_id: str = Form(...)):
    SESSIONS.pop(session_id, None)
    return {"ok": True}


if __name__ == "__main__":
    import os, uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)