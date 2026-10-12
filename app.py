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

# In-memory storage
SESSIONS: Dict[str, Dict] = {}
DELETE_JOBS: Dict[str, Dict] = {}

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
            while url and url not in visited_urls:
                visited_urls.add(url)
                resp = await client.get(url, params=params)
                data = resp.json()

                if "error" in data:
                    raise HTTPException(status_code=400, detail=data["error"].get("message", "Token error"))

                for p in data.get("data", []):
                    pages.append({
                        "id": p["id"],
                        "name": p.get("name", ""),
                        "access_token": p.get("access_token", ""),
                        "category": p.get("category", ""),
                        "tasks": p.get("tasks", []),
                    })

                url = (data.get("paging") or {}).get("next")
                params = {}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    session_id = f"sess_{int(time.time())}"
    SESSIONS[session_id] = {
        "token": token,
        "pages": {p["id"]: p for p in pages},
        "created": datetime.utcnow().isoformat(),
    }

    return {"session_id": session_id, "pages": pages}


async def get_stories_post_ids(page_id: str, access_token: str) -> set:
    story_ids = set()
    url = f"{BASE_URL}/{page_id}/stories"
    params = {
        "access_token": access_token,
        "fields": "post_id,status",
        "limit": 100,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        while url:
            try:
                resp = await client.get(url, params=params)
                data = resp.json()
                if "error" in data:
                    break
                for item in data.get("data", []):
                    pid = item.get("post_id")
                    if pid:
                        story_ids.add(str(pid))
                url = (data.get("paging") or {}).get("next")
                params = {}
            except Exception:
                break
    return story_ids


@app.post("/api/posts")
async def get_posts(
    session_id: str = Form(...),
    page_id: str = Form(...),
    limit: int = Form(50),
    since: str = Form(""),
    until: str = Form(""),
):
    session = SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Session tidak valid")

    page = session["pages"].get(page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page tidak ditemukan")

    access_token = page["access_token"]
    story_post_ids = await get_stories_post_ids(page_id, access_token)

    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Postingan biasa
        url = f"{BASE_URL}/{page_id}/published_posts"
        params = {
            "access_token": access_token,
            "fields": "id,message,created_time,permalink_url,full_picture,attachments{media_type,media}",
            "limit": min(limit, 100),
        }
        if since:
            params["since"] = since
        if until:
            params["until"] = until

        try:
            resp = await client.get(url, params=params)
            data = resp.json()
            if "error" not in data:
                for p in data.get("data", []):
                    post_id = p.get("id", "")
                    post_part = post_id.split("_")[-1] if "_" in post_id else post_id
                    if post_id in story_post_ids or post_part in story_post_ids:
                        continue
                    results.append({
                        "id": post_id,
                        "message": p.get("message") or "",
                        "created_time": p.get("created_time"),
                        "permalink_url": p.get("permalink_url"),
                        "full_picture": p.get("full_picture"),
                        "type": "post",
                    })
        except Exception:
            pass

        # 2. Reels
        url = f"{BASE_URL}/{page_id}/video_reels"
        params = {
            "access_token": access_token,
            "fields": "id,description,updated_time",
            "limit": min(limit, 50),
        }
        try:
            resp = await client.get(url, params=params)
            data = resp.json()
            if "error" not in data:
                for r in data.get("data", []):
                    vid = r.get("id")
                    if not vid:
                        continue
                    results.append({
                        "id": vid,
                        "message": r.get("description") or "(Reel)",
                        "created_time": r.get("updated_time"),
                        "permalink_url": None,
                        "full_picture": None,
                        "type": "reel",
                    })
        except Exception:
            pass

    return {
        "data": results[:limit],
        "story_excluded": len(story_post_ids),
    }


async def run_delete_job(job_id: str, post_ids: List[str], page_tokens: Dict[str, str], dry_run: bool, delay_ms: int, concurrency: int):
    job = DELETE_JOBS[job_id]
    sem = asyncio.Semaphore(concurrency)
    success = 0
    failed = 0

    async def delete_one(post_id: str):
        nonlocal success, failed
        async with sem:
            if dry_run:
                job["results"].append({"id": post_id, "ok": True, "msg": "dry"})
                success += 1
                return

            page_id = post_id.split("_")[0] if "_" in post_id else None
            token = page_tokens.get(page_id) or next(iter(page_tokens.values()), None)
            if not token:
                job["results"].append({"id": post_id, "ok": False, "msg": "no token"})
                failed += 1
                return

            url = f"{BASE_URL}/{post_id}"
            try:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    resp = await client.delete(url, params={"access_token": token})
                    data = resp.json()
                    ok = data.get("success") is True or "error" not in data
                    job["results"].append({
                        "id": post_id,
                        "ok": ok,
                        "msg": "ok" if ok else data.get("error", {}).get("message", "fail"),
                    })
                    if ok:
                        success += 1
                    else:
                        failed += 1
            except Exception as e:
                job["results"].append({"id": post_id, "ok": False, "msg": str(e)})
                failed += 1

            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)

    await asyncio.gather(*(delete_one(pid) for pid in post_ids))
    job["done"] = True
    job["success"] = success
    job["failed"] = failed


@app.post("/api/delete")
async def delete_posts(req: DeleteRequest):
    session = SESSIONS.get(req.session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Session tidak valid")

    job_id = f"job_{int(time.time() * 1000)}"
    DELETE_JOBS[job_id] = {
        "total": len(req.post_ids),
        "done": False,
        "success": 0,
        "failed": 0,
        "results": [],
    }

    asyncio.create_task(run_delete_job(
        job_id, req.post_ids, req.page_tokens, req.dry_run, req.delay_ms, req.concurrency
    ))

    return {"job_id": job_id}


@app.get("/api/delete/status/{job_id}")
async def delete_status(job_id: str):
    job = DELETE_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")
    return job


@app.post("/api/ads-protected")
async def get_ads_protected_posts(session_id: str = Form(...)):
    session = SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Session tidak valid")

    token = session["token"]
    protected = set()
    ad_accounts = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        url = f"{BASE_URL}/me/adaccounts"
        params = {"access_token": token, "fields": "id,name", "limit": 50}
        try:
            resp = await client.get(url, params=params)
            data = resp.json()
            ad_accounts = data.get("data", [])
        except Exception:
            pass

        for account in ad_accounts:
            act_id = account["id"]
            url2 = f"{BASE_URL}/{act_id}/ads"
            params2 = {
                "access_token": token,
                "fields": "creative{effective_object_story_id}",
                "filtering": '[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]',
                "limit": 100,
            }
            try:
                while url2:
                    resp2 = await client.get(url2, params=params2)
                    data2 = resp2.json()
                    for ad in data2.get("data", []):
                        story_id = (ad.get("creative") or {}).get("effective_object_story_id")
                        if story_id:
                            story_id = str(story_id)
                            protected.add(story_id)
                            post_part = story_id.split("_", 1)[-1]
                            if post_part:
                                protected.add(post_part)
                    url2 = (data2.get("paging") or {}).get("next")
                    params2 = {}
            except Exception:
                continue

    return {
        "protected_posts": list(protected),
        "count": len(protected),
        "ad_accounts": [{"id": a["id"], "name": a.get("name", "")} for a in ad_accounts],
    }


@app.post("/api/logout")
async def logout(session_id: str = Form(...)):
    SESSIONS.pop(session_id, None)
    return {"ok": True}


if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)