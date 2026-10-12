"""
FB Page Cleaner - versi lama + tambahan Reels (minimal).
Perubahan ditandai komentar [REELS].
"""

import asyncio
import os
import re
import secrets
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

import httpx
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

app = FastAPI(title="FB Page Cleaner")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

SESSIONS: Dict[str, Dict] = {}

GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v26.0")
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"
FB_WEB = "https://www.facebook.com"

# [REELS] field ringan untuk Reels (fallback jika field pertama ditolak)
REEL_FIELDS_TRY = (
    "id,description,created_time,updated_time,picture,permalink_url",
    "id,description,updated_time",
)
_REEL_ID_RE = re.compile(r"/(?:reels?|videos?)/(\d+)")


class DeleteRequest(BaseModel):
    session_id: str
    post_ids: List[str]
    # [REELS] post_id -> page_id (wajib untuk Reels karena ID-nya tanpa prefix "pageid_")
    post_pages: Dict[str, str] = Field(default_factory=dict)
    page_tokens: Dict[str, str] = Field(default_factory=dict)
    dry_run: bool = True
    delay_ms: int = 800
    concurrency: int = 3  # dibatasi server maksimal 5


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/login")
async def login(token: str = Form(...)):
    """Validasi token dan ambil daftar page."""
    if not token or len(token) < 50:
        raise HTTPException(400, "Token tidak valid")

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            r = await client.get(
                f"{BASE_URL}/me/accounts",
                params={
                    "fields": "id,name,access_token,category,tasks",
                    "access_token": token,
                    "limit": 100,
                },
            )
            data = r.json()
            if "error" in data:
                raise HTTPException(400, data["error"].get("message", "Token error"))

            pages = data.get("data", [])
            while "paging" in data and "next" in data["paging"]:
                r = await client.get(data["paging"]["next"])
                data = r.json()
                pages.extend(data.get("data", []))

            session_id = "sess_" + secrets.token_urlsafe(16)
            SESSIONS[session_id] = {
                "user_token": token,
                "pages": pages,
                "created": datetime.now().isoformat(),
            }

            return {
                "session_id": session_id,
                "pages": [
                    {
                        "id": p["id"],
                        "name": p["name"],
                        "category": p.get("category", ""),
                        "has_token": bool(p.get("access_token")),
                    }
                    for p in pages
                ],
            }
        except httpx.HTTPError as e:
            raise HTTPException(500, f"Network error: {str(e)}")


# [REELS] ambil Reels: satu loop paginasi sederhana, tanpa retry / jendela waktu
async def fetch_reels(client: httpx.AsyncClient, page_id: str, token: str, limit: int):
    last_err = None
    for fields in REEL_FIELDS_TRY:
        reels: List[dict] = []
        url: Optional[str] = f"{BASE_URL}/{page_id}/video_reels"
        params: Optional[dict] = {"fields": fields, "limit": 25, "access_token": token}
        try:
            while url and len(reels) < limit:
                r = await client.get(url, params=params)
                data = r.json()
                if "error" in data:
                    raise ValueError(data["error"].get("message", "Reels error"))
                reels.extend(x for x in data.get("data", []) if isinstance(x, dict))
                url = (data.get("paging") or {}).get("next")
                params = None
            return reels[:limit], None
        except Exception as e:
            last_err = str(e)
    return [], last_err


def _abs_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    return u if u.startswith("http") else FB_WEB + u


def _reel_ids_in_post(p: dict) -> set:
    """[REELS] kumpulkan ID video/reel yang terkait sebuah post feed (untuk dedup)."""
    ids = set(_REEL_ID_RE.findall(p.get("permalink_url") or ""))
    for a in (p.get("attachments") or {}).get("data", []) or []:
        ids.update(_REEL_ID_RE.findall(a.get("url") or ""))
    return ids


@app.get("/api/posts/{session_id}/{page_id}")
async def get_posts(
    session_id: str,
    page_id: str,
    limit: int = 50,
    since: Optional[str] = None,  # YYYY-MM-DD
    until: Optional[str] = None,
):
    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(404, "Session expired")

    page = next((p for p in sess["pages"] if p["id"] == page_id), None)
    if not page or not page.get("access_token"):
        raise HTTPException(400, "Page token tidak ditemukan")

    params = {
        "fields": "id,message,created_time,permalink_url,status_type,full_picture,attachments{media,type,title,description,url}",
        "limit": min(limit, 100),
        "access_token": page["access_token"],
    }
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    posts = []
    seen = set()
    warning = None
    async with httpx.AsyncClient(timeout=30.0) as client:
        url = f"{BASE_URL}/{page_id}/posts"
        while url and len(posts) < limit:
            r = await client.get(url, params=params if url.endswith("/posts") else None)
            data = r.json()
            if "error" in data:
                raise HTTPException(400, data["error"].get("message"))
            for p in data.get("data", []):
                if p.get("id") and p["id"] not in seen and len(posts) < limit:
                    seen.add(p["id"])
                    posts.append(p)
            url = data.get("paging", {}).get("next")
            params = None

        # [REELS] ambil Reels, gagal pun postingan biasa tetap tampil
        raw_reels, reel_err = await fetch_reels(client, page_id, page["access_token"], limit)
        if reel_err:
            warning = f"Reels gagal diambil: {reel_err}"

    # [REELS] normalisasi Reels + filter tanggal (sisi klien)
    reels = []
    reel_ids = set()
    for rl in raw_reels:
        rid = str(rl.get("id", ""))
        if not rid:
            continue
        created = rl.get("created_time") or rl.get("updated_time") or ""
        day = created[:10]
        if since and day and day < since:
            continue
        if until and day and day > until:
            continue
        reel_ids.add(rid)
        reels.append({
            "id": rid,
            "message": rl.get("description") or "(Reel)",
            "created_time": created,
            "permalink_url": _abs_url(rl.get("permalink_url")) or f"{FB_WEB}/reel/{rid}",
            "full_picture": rl.get("picture"),
            "type": "reel",
        })

    # [REELS] buang post feed yang sebenarnya Reel yang sama
    merged = []
    for p in posts:
        if (_reel_ids_in_post(p) & reel_ids) or (str(p["id"]).split("_")[-1] in reel_ids):
            continue
        p["type"] = "post"
        merged.append(p)
    merged.extend(reels)
    merged.sort(key=lambda x: x.get("created_time") or "", reverse=True)

    return {"posts": merged, "count": len(merged), "warning": warning}


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
        async def process_one(post_id: str) -> Dict[str, Any]:
            # [REELS] pakai mapping dari frontend dulu, baru prefix ID
            page_id = req.post_pages.get(post_id) or (post_id.split("_")[0] if "_" in post_id else None)
            page_token = page_token_by_id.get(str(page_id)) or req.page_tokens.get(post_id)
            if not page_token:
                result = {"post_id": post_id, "success": False, "error": "Page token tidak ditemukan"}
            elif req.dry_run:
                result = {"post_id": post_id, "success": True, "dry_run": True,
                          "message": "Simulasi berhasil (tidak dihapus)"}
            else:
                async with semaphore:
                    try:
                        r = await client.delete(f"{BASE_URL}/{post_id}",
                                                params={"access_token": page_token})
                        try:
                            data = r.json()
                        except ValueError:
                            data = {"error": {"message": f"Respons Meta bukan JSON (HTTP {r.status_code})"}}
                        success = r.is_success and bool(data.get("success"))
                        result = {"post_id": post_id, "success": success, "response": data,
                                  "error": data.get("error", {}).get("message") if not success else None}
                    except httpx.TimeoutException:
                        result = {"post_id": post_id, "success": False,
                                  "error": "Timeout saat menunggu Meta API; periksa status postingan sebelum mencoba lagi."}
                    except httpx.HTTPError as e:
                        result = {"post_id": post_id, "success": False, "error": f"Network error: {str(e)}"}
                    except Exception as e:
                        result = {"post_id": post_id, "success": False, "error": str(e)}
                    if delay_seconds:
                        await asyncio.sleep(delay_seconds)

            job["results"].append(result)
            job["completed"] += 1
            if result.get("success"):
                job["success"] += 1
            else:
                job["failed"] += 1

        try:
            await asyncio.gather(*(process_one(post_id) for post_id in req.post_ids))
            job["status"] = "completed"
        except Exception as e:
            job.update({"status": "error", "error": str(e)})


@app.post("/api/delete")
async def delete_posts(req: DeleteRequest):
    if req.session_id not in SESSIONS:
        raise HTTPException(404, "Session expired")
    job_id = "del_" + secrets.token_urlsafe(8)
    DELETE_JOBS[job_id] = {
        "job_id": job_id, "status": "running", "total": len(req.post_ids),
        "completed": 0, "success": 0, "failed": 0, "results": [],
        "concurrency": max(1, min(int(req.concurrency or 3), 5)),
        "dry_run": req.dry_run, "created": time.time()
    }
    asyncio.create_task(run_delete_job(job_id, req))
    return {"job_id": job_id, "total": len(req.post_ids), "status": "running"}


@app.get("/api/delete/progress/{job_id}")
async def delete_progress(job_id: str):
    job = DELETE_JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Delete job not found")
    return {
        "job_id": job["job_id"], "status": job["status"], "total": job["total"],
        "completed": job["completed"], "success": job["success"], "failed": job["failed"],
        "concurrency": job["concurrency"], "dry_run": job["dry_run"],
        "results": list(job["results"]) if job["status"] != "running" else [], "error": job.get("error")
    }


@app.get("/api/ads-posts/{session_id}")
async def get_ads_protected_posts(session_id: str):
    """Ambil daftar postingan yang sedang dipakai di Meta Ads (ACTIVE + PAUSED)."""
    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(404, "Session expired")

    token = sess["user_token"]
    protected = set()
    ad_accounts = []

    async with httpx.AsyncClient(timeout=40.0) as client:
        try:
            r = await client.get(
                f"{BASE_URL}/me/adaccounts",
                params={"fields": "id,name,account_status", "access_token": token, "limit": 50},
            )
            data = r.json()
            if "error" in data:
                return {"protected_posts": [], "warning": data["error"].get("message"), "ad_accounts": []}
            ad_accounts = data.get("data", [])
        except Exception as e:
            return {"protected_posts": [], "warning": str(e), "ad_accounts": []}

        for acc in ad_accounts:
            acc_id = acc["id"]
            try:
                url = f"{BASE_URL}/{acc_id}/ads"
                params = {
                    "fields": "id,name,status,creative{object_story_id,effective_object_story_id}",
                    "filtering": '[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]',
                    "limit": 200,
                    "access_token": token,
                }
                while url:
                    r = await client.get(url, params=params if "ads" in url else None)
                    data = r.json()
                    if "error" in data:
                        break
                    for ad in data.get("data", []):
                        creative = ad.get("creative") or {}
                        for key in ("object_story_id", "effective_object_story_id"):
                            sid = creative.get(key)
                            if sid and "_" in str(sid):
                                protected.add(str(sid))
                                post_part = str(sid).split("_", 1)[-1]
                                if post_part:
                                    protected.add(post_part)
                    url = data.get("paging", {}).get("next")
                    params = None

                try:
                    url2 = f"{BASE_URL}/{acc_id}/adcreatives"
                    params2 = {
                        "fields": "id,object_story_id,effective_object_story_id",
                        "limit": 200,
                        "access_token": token,
                    }
                    while url2:
                        r2 = await client.get(url2, params=params2 if "adcreatives" in url2 else None)
                        data2 = r2.json()
                        if "error" in data2:
                            break
                        for cr in data2.get("data", []):
                            for key in ("object_story_id", "effective_object_story_id"):
                                sid = cr.get(key)
                                if sid and "_" in str(sid):
                                    protected.add(str(sid))
                                    post_part = str(sid).split("_", 1)[-1]
                                    if post_part:
                                        protected.add(post_part)
                        url2 = data2.get("paging", {}).get("next")
                        params2 = None
                except Exception:
                    pass
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
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))