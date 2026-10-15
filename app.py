"""
Facebook Page Post Bulk Cleaner
Versi aman - token hanya hidup di memory server lokal.
Jalankan di mesin kamu sendiri.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

import httpx
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

app = FastAPI(title="FB Page Cleaner")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# In-memory storage (hanya selama server hidup)
SESSIONS: Dict[str, Dict] = {}  # session_id -> {user_token, pages, etc}

GRAPH_VERSION = "v26.0"
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"


class DeleteRequest(BaseModel):
    session_id: str
    post_ids: List[str]  # list of "pageid_postid" or just post ids with page mapping
    page_tokens: Dict[str, str]  # post_id -> page_token
    dry_run: bool = True
    delay_ms: int = 800
    concurrency: int = 3  # jumlah DELETE bersamaan; dibatasi server maksimal 5


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
            # Pagination sederhana jika ada banyak page
            while "paging" in data and "next" in data["paging"]:
                r = await client.get(data["paging"]["next"])
                data = r.json()
                pages.extend(data.get("data", []))

            session_id = f"sess_{int(time.time())}"
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

    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or limit > 200:
        raise HTTPException(422, "Limit harus antara 1 dan 200")
    for value in (since, until):
        if value is not None:
            try:
                if datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d") != value:
                    raise ValueError()
            except ValueError:
                raise HTTPException(422, "Tanggal harus valid dalam format YYYY-MM-DD")
    if since and until and since > until:
        raise HTTPException(422, "Tanggal sejak melebihi tanggal akhir")

    params = {
        "fields": "id,message,created_time,permalink_url,status_type,full_picture,attachments{media,type,title,description,url}",
        "limit": min(limit, 100),
        "access_token": page["access_token"],
    }
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    posts, seen, visited, cursors = [], set(), set(), set()
    requests = pages = received = 0
    reason, failure = "no_next", None
    # A finite request budget also covers empty pages and duplicate-only pages.
    budget = 20
    url = httpx.URL(f"{BASE_URL}/{page_id}/posts")
    async with httpx.AsyncClient(timeout=30.0) as client:
        while url and len(posts) < limit:
            key = str(url.copy_with(params=sorted(url.params.multi_items())))
            cursor = url.params.get("after")
            if key in visited or (cursor and cursor in cursors):
                reason, failure = "repeated_pagination", "Pagination berulang; pemuatan dihentikan"
                break
            if requests >= budget:
                reason, failure = "request_budget", "Batas request pemuatan tercapai"
                break
            visited.add(key)
            if cursor:
                cursors.add(cursor)
            requests += 1
            try:
                r = await client.get(url, params=params if requests == 1 else None)
                if not r.is_success:
                    raise HTTPException(502, f"Meta API HTTP {r.status_code}")
                try:
                    data = r.json()
                except ValueError:
                    raise HTTPException(502, "Respons Meta bukan JSON valid")
                if not isinstance(data, dict):
                    raise HTTPException(502, "Struktur respons Meta tidak valid")
                if "error" in data:
                    error = data["error"]
                    code = error.get("code") if isinstance(error, dict) else None
                    suffix = f" (kode {code})" if isinstance(code, int) else ""
                    raise HTTPException(502, "Meta API menolak pemuatan" + suffix)
                items = data.get("data")
                if not isinstance(items, list) or any(not isinstance(p, dict) or not isinstance(p.get("id"), str) or not p["id"] for p in items):
                    raise HTTPException(502, "Data postingan Meta tidak valid")
                pages += 1
                received += len(items)
                for post in items:
                    if post["id"] not in seen and len(posts) < limit:
                        seen.add(post["id"])
                        posts.append(post)
                if len(posts) >= limit:
                    reason = "limit_reached"
                    break
                paging = data.get("paging", {})
                if not isinstance(paging, dict):
                    raise HTTPException(502, "Struktur pagination Meta tidak valid")
                nxt = paging.get("next")
                if nxt is None:
                    reason = "no_next"
                    break
                if not isinstance(nxt, str) or not nxt:
                    raise HTTPException(502, "Pagination Meta tidak valid")
                try:
                    url = httpx.URL(nxt)
                except httpx.InvalidURL:
                    raise HTTPException(502, "Pagination Meta tidak valid")
                if url.scheme != "https" or url.host != "graph.facebook.com" or url.userinfo or url.port not in (None, 443):
                    raise HTTPException(502, "Tujuan pagination Meta tidak valid")
            except httpx.HTTPError:
                reason, failure = "network_error", "Koneksi ke Meta gagal atau timeout"
                break
            except HTTPException as exc:
                reason, failure = "invalid_response", exc.detail
                break

    summary = {"requests": requests, "pages": pages, "items_received": received, "stop_reason": reason}
    if failure and not posts:
        return JSONResponse(status_code=502, content={"detail": failure, "summary": summary})
    return {"posts": posts, "count": len(posts), "partial": bool(failure), "warning": failure, "summary": summary}


DELETE_JOBS: Dict[str, Dict[str, Any]] = {}


async def run_delete_job(job_id: str, req: DeleteRequest):
    job = DELETE_JOBS[job_id]
    sess = SESSIONS.get(req.session_id)
    if not sess:
        job.update({"status": "error", "error": "Session expired"})
        return

    page_token_by_id = {p["id"]: p.get("access_token") for p in sess["pages"]}
    concurrency = max(1, min(int(req.concurrency or 3), 5))
    delay_seconds = max(0, min(int(req.delay_ms or 0), 60000)) / 1000.0
    semaphore = asyncio.Semaphore(concurrency)
    job["concurrency"] = concurrency

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def process_one(post_id: str) -> Dict[str, Any]:
            page_id = post_id.split("_")[0] if "_" in post_id else None
            page_token = page_token_by_id.get(page_id) or req.page_tokens.get(post_id)
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

            # Update small in-memory counters as each item finishes; no extra Meta API calls.
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
    job_id = f"del_{int(time.time() * 1000)}_{len(DELETE_JOBS)}"
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
    # Return a snapshot; no network calls and negligible CPU/memory overhead.
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
        # 1. Ambil ad accounts
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

        # 2. Untuk setiap ad account, ambil ads yang ACTIVE atau PAUSED
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
                                # Tambahan: simpan juga bagian post ID saja (setelah _)
                                post_part = str(sid).split("_", 1)[-1]
                                if post_part:
                                    protected.add(post_part)
                    url = data.get("paging", {}).get("next")
                    params = None

                # Tambahan: ambil juga dari endpoint adcreatives langsung (lebih lengkap)
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
    uvicorn.run(app, host="127.0.0.1", port=8000)
