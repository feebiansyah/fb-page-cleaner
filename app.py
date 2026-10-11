
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

# In-memory storage (hanya selama server hidup)
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
    """Validasi token dan ambil daftar Page."""

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
                    raise HTTPException(
                        status_code=502,
                        detail="Pagination daftar Page berulang. Proses dihentikan.",
                    )

                visited_urls.add(url)

                response = await client.get(
                    url,
                    params=params,
                )
                params = None

                try:
                    data = response.json()
                except ValueError:
                    raise HTTPException(
                        status_code=502,
                        detail="Meta mengirim respons yang bukan JSON.",
                    )

                if not isinstance(data, dict):
                    raise HTTPException(
                        status_code=502,
                        detail="Format respons daftar Page dari Meta tidak valid.",
                    )

                if "error" in data:
                    error = data.get("error") or {}
                    raise HTTPException(
                        status_code=400,
                        detail=error.get("message", "Token atau izin Meta API bermasalah."),
                    )

                if response.is_error:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Meta mengembalikan HTTP {response.status_code}.",
                    )

                batch = data.get("data", [])
                if not isinstance(batch, list):
                    raise HTTPException(
                        status_code=502,
                        detail="Format daftar Page dari Meta tidak valid.",
                    )

                pages.extend(batch)

                paging = data.get("paging") or {}
                if not isinstance(paging, dict):
                    raise HTTPException(
                        status_code=502,
                        detail="Informasi pagination daftar Page tidak valid.",
                    )

                url = paging.get("next")

    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Koneksi ke Meta gagal: {type(exc).__name__}",
        )

    session_id = f"sess_{int(time.time() * 1000)}"
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


@app.get("/api/posts/{session_id}/{page_id}")
async def get_posts(
    session_id: str,
    page_id: str,
    limit: int = 50,
    since: Optional[str] = None,
    until: Optional[str] = None,
):
    if limit < 1 or limit > 10000:
        raise HTTPException(status_code=400, detail="Jumlah postingan harus antara 1 dan 10.000 per Page.")

    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Sesi sudah berakhir.")

    page = next((p for p in sess["pages"] if str(p.get("id")) == str(page_id)), None)
    if not page or not page.get("access_token"):
        raise HTTPException(status_code=400, detail="Token Page tidak ditemukan.")

    page_token = page["access_token"]
    fields = (
        "id,message,created_time,permalink_url,status_type,full_picture,"
        "attachments{media_type,media,type,title,description,url,subattachments}"
    )

    posts = []
    seen = set()
    visited = set()
    warning = None
    url = f"{BASE_URL}/{page_id}/posts"
    params = {"fields": fields, "limit": 100, "access_token": page_token}
    if since:
        params["since"] = since
    if until:
        params["until"] = until
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
                warning = "Respons bukan JSON"
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

    return {
        "page_id": page_id,
        "posts": posts,
        "count": len(posts),
        "requested_limit": limit,
        "limit_reached": len(posts) >= limit,
        "warning": warning,
    }
    if limit < 1 or limit > 10000:
        raise HTTPException(status_code=400, detail="Jumlah postingan harus antara 1 dan 10.000 per Page.")

    sess = SESSIONS.get(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Sesi sudah berakhir.")

    page = next((p for p in sess["pages"] if str(p.get("id")) == str(page_id)), None)
    if not page or not page.get("access_token"):
        raise HTTPException(status_code=400, detail="Token Page tidak ditemukan.")

    page_token = page["access_token"]

    fields = (
        "id,message,created_time,permalink_url,status_type,full_picture,"
        "attachments{media_type,media,type,title,description,url,subattachments}"
    )

    async def fetch(endpoint: str):
        posts, seen, visited = [], set(), set()
        warning = None
        url = f"{BASE_URL}/{page_id}/{endpoint}"
        params = {"fields": fields, "limit": 100, "access_token": page_token}
        if since: params["since"] = since
        if until: params["until"] = until
        first = True

        async with httpx.AsyncClient(timeout=45.0) as client:
            while url and len(posts) < limit:
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
        return posts, warning

    all_posts = []
    seen_ids = set()
    warnings = []

    for endpoint in ("posts", "feed", "published_posts"):
        batch, warn = await fetch(endpoint)
        if warn:
            warnings.append(f"{endpoint}: {warn}")
        for p in batch:
            if p["id"] not in seen_ids and len(all_posts) < limit:
                seen_ids.add(p["id"])
                all_posts.append(p)
        if len(all_posts) >= limit:
            break

    return {
        "page_id": page_id,
        "posts": all_posts,
        "count": len(all_posts),
        "requested_limit": limit,
        "limit_reached": len(all_posts) >= limit,
        "warning": "; ".join(warnings) if warnings else None,
    }










DELETE_JOBS: Dict[str, Dict[str, Any]] = {}


async def run_delete_job(job_id: str, req: DeleteRequest):
    job = DELETE_JOBS[job_id]
    sess = SESSIONS.get(req.session_id)

    if not sess:
        job.update({"status": "error", "error": "Session expired"})
        return

    page_token_by_id = {
        str(p["id"]): p.get("access_token")
        for p in sess["pages"]
    }

    concurrency = max(1, min(int(req.concurrency or 3), 5))
    delay_seconds = max(0, min(int(req.delay_ms or 0), 60000)) / 1000.0
    semaphore = asyncio.Semaphore(concurrency)
    job["concurrency"] = concurrency

    async with httpx.AsyncClient(timeout=30.0) as client:

        async def process_one(post_id: str) -> Dict[str, Any]:
            page_id = post_id.split("_")[0] if "_" in post_id else None

            page_token = (
                page_token_by_id.get(str(page_id))
                or req.page_tokens.get(post_id)
            )

            if not page_token:
                result = {
                    "post_id": post_id,
                    "success": False,
                    "error": "Page token tidak ditemukan",
                }

            elif req.dry_run:
                result = {
                    "post_id": post_id,
                    "success": True,
                    "dry_run": True,
                    "message": "Simulasi berhasil (tidak dihapus)",
                }

            else:
                async with semaphore:
                    try:
                        response = await client.delete(
                            f"{BASE_URL}/{post_id}",
                            params={"access_token": page_token},
                        )

                        try:
                            data = response.json()
                        except ValueError:
                            data = {
                                "error": {
                                    "message": (
                                        "Respons Meta bukan JSON "
                                        f"(HTTP {response.status_code})"
                                    )
                                }
                            }

                        success = (
                            response.is_success
                            and isinstance(data, dict)
                            and bool(data.get("success"))
                        )

                        error_data = (
                            data.get("error") or {}
                            if isinstance(data, dict)
                            else {}
                        )

                        result = {
                            "post_id": post_id,
                            "success": success,
                            "response": data,
                            "error": (
                                error_data.get("message")
                                if not success
                                else None
                            ),
                        }

                    except httpx.TimeoutException:
                        result = {
                            "post_id": post_id,
                            "success": False,
                            "error": (
                                "Timeout saat menunggu Meta API; "
                                "periksa status postingan sebelum mencoba lagi."
                            ),
                        }

                    except httpx.HTTPError as exc:
                        result = {
                            "post_id": post_id,
                            "success": False,
                            "error": f"Network error: {str(exc)}",
                        }

                    except Exception as exc:
                        result = {
                            "post_id": post_id,
                            "success": False,
                            "error": str(exc),
                        }

                    if delay_seconds:
                        await asyncio.sleep(delay_seconds)

            job["results"].append(result)
            job["completed"] += 1

            if result.get("success"):
                job["success"] += 1
            else:
                job["failed"] += 1

        try:
            await asyncio.gather(
                *(process_one(post_id) for post_id in req.post_ids)
            )
            job["status"] = "completed"

        except Exception as exc:
            job.update({"status": "error", "error": str(exc)})


@app.post("/api/delete")
async def delete_posts(req: DeleteRequest):
    if req.session_id not in SESSIONS:
        raise HTTPException(status_code=404, detail="Session expired")

    job_id = f"del_{int(time.time() * 1000)}_{len(DELETE_JOBS)}"

    DELETE_JOBS[job_id] = {
        "job_id": job_id,
        "status": "running",
        "total": len(req.post_ids),
        "completed": 0,
        "success": 0,
        "failed": 0,
        "results": [],
        "concurrency": max(1, min(int(req.concurrency or 3), 5)),
        "dry_run": req.dry_run,
        "created": time.time(),
    }

    asyncio.create_task(run_delete_job(job_id, req))

    return {
        "job_id": job_id,
        "total": len(req.post_ids),
        "status": "running",
    }


@app.get("/api/delete/progress/{job_id}")
async def delete_progress(job_id: str):
    job = DELETE_JOBS.get(job_id)

    if not job:
        raise HTTPException(
            status_code=404,
            detail="Delete job not found",
        )

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "total": job["total"],
        "completed": job["completed"],
        "success": job["success"],
        "failed": job["failed"],
        "concurrency": job["concurrency"],
        "dry_run": job["dry_run"],
        "results": (
            list(job["results"])
            if job["status"] != "running"
            else []
        ),
        "error": job.get("error"),
    }


@app.get("/api/ads-posts/{session_id}")
async def get_ads_protected_posts(session_id: str):
    """Ambil daftar postingan yang dipakai di Meta Ads (ACTIVE + PAUSED)."""

    sess = SESSIONS.get(session_id)

    if not sess:
        raise HTTPException(
            status_code=404,
            detail="Session expired",
        )

    token = sess["user_token"]
    protected = set()
    ad_accounts = []

    async with httpx.AsyncClient(timeout=40.0) as client:

        # 1. Ambil ad accounts.
        try:
            response = await client.get(
                f"{BASE_URL}/me/adaccounts",
                params={
                    "fields": "id,name,account_status",
                    "access_token": token,
                    "limit": 50,
                },
            )

            data = response.json()

            if "error" in data:
                return {
                    "protected_posts": [],
                    "warning": data["error"].get("message"),
                    "ad_accounts": [],
                }

            ad_accounts = data.get("data", [])

        except Exception as exc:
            return {
                "protected_posts": [],
                "warning": str(exc),
                "ad_accounts": [],
            }

        # 2. Ambil iklan ACTIVE dan PAUSED dari setiap ad account.
        for account in ad_accounts:
            account_id = account["id"]

            try:
                url = f"{BASE_URL}/{account_id}/ads"

                params = {
                    "fields": (
                        "id,name,status,"
                        "creative{object_story_id,effective_object_story_id}"
                    ),
                    "filtering": (
                        '[{"field":"effective_status","operator":"IN",'
                        '"value":["ACTIVE","PAUSED"]}]'
                    ),
                    "limit": 200,
                    "access_token": token,
                }

                visited_urls = set()

                while url:
                    if url in visited_urls:
                        break

                    visited_urls.add(url)

                    response = await client.get(
                        url,
                        params=params,
                    )
                    params = None
                    data = response.json()

                    if "error" in data:
                        break

                    for ad in data.get("data", []):
                        creative = ad.get("creative") or {}

                        for key in (
                            "object_story_id",
                            "effective_object_story_id",
                        ):
                            story_id = creative.get(key)

                            if story_id and "_" in str(story_id):
                                story_id = str(story_id)
                                protected.add(story_id)

                                post_part = story_id.split("_", 1)[-1]
                                if post_part:
                                    protected.add(post_part)

                    url = (data.get("paging") or {}).get("next")

                # Tambahan: coba ambil data adcreatives.
                try:
                    url2 = f"{BASE_URL}/{account_id}/adcreatives"

                    params2 = {
                        "fields": (
                            "id,object_story_id,effective_object_story_id"
                        ),
                        "limit": 200,
                        "access_token": token,
                    }

                    visited_urls2 = set()

                    while url2:
                        if url2 in visited_urls2:
                            break

                        visited_urls2.add(url2)

                        response2 = await client.get(
                            url2,
                            params=params2,
                        )
                        params2 = None
                        data2 = response2.json()

                        if "error" in data2:
                            break

                        for creative_item in data2.get("data", []):
                            for key in (
                                "object_story_id",
                                "effective_object_story_id",
                            ):
                                story_id = creative_item.get(key)

                                if story_id and "_" in str(story_id):
                                    story_id = str(story_id)
                                    protected.add(story_id)

                                    post_part = story_id.split("_", 1)[-1]
                                    if post_part:
                                        protected.add(post_part)

                        url2 = (data2.get("paging") or {}).get("next")

                except Exception:
                    pass

            except Exception:
                continue

    return {
        "protected_posts": list(protected),
        "count": len(protected),
        "ad_accounts": [
            {
                "id": account["id"],
                "name": account.get("name", ""),
            }
            for account in ad_accounts
        ],
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
