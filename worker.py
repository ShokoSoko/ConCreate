# worker.py
import os, time, subprocess, sys
import requests
from supabase import create_client, Client

def log(*args):
    print(*args, flush=True)

def mask(s: str, keep=4):
    if not s: return "MISSING"
    return "*"*(len(s)-keep) + s[-keep:]

# ---- Env check (no secrets) ----
log("ENV CHECK:", "URL:", "ok" if os.getenv("SUPABASE_URL") else "MISSING",
    "| SRK:", mask(os.getenv("SUPABASE_SERVICE_ROLE_KEY")))
log("BUCKET:", os.getenv("BUCKET","videos"))

# ---- Env & client bootstrap ----
SB_URL = os.getenv("SUPABASE_URL")
SB_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET = os.getenv("BUCKET", "videos")

if not SB_URL or not SB_KEY:
    log("FATAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars.")
    time.sleep(5)
    sys.exit(1)

try:
    sb: Client = create_client(SB_URL, SB_KEY)
except Exception as e:
    log("FATAL: Failed to create Supabase client:", e)
    time.sleep(5)
    sys.exit(1)

PUBLIC_BASE = f"{SB_URL}/storage/v1/object/public"

def claim_job():
    # Oldest 'ready' job
    res = sb.table("jobs").select("*").eq("status","ready").order("created_at", False).limit(1).execute()
    items = res.data or []
    return items[0] if items else None

def update(job_id, **fields):
    fields["updated_at"] = "now()"
    sb.table("jobs").update(fields).eq("id", job_id).execute()

def upload_public(src_path, dest_path):
    with open(src_path, "rb") as f:
        sb.storage.from_(BUCKET).upload(dest_path, f, {"content-type":"video/mp4", "upsert": True})
    return f"{PUBLIC_BASE}/{dest_path}"

def ensure_bucket():
    try:
        sb.storage.from_(BUCKET).list("", {"limit": 1})
    except Exception as e:
        log(f"FATAL: Storage bucket '{BUCKET}' not found or not public. Create it in Supabase → Storage (name: {BUCKET}). Error:", e)
        time.sleep(5)
        sys.exit(1)

def process_job(job):
    job_id = job["id"]
    source_url = job["source_url"]
    log(f"[{job_id}] Claiming job: {source_url}")
    update(job_id, status="processing", progress=1)

    in_path = "/tmp/in.mp4"
    out1 = "/tmp/clip1.mp4"

    # 1) Download from YouTube
    log(f"[{job_id}] yt-dlp start")
    subprocess.check_call(["yt-dlp","-f","mp4","-o",in_path, source_url])
    log(f"[{job_id}] yt-dlp done")

    # 2) DEMO transform
    log(f"[{job_id}] ffmpeg demo clip")
    subprocess.check_call(["ffmpeg","-y","-ss","00:00:05","-i",in_path,"-t","5","-c","copy", out1])

    # 3) Upload to storage
    dest = f"outputs/{job_id}/clip1.mp4"  # NOTE: no_
