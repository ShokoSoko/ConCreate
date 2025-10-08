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
    res = sb.table("jobs").select("*").eq("status", "ready").order("created_at", desc=False).limit(1).execute()
    items = res.data or []
    return items[0] if items else None

def update(job_id, **fields):
    fields["updated_at"] = "now()"
    sb.table("jobs").update(fields).eq("id", job_id).execute()

def upload_public(src_path, dest_path):
    with open(src_path, "rb") as f:
        # Use string options (or just drop upsert entirely)
        sb.storage.from_(BUCKET).upload(
            dest_path, 
            f, 
            {"contentType": "video/mp4", "upsert": "true"}  # <- strings, not bool
        )
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
    subprocess.check_call(["yt-dlp","-t","mp4","-o",in_path, source_url])
    log(f"[{job_id}] yt-dlp done")

    # 2) DEMO transform
    log(f"[{job_id}] ffmpeg demo clip")
    subprocess.check_call(["ffmpeg","-y","-ss","00:00:05","-i",in_path,"-t","5","-c","copy", out1])

    # 3) Upload to storage
    dest = f"outputs/{job_id}/clip1.mp4"  # NOTE: no bucket prefix here
    url = upload_public(out1, dest)
    log(f"[{job_id}] Uploaded -> {url}")

    # 4) Finalize
    outputs = [{"url": url, "label": "clip1"}]
    update(job_id, progress=100, status="done", outputs=outputs)
    log(f"[{job_id}] DONE")

def main():
    log("Worker booted. Checking bucket...")
    ensure_bucket()
    log("Bucket OK. Polling for jobs...")
    while True:
        try:
            job = claim_job()
            if not job:
                time.sleep(2)
                continue
            try:
                process_job(job)
            except subprocess.CalledProcessError as e:
                log(f"[{job['id']}] Subprocess error:", e)
                update(job["id"], status="error", error=str(e))
            except Exception as e:
                log(f"[{job['id']}] UNEXPECTED ERROR:", e)
                update(job["id"], status="error", error=str(e))
        except Exception as loop_err:
            log("LOOP ERROR (will retry):", loop_err)
            time.sleep(3)

if __name__ == "__main__":
    main()
