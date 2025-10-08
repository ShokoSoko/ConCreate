import os, time, subprocess, requests
from supabase import create_client, Client


def mask(s: str, keep=4):
    if not s: return "MISSING"
    return "*"*(len(s)-keep) + s[-keep:]

print("ENV CHECK:",
      "URL:", "ok" if os.getenv("SUPABASE_URL") else "MISSING",
      "| SRK:", mask(os.getenv("SUPABASE_SERVICE_ROLE_KEY")))
print("BUCKET:", os.getenv("BUCKET","videos"), flush=True)

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # SERVICE key (server-only)
BUCKET = "videos"
PUBLIC_BASE = f"{SB_URL}/storage/v1/object/public"

sb: Client = create_client(SB_URL, SB_KEY)

def claim_job():
  res = sb.table("jobs").select("*").eq("status", "ready").order("created_at", desc=False).limit(1).execute()
  arr = res.data or []
  return arr[0] if arr else None

def update(job_id, **fields):
  sb.table("jobs").update(fields).eq("id", job_id).execute()

def upload_public(src_path, dest_path):
  with open(src_path, "rb") as f:
    sb.storage.from_(BUCKET).upload(dest_path, f, {"content-type": "video/mp4", "upsert": True})
  return f"{PUBLIC_BASE}/{dest_path}"

while True:
  job = claim_job()
  if not job:
    time.sleep(2); continue
  job_id = job["id"]
  try:
    update(job_id, status="processing", progress=1)

    in_path = "/tmp/in.mp4"
    # Download YouTube to local file
    subprocess.check_call(["yt-dlp","-f","mp4","-o",in_path, job["source_url"]])

    # DEMO: make a 5-second clip to prove the pipeline
    out1 = "/tmp/clip1.mp4"
    subprocess.check_call(["ffmpeg","-y","-ss","00:00:05","-i",in_path,"-t","5","-c","copy", out1])

    dest = f"{BUCKET}/outputs/{job_id}/clip1.mp4".replace(f"{BUCKET}/", "")
    url = upload_public(out1, dest)

    outputs = [{"url": url, "label": "clip1"}]
    update(job_id, progress=100, status="done", outputs=outputs)
  except Exception as e:
    update(job_id, status="error", error=str(e))
