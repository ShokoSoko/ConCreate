FROM python:3.11-slim
RUN apt-get update && apt-get install -y ffmpeg curl && rm -rf /var/lib/apt/lists/*
RUN pip install yt-dlp supabase requests
WORKDIR /app
COPY worker.py .
CMD ["python","worker.py"]
