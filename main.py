from fastapi import FastAPI, UploadFile, BackgroundTasks
import uuid
import csv
import os
import sqlite3
import json
from datetime import datetime

app = FastAPI()
DB = "jobs.db"
UPLOAD_DIR = "uploads"

os.makedirs(UPLOAD_DIR, exist_ok=True)

def get_db():
    return sqlite3.connect(DB)

# --- DB setup ---
conn = sqlite3.connect(DB, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT,
    result TEXT,
    progress INTEGER DEFAULT 0,
    created_at TEXT
)
""")
conn.commit()

def process_csv(job_id: str, file_path: str):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "UPDATE jobs SET status=? WHERE id=?",
        ("RUNNING", job_id)
    )
    db.commit()

    try:
        cursor.execute(
            "UPDATE jobs SET progress=? WHERE id=?",
            (10, job_id)
        )

        with open(file_path, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        result = {
            "rows": len(rows) - 1,
            "columns": rows[0]
        }

        cursor.execute(
            "UPDATE jobs SET progress=? WHERE id=?",
            (80, job_id)
        )

        cursor.execute(
            "UPDATE jobs SET status=?, result=? WHERE id=?",
            ("SUCCESS", json.dumps(result), job_id)
        )

        cursor.execute(
            "UPDATE jobs SET progress=? WHERE id=?",
            (100, job_id)
        )

    except Exception as e:
        cursor.execute(
            "UPDATE jobs SET status=?, result=? WHERE id=?",
            ("FAILED", str(e), job_id)
        )

    db.commit()
    db.close()
    cursor.execute(
        "UPDATE jobs SET status=? WHERE id=?",
        ("RUNNING", job_id)
    )
    conn.commit()

    with open(file_path, newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    result = {
        "rows": len(rows) - 1,
        "columns": rows[0]
    }

    cursor.execute(
        "UPDATE jobs SET status=?, result=? WHERE id=?",
        ("SUCCESS", str(result), job_id)
    )
    conn.commit()

@app.post("/jobs/upload")
async def upload_csv(
    file: UploadFile,
    background_tasks: BackgroundTasks
):
    job_id = str(uuid.uuid4())
    file_path = f"{UPLOAD_DIR}/{job_id}.csv"

    with open(file_path, "wb") as f:
        f.write(await file.read())

    cursor.execute(
        "INSERT INTO jobs VALUES (?, ?, ?, ?)",
        (job_id, "PENDING", None, datetime.utcnow().isoformat())
    )
    conn.commit()

    background_tasks.add_task(process_csv, job_id, file_path)

    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
def get_status(job_id: str):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT status FROM jobs WHERE id=?", (job_id,)
    )
    row = cursor.fetchone()
    db.close()

    if not row:
        return {"error": "Job not found"}

    return {"status": row[0]}

@app.get("/jobs/{job_id}/result")
def get_result(job_id: str):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT status, result FROM jobs WHERE id=?", (job_id,)
    )
    row = cursor.fetchone()
    db.close()

    if not row:
        return {"error": "Job not found"}

    status, result = row

    if status != "SUCCESS":
        return {"status": status}

    return json.loads(result)