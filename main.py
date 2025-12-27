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
    return sqlite3.connect(DB, check_same_thread=False)

def process_csv(job_id: str, file_path: str):
    db = get_db()
    cursor = db.cursor()

    try:
        cursor.execute(
            "UPDATE jobs SET status=?, progress=? WHERE id=?",
            ("RUNNING", 10, job_id)
        )
        db.commit()

        with open(file_path, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            raise ValueError("CSV is empty")

        headers = rows[0]
        data_rows = rows[1:] if len(rows) > 1 else []

        result = {
            "rows": len(data_rows),
            "columns": headers
        }

        cursor.execute(
            "UPDATE jobs SET status=?, progress=?, result=? WHERE id=?",
            ("SUCCESS", 100, json.dumps(result), job_id)
        )
        db.commit()

    except Exception as e:
        print("CSV PROCESSING ERROR:", e)
        cursor.execute(
            "UPDATE jobs SET status=?, result=? WHERE id=?",
            ("FAILED", str(e), job_id)
        )
        db.commit()

    finally:
        db.close()

@app.post("/jobs/upload")
async def upload_csv(
    file: UploadFile,
    background_tasks: BackgroundTasks
):
    db = get_db()
    cursor = db.cursor()

    job_id = str(uuid.uuid4())
    file_path = f"{UPLOAD_DIR}/{job_id}.csv"

    with open(file_path, "wb") as f:
        f.write(await file.read())

    cursor.execute(
        "INSERT INTO jobs VALUES (?, ?, ?, ?, ?)",
        (job_id, "PENDING", None, 0, datetime.utcnow().isoformat())
    )
    db.commit()
    db.close()

    background_tasks.add_task(process_csv, job_id, file_path)

    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
def get_status(job_id: str):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT status, progress FROM jobs WHERE id=?",
        (job_id,)
    )
    row = cursor.fetchone()
    db.close()

    if row is None:
        return {"error": "Job not found"}

    status, progress = row
    return {
        "status": status,
        "progress": progress or 0
    }


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

def init_db():
    db = sqlite3.connect(DB)
    cursor = db.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        status TEXT,
        result TEXT,
        progress INTEGER DEFAULT 0,
        created_at TEXT
    )
    """)
    db.commit()
    db.close()

init_db()