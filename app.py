"""
app.py  –  Flask backend for NaukriGulf Scraper UI
Run:  python app.py
Open: http://localhost:5000
"""

import json
import uuid
import csv
import io
import threading
import logging
import os
from datetime import datetime
from flask import Flask, request, jsonify, render_template, Response

from scraper_core import scrape_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

# ── In-memory store ───────────────────────────────────────────────────────────
# jobs_store:  { job_id: {job dict} }
# scrape_tasks: { task_id: { status, url, count, error, started_at, finished_at } }
jobs_store: dict[str, dict] = {}
scrape_tasks: dict[str, dict] = {}
store_lock = threading.Lock()
job_keys_seen: set[str] = set()


def _job_key(job: dict) -> str:
    # Cross-run dedupe key (listing-only scraping; no job URL)
    return "|".join(
        [
            (job.get("source_url") or "").strip().lower(),
            (job.get("title") or "").strip().lower(),
            (job.get("company") or "").strip().lower(),
            (job.get("location") or "").strip().lower(),
            (job.get("posted_date") or "").strip().lower(),
        ]
    )


# ── Background scrape worker ─────────────────────────────────────────────────
def _worker(task_id: str, url: str, pages: int, workers: int):
    with store_lock:
        scrape_tasks[task_id]["status"] = "running"

    try:
        result = scrape_url(url, max_pages=pages, workers=workers)
    except Exception as exc:
        logging.error("Worker crashed for task %s: %s", task_id, exc, exc_info=True)
        result = {
            "jobs": [],
            "count": 0,
            "pages_scraped": 0,
            "total_jobs_reported": None,
            "error": str(exc),
        }

    with store_lock:
        added = 0
        for job in result["jobs"]:
            key = _job_key(job)
            if key in job_keys_seen:
                continue
            job_keys_seen.add(key)
            jid = str(uuid.uuid4())
            job["id"] = jid
            jobs_store[jid] = job
            added += 1

        scrape_tasks[task_id]["status"] = "done" if not result["error"] else "error"
        # count = newly added rows (deduped across previous runs)
        scrape_tasks[task_id]["count"] = added
        scrape_tasks[task_id]["pages_scraped"] = result.get("pages_scraped", 0)
        scrape_tasks[task_id]["total_jobs_reported"] = result.get("total_jobs_reported")
        scrape_tasks[task_id]["error"] = result["error"]
        scrape_tasks[task_id]["finished_at"] = datetime.now().isoformat()


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    data = request.get_json(force=True)
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "url is required"}), 400
    if not url.startswith("http"):
        url = "https://" + url

    try:
        pages = int(data.get("pages", 1))
    except (TypeError, ValueError):
        pages = 1
    pages = max(1, min(pages, 1000))

    try:
        workers = int(data.get("workers", 1))
    except (TypeError, ValueError):
        workers = 1
    workers = max(1, min(workers, 5))

    task_id = str(uuid.uuid4())
    with store_lock:
        scrape_tasks[task_id] = {
            "id": task_id,
            "url": url,
            "pages": pages,
            "workers": workers,
            "status": "queued",
            "count": 0,
            "pages_scraped": 0,
            "total_jobs_reported": None,
            "error": None,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    thread = threading.Thread(
        target=_worker, args=(task_id, url, pages, workers), daemon=True
    )
    thread.start()

    return jsonify({"task_id": task_id, "url": url, "pages": pages, "workers": workers})


@app.route("/api/tasks", methods=["GET"])
def api_tasks():
    with store_lock:
        return jsonify(list(scrape_tasks.values()))


@app.route("/api/tasks/<task_id>", methods=["GET"])
def api_task_status(task_id):
    with store_lock:
        task = scrape_tasks.get(task_id)
    if not task:
        return jsonify({"error": "task not found"}), 404
    return jsonify(task)


@app.route("/api/jobs", methods=["GET"])
def api_jobs():
    source = request.args.get("source", "")
    search = request.args.get("q", "").lower()
    posted = request.args.get("posted", "").lower()
    with store_lock:
        jobs = list(jobs_store.values())

    if source:
        jobs = [j for j in jobs if j.get("source_url", "") == source]
    if search:
        jobs = [
            j for j in jobs
            if search in (
                j.get("title", "")
                + j.get("company", "")
                + j.get("location", "")
                + j.get("description_snippet", "")
                + j.get("posted_date", "")
            ).lower()
        ]
    if posted:
        jobs = [
            j for j in jobs
            if posted in (j.get("posted_date") or "").lower()
        ]
    return jsonify(jobs)


@app.route("/api/jobs/export", methods=["GET"])
def api_export():
    with store_lock:
        jobs = list(jobs_store.values())

    if not jobs:
        return jsonify({"error": "no jobs to export"}), 400

    fieldnames = [
        "title",
        "company",
        "location",
        "experience",
        "posted_date",
        "job_type",
        "industry",
        "description_snippet",
        "easy_apply",
        "employer_active",
        "source_url",
        "scraped_at",
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(jobs)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=scraped_jobs.csv"}
    )


@app.route("/api/jobs", methods=["DELETE"])
def api_clear_jobs():
    with store_lock:
        jobs_store.clear()
        scrape_tasks.clear()
        job_keys_seen.clear()
    return jsonify({"message": "cleared"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True)