import time
import subprocess
import db
import sqlite3
from datetime import datetime, timedelta, timezone
import os
import config

app_config = config.load_config()
BASE_BACKOFF_SECONDS = app_config.get('base_backoff_seconds', 2)

def run_worker():
    """The main worker loop."""
    print(f"Worker process starting... (PID: {os.getpid()})")
    
    while True:
        job = fetch_and_lock_job()
        if job:
            print(f"Worker (PID: {os.getpid()}) picked up job: {job['id']}")
            execute_job(job)
        else:
            time.sleep(1) 

def fetch_and_lock_job():
    """
    Atomically fetches and locks a single 'pending' job.
    This is the key to preventing two workers from grabbing the same job.
    """
    with db.get_db() as conn:
        try:
            conn.execute("BEGIN EXCLUSIVE") 
            
            now_utc = datetime.now(timezone.utc)
            now_iso = now_utc.isoformat()
            
            cursor = conn.execute(
                """
                SELECT * FROM jobs 
                WHERE state = 'pending' AND run_at <= ?
                ORDER BY created_at ASC 
                LIMIT 1
                """,
                (now_iso,)
            )
            job = cursor.fetchone()
            
            if job:
                conn.execute(
                    "UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ?",
                    (now_iso, job['id'])
                )
                conn.commit()
                return job
            else:
                conn.commit()
                return None
        except sqlite3.Error as e:
            print(f"Database error in fetch_and_lock: {e}")
            conn.rollback()
            return None

def execute_job(job):
    """Executes a job's command and handles the result."""
    job_id = job['id']
    command = job['command']
    start_time = time.time()
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        
        duration = time.time() - start_time
        output = result.stdout + "\n" + result.stderr
        
        if result.returncode == 0:
            print(f"Job {job_id} completed successfully in {duration:.2f}s.")
            update_job_state(job_id, 'completed', output)
        else:
            print(f"Job {job_id} failed with code {result.returncode}.")
            handle_job_failure(job, output)
            
    except subprocess.TimeoutExpired:
        print(f"Job {job_id} timed out.")
        handle_job_failure(job, "Error: Job timed out after 30 seconds.")
    except Exception as e:
        print(f"Job {job_id} execution error: {e}")
        handle_job_failure(job, f"Error: {e}")

def handle_job_failure(job, output):
    """Handles retry logic, backoff, and DLQ."""
    now_utc = datetime.now(timezone.utc)
    job_id = job['id']
    new_attempts = job['attempts'] + 1
    
    if new_attempts > job['max_retries']:
        print(f"Job {job_id} exhausted retries. Moving to DLQ.")
        with db.get_db() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO dead_letter_queue (id, command, state, attempts, max_retries, created_at, updated_at, output)
                    VALUES (?, ?, 'dead', ?, ?, ?, ?, ?)
                    """,
                    (job['id'], job['command'], new_attempts, job['max_retries'], job['created_at'], now_utc.isoformat(), output)
                )
                conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
                conn.commit()
            except Exception as e:
                print(f"Error moving job {job_id} to DLQ: {e}")
                conn.rollback()
    else:
        current_config = config.load_config()
        backoff_base = current_config.get('base_backoff_seconds', 2)
        
        delay_seconds = backoff_base ** new_attempts
        run_at = now_utc + timedelta(seconds=delay_seconds)
        
        print(f"Job {job_id} failed. Retrying in {delay_seconds}s (Attempt {new_attempts}).")
        
        with db.get_db() as conn:
            conn.execute(
                """
                UPDATE jobs 
                SET state = 'pending', attempts = ?, run_at = ?, updated_at = ?, output = ?
                WHERE id = ?
                """,
                (new_attempts, run_at.isoformat(), now_utc.isoformat(), output, job_id)
            )
            conn.commit()

def update_job_state(job_id, state, output=""):
    """Generic helper to update a job's final state."""
    with db.get_db() as conn:
        conn.execute(
            "UPDATE jobs SET state = ?, output = ?, updated_at = ? WHERE id = ?",
            (state, output, datetime.now(timezone.utc).isoformat(), job_id)
        )
        conn.commit()