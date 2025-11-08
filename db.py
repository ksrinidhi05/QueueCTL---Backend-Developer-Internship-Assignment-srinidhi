import sqlite3
import os

DB_NAME = 'queue.db'

def get_db():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    if os.path.exists(DB_NAME):
        print("Database already exists. Skipping initialization.")
    else:
        with get_db() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    run_at TEXT NOT NULL,
                    output TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT DEFAULT 'dead',
                    attempts INTEGER,
                    max_retries INTEGER,
                    created_at TEXT,
                    updated_at TEXT,
                    output TEXT
                )
            ''')
            conn.commit()
            print("Database initialized.")

if __name__ == "__main__":
    init_db()