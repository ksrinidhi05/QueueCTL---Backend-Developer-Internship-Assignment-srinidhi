import sqlite3

conn = sqlite3.connect('queue.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("--- Jobs Table ---")
cursor.execute("SELECT * FROM jobs")
jobs = cursor.fetchall()

if not jobs:
    print("No jobs found.")
else:
    for job in jobs:
        print(dict(job)) # Print as a clear dictionary

conn.close()