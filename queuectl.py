import click
import json
import db
import sqlite3
from datetime import datetime, timezone
import worker
from multiprocessing import Process
import os
import sys
from prettytable import PrettyTable
import config

@click.group()
def cli():
    """A CLI for the QueueCTL Job Queue System."""
    pass

@cli.command()
@click.argument('job_spec_json', type=str)
def enqueue(job_spec_json):
    """Add a new job to the queue."""
    try:
        spec = json.loads(job_spec_json)
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON provided.", err=True)
        return

    job_id = spec.get('id')
    if not job_id:
        click.echo("Error: 'id' is a required field.", err=True)
        return
        
    command = spec.get('command')
    if not command:
        click.echo("Error: 'command' is a required field.", err=True)
        return

    now = datetime.now(timezone.utc).isoformat()
    
    app_config = config.load_config()
    default_retries = app_config.get('default_max_retries', 3)
    max_retries = spec.get('max_retries', default_retries)

    try:
        with db.get_db() as conn:
            conn.execute(
                """
                INSERT INTO jobs (id, command, max_retries, created_at, updated_at, run_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (job_id, command, max_retries, now, now, now) 
            )
            conn.commit()
        click.echo(f"Job {job_id} enqueued successfully.")
    except sqlite3.IntegrityError:
        click.echo(f"Error: Job with ID {job_id} already exists.", err=True)
    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)


@cli.group()
def worker_group():
    """Manage worker processes."""
    pass

@worker_group.command(name="start")
@click.option('--count', default=1, help='Number of worker processes to start.')
def start(count):
    """Start one or more worker processes."""
    if count < 1:
        click.echo("Error: --count must be at least 1.", err=True)
        return

    click.echo(f"Starting {count} worker(s)...")
    processes = []
    for _ in range(count):
        p = Process(target=worker.run_worker) 
        p.start()
        processes.append(p)
    
    click.echo("Workers are running. Press Ctrl+C to stop.")
    
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        click.echo("\nGraceful shutdown requested...")
        for p in processes:
            p.terminate()
            p.join()
        click.echo("All workers stopped.")

cli.add_command(worker_group, name="worker")

@cli.command()
def status():
    """Show summary of all job states."""
    click.echo("--- Job Status ---")
    try:
        with db.get_db() as conn:
            cursor = conn.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
            states = cursor.fetchall()
            
            cursor.execute("SELECT COUNT(*) FROM dead_letter_queue")
            dlq_count_row = cursor.fetchone()
            dlq_count = dlq_count_row[0] if dlq_count_row else 0

        if not states and dlq_count == 0:
            click.echo("No jobs in the system.")
            return

        table = PrettyTable()
        table.field_names = ["State", "Count"]
        for row in states:
            table.add_row([row['state'], row['COUNT(*)']])
        
        if dlq_count > 0:
            table.add_row(["dead", dlq_count])
            
        print(table)
        
    except Exception as e:
        click.echo(f"Error reading database: {e}", err=True)

@cli.command(name="list")
@click.option('--state', default='pending', help='Filter jobs by state (pending, completed, etc.)')
def list_jobs(state):
    """List jobs by state."""
    click.echo(f"--- Jobs in '{state}' state ---")
    try:
        with db.get_db() as conn:
            cursor = conn.execute("SELECT * FROM jobs WHERE state = ?", (state,))
            jobs = cursor.fetchall()

        if not jobs:
            click.echo("No jobs found with this state.")
            return

        table = PrettyTable()
        table.field_names = ["ID", "Command", "Attempts", "Run At"]
        for job in jobs:
            command_display = (job['command'][:37] + '...') if len(job['command']) > 40 else job['command']
            table.add_row([job['id'], command_display, job['attempts'], job['run_at']])
        print(table)
        
    except Exception as e:
        click.echo(f"Error reading database: {e}", err=True)

@cli.group()
def dlq():
    """Manage the Dead Letter Queue (DLQ)."""
    pass

@dlq.command(name="list")
def dlq_list():
    """List jobs in the DLQ."""
    click.echo("--- Dead Letter Queue ---")
    try:
        with db.get_db() as conn:
            cursor = conn.execute("SELECT * FROM dead_letter_queue")
            jobs = cursor.fetchall()

        if not jobs:
            click.echo("DLQ is empty.")
            return

        table = PrettyTable()
        table.field_names = ["ID", "Command", "Attempts", "Failed At", "Output"]
        for job in jobs:
            command_display = (job['command'][:37] + '...') if len(job['command']) > 40 else job['command']
            output_display = (job['output'][:47] + '...') if job['output'] and len(job['output']) > 50 else job['output']
            
            table.add_row([job['id'], command_display, job['attempts'], job['updated_at'], output_display.replace("\n", " ") if output_display else ""])
        print(table)
        
    except Exception as e:
        click.echo(f"Error reading database: {e}", err=True)

@dlq.command(name="retry")
@click.argument('job_id')
def dlq_retry(job_id):
    """Move a specific job from the DLQ back to the 'pending' queue."""
    with db.get_db() as conn:
        cursor = conn.execute("SELECT * FROM dead_letter_queue WHERE id = ?", (job_id,))
        job = cursor.fetchone()

        if not job:
            click.echo(f"Error: Job {job_id} not found in DLQ.", err=True)
            return

        try:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """
                INSERT INTO jobs (id, command, max_retries, created_at, updated_at, run_at, state, attempts, output)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?)
                """,
                (job['id'], job['command'], job['max_retries'], job['created_at'], now, now, "Retrying from DLQ...")
            )
            
            conn.execute("DELETE FROM dead_letter_queue WHERE id = ?", (job_id,))
            
            conn.commit()
            click.echo(f"Job {job_id} has been moved from DLQ back to 'pending' queue.")

        except sqlite3.IntegrityError:
            click.echo(f"Error: Job {job_id} already exists in the main 'jobs' queue.", err=True)
            conn.rollback()
        except Exception as e:
            click.echo(f"An error occurred: {e}", err=True)
            conn.rollback()

cli.add_command(dlq)

@cli.group()
def config_group():
    """View or set configuration values."""
    pass

@config_group.command(name="show")
def config_show():
    """Show the current configuration."""
    app_config = config.load_config()
    click.echo(json.dumps(app_config, indent=4))

@config_group.command(name="set")
@click.argument('key')
@click.argument('value')
def config_set(key, value):
    """Set a configuration value (e.g., default_max_retries 5)."""
    app_config = config.load_config()
    
    try:
        value = int(value)
    except ValueError:
        try:
            value = float(value)
        except ValueError:
            pass
    
    app_config[key] = value
    
    if config.save_config(app_config):
        click.echo(f"Config updated: {key} = {value}")
    else:
        click.echo(f"Error: Could not save config.", err=True)

cli.add_command(config_group, name="config")

if __name__ == "__main__":
    cli()