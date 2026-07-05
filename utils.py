#!/usr/bin/env python3
"""
Enhanced Utilities with Directory Safety + Run Isolation
"""

import os
import time
import gzip
import shutil
import logging
import smtplib
from typing import Tuple
import subprocess
from pathlib import Path
from typing import List, Dict, Optional, Union
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import DB_CONFIG, SENDER, RECIPIENT, CC_RECIPIENTS

def ensure_output_dir(output_dir, criteria_type):
    """Create output directory if not exists + add timestamped run subdir.

    Always creates a run_age_<HHMMSS> subdir inside output_dir so each
    execution gets its own isolated folder.
    """
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%H%M%S')
    safe_dir = path / f"run_age_{timestamp}"
    safe_dir.mkdir(exist_ok=True)
    logging.info(f"Using run directory: {safe_dir}")
    return safe_dir


def run_command(cmd, cwd=None, timeout=3600, stdout=None):
    """Execute command with full error handling"""
    cmd_str = ' '.join(cmd) if isinstance(cmd, list) else cmd
    logging.info(f"Running: {cmd_str}")
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            shell=isinstance(cmd, str),
            cwd=cwd,
            stdout=subprocess.PIPE if stdout is None else stdout,
            stderr=subprocess.PIPE if stdout is None else None,
            universal_newlines=True,
            timeout=timeout,
        )

        elapsed = time.time() - start_time
        logging.info(f"Completed: {elapsed:.1f}s (code: {result.returncode})")

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed (code {result.returncode}):\n{result.stderr}"
            )

        return result.stdout.strip() if result.stdout else ""

    except subprocess.TimeoutExpired:
        raise TimeoutError(f"Timeout: {timeout}s")


def download_combine(s3_path, output_file, cwd):
    """Download S3 -> Combine -> Count -> Zip -> Return (zip_filename, line_count)"""
    logging.info(f"{s3_path} -> {output_file}")
    start_total = time.time()

    run_command(["aws", "s3", "cp", s3_path, ".", "--recursive", "--quiet"], cwd=cwd)

    data_files = sorted(Path(cwd).glob("data*"))
    if not data_files:
        raise RuntimeError("No data files downloaded")

    start = time.time()
    output_path = Path(cwd) / output_file
    line_count = 0

    with open(output_path, "w") as out_f:
        for gz_file in data_files:
            with gzip.open(gz_file, "rt") as in_f:
                for line in in_f:
                    clean_line = line.replace('"', '').strip()
                    if clean_line:
                        out_f.write(clean_line + '\n')
                        line_count += 1

    elapsed = time.time() - start
    logging.info(f"Combined {line_count:,} lines in {elapsed:.1f}s")

    start = time.time()
    zip_name = f"{output_file}.zip"
    zip_path = Path(cwd) / zip_name
    shutil.make_archive(
        base_name=zip_path.with_suffix('').as_posix(),
        format='zip',
        root_dir=cwd,
        base_dir=output_file,
    )

    if not zip_path.exists():
        raise RuntimeError("Zip file not created")

    elapsed = time.time() - start
    logging.info(f"Zipped file in {elapsed:.1f}s")

    size_mb = zip_path.stat().st_size / 1e6
    logging.info(f"{zip_name}: {size_mb:.1f}MB ({line_count:,} records)")

    start = time.time()
    if output_path.exists():
        output_path.unlink()
    for f in data_files:
        f.unlink()
    elapsed = time.time() - start
    logging.info(f"Cleanup in {elapsed:.1f}s")

    total_elapsed = time.time() - start_total
    logging.info(f"Total: {total_elapsed:.1f}s")

    return zip_name, line_count


def get_db_connection(channel):
    """Get DB connection params from config"""
    config = DB_CONFIG.get(channel)
    if not config:
        raise ValueError(f"Unknown channel: {channel}")
    return config


def send_email(subject, body_text, is_error=False):
    """Email notification"""
    try:
        msg = MIMEMultipart()
        msg['Subject'] = f"[{'ERROR' if is_error else 'SUCCESS'}] {subject}"
        msg['From'] = SENDER
        msg['To'] = RECIPIENT
        msg['Cc'] = CC_RECIPIENTS

        msg_body = MIMEMultipart('alternative')
        textpart = MIMEText(body_text, 'plain')
        msg_body.attach(textpart)
        msg.attach(msg_body)

        all_recipients = [RECIPIENT] + CC_RECIPIENTS.split(',')
        server = smtplib.SMTP('localhost')
        server.sendmail(SENDER, all_recipients, msg.as_string())
        server.quit()
        logging.info(f"{'ERROR' if is_error else 'SUCCESS'} email: {subject}")
    except Exception as e:
        logging.error(f"Email failed: {e}")


def send_success_email(subject, files, output_dir):
    """Success email with output dir info (files already uploaded to FTP)"""
    safe_files = [f for f in files if os.path.exists(f)]
    if safe_files:
        file_details = "\n".join(
            [f"{os.path.basename(f)} ({os.path.getsize(f)/1e6:.1f} MB)" for f in safe_files]
        )
    else:
        file_details = "Files uploaded to FTP (local copies removed)"
    body = f"""SUCCESS: {subject}

Output Directory: {output_dir}

Generated files:
{file_details}

Processing completed successfully!"""
    send_email(subject, body, is_error=False)


def send_error_email(subject, error_msg):
    """Error email"""
    body = f"ERROR: {subject}\n\n{error_msg}"
    send_email(subject, body, is_error=True)


def load_zip_to_pg(zip_file, table_name, pg_config, truncate=True):
    """Load ZIP directly to PostgreSQL staging table"""
    import psycopg2
    from zipfile import ZipFile

    zip_path = Path(zip_file)
    logger = logging.getLogger("zip_pg_loader")

    conn = psycopg2.connect(
        host=pg_config['host'],
        database=pg_config['db'],
        user=pg_config['user'],
    )

    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"CREATE TABLE {table_name} (zip_code VARCHAR);")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        with open(zip_file, "r") as f:
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cur.fetchone()[0]
        logging.info(f"PG Load complete: {count:,} records in {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()
    return count
