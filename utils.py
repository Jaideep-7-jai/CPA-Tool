#!/usr/bin/env python3
"""
Enhanced Utilities with Directory Safety + Run Isolation
"""

import os
import json
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
    """
    Create output directory if not exists + add timestamped run subdir.

    Layout:
        <output_dir>/run_age_<YYYYMMDD_HHMMSS>/
            logs/         <- combined + per-channel log files + filedetails.json
            FINAL_DIR/    <- final output CSVs/ZIPs that remain after run

    Each execution gets its own isolated folder with full date+time stamp.
    """
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_dir = path / f"run_age_{timestamp}"
    safe_dir.mkdir(exist_ok=True)

    # Pre-create logs/ and FINAL_DIR/ so they always exist
    (safe_dir / "logs").mkdir(exist_ok=True)
    (safe_dir / "FINAL_DIR").mkdir(exist_ok=True)

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


def build_file_details_json(final_files_dir, results=None):
    """
    Scan FINAL_FILES/ directory and build a list of file-detail dicts.
    Each entry contains:
        filename  - basename of the file
        size_mb   - size in MB (2 decimal places)
        row_count - from results dict if available, else 0
        channel   - channel name derived from results dict or filename
        path      - full absolute path (useful while disk is still intact)

    Also merges in row_count and channel from the `results` dict returned
    by process_age_state_request() so the JSON is as rich as possible.

    Returns a list of dicts sorted by channel name.
    """
    final_dir = Path(final_files_dir)
    file_details = []

    # Build a quick lookup: filename -> result entry
    result_by_file = {}
    if results and isinstance(results, dict):
        for ch, r in results.items():
            if r and r.get('file'):
                result_by_file[r['file']] = r

    if final_dir.exists():
        for fp in sorted(final_dir.iterdir()):
            if not fp.is_file():
                continue
            fname = fp.name
            size_mb = round(fp.stat().st_size / 1e6, 2)
            result_entry = result_by_file.get(fname, {})
            file_details.append({
                "filename":  fname,
                "size_mb":   size_mb,
                "row_count": result_entry.get('count', 0),
                "channel":   result_entry.get('channel', ''),
                "path":      str(fp),
            })
    else:
        logging.warning(f"build_file_details_json: directory not found: {final_dir}")

    return file_details


def _read_filedetails_json(json_path):
    """
    Safely read and parse a filedetails.json file.
    Returns parsed list on success, or empty list on any error.
    """
    try:
        with open(str(json_path), "r") as jf:
            return json.load(jf)
    except Exception as e:
        logging.error(f"Failed to read filedetails.json at {json_path}: {e}")
        return []


def send_success_email(subject, results, run_dir):
    """
    Success email with rich per-file details.

    Steps performed:
    1. Scan <run_dir>/FINAL_FILES/ to collect real file names + sizes.
    2. Merge row_count / channel from the `results` dict.
    3. Write <run_dir>/logs/filedetails.json  (persists for audit and DB
       upsert which is handled by app.py:_persist_filedetails_to_db after
       the subprocess returns).
    4. Build the email body by reading from filedetails.json (so mail
       content is always consistent with what was persisted to disk/DB).
    5. Send the email.

    Parameters
    ----------
    subject   : str   – email subject (e.g. "Request 43 completed")
    results   : dict  – channel -> result dict from process_age_state_request()
    run_dir   : str or Path – the run directory (contains FINAL_FILES/ and logs/)

    Returns
    -------
    file_details : list  – list of file-detail dicts (also written to JSON + DB)
    json_path    : Path  – path to the written filedetails.json
    """
    run_dir_path    = Path(run_dir)
    final_files_dir = run_dir_path / "FINAL_FILES"
    logs_dir        = run_dir_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Build file details list ─────────────────────────────
    file_details = build_file_details_json(final_files_dir, results)

    # ── 2. Write filedetails.json to logs/ ────────────────────
    json_path = logs_dir / "filedetails.json"
    try:
        with open(str(json_path), "w") as jf:
            json.dump(file_details, jf, indent=2)
        logging.info(f"filedetails.json written -> {json_path}")
    except Exception as e:
        logging.error(f"Failed to write filedetails.json: {e}")

    # ── 3. Read back from JSON file for email body ─────────────
    # Using the JSON file as the source of truth so the email is
    # always consistent with what is stored on disk (and later DB).
    email_file_details = _read_filedetails_json(json_path) if json_path.exists() else file_details

    # ── 4. Build email body ────────────────────────────────────
    if email_file_details:
        lines = []
        for fd in email_file_details:
            ch_tag = f"[{fd['channel']}] " if fd.get('channel') else ""
            rc     = f"  |  {fd['row_count']:,} rows" if fd.get('row_count') else ""
            lines.append(f". {ch_tag}{fd['filename']} ({fd['size_mb']:.1f} MB){rc}")
        file_section = "\n".join(lines)
    else:
        file_section = "(no files found in FINAL_FILES directory)"

    body = f"""SUCCESS: {subject}

Output Directory: {final_files_dir}

Generated files:
{file_section}

Processing completed successfully!"""

    send_email(subject, body, is_error=False)
    # Return both so callers that need the data or path can use them
    return file_details, json_path


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
