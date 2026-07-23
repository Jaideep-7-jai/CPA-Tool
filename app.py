from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from pathlib import Path
from datetime import datetime
import subprocess
import threading
import uuid
import shlex
import json
import os

try:
    import pymysql
except ImportError:
    pymysql = None

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-this-secret")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

DB_CONFIG = {
    "host": "zds-prod-jbdb3-vip.bo3.e-dialog.com",
    "user": "techuser",
    "password": "tech12#$",
    "database": "CUST_TECH_DB",
    "charset": "utf8mb4",
    "autocommit": True,
}

SCRIPT_NAME = os.getenv("SUPPRESSION_SCRIPT_PATH", str(BASE_DIR / "main.py"))
PYTHON_BIN = os.getenv("APP_PYTHON_BIN", "python3.6")


def get_db():
    if pymysql is None:
        raise RuntimeError("PyMySQL is not installed. Run: pip install pymysql")
    return pymysql.connect(**DB_CONFIG)


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    password_hash VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS requests (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    request_uuid    VARCHAR(64)  NOT NULL UNIQUE,
                    request_name    VARCHAR(255) NOT NULL UNIQUE,
                    request_type    ENUM('Suppression','Mailing','Doordash') NOT NULL DEFAULT 'Suppression',
                    client_name     VARCHAR(255) NOT NULL DEFAULT '',
                    created_by      INT NOT NULL,
                    criteria_type   ENUM('age','state','zips') NOT NULL,
                    comp_type       ENUM('greater','less','include','exclude') NOT NULL DEFAULT 'include',
                    channel         VARCHAR(100) NOT NULL DEFAULT 'ALL',
                    criteria_value  VARCHAR(500) NULL,
                    zip_file_path   VARCHAR(500) NULL,
                    output_dir      VARCHAR(255) NOT NULL,
                    overall_status  ENUM('inprogress','completed','failed') NOT NULL DEFAULT 'inprogress',
                    GREEN_STATUS    VARCHAR(50)  NULL,
                    BLUE_STATUS     VARCHAR(50)  NULL,
                    ARCAMAX_STATUS  VARCHAR(50)  NULL,
                    ORANGE_STATUS   VARCHAR(50)  NULL,
                    APPTNESS_STATUS VARCHAR(50)  NULL,
                    GREEN_FTP       VARCHAR(500) NULL,
                    BLUE_FTP        VARCHAR(500) NULL,
                    ARCAMAX_FTP     VARCHAR(500) NULL,
                    ORANGE_FTP      VARCHAR(500) NULL,
                    GREEN_FILECOUNT VARCHAR(50)  NULL,
                    BLUE_FILECOUNT  VARCHAR(50)  NULL,
                    ARCAMAX_FILECOUNT VARCHAR(50) NULL,
                    ORANGE_FILECOUNT VARCHAR(50) NULL,
                    GREEN_FILENAME  VARCHAR(500) NULL,
                    BLUE_FILENAME   VARCHAR(500) NULL,
                    ARCAMAX_FILENAME VARCHAR(500) NULL,
                    ORANGE_FILENAME VARCHAR(500) NULL,
                    command_text    TEXT NULL,
                    log_file        VARCHAR(500) NULL,
                    stdout_text     MEDIUMTEXT NULL,
                    stderr_text     MEDIUMTEXT NULL,
                    return_code     INT NULL,
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at      DATETIME NULL,
                    finished_at     DATETIME NULL,
                    FOREIGN KEY (created_by) REFERENCES users(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # ── Schema migration: add columns that may be absent in older tables ──
            # ALTER TABLE ... ADD COLUMN IF NOT EXISTS is safe to run repeatedly;
            # it is a no-op when the column already exists (MySQL 8+).
            # For MySQL 5.x / MariaDB we catch the duplicate-column error silently.
            _add_column_if_missing(cur, "requests", "APPTNESS_STATUS", "VARCHAR(50) NULL")
            _add_column_if_missing(cur, "requests", "GREEN_FTP",        "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "BLUE_FTP",         "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "ARCAMAX_FTP",      "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "ORANGE_FTP",       "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "GREEN_FILECOUNT",  "VARCHAR(50) NULL")
            _add_column_if_missing(cur, "requests", "BLUE_FILECOUNT",   "VARCHAR(50) NULL")
            _add_column_if_missing(cur, "requests", "ARCAMAX_FILECOUNT","VARCHAR(50) NULL")
            _add_column_if_missing(cur, "requests", "ORANGE_FILECOUNT", "VARCHAR(50) NULL")
            _add_column_if_missing(cur, "requests", "GREEN_FILENAME",   "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "BLUE_FILENAME",    "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "ARCAMAX_FILENAME", "VARCHAR(500) NULL")
            _add_column_if_missing(cur, "requests", "ORANGE_FILENAME",  "VARCHAR(500) NULL")

            # ── filedetails table ──────────────────────────────────────────
            # Stores per-request file metadata as JSON so the UI can still
            # display file info after the server paths have been cleaned up.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS filedetails (
                    id           INT AUTO_INCREMENT PRIMARY KEY,
                    requestid    VARCHAR(64)  NOT NULL,
                    requestname  VARCHAR(255) NOT NULL,
                    filespath    VARCHAR(500) NULL,
                    jsondata     MEDIUMTEXT   NULL,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                             ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_requestid (requestid)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            admin_user = os.getenv("APP_DEFAULT_ADMIN", "admin")
            admin_pass = os.getenv("APP_DEFAULT_ADMIN_PASSWORD", "admin123")
            cur.execute("SELECT id FROM users WHERE username=%s", (admin_user,))
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                    (admin_user, generate_password_hash(admin_pass)),
                )
    finally:
        conn.close()


def _add_column_if_missing(cur, table, column, column_def):
    """
    Add *column* to *table* if it does not already exist.

    Works on MySQL 5.x / MariaDB which lack 'ADD COLUMN IF NOT EXISTS'.
    Silently ignores error 1060 (Duplicate column name).
    """
    try:
        cur.execute(
            f"ALTER TABLE `{table}` ADD COLUMN `{column}` {column_def}"
        )
    except Exception as exc:
        # pymysql raises InternalError(1060, "Duplicate column name '...'")
        # when the column is already present – that is fine, skip it.
        if "1060" not in str(exc) and "Duplicate column" not in str(exc):
            raise


# ─── DB helpers ────────────────────────────────────────────────────

def get_user_by_username(username):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, password_hash FROM users WHERE username=%s", (username,))
            row = cur.fetchone()
            if not row:
                return None
            return {"id": row[0], "username": row[1], "password_hash": row[2]}
    finally:
        conn.close()


def is_request_name_taken(name):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM requests WHERE request_name=%s", (name,))
            return cur.fetchone() is not None
    finally:
        conn.close()


def insert_request(record):
    """Insert a new request row and return the auto-increment DB id."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO requests (
                    request_uuid, request_name, request_type, client_name,
                    created_by, criteria_type, comp_type, channel,
                    criteria_value, zip_file_path, output_dir, overall_status,
                    command_text, log_file, stdout_text, stderr_text,
                    return_code, started_at, finished_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    record["request_uuid"], record["request_name"], record["request_type"],
                    record["client_name"], record["created_by"], record["criteria_type"],
                    record["comp_type"], record["channel"], record.get("criteria_value"),
                    record.get("zip_file_path"), record["output_dir"], record["overall_status"],
                    record.get("command_text"), record.get("log_file"),
                    record.get("stdout_text", ""), record.get("stderr_text", ""),
                    record.get("return_code"), record.get("started_at"), record.get("finished_at")
                )
            )
            return cur.lastrowid
    finally:
        conn.close()


def upsert_filedetails(request_uuid, request_name, filespath, file_details):
    """
    Insert or update a row in the filedetails table.

    Parameters
    ----------
    request_uuid  : str  – UUID of the parent request (used as PK)
    request_name  : str  – human-readable request name
    filespath     : str  – path to the filedetails.json on disk
    file_details  : list – list of file-detail dicts (will be JSON-serialised)
    """
    jsondata = json.dumps(file_details, indent=2)
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filedetails (requestid, requestname, filespath, jsondata)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    requestname = VALUES(requestname),
                    filespath   = VALUES(filespath),
                    jsondata    = VALUES(jsondata)
                """,
                (request_uuid, request_name, filespath, jsondata)
            )
    finally:
        conn.close()


def fetch_filedetails(request_uuid):
    """
    Fetch filedetails row for a given request UUID.
    Returns parsed list of file-detail dicts, or [] if not found.
    Reads from DB (table) so it works even after disk cleanup.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT jsondata FROM filedetails WHERE requestid=%s",
                (request_uuid,)
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return []
            return json.loads(row[0])
    finally:
        conn.close()


def update_request_db(request_uuid, **kwargs):
    if not kwargs:
        return
    allowed = {"overall_status", "command_text", "log_file", "stdout_text",
               "stderr_text", "return_code", "started_at", "finished_at"}
    fields, values = [], []
    for key, value in kwargs.items():
        if key in allowed:
            fields.append(f"{key}=%s")
            values.append(value)
    if not fields:
        return
    values.append(request_uuid)
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE requests SET {', '.join(fields)} WHERE request_uuid=%s", values)
    finally:
        conn.close()


def fetch_all_requests(limit=200):
    """Fetch all requests including per-channel statuses and file details."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    r.request_uuid, r.request_name, r.request_type, r.client_name,
                    r.criteria_type, r.comp_type, r.channel, r.criteria_value,
                    r.zip_file_path, r.output_dir, r.overall_status,
                    r.created_at, r.started_at, r.finished_at, r.return_code,
                    u.username,
                    -- Per-channel statuses
                    r.GREEN_STATUS, r.BLUE_STATUS, r.ARCAMAX_STATUS, r.ORANGE_STATUS,
                    r.APPTNESS_STATUS,
                    -- Per-channel file details
                    r.GREEN_FTP,    r.BLUE_FTP,    r.ARCAMAX_FTP,    r.ORANGE_FTP,
                    r.GREEN_FILECOUNT, r.BLUE_FILECOUNT, r.ARCAMAX_FILECOUNT, r.ORANGE_FILECOUNT,
                    r.GREEN_FILENAME,  r.BLUE_FILENAME,  r.ARCAMAX_FILENAME,  r.ORANGE_FILENAME
                FROM requests r
                JOIN users u ON u.id = r.created_by
                ORDER BY r.id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            results = []
            for row in rows:
                results.append({
                    "request_uuid":     row[0],
                    "request_name":     row[1],
                    "request_type":     row[2],
                    "client_name":      row[3],
                    "criteria_type":    row[4],
                    "comp_type":        row[5],
                    "channel":          row[6],
                    "criteria_value":   row[7],
                    "zip_file_path":    row[8],
                    "output_dir":       row[9],
                    "overall_status":   row[10],
                    "created_at":       str(row[11]),
                    "started_at":       str(row[12]) if row[12] else None,
                    "finished_at":      str(row[13]) if row[13] else None,
                    "return_code":      row[14],
                    "username":         row[15],
                    # Per-channel statuses
                    "GREEN_STATUS":     row[16] or "",
                    "BLUE_STATUS":      row[17] or "",
                    "ARCAMAX_STATUS":   row[18] or "",
                    "ORANGE_STATUS":    row[19] or "",
                    "APPTNESS_STATUS":  row[20] or "",
                    # Per-channel file details
                    "GREEN_FTP":        row[21] or "",
                    "BLUE_FTP":         row[22] or "",
                    "ARCAMAX_FTP":      row[23] or "",
                    "ORANGE_FTP":       row[24] or "",
                    "GREEN_FILECOUNT":  row[25] or "",
                    "BLUE_FILECOUNT":   row[26] or "",
                    "ARCAMAX_FILECOUNT":row[27] or "",
                    "ORANGE_FILECOUNT": row[28] or "",
                    "GREEN_FILENAME":   row[29] or "",
                    "BLUE_FILENAME":    row[30] or "",
                    "ARCAMAX_FILENAME": row[31] or "",
                    "ORANGE_FILENAME":  row[32] or "",
                })
            return results
    finally:
        conn.close()


def fetch_dashboard_stats():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(overall_status='completed') AS completed,
                    SUM(overall_status='failed') AS failed,
                    SUM(overall_status='inprogress') AS inprogress,
                    COUNT(DISTINCT request_type) AS types
                FROM requests
            """)
            row = cur.fetchone()
            total = row[0] or 0
            completed = int(row[1] or 0)
            failed = int(row[2] or 0)
            inprogress = int(row[3] or 0)

            cur.execute("""
                SELECT request_type,
                       SUM(overall_status='completed') AS completed,
                       SUM(overall_status='failed') AS failed,
                       COUNT(*) AS total
                FROM requests GROUP BY request_type
            """)
            by_type = {}
            for r in cur.fetchall():
                by_type[r[0]] = {"completed": int(r[1] or 0), "failed": int(r[2] or 0), "total": int(r[3] or 0)}

            cur.execute("""
                SELECT criteria_type, COUNT(*) AS total
                FROM requests GROUP BY criteria_type
            """)
            by_criteria = {r[0]: int(r[1] or 0) for r in cur.fetchall()}

            cur.execute("""
                SELECT channel, COUNT(*) AS total
                FROM requests GROUP BY channel
            """)
            by_channel = {r[0]: int(r[1] or 0) for r in cur.fetchall()}

            return {
                "total": total,
                "completed": completed,
                "failed": failed,
                "inprogress": inprogress,
                "completed_pct": round(completed / total * 100) if total else 0,
                "failed_pct": round(failed / total * 100) if total else 0,
                "by_type": by_type,
                "by_criteria": by_criteria,
                "by_channel": by_channel,
            }
    finally:
        conn.close()


def build_chart_data(stats):
    by_type     = stats.get("by_type", {})
    by_criteria = stats.get("by_criteria", {})
    by_channel  = stats.get("by_channel", {})

    type_labels    = list(by_type.keys())
    type_values    = [v["total"]     for v in by_type.values()]
    type_completed = [v["completed"] for v in by_type.values()]
    type_failed    = [v["failed"]    for v in by_type.values()]

    return {
        "type_labels":      type_labels    if type_labels    else ["No data"],
        "type_values":      type_values    if type_values    else [0],
        "type_completed":   type_completed if type_completed else [0],
        "type_failed":      type_failed    if type_failed    else [0],
        "criteria_labels":  list(by_criteria.keys())   if by_criteria else ["No data"],
        "criteria_values":  list(by_criteria.values()) if by_criteria else [0],
        "channel_labels":   list(by_channel.keys())    if by_channel  else ["No data"],
        "channel_values":   list(by_channel.values())  if by_channel  else [0],
    }


def find_latest_log(output_dir):
    log_dir = Path(output_dir) / "logs"
    if not log_dir.exists():
        return None
    files = sorted(log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(files[0]) if files else None


def build_command(payload, db_id, uploaded_zip=None):
    criteria = payload["criteria_type"]

    cmd = [
        PYTHON_BIN, SCRIPT_NAME,
        "--request-type",  payload["request_type"],
        "--criteria-type", criteria,
        "--comp-type",     payload["comp_type"],
        "--channel",       payload["channel"],
        "--output-dir",    payload["output_dir"],
    ]

    if criteria in ("age", "state"):
        cmd.extend(["--request-id", str(db_id)])
        if criteria == "age":
            cmd.extend(["--age", str(payload["criteria_value"])])
        else:
            cmd.extend(["--states"] + payload["criteria_value"].split(","))
    else:
        cmd.extend(["--zip-file", str(uploaded_zip)])

    return cmd


def run_job(request_uuid, request_name, cmd, output_dir):
    """
    Execute the suppression job in a background thread.

    After the subprocess completes the function:
      1. Reads <run_dir>/logs/filedetails.json (written by utils.send_success_email)
      2. Persists the file details to the filedetails DB table via upsert_filedetails()
         so the UI can still render them after the server paths are cleaned.
    """
    update_request_db(
        request_uuid,
        overall_status="inprogress",
        started_at=now_str(),
        command_text=" ".join(shlex.quote(c) for c in cmd)
    )
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(BASE_DIR)
        )
        stdout_text = proc.stdout.decode("utf-8", "ignore") if proc.stdout else ""
        stderr_text = proc.stderr.decode("utf-8", "ignore") if proc.stderr else ""
        log_file    = find_latest_log(output_dir)
        update_request_db(
            request_uuid,
            overall_status="completed" if proc.returncode == 0 else "failed",
            finished_at=now_str(),
            return_code=proc.returncode,
            stdout_text=stdout_text[-20000:],
            stderr_text=stderr_text[-20000:],
            log_file=log_file,
        )

        # ── Persist filedetails to DB after job completes ──────────────
        # Scan all run sub-directories for a filedetails.json written by
        # utils.send_success_email and upsert the latest one found.
        if proc.returncode == 0:
            _persist_filedetails_to_db(request_uuid, request_name, output_dir)

    except Exception as exc:
        update_request_db(
            request_uuid,
            overall_status="failed",
            finished_at=now_str(),
            return_code=-1,
            stderr_text=str(exc),
            log_file=find_latest_log(output_dir),
        )


def _persist_filedetails_to_db(request_uuid, request_name, output_dir):
    """
    Walk <output_dir>/**/logs/filedetails.json, pick the most recently
    modified one, and upsert it into the filedetails table.
    """
    out_path  = Path(output_dir)
    json_files = sorted(
        out_path.glob("**/logs/filedetails.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not json_files:
        return
    latest_json = json_files[0]
    try:
        with open(str(latest_json), "r") as jf:
            file_details = json.load(jf)
        upsert_filedetails(request_uuid, request_name, str(latest_json), file_details)
    except Exception as exc:
        # Non-fatal — log but do not crash the background thread
        import logging
        logging.getLogger(__name__).error(
            f"_persist_filedetails_to_db failed for {request_uuid}: {exc}"
        )


# ─── Auth routes ────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        user = get_user_by_username(username)
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            session['username'] = user['username']
            return redirect(url_for('dashboard'))
        flash('Invalid username or password', 'error')
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    session.clear()
    return redirect(url_for('login'))


# ─── Page routes ────────────────────────────────────────────────────

@app.route('/')
@login_required
def dashboard():
    stats = fetch_dashboard_stats()
    recent = fetch_all_requests(limit=10)
    chart_data = build_chart_data(stats)
    return render_template('dashboard_home.html',
                           stats=stats,
                           recent=recent,
                           chart_data=chart_data,
                           username=session.get('username'),
                           active_page='dashboard')


@app.route('/new-request')
@login_required
def new_request():
    recent = fetch_all_requests(limit=20)
    return render_template('new_request.html',
                           recent=recent,
                           username=session.get('username'),
                           active_page='new_request')


@app.route('/requests')
@login_required
def requests_list():
    all_reqs = fetch_all_requests(limit=500)
    return render_template('requests_list.html',
                           requests=all_reqs,
                           username=session.get('username'),
                           active_page='requests')


@app.route('/home')
@login_required
def home():
    return redirect(url_for('dashboard'))


# ─── API routes ────────────────────────────────────────────────────

@app.route('/api/check-name')
@login_required
def api_check_name():
    name = request.args.get('name', '').strip()
    if not name:
        return jsonify({'available': False, 'error': 'Name is empty'})
    taken = is_request_name_taken(name)
    return jsonify({'available': not taken})


@app.route('/api/requests')
@login_required
def api_requests():
    return jsonify({'items': fetch_all_requests()})


@app.route('/api/analytics')
@login_required
def api_analytics():
    stats = fetch_dashboard_stats()
    recent = fetch_all_requests(limit=10)
    stats['recent_requests'] = recent
    return jsonify(stats)


@app.route('/api/filedetails/<request_uuid>')
@login_required
def api_filedetails(request_uuid):
    """Return file details for a given request UUID from the DB table."""
    return jsonify({'items': fetch_filedetails(request_uuid)})


@app.route('/api/submit', methods=['POST'])
@login_required
def submit_request():
    form = request.form
    zip_file_upload = request.files.get('zip_file')

    request_name   = form.get('request_name', '').strip()
    request_type   = form.get('request_type', '').strip()
    client_name    = form.get('client_name', '').strip()
    criteria_type  = form.get('criteria_type', '').strip().lower()
    comp_type      = form.get('comp_type', '').strip().lower()
    # Accept multi-channel value (e.g. "GREEN,BLUE" or "ALL")
    channels_raw   = form.get('channels', form.get('channel', 'ALL')).strip().upper()
    criteria_value = form.get('criteria_value', '').strip()

    if not request_name:
        return jsonify({'ok': False, 'error': 'Request Name is required.'}), 400
    if is_request_name_taken(request_name):
        return jsonify({'ok': False, 'error': f'Request name "{request_name}" is already taken.'}), 400
    if request_type not in {'Suppression', 'Mailing', 'Doordash'}:
        return jsonify({'ok': False, 'error': 'Invalid request type.'}), 400
    if criteria_type not in {'age', 'state', 'zips'}:
        return jsonify({'ok': False, 'error': 'Criteria type must be age, state, or zips.'}), 400

    if request_type == 'Doordash':
        client_name   = 'Doordash'
        criteria_type = 'zips'
        comp_type     = 'include'
        channels_raw  = 'ALL'

    if criteria_type == 'age' and comp_type not in {'greater', 'less'}:
        return jsonify({'ok': False, 'error': 'Age criteria requires comp type greater or less.'}), 400
    if criteria_type in {'state', 'zips'} and comp_type not in {'include', 'exclude'}:
        return jsonify({'ok': False, 'error': 'State/Zips criteria requires include or exclude comp type.'}), 400

    if criteria_type == 'age':
        if not criteria_value.isdigit():
            return jsonify({'ok': False, 'error': 'Valid age number is required.'}), 400
    elif criteria_type == 'state':
        states = [s.strip() for s in criteria_value.split(',') if s.strip()]
        if not states:
            return jsonify({'ok': False, 'error': 'At least one state code is required.'}), 400
        criteria_value = ','.join(s.upper() for s in states)
    else:
        criteria_value = None

    saved_zip = None
    if criteria_type == 'zips':
        if not zip_file_upload or not zip_file_upload.filename:
            return jsonify({'ok': False, 'error': 'A ZIP codes file is required for zips/Doordash requests.'}), 400
        suffix = Path(zip_file_upload.filename).suffix or '.csv'
        saved_zip = UPLOAD_DIR / f"{uuid.uuid4().hex}{suffix}"
        zip_file_upload.save(saved_zip)

    safe_name  = "".join(c if c.isalnum() or c in '-_' else '_' for c in request_name)
    output_dir = str(BASE_DIR / "output" / safe_name)

    payload = {
        "request_type":   request_type,
        "criteria_type":  criteria_type,
        "comp_type":      comp_type,
        "channel":        channels_raw,
        "criteria_value": criteria_value,
        "output_dir":     output_dir,
    }

    request_uuid = uuid.uuid4().hex

    db_id = insert_request({
        "request_uuid":   request_uuid,
        "request_name":   request_name,
        "request_type":   request_type,
        "client_name":    client_name,
        "created_by":     session['user_id'],
        "criteria_type":  criteria_type,
        "comp_type":      comp_type,
        "channel":        channels_raw,
        "criteria_value": criteria_value,
        "zip_file_path":  str(saved_zip) if saved_zip else None,
        "output_dir":     output_dir,
        "overall_status": "inprogress",
        "command_text":   None,
        "log_file":       None,
        "stdout_text":    "",
        "stderr_text":    "",
        "return_code":    None,
        "started_at":     None,
        "finished_at":    None,
    })

    cmd = build_command(payload, db_id, saved_zip)
    update_request_db(request_uuid, command_text=" ".join(shlex.quote(c) for c in cmd))

    # Pass request_name into run_job so it can populate filedetails table
    threading.Thread(
        target=run_job,
        args=(request_uuid, request_name, cmd, output_dir),
        daemon=True
    ).start()
    return jsonify({'ok': True, 'request_uuid': request_uuid, 'request_name': request_name})


@app.route('/health')
def health():
    return jsonify({'ok': True, 'time': now_str()})


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
