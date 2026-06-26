from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from pathlib import Path
from datetime import datetime
import subprocess
import threading
import uuid
import shlex
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
                    request_uuid VARCHAR(64) NOT NULL UNIQUE,
                    created_by INT NOT NULL,
                    criteria ENUM('age','state','zip') NOT NULL,
                    comp ENUM('greater','less','include','exclude') NOT NULL,
                    age INT NULL,
                    states VARCHAR(500) NULL,
                    zip_file_path VARCHAR(500) NULL,
                    output_dir VARCHAR(255) NOT NULL,
                    status ENUM('inprogress','completed','failed') NOT NULL DEFAULT 'inprogress',
                    command_text TEXT NULL,
                    log_file VARCHAR(500) NULL,
                    stdout_text MEDIUMTEXT NULL,
                    stderr_text MEDIUMTEXT NULL,
                    return_code INT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at DATETIME NULL,
                    finished_at DATETIME NULL,
                    FOREIGN KEY (created_by) REFERENCES users(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            admin_user = os.getenv("APP_DEFAULT_ADMIN", "admin")
            admin_pass = os.getenv("APP_DEFAULT_ADMIN_PASSWORD", "admin123")
            cur.execute("SELECT id FROM users WHERE username=%s", (admin_user,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                    (admin_user, generate_password_hash(admin_pass)),
                )
    finally:
        conn.close()


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


def insert_request(record):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO requests (
                    request_uuid, created_by, criteria, comp, age, states, zip_file_path,
                    output_dir, status, command_text, log_file, stdout_text, stderr_text,
                    return_code, started_at, finished_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    record["request_uuid"], record["created_by"], record["criteria"], record["comp"],
                    record.get("age"), record.get("states"), record.get("zip_file_path"),
                    record["output_dir"], record["status"], record.get("command_text"),
                    record.get("log_file"), record.get("stdout_text", ""), record.get("stderr_text", ""),
                    record.get("return_code"), record.get("started_at"), record.get("finished_at")
                )
            )
    finally:
        conn.close()


def update_request_db(request_uuid, **kwargs):
    if not kwargs:
        return
    allowed = {"status", "command_text", "log_file", "stdout_text", "stderr_text", "return_code", "started_at", "finished_at"}
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


def fetch_requests_for_sidebar():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.request_uuid, r.criteria, r.comp, r.age, r.states, r.zip_file_path,
                       r.output_dir, r.status, r.created_at, r.return_code, u.username
                FROM requests r
                JOIN users u ON u.id = r.created_by
                ORDER BY r.id DESC
            """)
            rows = cur.fetchall()
            return [{
                "request_uuid": row[0], "criteria": row[1], "comp": row[2], "age": row[3],
                "states": row[4], "zip_file_path": row[5], "output_dir": row[6], "status": row[7],
                "created_at": str(row[8]), "return_code": row[9], "username": row[10]
            } for row in rows]
    finally:
        conn.close()


def validate_payload(form, has_file=False):
    criteria = form.get("criteria", "").strip().lower()
    comp = form.get("comp", "").strip().lower()
    if criteria not in {"age", "state", "zip"}:
        return False, "Criteria must be one of age, state, or zip."
    if criteria == "age":
        if comp not in {"greater", "less"}:
            return False, "Age supports only greater or less."
        if not form.get("age", "").strip().isdigit():
            return False, "Valid age is required."
    else:
        if comp not in {"include", "exclude"}:
            return False, "State and zip support only include or exclude."
    if criteria == "state":
        states = [s.strip() for s in form.get("states", "").split(",") if s.strip()]
        if not states:
            return False, "At least one state is required."
    if criteria == "zip" and not has_file:
        return False, "ZIP file upload is required for zip criteria."
    return True, ""


def normalize_payload(form):
    criteria = form.get("criteria", "").strip().lower()
    payload = {
        "criteria": criteria,
        "comp": form.get("comp", "").strip().lower(),
        "output_dir": form.get("output_dir", "./output").strip() or "./output",
    }
    if criteria == "age":
        payload["age"] = int(form.get("age"))
    elif criteria == "state":
        payload["states"] = [s.strip().upper() for s in form.get("states", "").split(",") if s.strip()]
    return payload


def find_latest_log(output_dir):
    log_dir = Path(output_dir) / "logs"
    if not log_dir.exists():
        return None
    files = sorted(log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(files[0]) if files else None


def build_command(payload, uploaded_zip=None):
    cmd = [PYTHON_BIN, SCRIPT_NAME, "--criteria", payload["criteria"], "--comp", payload["comp"], "--output-dir", payload["output_dir"]]
    if payload["criteria"] == "age":
        cmd.extend(["--age", str(payload["age"])])
    elif payload["criteria"] == "state":
        cmd.append("--states")
        cmd.extend(payload["states"])
    else:
        cmd.extend(["--zip-file", str(uploaded_zip)])
    return cmd


def run_job(request_uuid, cmd, output_dir):
    update_request_db(
        request_uuid,
        status="inprogress",
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
        update_request_db(
            request_uuid,
            status="completed" if proc.returncode == 0 else "failed",
            finished_at=now_str(),
            return_code=proc.returncode,
            stdout_text=stdout_text[-20000:],
            stderr_text=stderr_text[-20000:],
            log_file=find_latest_log(output_dir),
        )
    except Exception as exc:
        update_request_db(
            request_uuid,
            status="failed",
            finished_at=now_str(),
            return_code=-1,
            stderr_text=str(exc),
            log_file=find_latest_log(output_dir),
        )

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        user = get_user_by_username(username)
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            session['username'] = user['username']
            return redirect(url_for('home'))
        flash('Invalid username or password', 'error')
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
@login_required
def home():
    return render_template('home.html', requests_data=fetch_requests_for_sidebar(), username=session.get('username'))


@app.route('/api/requests')
@login_required
def api_requests():
    return jsonify({'items': fetch_requests_for_sidebar()})


@app.route('/api/submit', methods=['POST'])
@login_required
def submit_request():
    zip_file = request.files.get('zip_file')
    ok, message = validate_payload(request.form, has_file=bool(zip_file and zip_file.filename))
    if not ok:
        return jsonify({'ok': False, 'error': message}), 400
    payload = normalize_payload(request.form)
    saved_zip = None
    if payload['criteria'] == 'zip' and zip_file and zip_file.filename:
        suffix = Path(zip_file.filename).suffix or '.csv'
        saved_zip = UPLOAD_DIR / f"{uuid.uuid4().hex}{suffix}"
        zip_file.save(saved_zip)
    request_uuid = uuid.uuid4().hex
    cmd = build_command(payload, saved_zip)
    insert_request({
        'request_uuid': request_uuid,
        'created_by': session['user_id'],
        'criteria': payload['criteria'],
        'comp': payload['comp'],
        'age': payload.get('age'),
        'states': ','.join(payload.get('states', [])) if payload.get('states') else None,
        'zip_file_path': str(saved_zip) if saved_zip else None,
        'output_dir': payload['output_dir'],
        'status': 'inprogress',
        'command_text': ' '.join(shlex.quote(c) for c in cmd),
        'log_file': None,
        'stdout_text': '',
        'stderr_text': '',
        'return_code': None,
        'started_at': None,
        'finished_at': None,
    })
    threading.Thread(target=run_job, args=(request_uuid, cmd, payload['output_dir']), daemon=True).start()
    return jsonify({'ok': True, 'request_uuid': request_uuid})


@app.route('/health')
def health():
    return jsonify({'ok': True, 'time': now_str()})


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
