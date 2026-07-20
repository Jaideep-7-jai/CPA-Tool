#!/usr/bin/env python3
"""
AGE STATE Processing - unique request-driven channel execution.
Supports single-channel or all-channel execution, with parallel execution
only when multiple channels are requested.

Run directory layout
--------------------

    <output_dir>/run_age_<YYYYMMDD_HHMMSS>/
        logs/
            age_<YYYYMMDD_HHMMSS>.log          <- MAIN log  (orchestrator only)
            GREEN_age_<YYYYMMDD_HHMMSS>.log    <- GREEN channel log (all GREEN work)
            BLUE_age_<YYYYMMDD_HHMMSS>.log
            ARCAMAX_age_<YYYYMMDD_HHMMSS>.log
            ORANGE_age_<YYYYMMDD_HHMMSS>.log
        FINAL_FILES/
            <client>_<request_type>_GREEN_<date>.csv   <- final deliverables only
            <client>_<request_type>_BLUE_<date>.csv
            ...
        GREEN_tmp/    <- temp working dir; deleted after final file moved to FINAL_FILES
        BLUE_tmp/
        ARCAMAX_tmp/
        ORANGE_tmp/

Log routing rules
-----------------
  age_main logger   → age_<ts>.log ONLY
      • All log.info/error/exception calls inside process_age_state_request
      • Explicit channel summary lines (started / completed / failed) written
        by the orchestrator so the main log has a high-level picture
      • propagate = False  →  nothing leaks into root logger

  age_<CHANNEL> logger  → <CH>_age_<ts>.log ONLY
      • Every log call inside process_green_blue / process_arcamax / process_orange
        and all helpers they invoke (_insert_into_perm_table, _export_*, _drop_*, etc.)
      • propagate = False  →  channel logs never bleed into main log
      • Channel processors receive their logger as a parameter; helpers also receive it

Processing flow per channel
---------------------------
  1. INSERT raw query results directly into permanent Snowflake table
       APT_CPA_<CHANNEL>_<YYYYMMDD>  (no S3 write for raw data)
  2. Export FINAL FILE  -> path_FINAL  (S3)
       DISTINCT email (with header)                  [GREEN / BLUE / ARCAMAX]
       DISTINCT email_address, account_name (header) [ORANGE]
  3. Export COMPLETE DATA FILE -> path_COMPLETE (S3)
       email, condition_field                         [GREEN / BLUE / ARCAMAX]
       email_address, condition_field, account_name   [ORANGE]
  4. DROP the permanent table
  5. Download FINAL FILE from path_FINAL -> combine -> <CH>_tmp/
  6. Move combined file to FINAL_FILES/  (temp dir deleted after move)
  7. FTP upload from FINAL_FILES/
     ORANGE suppression: email-only CSV
     ORANGE mailing    : ESP-wise split CSVs in a ZIP
"""

import os
import time
import shutil
import logging
import pymysql
import subprocess
import shlex
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import (
    run_command,
    send_success_email,
    send_error_email,
    ensure_output_dir,
)

DB_CONFIG = {
    "host": "zds-prod-jbdb3-vip.bo3.e-dialog.com",
    "user": "techuser",
    "password": "tech12#$",
    "database": "CUST_TECH_DB",
    "charset": "utf8mb4",
    "autocommit": True,
}

CHANNELS = ["GREEN", "BLUE", "ARCAMAX", "ORANGE"]

_FMT = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

# ---------------------------------------------------------------------------
# DB retry constants
# ---------------------------------------------------------------------------

_DB_RETRY_ATTEMPTS   = 3    # total tries (1 original + 2 retries)
_DB_RETRY_BASE_DELAY = 2    # seconds; doubles on each retry: 2s, 4s


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _file_handler(log_path: Path) -> logging.FileHandler:
    """Create a FileHandler with the standard formatter."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.FileHandler(str(log_path))
    h.setFormatter(_FMT)
    return h


def setup_main_logging(run_dir: Path, criteria_type: str) -> logging.Logger:
    """
    Create (or reset) the MAIN logger 'age_main'.

    Writes to:  <run_dir>/logs/age_<YYYYMMDD_HHMMSS>.log

    propagate=False ensures nothing from this logger leaks into root or any
    channel logger, and channel messages never appear here unless the
    orchestrator explicitly calls logger_main.info(...).
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / f"age_{ts}.log"

    lg = logging.getLogger("age_main")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    lg.handlers.clear()
    lg.addHandler(_file_handler(log_file))
    return lg


def setup_channel_logging(run_dir: Path, channel_name: str,
                           criteria_type: str) -> logging.Logger:
    """
    Create (or reset) a per-channel logger  'age_<CHANNEL>'.

    Writes to:  <run_dir>/logs/<CHANNEL>_age_<YYYYMMDD_HHMMSS>.log

    propagate=False ensures channel logs never bleed into the main log or
    root logger.  All work done inside a channel processor (and the helpers
    it calls) must use this logger.
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / f"{channel_name.upper()}_age_{ts}.log"

    lg = logging.getLogger(f"age_{channel_name.upper()}")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    lg.handlers.clear()
    lg.addHandler(_file_handler(log_file))
    return lg


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def get_dob_cutoff(min_age, comp_type):
    if isinstance(min_age, str):
        min_age = int(min_age)
    cutoff_date = date.today() - timedelta(days=365.25 * min_age)
    return cutoff_date.strftime("%Y-%m-%d")


def get_db():
    """Open a fresh pymysql connection (no retry — use get_db_with_retry)."""
    return pymysql.connect(**DB_CONFIG)


def get_db_with_retry(log=None):
    """
    Open a pymysql connection with exponential-backoff retry.

    Retries up to _DB_RETRY_ATTEMPTS times on OperationalError (covers
    errno 2013 'Lost connection' and errno 2003 'Can't connect').
    Waits _DB_RETRY_BASE_DELAY * 2^attempt seconds between tries.

    Parameters
    ----------
    log : logging.Logger or None
        If provided, warning messages are emitted on each failed attempt.

    Returns
    -------
    pymysql.Connection
    """
    last_exc = None
    for attempt in range(_DB_RETRY_ATTEMPTS):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.err.OperationalError as exc:
            last_exc = exc
            if attempt < _DB_RETRY_ATTEMPTS - 1:
                delay = _DB_RETRY_BASE_DELAY * (2 ** attempt)
                msg = (
                    f"DB connect failed (attempt {attempt + 1}/{_DB_RETRY_ATTEMPTS}): "
                    f"{exc} — retrying in {delay}s"
                )
                if log:
                    log.warning(msg)
                else:
                    print(msg)
                time.sleep(delay)
    raise last_exc


def fetch_request_details(request_id):
    conn = get_db_with_retry()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    client_name,
                    request_type,
                    request_name,
                    criteria_type,
                    criteria_value,
                    comp_type,
                    output_dir
                FROM requests
                WHERE id=%s
                """,
                (request_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def update_request_status(request_id, status, status_column, log):
    """
    Update DB status with retry and write to the supplied logger.

    Uses get_db_with_retry so transient connection resets (errno 104 /
    OperationalError 2013) do not crash the channel immediately.
    Retries up to _DB_RETRY_ATTEMPTS times on OperationalError.
    """
    last_exc = None
    for attempt in range(_DB_RETRY_ATTEMPTS):
        try:
            conn = get_db_with_retry(log)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE requests SET {status_column}=%s WHERE id=%s",
                        (status, request_id),
                    )
                conn.commit()
            finally:
                conn.close()
            log.info(f"{status_column} -> '{status}'  (request_id={request_id})")
            return  # success
        except pymysql.err.OperationalError as exc:
            last_exc = exc
            if attempt < _DB_RETRY_ATTEMPTS - 1:
                delay = _DB_RETRY_BASE_DELAY * (2 ** attempt)
                log.warning(
                    f"update_request_status failed (attempt {attempt + 1}/"
                    f"{_DB_RETRY_ATTEMPTS}): {exc} — retrying in {delay}s"
                )
                time.sleep(delay)
        except Exception:
            log.exception(f"Failed updating {status_column}")
            raise
    # All retries exhausted
    log.error(
        f"update_request_status gave up after {_DB_RETRY_ATTEMPTS} attempts: "
        f"{last_exc}"
    )
    raise last_exc


def _build_common_context(request_id, channel_name, run_dir: Path):
    """
    Build the shared context dict for a channel run.

    Directory layout inside run_dir
    --------------------------------
        run_dir/
            logs/           <- created by setup_*_logging
            FINAL_FILES/    <- final deliverables only; never deleted
            <CH>_tmp/       <- temp dir for S3 download + combine; deleted after use

    Snowflake permanent table:
        perm_table  = APT_CPA_<CHANNEL>_<YYYYMMDD>

    S3 export paths:
        path_FINAL    = S3_BASE/<request_type>/<date>/<request_name>/<channel>_FINAL
        path_COMPLETE = S3_BASE/<request_type>/<date>/<request_name>/<channel>_COMPLETE
    """
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found")

    client_name    = request_data["client_name"]
    request_type   = request_data["request_type"]
    request_name   = request_data["request_name"]
    criteria_type  = request_data["criteria_type"]
    criteria_value = request_data["criteria_value"]
    comp_type      = request_data["comp_type"]

    # FINAL_FILES lives directly inside run_dir
    final_files_dir = run_dir / "FINAL_FILES"
    final_files_dir.mkdir(parents=True, exist_ok=True)

    # Per-channel temp working dir inside run_dir
    channel_tmp = run_dir / f"{channel_name.upper()}_tmp"
    channel_tmp.mkdir(parents=True, exist_ok=True)

    path_date  = datetime.now().strftime("%Y%m%d")
    perm_table = f"APT_CPA_{channel_name.upper()}_{path_date}"

    path_FINAL    = (
        f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}_FINAL"
    )
    path_COMPLETE = (
        f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}_COMPLETE"
    )

    output_file = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"

    return {
        "request_data"   : request_data,
        "client_name"    : client_name,
        "request_type"   : request_type,
        "request_name"   : request_name,
        "criteria_type"  : criteria_type,
        "criteria_value" : criteria_value,
        "comp_type"      : comp_type,
        "final_files_dir": final_files_dir,   # FINAL_FILES/  <- final files only
        "channel_tmp"    : channel_tmp,        # <CH>_tmp/     <- deleted after use
        "path_date"      : path_date,
        "path_FINAL"     : path_FINAL,
        "path_COMPLETE"  : path_COMPLETE,
        "output_file"    : output_file,
        "perm_table"     : perm_table,
    }


def _count_file_lines(file_path):
    """Fast line count using wc -l."""
    cmd    = f"wc -l < {shlex.quote(file_path)}"
    result = subprocess.check_output(
        cmd, shell=True, universal_newlines=True
    ).strip()
    return int(result) if result else 0


def _download_and_combine(s3_path, download_dir, work_dir,
                           output_file, channel_name, log):
    """
    Download gzipped S3 parts into *download_dir* and concatenate into one
    pipe-delimited file at <work_dir>/<output_file>.
    Non-ORANGE: keep col-1 (email) only.  ORANGE: keep all columns.
    Header row (written by Snowflake COPY INTO HEADER=TRUE) is stripped.
    """
    download_dir = Path(download_dir)
    work_dir     = Path(work_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Downloading from S3: {s3_path}  ->  {download_dir}")
    run_command(
        ["aws", "s3", "cp", s3_path, str(download_dir), "--recursive", "--quiet"]
    )

    downloaded = sorted(download_dir.glob("data*"))
    if not downloaded:
        raise RuntimeError(
            f"aws s3 cp from {s3_path} downloaded 0 files into {download_dir}."
        )

    out_path = work_dir / output_file
    if channel_name != "ORANGE":
        run_command(
            f"zcat {shlex.quote(str(download_dir) + '/')}data* "
            f"| tail -n +2 "
            f"| cut -d'|' -f1 > {shlex.quote(str(out_path))}"
        )
    else:
        run_command(
            f"zcat {shlex.quote(str(download_dir) + '/')}data* "
            f"| tail -n +2 > {shlex.quote(str(out_path))}"
        )

    run_command(f"rm -f {str(download_dir / 'data*')}", cwd=str(download_dir))
    log.info(f"Combined file written: {out_path}")


# ---------------------------------------------------------------------------
# Snowflake helpers  (all accept a 'log' parameter — channel logger)
# ---------------------------------------------------------------------------

def _insert_into_perm_table(perm_table, channel_name, criteria_type,
                              criteria_value, comp_type, log):
    """
    CREATE OR REPLACE permanent table and INSERT query results directly.
    No S3 write for raw data.

    Schemas
    -------
    GREEN / BLUE  : (email STRING, condition_field STRING)
    ARCAMAX       : (email STRING, condition_field STRING)
    ORANGE        : (email_address STRING, condition_field STRING,
                     account_name STRING)
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    if channel_name in ("GREEN", "BLUE"):
        profile_table = (
            "GREEN_LPT.UNIVERSAL_PROFILE"
            if channel_name == "GREEN"
            else "INFS_LPT.INFS_PROFILE"
        )
        if criteria_type == "age":
            cond_col  = "b.AGE"
            op        = ">=" if comp_type == "greater" else "<"
            condition = f"b.AGE {op} {criteria_value}"
        else:
            cond_col = "b.STATE"
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            kw        = "IN" if comp_type == "include" else "NOT IN"
            condition = f"b.STATE {kw} ('{states}')"

        create_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} "
            f"(email STRING, condition_field STRING);"
        )
        insert_sql = (
            f"INSERT INTO {perm_table} (email, condition_field) "
            f"SELECT a.email, {cond_col} "
            f"FROM {profile_table} a "
            f"JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5 "
            f"WHERE {condition};"
        )

    elif channel_name == "ARCAMAX":
        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            cond_col    = "birthday"
            op          = "<=" if comp_type == "greater" else ">="
            condition   = (
                f"birthday IS NOT NULL AND TRY_TO_DATE(birthday) "
                f"{op} '{date_cutoff}'"
            )
        else:
            cond_col = "STATE"
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            kw        = "IN" if comp_type == "include" else "NOT IN"
            condition = f"STATE {kw} ('{states}')"

        create_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} "
            f"(email STRING, condition_field STRING);"
        )
        insert_sql = (
            f"INSERT INTO {perm_table} (email, condition_field) "
            f"SELECT email, {cond_col} "
            f"FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            f"WHERE {condition};"
        )

    else:  # ORANGE
        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            cond_col    = "a.dob"
            op          = "<=" if comp_type == "greater" else ">="
            condition   = f"dob {op} '{date_cutoff}'"
        else:
            cond_col = "a.STATE"
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            kw        = "IN" if comp_type == "include" else "NOT IN"
            condition = f"STATE {kw} ('{states}')"

        create_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} "
            f"(email_address STRING, condition_field STRING, account_name STRING);"
        )
        insert_sql = (
            f"INSERT INTO {perm_table} (email_address, condition_field, account_name) "
            f"SELECT a.email_address, {cond_col}, b.ACCOUNT_NAME "
            f"FROM ("
            f"  SELECT a.FEED_ID, a.email_address, a.dob, a.STATE "
            f"  FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a "
            f"  JOIN ("
            f"    SELECT email_address, MAX(created_at) AS maxdate "
            f"    FROM APT_CUSTOM_ORANGE_TRANSACTION_DND "
            f"    WHERE {condition} GROUP BY 1"
            f"  ) b ON a.email_address = b.email_address AND a.created_at = b.maxdate"
            f") a "
            f"JOIN APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 b "
            f"  ON a.FEED_ID = b.FEEDID "
            f"JOIN APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND c "
            f"  ON a.email_address = c.email_address "
            f"JOIN APT_CUSTOM_L90_ORANGE_UNIQ_RESPONDERS_UNIQ_DND d "
            f"  ON a.email_address = d.email;"
        )

    combined_sql = f"{create_sql} {insert_sql}"
    log.info(f"Creating permanent table and inserting data: {perm_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", combined_sql])
    log.info(f"Data inserted into {perm_table} successfully")


def _export_final_file(perm_table, path_FINAL, channel_name, log):
    """
    Export FINAL FILE from permanent table -> path_FINAL (S3).

    GREEN / BLUE / ARCAMAX : DISTINCT email           (header: email)
    ORANGE                 : DISTINCT email_address, account_name
                             (header: email_address|account_name)
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    select_clause = (
        "DISTINCT email_address, account_name"
        if channel_name == "ORANGE"
        else "DISTINCT email"
    )
    sql = (
        f"COPY INTO '{path_FINAL}/' "
        f"FROM (SELECT {select_clause} FROM {perm_table}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' "
        f"NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE) "
        f"HEADER=TRUE "
        f"MAX_FILE_SIZE=490000000;"
    )
    log.info(f"Exporting FINAL FILE: {perm_table} -> {path_FINAL}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info("FINAL FILE export complete")


def _export_complete_file(perm_table, path_COMPLETE, channel_name, log):
    """
    Export COMPLETE DATA FILE from permanent table -> path_COMPLETE (S3).

    GREEN / BLUE / ARCAMAX : email, condition_field
    ORANGE                 : email_address, condition_field, account_name
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    select_clause = (
        "email_address, condition_field, account_name"
        if channel_name == "ORANGE"
        else "email, condition_field"
    )
    sql = (
        f"COPY INTO '{path_COMPLETE}/' "
        f"FROM (SELECT {select_clause} FROM {perm_table}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' "
        f"NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE) "
        f"HEADER=TRUE "
        f"MAX_FILE_SIZE=490000000;"
    )
    log.info(f"Exporting COMPLETE DATA FILE: {perm_table} -> {path_COMPLETE}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info("COMPLETE DATA FILE export complete")


def _drop_perm_table(perm_table, log):
    """DROP the permanent table after both S3 exports are done."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"DROP TABLE IF EXISTS {perm_table};"
    log.info(f"Dropping permanent table: {perm_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"Table {perm_table} dropped successfully")


def _post_to_ftp(final_files_dir, path_date, output_file, log):
    """FTP upload from FINAL_FILES/."""
    ftp_cmd = (
        f'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    log.info(f"FTP upload: {output_file} -> /CPA/{path_date}")
    run_command(ftp_cmd, cwd=str(final_files_dir))
    log.info("FTP upload complete")


def _cleanup_channel_tmp(channel_tmp, log):
    """
    Remove the per-channel temp directory after the final file has been
    moved to FINAL_FILES/.  FINAL_FILES/ and logs/ are never touched here.
    """
    try:
        shutil.rmtree(str(channel_tmp))
        log.info(f"Removed temp dir: {channel_tmp}")
    except Exception as exc:
        log.warning(f"Could not remove temp dir {channel_tmp}: {exc}")


def _success_result(channel_name, output_file, final_file_path, elapsed,
                    record_count=0):
    return {
        "channel"         : channel_name,
        "file"            : output_file,
        "final_file_path" : final_file_path,
        "status"          : "SUCCESS",
        "elapsed"         : elapsed,
        "count"           : record_count,
    }


# ---------------------------------------------------------------------------
# Channel processors
# ---------------------------------------------------------------------------

def process_green_blue(request_id, channel_name, run_dir: Path):
    """
    GREEN and BLUE channel processor.

    All log calls go to the channel logger (age_GREEN / age_BLUE).
    The main log receives only the channel summary lines written explicitly
    by process_age_state_request.

    Flow:
      1. INSERT directly into permanent Snowflake table
      2. Export FINAL FILE  (DISTINCT email, header)   -> path_FINAL
      3. Export COMPLETE DATA FILE (email, cond_field) -> path_COMPLETE
      4. DROP permanent table
      5. Download path_FINAL -> <CH>_tmp/
      6. Move combined file to FINAL_FILES/ ; delete <CH>_tmp/
      7. FTP upload from FINAL_FILES/
    """
    channel_name   = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log            = setup_channel_logging(run_dir, channel_name, "age")

    log.info(f"=== {channel_name} processing started  (request_id={request_id}) ===")
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"criteria_type={ctx['criteria_type']}  comp_type={ctx['comp_type']}  "
        f"criteria_value={ctx['criteria_value']}"
    )

    try:
        channel_tmp      = ctx["channel_tmp"]
        final_files_dir  = ctx["final_files_dir"]
        perm_table       = ctx["perm_table"]
        path_FINAL       = ctx["path_FINAL"]
        path_COMPLETE    = ctx["path_COMPLETE"]
        start_time       = time.time()

        # Step 1
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        _insert_into_perm_table(
            perm_table, channel_name,
            ctx["criteria_type"], ctx["criteria_value"], ctx["comp_type"], log
        )

        # Step 2
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_final_file(perm_table, path_FINAL, channel_name, log)

        # Step 3
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_file(perm_table, path_COMPLETE, channel_name, log)

        # Step 4
        _drop_perm_table(perm_table, log)

        # Step 5
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir = channel_tmp / f"{channel_name}_FINAL_DL"
        _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )

        # Step 6 – move to FINAL_FILES/
        tmp_file_path   = channel_tmp    / ctx["output_file"]
        final_file_path = final_files_dir / ctx["output_file"]
        shutil.move(str(tmp_file_path), str(final_file_path))
        record_count = _count_file_lines(str(final_file_path))
        log.info(
            f"{channel_name}: {record_count} distinct emails "
            f"-> FINAL_FILES/{ctx['output_file']}"
        )

        # Step 7
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        log.info(
            f"=== {channel_name} processing completed in {elapsed:.2f}s | "
            f"final file: FINAL_FILES/{ctx['output_file']} ==="
        )

        result = _success_result(
            channel_name, ctx["output_file"],
            str(final_file_path), elapsed, record_count
        )
        _cleanup_channel_tmp(channel_tmp, log)
        return result

    except Exception as exc:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(f"{channel_name} processing failed")
        send_error_email(f"{channel_name} Processing Failed", str(exc))
        raise


def process_arcamax(request_id, run_dir: Path):
    """
    ARCAMAX channel processor.

    All log calls go to the age_ARCAMAX channel logger only.

    Flow: same 7-step pattern as GREEN/BLUE.
    """
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    log            = setup_channel_logging(run_dir, channel_name, "age")

    log.info(f"=== ARCAMAX processing started  (request_id={request_id}) ===")
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"criteria_type={ctx['criteria_type']}  comp_type={ctx['comp_type']}  "
        f"criteria_value={ctx['criteria_value']}"
    )

    try:
        channel_tmp      = ctx["channel_tmp"]
        final_files_dir  = ctx["final_files_dir"]
        perm_table       = ctx["perm_table"]
        path_FINAL       = ctx["path_FINAL"]
        path_COMPLETE    = ctx["path_COMPLETE"]
        start_time       = time.time()

        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        _insert_into_perm_table(
            perm_table, channel_name,
            ctx["criteria_type"], ctx["criteria_value"], ctx["comp_type"], log
        )

        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_final_file(perm_table, path_FINAL, channel_name, log)

        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_file(perm_table, path_COMPLETE, channel_name, log)

        _drop_perm_table(perm_table, log)

        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir = channel_tmp / "ARCAMAX_FINAL_DL"
        _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )

        tmp_file_path   = channel_tmp    / ctx["output_file"]
        final_file_path = final_files_dir / ctx["output_file"]
        shutil.move(str(tmp_file_path), str(final_file_path))
        record_count = _count_file_lines(str(final_file_path))
        log.info(
            f"ARCAMAX: {record_count} distinct emails "
            f"-> FINAL_FILES/{ctx['output_file']}"
        )

        update_request_status(request_id, "Posting To FTP", channel_status, log)
        _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        log.info(
            f"=== ARCAMAX processing completed in {elapsed:.2f}s | "
            f"final file: FINAL_FILES/{ctx['output_file']} ==="
        )

        result = _success_result(
            channel_name, ctx["output_file"],
            str(final_file_path), elapsed, record_count
        )
        _cleanup_channel_tmp(channel_tmp, log)
        return result

    except Exception as exc:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception("ARCAMAX processing failed")
        send_error_email("ARCAMAX Processing Failed", str(exc))
        raise


def process_orange(request_id, run_dir: Path):
    """
    ORANGE channel processor.

    All log calls go to the age_ORANGE channel logger only.

    Flow:
      Steps 1-6 same as other channels.
      Step 7: build output based on request_type:
        suppression -> email-only CSV in FINAL_FILES/
        mailing     -> ESP-wise split CSVs packed into ZIP in FINAL_FILES/
      Step 8: FTP from FINAL_FILES/
    """
    channel_name   = "ORANGE"
    channel_status = "ORANGE_STATUS"
    log            = setup_channel_logging(run_dir, channel_name, "age")

    log.info(f"=== ORANGE processing started  (request_id={request_id}) ===")
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"criteria_type={ctx['criteria_type']}  comp_type={ctx['comp_type']}  "
        f"criteria_value={ctx['criteria_value']}  "
        f"request_type={ctx['request_type']}"
    )

    try:
        request_type    = ctx["request_type"]
        client_name     = ctx["client_name"]
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        path_date       = ctx["path_date"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        _insert_into_perm_table(
            perm_table, channel_name,
            ctx["criteria_type"], ctx["criteria_value"], ctx["comp_type"], log
        )

        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_final_file(perm_table, path_FINAL, channel_name, log)

        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_file(perm_table, path_COMPLETE, channel_name, log)

        _drop_perm_table(perm_table, log)

        update_request_status(request_id, "Combining Data", channel_status, log)
        raw_combined = f"ORANGE_FINAL_{path_date}.csv"
        download_dir = channel_tmp / "ORANGE_FINAL_DL"
        _download_and_combine(
            path_FINAL, download_dir, channel_tmp, raw_combined, channel_name, log
        )

        df_final = pd.read_csv(
            str(channel_tmp / raw_combined),
            sep="|",
            header=None,
            names=["email_address", "account_name"],
            dtype=str,
        )
        total_count = len(df_final)
        log.info(f"ORANGE FINAL FILE records loaded: {total_count}")

        update_request_status(request_id, "Posting To FTP", channel_status, log)

        if request_type.lower() == "suppression":
            output_file      = (
                f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"
            )
            suppression_path = final_files_dir / output_file
            (
                df_final[["email_address"]]
                .drop_duplicates()
                .to_csv(str(suppression_path), index=False, header=False)
            )
            record_count = len(pd.read_csv(str(suppression_path), header=None))
            log.info(
                f"ORANGE suppression: {output_file} "
                f"({record_count} records) -> FINAL_FILES/"
            )
            _post_to_ftp(final_files_dir, path_date, output_file, log)
            final_file_path = str(suppression_path)

        else:  # mailing
            esp_split_dir = channel_tmp / "ORANGE_ESP_SPLIT"
            esp_split_dir.mkdir(exist_ok=True)
            esp_names = df_final["account_name"].dropna().unique()
            zip_parts = []
            for esp in esp_names:
                esp_df = (
                    df_final[df_final["account_name"] == esp][["email_address"]]
                    .drop_duplicates()
                )
                esp_file = (
                    esp_split_dir
                    / f"{client_name}_{request_type}_{esp}_{path_date}.csv"
                )
                esp_df.to_csv(str(esp_file), index=False, header=False)
                zip_parts.append(esp_file)
                log.info(
                    f"ORANGE ESP split: {esp_file.name} ({len(esp_df)} records)"
                )

            zip_name = f"{client_name}_{request_type}_ORANGE_{path_date}.zip"
            zip_path = final_files_dir / zip_name
            run_command(
                ["zip", "-j", str(zip_path)] + [str(p) for p in zip_parts]
            )
            log.info(
                f"ORANGE mailing ZIP: {zip_name} "
                f"({len(zip_parts)} ESPs) -> FINAL_FILES/"
            )
            _post_to_ftp(final_files_dir, path_date, zip_name, log)
            final_file_path = str(zip_path)
            output_file     = zip_name
            record_count    = total_count

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        log.info(
            f"=== ORANGE processing completed in {elapsed:.2f}s | "
            f"final file: FINAL_FILES/{output_file} ==="
        )

        result = _success_result(
            channel_name, output_file,
            final_file_path, elapsed, record_count
        )
        _cleanup_channel_tmp(channel_tmp, log)
        return result

    except Exception as exc:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception("ORANGE processing failed")
        send_error_email("ORANGE Processing Failed", str(exc))
        raise


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def process_age_state_request(request_id, channel="ALL"):
    """
    Orchestrate channel processing for a single request.
    channel: "ALL" | "GREEN" | "BLUE" | "ARCAMAX" | "ORANGE"

    LOG ROUTING
    -----------
    This function owns the MAIN log (age_main logger).
    It logs:
      - Run start / channels to run
      - Per-channel start / completed / failed summary lines
      - Overall completion / failure
    Channel processors own their own loggers; all detailed work logs stay
    inside the respective <CH>_age_<ts>.log file.
    """
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found in DB")

    output_dir    = request_data["output_dir"]
    criteria_type = request_data["criteria_type"]

    # Build run_dir via ensure_output_dir  (creates run_age_YYYYMMDD_HHMMSS/)
    run_dir = Path(ensure_output_dir(output_dir, criteria_type))

    # Ensure FINAL_FILES/ and logs/ exist inside run_dir
    (run_dir / "FINAL_FILES").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)

    # MAIN logger — only this function writes to it (plus explicit summaries below)
    log_main = setup_main_logging(run_dir, criteria_type)

    channels_to_run = CHANNELS if channel.upper() == "ALL" else [channel.upper()]
    log_main.info(
        f"=== process_age_state_request started | "
        f"request_id={request_id}  channels={','.join(channels_to_run)} ==="
    )
    log_main.info(
        f"run_dir={run_dir}  "
        f"criteria_type={criteria_type}  "
        f"criteria_value={request_data['criteria_value']}  "
        f"comp_type={request_data['comp_type']}"
    )

    for ch in channels_to_run:
        update_request_status(request_id, "Queued", f"{ch}_STATUS", log_main)

    def _run_channel(ch):
        if ch in ("GREEN", "BLUE"):
            return process_green_blue(request_id, ch, run_dir)
        elif ch == "ARCAMAX":
            return process_arcamax(request_id, run_dir)
        elif ch == "ORANGE":
            return process_orange(request_id, run_dir)
        else:
            raise ValueError(f"Unknown channel: {ch}")

    results = {}
    failed  = []

    if len(channels_to_run) == 1:
        ch = channels_to_run[0]
        log_main.info(f"[{ch}] starting")
        try:
            results[ch] = _run_channel(ch)
            log_main.info(
                f"[{ch}] completed | "
                f"file={results[ch]['file']}  count={results[ch]['count']}  "
                f"elapsed={results[ch]['elapsed']:.2f}s"
            )
        except Exception as exc:
            failed.append(ch)
            log_main.error(f"[{ch}] FAILED: {exc}")

    else:
        with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
            future_map = {
                executor.submit(_run_channel, ch): ch
                for ch in channels_to_run
            }
            for future in as_completed(future_map):
                ch = future_map[future]
                try:
                    results[ch] = future.result()
                    log_main.info(
                        f"[{ch}] completed | "
                        f"file={results[ch]['file']}  count={results[ch]['count']}  "
                        f"elapsed={results[ch]['elapsed']:.2f}s"
                    )
                except Exception as exc:
                    failed.append(ch)
                    log_main.error(f"[{ch}] FAILED: {exc}")

    overall_status = "failed" if failed else "completed"
    conn = get_db_with_retry(log_main)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET overall_status=%s WHERE id=%s",
                (overall_status, request_id),
            )
        conn.commit()
    finally:
        conn.close()

    log_main.info(
        f"=== process_age_state_request {overall_status.upper()} | "
        f"request_id={request_id}  "
        f"succeeded={[c for c in channels_to_run if c not in failed]}  "
        f"failed={failed} ==="
    )

    if not failed:
        send_success_email(
            f"Request {request_id} completed",
            f"Channels: {', '.join(channels_to_run)}\nResults: {results}",
        )

    return results
