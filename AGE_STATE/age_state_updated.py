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
  age_main logger   -> age_<ts>.log ONLY
      - All log.info/error/exception calls inside process_age_state_request
      - Explicit channel summary lines (started / completed / failed) written
        by the orchestrator so the main log has a high-level picture
      - propagate = False  ->  nothing leaks into root logger

  age_<CHANNEL> logger  -> <CH>_age_<ts>.log ONLY
      - Every log call inside process_green_blue / process_arcamax / process_orange
        and all helpers they invoke (_insert_into_perm_table, _export_*, _drop_*, etc.)
      - propagate = False  ->  channel logs never bleed into main log
      - Channel processors receive their logger as a parameter; helpers also receive it

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
import re
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
# Step banner helper  (makes log files easy to scan)
# ---------------------------------------------------------------------------

def _step(log, step_num, total_steps, description, channel_name=""):
    """
    Emit a clearly visible step-banner line so every stage is traceable.
    Example:
        [GREEN] ── STEP 1/7 ──  Loading data into Snowflake permanent table
    """
    prefix = f"[{channel_name}] " if channel_name else ""
    log.info(
        f"{prefix}{'─' * 4} STEP {step_num}/{total_steps} {'─' * 4}  {description}"
    )


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
    propagate=False ensures nothing leaks into root or channel loggers.
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
    Create (or reset) a per-channel logger 'age_<CHANNEL>'.
    Writes to:  <run_dir>/logs/<CHANNEL>_age_<YYYYMMDD_HHMMSS>.log
    propagate=False ensures channel logs never bleed into main log.
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
    Retries up to _DB_RETRY_ATTEMPTS times on OperationalError.
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
            log.info(f"DB STATUS UPDATE: {status_column} -> '{status}'  (request_id={request_id})")
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
    log.error(
        f"update_request_status gave up after {_DB_RETRY_ATTEMPTS} attempts: "
        f"{last_exc}"
    )
    raise last_exc


def _build_common_context(request_id, channel_name, run_dir: Path):
    """
    Build the shared context dict for a channel run.
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

    final_files_dir = run_dir / "FINAL_FILES"
    final_files_dir.mkdir(parents=True, exist_ok=True)

    channel_tmp = run_dir / f"{channel_name.upper()}_tmp"
    channel_tmp.mkdir(parents=True, exist_ok=True)

    path_date  = datetime.now().strftime("%Y%m%d")
    perm_table = f"APT_CPA_{channel_name.upper()}_{client_name}_{request_name}_{request_type}_{path_date}"

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
        "final_files_dir": final_files_dir,
        "channel_tmp"    : channel_tmp,
        "path_date"      : path_date,
        "path_FINAL"     : path_FINAL,
        "path_COMPLETE"  : path_COMPLETE,
        "output_file"    : output_file,
        "perm_table"     : perm_table,
    }


def _count_file_lines(file_path):
    """Fast line count using wc -l (excludes header if present)."""
    cmd    = f"wc -l < {shlex.quote(str(file_path))}"
    result = subprocess.check_output(
        cmd, shell=True, universal_newlines=True
    ).strip()
    return int(result) if result else 0


def _query_snowflake(query_sql, log):
    """
    Run SELECT COUNT(*) FROM <table> via snowsql and return the integer.
    Returns -1 if the query fails (non-fatal).
    """
    try:
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        #sql    = f"SELECT COUNT(*) FROM {table_name};"
        output = subprocess.check_output(
            ["snowsql", "-c", "datateam1", "-q", query_sql, "-o", "output_format=csv",
             "-o", "header=false", "-o", "timing=false", "-o", "friendly=false"],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
        # Extract first numeric token from output
        match = re.search(r"(\d+)", output)
        return int(match.group(1)) if match else -1
    except Exception as exc:
        log.warning(f"_query_snowflake failed (non-fatal): {exc}")
        #print(f"_query_snowflake failed (non-fatal): {exc}")
        return -1


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

    log.info(f"  S3 source   : {s3_path}")
    log.info(f"  Download dir: {download_dir}")
    log.info(f"  Output file : {work_dir / output_file}")
    log.info("  Starting aws s3 cp (recursive) ...")

    run_command(
        ["aws", "s3", "cp", s3_path, str(download_dir), "--recursive", "--quiet"]
    )

    downloaded = sorted(download_dir.glob("data*"))
    if not downloaded:
        raise RuntimeError(
            f"aws s3 cp from {s3_path} downloaded 0 files into {download_dir}."
        )
    log.info(f"  Downloaded {len(downloaded)} part file(s): "
             f"{[p.name for p in downloaded]}")

    out_path = work_dir / output_file

    if channel_name != "ORANGE":
        run_command(
            f"printf 'email\n' > {shlex.quote(str(out_path))} && "
            f"zcat {shlex.quote(str(download_dir) + '/')}data* "
            f"| sed 's/\"//g' | tail -n +2 | cut -d'|' -f1 >> {shlex.quote(str(out_path))}"
            )
    else:
        run_command(
            f"printf 'email\n' > {shlex.quote(str(out_path))} && "
            f"zcat {shlex.quote(str(download_dir) + '/')}data* "
            f"| sed 's/\"//g' | tail -n +2 >> {shlex.quote(str(out_path))}"
            )

    run_command(f"rm -f {str(download_dir / 'data*')}", cwd=str(download_dir))

    combined_count = _count_file_lines(str(out_path))
    log.info(f"  Combined file written: {out_path}  |  rows in file: {combined_count:,}")
    return combined_count


# ---------------------------------------------------------------------------
# Snowflake helpers  (all accept a 'log' parameter — channel logger)
# ---------------------------------------------------------------------------

def _insert_into_perm_table(perm_table, channel_name, criteria_type,
                              criteria_value, comp_type, log):
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

        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
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

        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
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

        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
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

    log.info(f"  Target table : {perm_table}")
    log.info(f"  INSERT SQL   : {insert_sql}")
    log.info("  Executing CREATE + INSERT via snowsql ...")

    run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
    log.info("  CREATE + INSERT executed successfully")

    # Verify row count in perm table
    count_sql = f'select count(*) from {perm_table}'
    inserted_rows = _query_snowflake(count_sql, log)
    if inserted_rows >= 0:
        log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
    else:
        log.warning(f"  Could not verify row count for {perm_table} (count query failed)")


def _export_complete_final_file(export_type, perm_table, export_path, channel_name, log):
    """
    Export COMPLETE/FINAL DATA FILE from permanent table -> export_path (S3).
    GREEN / BLUE / ARCAMAX : email, condition_field
    ORANGE                 : email_address, condition_field, account_name
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    if export_type == 'FINAL':
        select_clause = (
        "DISTINCT email_address, account_name"
        if channel_name == "ORANGE"
        else "DISTINCT email")
    else:
        select_clause = (
        "email_address, condition_field, account_name"
        if channel_name == "ORANGE"
        else "email, condition_field")

    sql = (
        f"COPY INTO '{export_path}/' "
        f"FROM (SELECT {select_clause} FROM {perm_table}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' "
        f"NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE) "
        f"HEADER=TRUE "
        f"MAX_FILE_SIZE=490000000;"
    )
    log.info(f"  Source table : {perm_table}")
    log.info(f"  S3 target    : {export_path}/")
    log.info(f"  SELECT clause: {select_clause}")
    log.info(f"  Executing COPY INTO ({export_type} FILE) via snowsql ...")

    unloaded_rows = _query_snowflake(sql, log)
    log.info(f"  COPY INTO ({export_type} FILE) completed successfully")

    # Count rows exported to S3 for COMPLETE
    #complete_s3_rows = _count_s3_rows(export_path, log)
    if unloaded_rows >= 0:
        log.info(f"  Rows unloaded to {export_type} S3 path: {unloaded_rows:,}")
    else:
        log.warning(f"  Could not verify {export_type} FILE S3 row count (non-fatal)")


def _drop_perm_table(perm_table, log):
    """DROP the permanent table after both S3 exports are done."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"DROP TABLE IF EXISTS {perm_table};"
    log.info(f"  Dropping permanent table: {perm_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  Table {perm_table} dropped successfully")


def _post_to_ftp(final_files_dir, path_date, output_file, log):
    """FTP upload from FINAL_FILES/ and verify."""
    ftp_dest = f"/CPA/{path_date}/{output_file}"
    ftp_cmd = (
        f'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    local_path = Path(final_files_dir) / output_file
    local_size = local_path.stat().st_size if local_path.exists() else -1

    log.info(f"  FTP destination : {ftp_dest}")
    log.info(f"  Local file size : {local_size:,} bytes  ({local_path})")
    log.info("  Starting lftp upload ...")

    run_command(ftp_cmd, cwd=str(final_files_dir))

    log.info(f"  FTP upload completed successfully -> {ftp_dest}")
    log.info(f"  File '{output_file}' has been posted to FTP at /CPA/{path_date}/")


def _cleanup_channel_tmp(channel_tmp, log):
    """
    Remove the per-channel temp directory after the final file has been
    moved to FINAL_FILES/.  FINAL_FILES/ and logs/ are never touched here.
    """
    try:
        shutil.rmtree(str(channel_tmp))
        log.info(f"  Removed temp dir: {channel_tmp}")
    except Exception as exc:
        log.warning(f"  Could not remove temp dir {channel_tmp}: {exc}")


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
    7-step flow with verbose step banners and row-count logging at every stage.
    """
    TOTAL_STEPS    = 7
    channel_name   = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log            = setup_channel_logging(run_dir, channel_name, "age")

    log.info("=" * 70)
    log.info(f"  {channel_name} CHANNEL PROCESSING STARTED")
    log.info(f"  request_id : {request_id}")
    log.info(f"  run_dir    : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  criteria_type  = {ctx['criteria_type']}\n"
        f"  comp_type      = {ctx['comp_type']}\n"
        f"  criteria_value = {ctx['criteria_value']}\n"
        f"  perm_table     = {ctx['perm_table']}\n"
        f"  path_FINAL     = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE  = {ctx['path_COMPLETE']}\n"
        f"  output_file    = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        _insert_into_perm_table(
            perm_table, channel_name,
            ctx["criteria_type"], ctx["criteria_value"], ctx["comp_type"], log
        )
        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file('FINAL', perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported to S3 -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + condition_field) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file('COMPLETE', perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported to S3 -> {path_COMPLETE}")

        # ── STEP 4/7 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/7 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir   = channel_tmp / f"{channel_name}_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )
        log.info(f"  STEP 5 DONE: Combined file rows (emails): {combined_count:,}")

        # ── STEP 6/7 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        tmp_file_path   = channel_tmp     / ctx["output_file"]
        final_file_path = final_files_dir / ctx["output_file"]
        shutil.move(str(tmp_file_path), str(final_file_path))
        record_count = _count_file_lines(str(final_file_path))
        log.info(
            f"  STEP 6 DONE: Final file -> FINAL_FILES/{ctx['output_file']}\n"
            f"               Distinct email rows in final file: {record_count:,}"
        )

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, f"FTP upload -> /CPA/{ctx['path_date']}/{ctx['output_file']}", channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        log.info(f"  STEP 7 DONE: FTP upload successful")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)

        log.info("=" * 70)
        log.info(f"  {channel_name} CHANNEL PROCESSING COMPLETED SUCCESSFULLY")
        log.info(f"  Total elapsed   : {elapsed:.2f}s")
        log.info(f"  Final file      : FINAL_FILES/{ctx['output_file']}")
        log.info(f"  Final row count : {record_count:,}")
        log.info(f"  FTP path        : /CPA/{ctx['path_date']}/{ctx['output_file']}")
        log.info("=" * 70)

        result = _success_result(
            channel_name, ctx["output_file"],
            str(final_file_path), elapsed, record_count
        )
        _cleanup_channel_tmp(channel_tmp, log)
        return result

    except Exception as exc:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(
            f"{channel_name} processing FAILED at the above step. "
            f"Exception: {exc}"
        )
        send_error_email(f"{channel_name} Processing Failed", str(exc))
        raise


def process_arcamax(request_id, run_dir: Path):
    """
    ARCAMAX channel processor.
    7-step flow with verbose step banners and row-count logging at every stage.
    """
    TOTAL_STEPS    = 7
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    log            = setup_channel_logging(run_dir, channel_name, "age")

    log.info("=" * 70)
    log.info(f"  {channel_name} CHANNEL PROCESSING STARTED")
    log.info(f"  request_id : {request_id}")
    log.info(f"  run_dir    : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  criteria_type  = {ctx['criteria_type']}\n"
        f"  comp_type      = {ctx['comp_type']}\n"
        f"  criteria_value = {ctx['criteria_value']}\n"
        f"  perm_table     = {ctx['perm_table']}\n"
        f"  path_FINAL     = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE  = {ctx['path_COMPLETE']}\n"
        f"  output_file    = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        _insert_into_perm_table(
            perm_table, channel_name,
            ctx["criteria_type"], ctx["criteria_value"], ctx["comp_type"], log
        )
        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file('FINAL', perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported to S3 -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + condition_field) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file('COMPLETE', perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported to S3 -> {path_COMPLETE}")

        # ── STEP 4/7 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/7 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir   = channel_tmp / "ARCAMAX_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )
        log.info(f"  STEP 5 DONE: Combined file rows (emails): {combined_count:,}")

        # ── STEP 6/7 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        tmp_file_path   = channel_tmp     / ctx["output_file"]
        final_file_path = final_files_dir / ctx["output_file"]
        shutil.move(str(tmp_file_path), str(final_file_path))
        record_count = _count_file_lines(str(final_file_path))
        log.info(
            f"  STEP 6 DONE: Final file -> FINAL_FILES/{ctx['output_file']}\n"
            f"               Distinct email rows in final file: {record_count:,}"
        )

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, f"FTP upload -> /CPA/{ctx['path_date']}/{ctx['output_file']}", channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        log.info(f"  STEP 7 DONE: FTP upload successful")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)

        log.info("=" * 70)
        log.info(f"  {channel_name} CHANNEL PROCESSING COMPLETED SUCCESSFULLY")
        log.info(f"  Total elapsed   : {elapsed:.2f}s")
        log.info(f"  Final file      : FINAL_FILES/{ctx['output_file']}")
        log.info(f"  Final row count : {record_count:,}")
        log.info(f"  FTP path        : /CPA/{ctx['path_date']}/{ctx['output_file']}")
        log.info("=" * 70)

        result = _success_result(
            channel_name, ctx["output_file"],
            str(final_file_path), elapsed, record_count
        )
        _cleanup_channel_tmp(channel_tmp, log)
        return result

    except Exception as exc:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(
            f"ARCAMAX processing FAILED at the above step. "
            f"Exception: {exc}"
        )
        send_error_email("ARCAMAX Processing Failed", str(exc))
        raise


def process_orange(request_id, run_dir: Path):
    """
    ORANGE channel processor.
    8-step flow with verbose step banners and row-count logging at every stage.
    Step 7 branches: suppression -> email-only CSV | mailing -> ESP ZIP.
    """
    TOTAL_STEPS    = 8
    channel_name   = "ORANGE"
    channel_status = "ORANGE_STATUS"
    log            = setup_channel_logging(run_dir, channel_name, "age")

    log.info("=" * 70)
    log.info(f"  {channel_name} CHANNEL PROCESSING STARTED")
    log.info(f"  request_id : {request_id}")
    log.info(f"  run_dir    : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  criteria_type  = {ctx['criteria_type']}\n"
        f"  comp_type      = {ctx['comp_type']}\n"
        f"  criteria_value = {ctx['criteria_value']}\n"
        f"  request_type   = {ctx['request_type']}\n"
        f"  perm_table     = {ctx['perm_table']}\n"
        f"  path_FINAL     = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE  = {ctx['path_COMPLETE']}\n"
        f"  output_file    = {ctx['output_file']}"
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

        # ── STEP 1/8 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        _insert_into_perm_table(
            perm_table, channel_name,
            ctx["criteria_type"], ctx["criteria_value"], ctx["comp_type"], log
        )
        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/8 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT email_address, account_name) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file('FINAL', perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported to S3 -> {path_FINAL}")

        # ── STEP 3/8 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email_address, condition_field, account_name) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file('COMPLETE', perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported to S3 -> {path_COMPLETE}")

        # ── STEP 4/8 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/8 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        raw_combined = f"ORANGE_FINAL_{path_date}.csv"
        download_dir = channel_tmp / "ORANGE_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp, raw_combined, channel_name, log
        )
        log.info(f"  STEP 5 DONE: Combined raw rows downloaded: {combined_count:,}")

        # ── STEP 6/8 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Loading combined file into DataFrame for processing", channel_name)
        df_final = pd.read_csv(
            str(channel_tmp / raw_combined),
            sep="|",
            header=None,
            names=["email_address", "account_name"],
            dtype=str,
        )
        total_count  = len(df_final)
        esp_breakdown = df_final.groupby("account_name")["email_address"].count().to_dict()
        log.info(f"  STEP 6 DONE: Records loaded into DataFrame: {total_count:,}")
        log.info(f"  ESP breakdown (account_name -> row count):")
        for esp_name, esp_cnt in sorted(esp_breakdown.items(), key=lambda x: -x[1]):
            log.info(f"    {esp_name}: {esp_cnt:,} rows")

        # ── STEP 7/8 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS,
              f"Building output files (request_type={request_type})",
              channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)

        if request_type.lower() == "suppression":
            output_file      = (
                f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"
            )
            suppression_path = final_files_dir / output_file
            deduped          = df_final[["email_address"]].drop_duplicates()
            deduped.to_csv(str(suppression_path), index=False, header=False)
            record_count = len(deduped)
            log.info(
                f"  Suppression file: {output_file}\n"
                f"  Raw rows        : {total_count:,}\n"
                f"  Deduped rows    : {record_count:,}"
            )
            final_file_path = str(suppression_path)
            log.info(f"  STEP 7 DONE: Suppression CSV written -> FINAL_FILES/{output_file}")

        else:  # mailing
            esp_split_dir = channel_tmp / "ORANGE_ESP_SPLIT"
            esp_split_dir.mkdir(exist_ok=True)
            esp_names  = df_final["account_name"].dropna().unique()
            zip_parts  = []
            total_esp_rows = 0
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
                total_esp_rows += len(esp_df)
                log.info(
                    f"  ESP split: {esp_file.name}  |  rows: {len(esp_df):,}"
                )

            zip_name = f"{client_name}_{request_type}_ORANGE_{path_date}.zip"
            zip_path = final_files_dir / zip_name
            run_command(
                ["zip", "-j", str(zip_path)] + [str(p) for p in zip_parts]
            )
            record_count    = total_esp_rows
            final_file_path = str(zip_path)
            output_file     = zip_name
            log.info(
                f"  Mailing ZIP     : {zip_name}\n"
                f"  ESPs packed     : {len(zip_parts)}\n"
                f"  Total email rows: {record_count:,}"
            )
            log.info(f"  STEP 7 DONE: Mailing ZIP written -> FINAL_FILES/{zip_name}")

        # ── STEP 8/8 ──────────────────────────────────────────────────────
        _step(log, 8, TOTAL_STEPS, f"FTP upload -> /CPA/{path_date}/{output_file}", channel_name)
        _post_to_ftp(final_files_dir, path_date, output_file, log)
        log.info(f"  STEP 8 DONE: FTP upload successful")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)

        log.info("=" * 70)
        log.info(f"  {channel_name} CHANNEL PROCESSING COMPLETED SUCCESSFULLY")
        log.info(f"  Total elapsed   : {elapsed:.2f}s")
        log.info(f"  Final file      : FINAL_FILES/{output_file}")
        log.info(f"  Final row count : {record_count:,}")
        log.info(f"  FTP path        : /CPA/{path_date}/{output_file}")
        log.info("=" * 70)

        result = _success_result(
            channel_name, output_file,
            final_file_path, elapsed, record_count
        )
        _cleanup_channel_tmp(channel_tmp, log)
        return result

    except Exception as exc:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(
            f"ORANGE processing FAILED at the above step. "
            f"Exception: {exc}"
        )
        send_error_email("ORANGE Processing Failed", str(exc))
        raise


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def process_age_state_request(request_id, channel="ALL"):
    """
    Orchestrate channel processing for a single request.
    channel: "ALL" | "GREEN" | "BLUE" | "ARCAMAX" | "ORANGE"
    """
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found in DB")

    output_dir    = request_data["output_dir"]
    criteria_type = request_data["criteria_type"]

    run_dir = Path(ensure_output_dir(output_dir, criteria_type))

    (run_dir / "FINAL_FILES").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)

    log_main = setup_main_logging(run_dir, criteria_type)

    channels_to_run = CHANNELS if channel.upper() == "ALL" else [channel.upper()]

    log_main.info("=" * 70)
    log_main.info("  AGE STATE PROCESSING — ORCHESTRATOR STARTED")
    log_main.info(f"  request_id     : {request_id}")
    log_main.info(f"  channels       : {', '.join(channels_to_run)}")
    log_main.info(f"  criteria_type  : {criteria_type}")
    log_main.info(f"  criteria_value : {request_data['criteria_value']}")
    log_main.info(f"  comp_type      : {request_data['comp_type']}")
    log_main.info(f"  run_dir        : {run_dir}")
    log_main.info("=" * 70)

    for ch in channels_to_run:
        update_request_status(request_id, "Queued", f"{ch}_STATUS", log_main)
        log_main.info(f"  [{ch}] status set to Queued")

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
        log_main.info(f"  [{ch}] single-channel mode — starting now")
        try:
            results[ch] = _run_channel(ch)
            log_main.info(
                f"  [{ch}] COMPLETED | "
                f"file={results[ch]['file']}  "
                f"count={results[ch]['count']:,}  "
                f"elapsed={results[ch]['elapsed']:.2f}s"
            )
        except Exception as exc:
            failed.append(ch)
            log_main.error(f"  [{ch}] FAILED: {exc}")

    else:
        log_main.info(
            f"  Parallel mode — launching {len(channels_to_run)} channels "
            f"via ThreadPoolExecutor"
        )
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
                        f"  [{ch}] COMPLETED | "
                        f"file={results[ch]['file']}  "
                        f"count={results[ch]['count']:,}  "
                        f"elapsed={results[ch]['elapsed']:.2f}s"
                    )
                except Exception as exc:
                    failed.append(ch)
                    log_main.error(f"  [{ch}] FAILED: {exc}")

    overall_status = "failed" if failed else "completed"
    conn = get_db_with_retry(log_main)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET overall_status=%s WHERE id=%s",
                (overall_status, request_id),
            )
        conn.commit()
        log_main.info(
            f"  DB overall_status updated to '{overall_status}' "
            f"(request_id={request_id})"
        )
    finally:
        conn.close()

    log_main.info("=" * 70)
    log_main.info(
        f"  AGE STATE PROCESSING — ORCHESTRATOR {overall_status.upper()}"
    )
    log_main.info(f"  request_id : {request_id}")
    log_main.info(
        f"  succeeded  : {[c for c in channels_to_run if c not in failed]}"
    )
    log_main.info(f"  failed     : {failed}")
    if results:
        log_main.info("  Channel summary:")
        for ch, r in results.items():
            log_main.info(
                f"    {ch}: file={r['file']}  rows={r['count']:,}  "
                f"elapsed={r['elapsed']:.2f}s  status={r['status']}"
            )
    log_main.info("=" * 70)

    if not failed:
        send_success_email(
            f"Request {request_id} completed",
            f"Channels: {', '.join(channels_to_run)}\nResults: {results}",
            f'{run_dir}/FINAL_FILES'
        )

    return results

