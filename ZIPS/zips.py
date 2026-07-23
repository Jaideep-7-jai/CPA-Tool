#!/usr/bin/env python3
"""
ZIPS Processing - unique request-driven channel execution.
Mirrors AGE_STATE/age_state.py exactly in structure, logging, and flow.

Key differences from age_state:
  - ZIP codes are provided via UI upload (file attachment)
  - A SINGLE shared Snowflake ZIP staging table is created ONCE before
    any channel runs and is DROPPED only after ALL channels complete.
  - Each channel's perm table is still created/dropped per-channel as in age_state.
  - Matching is done by ZIP field (b.ZIP / ZIP column) via IN / NOT IN
    against the shared zip staging table.

Run directory layout
--------------------

    <output_dir>/run_zips_<YYYYMMDD_HHMMSS>/
        logs/
            zips_<YYYYMMDD_HHMMSS>.log          <- MAIN log  (orchestrator only)
            GREEN_zips_<YYYYMMDD_HHMMSS>.log
            BLUE_zips_<YYYYMMDD_HHMMSS>.log
            ARCAMAX_zips_<YYYYMMDD_HHMMSS>.log
            ORANGE_zips_<YYYYMMDD_HHMMSS>.log
        FINAL_FILES/
            <client>_<request_type>_GREEN_<date>.csv
            ...
        GREEN_tmp/
        BLUE_tmp/
        ARCAMAX_tmp/
        ORANGE_tmp/

Processing flow
---------------
  PRE-CHANNEL (orchestrator):
    1. Upload ZIP codes file from UI attachment -> S3
    2. CREATE shared ZIP staging table (APT_CPA_ZIPS_STAGING_<YYYYMMDD_HHMMSS>) ONCE
    3. COPY ZIP codes from S3 into staging table

  PER CHANNEL (same 7-step flow as age_state):
    1. INSERT raw query results into permanent Snowflake table
         APT_CPA_<CHANNEL>_<YYYYMMDD>  (matching on ZIP via staging table)
    2. Export FINAL FILE  -> path_FINAL  (S3)
    3. Export COMPLETE DATA FILE -> path_COMPLETE (S3)
    4. DROP the per-channel permanent table
    5. Download FINAL FILE parts + combine
    6. Move combined file to FINAL_FILES/
    7. FTP upload from FINAL_FILES/

  POST-CHANNEL (orchestrator):
    - DROP the shared ZIP staging table (only after ALL channels finish)
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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import (
    run_command,
    send_success_email,
    send_error_email,
    ensure_output_dir,
)

DB_CONFIG = {
    "host":      "zds-prod-jbdb3-vip.bo3.e-dialog.com",
    "user":      "techuser",
    "password":  "tech12#$",
    "database":  "CUST_TECH_DB",
    "charset":   "utf8mb4",
    "autocommit": True,
}

CHANNELS = ["GREEN", "BLUE", "ARCAMAX", "ORANGE"]

_FMT = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

# ---------------------------------------------------------------------------
# DB retry constants (same as age_state)
# ---------------------------------------------------------------------------

_DB_RETRY_ATTEMPTS   = 3
_DB_RETRY_BASE_DELAY = 2


# ---------------------------------------------------------------------------
# Step banner helper
# ---------------------------------------------------------------------------

def _step(log, step_num, total_steps, description, channel_name=""):
    prefix = f"[{channel_name}] " if channel_name else ""
    log.info(
        f"{prefix}{'─' * 4} STEP {step_num}/{total_steps} {'─' * 4}  {description}"
    )


# ---------------------------------------------------------------------------
# Logging helpers  (mirrors age_state exactly)
# ---------------------------------------------------------------------------

def _file_handler(log_path: Path) -> logging.FileHandler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.FileHandler(str(log_path))
    h.setFormatter(_FMT)
    return h


def setup_main_logging(run_dir: Path) -> logging.Logger:
    """
    Create the MAIN logger 'zips_main'.
    Writes to:  <run_dir>/logs/zips_<YYYYMMDD_HHMMSS>.log
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / f"zips_{ts}.log"

    lg = logging.getLogger("zips_main")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    lg.handlers.clear()
    lg.addHandler(_file_handler(log_file))
    return lg


def setup_channel_logging(run_dir: Path, channel_name: str) -> logging.Logger:
    """
    Create a per-channel logger 'zips_<CHANNEL>'.
    Writes to:  <run_dir>/logs/<CHANNEL>_zips_<YYYYMMDD_HHMMSS>.log
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / f"{channel_name.upper()}_zips_{ts}.log"

    lg = logging.getLogger(f"zips_{channel_name.upper()}")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    lg.handlers.clear()
    lg.addHandler(_file_handler(log_file))
    return lg


# ---------------------------------------------------------------------------
# DB helpers  (mirrors age_state exactly)
# ---------------------------------------------------------------------------

def get_db_with_retry(log=None):
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
            return
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


# ---------------------------------------------------------------------------
# Common context builder  (same pattern as age_state._build_common_context)
# ---------------------------------------------------------------------------

def _build_common_context(request_id, channel_name, run_dir: Path):
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found")

    client_name   = request_data["client_name"]
    request_type  = request_data["request_type"]
    request_name  = request_data["request_name"]
    criteria_type = request_data["criteria_type"]
    comp_type     = request_data["comp_type"]

    final_files_dir = run_dir / "FINAL_FILES"
    final_files_dir.mkdir(parents=True, exist_ok=True)

    channel_tmp = run_dir / f"{channel_name.upper()}_tmp"
    channel_tmp.mkdir(parents=True, exist_ok=True)

    path_date  = datetime.now().strftime("%Y%m%d")
    perm_table = (
        f"APT_CPA_{channel_name.upper()}_{client_name}_{request_name}_{request_type}_{path_date}"
    )

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
        "comp_type"      : comp_type,
        "final_files_dir": final_files_dir,
        "channel_tmp"    : channel_tmp,
        "path_date"      : path_date,
        "path_FINAL"     : path_FINAL,
        "path_COMPLETE"  : path_COMPLETE,
        "output_file"    : output_file,
        "perm_table"     : perm_table,
    }


# ---------------------------------------------------------------------------
# Snowflake helpers  (mirrors age_state helpers; log param on every helper)
# ---------------------------------------------------------------------------

def _query_snowflake(query_sql, log):
    """Run a snowsql query and return the first integer found in output, else -1."""
    try:
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        output = subprocess.check_output(
            ["snowsql", "-c", "datateam1", "-q", query_sql,
             "-o", "output_format=csv",
             "-o", "header=false",
             "-o", "timing=false",
             "-o", "friendly=false"],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
        match = re.search(r"(\d+)", output)
        return int(match.group(1)) if match else -1
    except Exception as exc:
        log.warning(f"_query_snowflake failed (non-fatal): {exc}")
        return -1


def _count_file_lines(file_path):
    cmd    = f"wc -l < {shlex.quote(str(file_path))}"
    result = subprocess.check_output(cmd, shell=True, universal_newlines=True).strip()
    return int(result) if result else 0


# ── ZIP staging table ────────────────────────────────────────────────────────

def _create_zip_staging_table(zip_staging_table: str, log) -> None:
    """
    CREATE the shared ZIP staging table ONCE in Snowflake.
    Called by the orchestrator before any channel runs.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = (
        f"CREATE OR REPLACE TABLE {zip_staging_table} "
        f"(zip_code VARCHAR(10));"
    )
    log.info(f"  Creating ZIP staging table: {zip_staging_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  ZIP staging table created: {zip_staging_table}")


def _load_zips_from_s3(zip_staging_table: str, s3_zip_path: str, log) -> int:
    """
    COPY the ZIP codes CSV file from S3 into the shared staging table.
    Called by the orchestrator ONCE after creating the staging table.
    Returns the number of ZIP codes loaded.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    copy_sql = (
        f"COPY INTO {zip_staging_table} "
        f"FROM '{s3_zip_path}' "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE='CSV' FIELD_DELIMITER=',' SKIP_HEADER=1 "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
        f"ON_ERROR='CONTINUE' PURGE=FALSE;"
    )
    log.info(f"  Loading ZIP codes from S3: {s3_zip_path}")
    log.info(f"  COPY SQL: {copy_sql}")
    run_command(["snowsql", "-c", "datateam1", "-q", copy_sql])

    count_sql    = f"SELECT COUNT(*) FROM {zip_staging_table};"
    loaded_count = _query_snowflake(count_sql, log)
    if loaded_count >= 0:
        log.info(f"  ZIP codes loaded into {zip_staging_table}: {loaded_count:,}")
    else:
        log.warning(f"  Could not verify ZIP code count (non-fatal)")
    return loaded_count


def _drop_zip_staging_table(zip_staging_table: str, log) -> None:
    """
    DROP the shared ZIP staging table.
    Called by the orchestrator ONLY after ALL channels have completed.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"DROP TABLE IF EXISTS {zip_staging_table};"
    log.info(f"  Dropping shared ZIP staging table: {zip_staging_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  Shared ZIP staging table {zip_staging_table} dropped successfully")


# ── Per-channel perm table helpers ───────────────────────────────────────────

def _insert_into_perm_table(
    perm_table, channel_name, zip_staging_table, comp_type, log
):
    """
    CREATE OR REPLACE the per-channel permanent table by joining each
    channel's source table against the shared ZIP staging table.

    Matching logic:
      include  → zip_col IN  (SELECT zip_code FROM <zip_staging_table>)
      exclude  → zip_col NOT IN (SELECT zip_code FROM <zip_staging_table>)
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    kw = "IN" if comp_type == "include" else "NOT IN"

    if channel_name in ("GREEN", "BLUE"):
        profile_table = (
            "GREEN_LPT.UNIVERSAL_PROFILE"
            if channel_name == "GREEN"
            else "INFS_LPT.INFS_PROFILE"
        )
        condition = (
            f"b.ZIP {kw} (SELECT zip_code FROM {zip_staging_table})"
        )
        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
            f"SELECT a.email, b.ZIP "
            f"FROM {profile_table} a "
            f"JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5 "
            f"WHERE {condition};"
        )

    elif channel_name == "ARCAMAX":
        condition = (
            f"ZIP {kw} (SELECT zip_code FROM {zip_staging_table})"
        )
        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
            f"SELECT email, ZIP "
            f"FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            f"WHERE {condition};"
        )

    else:  # ORANGE
        condition = (
            f"a.ZIP {kw} (SELECT zip_code FROM {zip_staging_table})"
        )
        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
            f"SELECT a.email_address, a.ZIP, b.ACCOUNT_NAME "
            f"FROM ("
            f"  SELECT a.FEED_ID, a.email_address, a.ZIP "
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

    log.info(f"  Target table     : {perm_table}")
    log.info(f"  ZIP staging table: {zip_staging_table}")
    log.info(f"  comp_type        : {comp_type}  ({kw})")
    log.info(f"  INSERT SQL       : {insert_sql}")
    log.info("  Executing CREATE + INSERT via snowsql ...")

    run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
    log.info("  CREATE + INSERT executed successfully")

    count_sql     = f"SELECT COUNT(*) FROM {perm_table};"
    inserted_rows = _query_snowflake(count_sql, log)
    if inserted_rows >= 0:
        log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
    else:
        log.warning(f"  Could not verify row count for {perm_table} (non-fatal)")

    return inserted_rows


def _export_complete_final_file(export_type, perm_table, export_path, channel_name, log):
    """
    Export FINAL / COMPLETE file from permanent table -> S3 path.
    FINAL   : DISTINCT email (GREEN/BLUE/ARCAMAX)  |  DISTINCT email_address, account_name (ORANGE)
    COMPLETE: email, ZIP     (GREEN/BLUE/ARCAMAX)  |  email_address, ZIP, account_name      (ORANGE)
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    if export_type == "FINAL":
        select_clause = (
            "DISTINCT email_address, account_name"
            if channel_name == "ORANGE"
            else "DISTINCT email"
        )
    else:  # COMPLETE
        select_clause = (
            "email_address, ZIP, account_name"
            if channel_name == "ORANGE"
            else "email, ZIP"
        )

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

    if unloaded_rows >= 0:
        log.info(f"  Rows unloaded to {export_type} S3 path: {unloaded_rows:,}")
    else:
        log.warning(f"  Could not verify {export_type} FILE S3 row count (non-fatal)")


def _drop_perm_table(perm_table, log):
    """DROP the per-channel permanent table after both S3 exports are done."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"DROP TABLE IF EXISTS {perm_table};"
    log.info(f"  Dropping permanent table: {perm_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  Table {perm_table} dropped successfully")


# ── Download + combine helper ─────────────────────────────────────────────────

def _download_and_combine(s3_path, download_dir, work_dir,
                           output_file, channel_name, log):
    """
    Download gzipped S3 parts and concatenate into one pipe-delimited file.
    ORANGE: keep all columns.  Others: keep col-1 (email) only.
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
    log.info(
        f"  Downloaded {len(downloaded)} part file(s): "
        f"{[p.name for p in downloaded]}"
    )

    out_path = work_dir / output_file

    if channel_name != "ORANGE":
        run_command(
            f"printf 'email\\n' > {shlex.quote(str(out_path))} && "
            f"zcat {shlex.quote(str(download_dir) + '/')}data* "
            f"| sed 's/\\\"//g' | tail -n +2 | cut -d'|' -f1 >> {shlex.quote(str(out_path))}"
        )
    else:
        run_command(
            f"printf 'email\\n' > {shlex.quote(str(out_path))} && "
            f"zcat {shlex.quote(str(download_dir) + '/')}data* "
            f"| sed 's/\\\"//g' | tail -n +2 >> {shlex.quote(str(out_path))}"
        )

    run_command(f"rm -f {str(download_dir / 'data*')}", cwd=str(download_dir))

    combined_count = _count_file_lines(str(out_path))
    log.info(f"  Combined file written: {out_path}  |  rows: {combined_count:,}")
    return combined_count


# ── FTP / cleanup helpers ─────────────────────────────────────────────────────

def _post_to_ftp(final_files_dir, path_date, output_file, log):
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


def _cleanup_channel_tmp(channel_tmp, log):
    try:
        shutil.rmtree(str(channel_tmp))
        log.info(f"  Removed temp dir: {channel_tmp}")
    except Exception as exc:
        log.warning(f"  Could not remove temp dir {channel_tmp}: {exc}")


def _success_result(channel_name, output_file, final_file_path, elapsed, record_count=0):
    return {
        "channel"         : channel_name,
        "file"            : output_file,
        "final_file_path" : final_file_path,
        "status"          : "SUCCESS",
        "elapsed"         : elapsed,
        "count"           : record_count,
    }


# ---------------------------------------------------------------------------
# Channel processors  (7-step flow — mirrors age_state channel processors)
# ---------------------------------------------------------------------------

def process_green_blue_zip(request_id, channel_name, zip_staging_table, run_dir: Path):
    """
    GREEN and BLUE channel processor for ZIPS.
    Same 7-step flow as age_state.process_green_blue.
    Receives the shared zip_staging_table name; does NOT create or drop it.
    """
    TOTAL_STEPS    = 7
    channel_name   = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log            = setup_channel_logging(run_dir, channel_name)

    log.info("=" * 70)
    log.info(f"  {channel_name} CHANNEL (ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  comp_type      = {ctx['comp_type']}\n"
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
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = _insert_into_perm_table(
            perm_table, channel_name, zip_staging_table, ctx["comp_type"], log
        )

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            log.warning(
                f"  NO DATA RETRIEVED: 0 rows inserted into {perm_table}. "
                f"Skipping remaining steps for {channel_name}."
            )
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "file": None, "final_file_path": None,
                "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0,
            }

        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported -> {path_COMPLETE}")

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
        log.info(f"  STEP 5 DONE: Combined file rows: {combined_count:,}")

        # ── STEP 6/7 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        src_file  = channel_tmp / ctx["output_file"]
        dest_file = final_files_dir / ctx["output_file"]
        shutil.move(str(src_file), str(dest_file))
        log.info(f"  STEP 6 DONE: Moved {src_file.name} -> FINAL_FILES/")
        _cleanup_channel_tmp(channel_tmp, log)

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, "FTP upload from FINAL_FILES/", channel_name)
        update_request_status(request_id, "FTP Upload", channel_status, log)
        _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        log.info(f"  STEP 7 DONE: FTP upload complete")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        log.info(f"  {channel_name} CHANNEL COMPLETED in {elapsed:.1f}s | {combined_count:,} records")

        return _success_result(
            channel_name, ctx["output_file"], str(dest_file), elapsed, combined_count
        )

    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(f"  {channel_name} CHANNEL FAILED")
        raise


def process_arcamax_zip(request_id, zip_staging_table, run_dir: Path):
    """
    ARCAMAX channel processor for ZIPS.
    Same 7-step flow as age_state.process_arcamax.
    Uses the shared zip_staging_table for ZIP matching in Snowflake.
    """
    TOTAL_STEPS    = 7
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    log            = setup_channel_logging(run_dir, channel_name)

    log.info("=" * 70)
    log.info(f"  ARCAMAX CHANNEL (ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  comp_type     = {ctx['comp_type']}\n"
        f"  perm_table    = {ctx['perm_table']}\n"
        f"  path_FINAL    = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE = {ctx['path_COMPLETE']}\n"
        f"  output_file   = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = _insert_into_perm_table(
            perm_table, channel_name, zip_staging_table, ctx["comp_type"], log
        )

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            log.warning(f"  NO DATA: 0 rows inserted. Skipping remaining steps.")
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "file": None, "final_file_path": None,
                "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0,
            }

        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported -> {path_COMPLETE}")

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
        log.info(f"  STEP 5 DONE: Combined file rows: {combined_count:,}")

        # ── STEP 6/7 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        src_file  = channel_tmp / ctx["output_file"]
        dest_file = final_files_dir / ctx["output_file"]
        shutil.move(str(src_file), str(dest_file))
        log.info(f"  STEP 6 DONE: Moved {src_file.name} -> FINAL_FILES/")
        _cleanup_channel_tmp(channel_tmp, log)

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, "FTP upload from FINAL_FILES/", channel_name)
        update_request_status(request_id, "FTP Upload", channel_status, log)
        _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        log.info(f"  STEP 7 DONE: FTP upload complete")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        log.info(f"  ARCAMAX CHANNEL COMPLETED in {elapsed:.1f}s | {combined_count:,} records")

        return _success_result(
            channel_name, ctx["output_file"], str(dest_file), elapsed, combined_count
        )

    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception("  ARCAMAX CHANNEL FAILED")
        raise


def process_orange_zip(request_id, zip_staging_table, run_dir: Path):
    """
    ORANGE channel processor for ZIPS.
    Same 7-step flow as age_state.process_orange.
    Uses the shared zip_staging_table for ZIP matching in Snowflake.
    ORANGE final file is split per-ESP and zipped.
    """
    TOTAL_STEPS    = 7
    channel_name   = "ORANGE"
    channel_status = "ORANGE_STATUS"
    log            = setup_channel_logging(run_dir, channel_name)

    log.info("=" * 70)
    log.info(f"  ORANGE CHANNEL (ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  comp_type     = {ctx['comp_type']}\n"
        f"  perm_table    = {ctx['perm_table']}\n"
        f"  path_FINAL    = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE = {ctx['path_COMPLETE']}\n"
        f"  output_file   = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = _insert_into_perm_table(
            perm_table, channel_name, zip_staging_table, ctx["comp_type"], log
        )

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            log.warning(f"  NO DATA: 0 rows inserted. Skipping remaining steps.")
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "file": None, "final_file_path": None,
                "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0,
            }

        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT email_address + account_name) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email_address + ZIP + account_name) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported -> {path_COMPLETE}")

        # ── STEP 4/7 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/7 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir   = channel_tmp / "ORANGE_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )
        log.info(f"  STEP 5 DONE: Combined file rows: {combined_count:,}")

        # ── STEP 6/7 ── Split per-ESP + ZIP archive ────────────────────────
        _step(log, 6, TOTAL_STEPS, "Splitting ORANGE file per-ESP + creating ZIP archive", channel_name)
        combined_path = channel_tmp / ctx["output_file"]
        df_final = pd.read_csv(
            str(combined_path),
            names=["email", "account_name"], delimiter="|", skiprows=1
        )
        esp_names   = df_final["account_name"].drop_duplicates().sort_values().tolist()
        total_count = len(df_final)
        log.info(f"  Total ORANGE records: {total_count:,} across {len(esp_names)} ESPs")

        output_path = channel_tmp / "ORANGE_OP_PATH"
        output_path.mkdir(exist_ok=True)

        for esp in esp_names:
            df_esp = df_final[df_final["account_name"] == esp][["email"]]
            df_esp.to_csv(output_path / f"{esp}_ORANGE_DATA.csv", index=False, header=False)
            log.info(f"  ESP {esp}: {len(df_esp):,} records")

        zip_out_name = ctx["output_file"].replace(".csv", ".zip")
        zip_out      = final_files_dir / zip_out_name
        run_command(
            ["zip", "-r", str(zip_out), output_path.name],
            cwd=str(channel_tmp)
        )
        log.info(f"  STEP 6 DONE: ESP ZIP archive created -> {zip_out}")
        _cleanup_channel_tmp(channel_tmp, log)

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, "FTP upload (ORANGE suppression email list + mailing ZIP)", channel_name)
        update_request_status(request_id, "FTP Upload", channel_status, log)

        # ORANGE suppression: email-only CSV
        supp_file = ctx["output_file"]
        shutil.copy(str(final_files_dir / ctx["output_file"]), str(final_files_dir / supp_file))
        _post_to_ftp(final_files_dir, ctx["path_date"], supp_file, log)

        # ORANGE mailing: ESP-split ZIP
        _post_to_ftp(final_files_dir, ctx["path_date"], zip_out_name, log)
        log.info(f"  STEP 7 DONE: FTP upload complete")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        log.info(f"  ORANGE CHANNEL COMPLETED in {elapsed:.1f}s | {total_count:,} records")

        return _success_result(
            channel_name, zip_out_name, str(zip_out), elapsed, total_count
        )

    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception("  ORANGE CHANNEL FAILED")
        raise


# ---------------------------------------------------------------------------
# Orchestrator  (mirrors process_age_state_request)
# ---------------------------------------------------------------------------

def process_zip_request(
    request_id: int,
    zip_file: str,
    channel,
    output_dir: str,
):
    """
    Main entry point called by main.py.

    request_id : DB request ID (used to fetch client_name, request_type, comp_type …)
    zip_file   : absolute path to the ZIP codes file uploaded via UI
    channel    : list like ['ALL'] or ['GREEN', 'BLUE'] or single string 'ALL'
    output_dir : base output directory

    Flow
    ----
      PRE:   Upload ZIP file -> S3
             CREATE shared ZIP staging table ONCE
             COPY ZIP codes from S3 into staging table

      PARALLEL: Run all requested channels concurrently (same as age_state)

      POST:  DROP shared ZIP staging table (only after all channels finish)
    """
    # ── Run directory ─────────────────────────────────────────────────────
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / f"run_zips_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    log = setup_main_logging(run_dir)

    log.info("=" * 70)
    log.info("  ZIP REQUEST STARTED")
    log.info(f"  request_id : {request_id}")
    log.info(f"  zip_file   : {zip_file}")
    log.info(f"  channel    : {channel}")
    log.info(f"  output_dir : {output_dir}")
    log.info(f"  run_dir    : {run_dir}")
    log.info("=" * 70)

    # ── Resolve channels ──────────────────────────────────────────────────
    if isinstance(channel, str):
        channel = [channel]

    if "ALL" in channel:
        channels_to_run = list(CHANNELS)
    else:
        channels_to_run = [ch.upper() for ch in channel if ch.upper() in CHANNELS]

    log.info(f"  Channels to run: {channels_to_run}")

    # ── Fetch request details (for comp_type) ─────────────────────────────
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found in DB")

    comp_type = request_data["comp_type"]  # 'include' | 'exclude'
    log.info(f"  comp_type: {comp_type}")

    # ── PRE-CHANNEL STEP A: Upload ZIP file -> S3 ─────────────────────────
    path_date   = datetime.now().strftime("%Y%m%d")
    s3_zip_dir  = f"{S3_BASE}/ZIPS/{path_date}/staging"
    s3_zip_path = f"{s3_zip_dir}/{os.path.basename(zip_file)}"

    log.info(f"  [PRE] Uploading ZIP codes file to S3: {s3_zip_path}")
    run_command(["aws", "s3", "cp", zip_file, s3_zip_path, "--quiet"])
    log.info(f"  [PRE] ZIP file uploaded -> {s3_zip_path}")

    # ── PRE-CHANNEL STEP B: Create shared ZIP staging table ONCE ─────────
    zip_staging_table = f"APT_CPA_ZIPS_STAGING_{ts}"
    log.info(f"  [PRE] Creating shared ZIP staging table: {zip_staging_table}")
    _create_zip_staging_table(zip_staging_table, log)

    # ── PRE-CHANNEL STEP C: Load ZIP codes into staging table ─────────────
    log.info(f"  [PRE] Loading ZIP codes into staging table from S3 ...")
    zip_count = _load_zips_from_s3(zip_staging_table, s3_zip_path, log)
    log.info(f"  [PRE] {zip_count:,} ZIP codes loaded into {zip_staging_table}")

    if zip_count == 0:
        log.error("  [PRE] No ZIP codes loaded into staging table — aborting all channels")
        _drop_zip_staging_table(zip_staging_table, log)
        raise RuntimeError("ZIP staging table is empty — no ZIP codes were loaded from the file.")

    # ── Channel processor map ──────────────────────────────────────────────
    def _run_channel(ch):
        if ch in ("GREEN", "BLUE"):
            return process_green_blue_zip(request_id, ch, zip_staging_table, run_dir)
        elif ch == "ARCAMAX":
            return process_arcamax_zip(request_id, zip_staging_table, run_dir)
        elif ch == "ORANGE":
            return process_orange_zip(request_id, zip_staging_table, run_dir)
        else:
            raise ValueError(f"Unknown channel: {ch}")

    # ── Parallel channel execution (same pattern as age_state) ────────────
    results = {}
    errors  = []

    if len(channels_to_run) == 1:
        ch = channels_to_run[0]
        log.info(f"  Single channel '{ch}' — running directly (no thread pool)")
        try:
            result = _run_channel(ch)
            results[ch] = result
            log.info(f"  Channel '{ch}' completed: {result.get('count', 0):,} records")
        except Exception as exc:
            log.error(f"  Channel '{ch}' FAILED: {exc}")
            errors.append((ch, str(exc)))
    else:
        log.info(f"  Multiple channels — running in parallel with ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
            future_to_ch = {
                executor.submit(_run_channel, ch): ch
                for ch in channels_to_run
            }
            for future in as_completed(future_to_ch):
                ch = future_to_ch[future]
                try:
                    result = future.result()
                    results[ch] = result
                    log.info(
                        f"  Channel '{ch}' completed: "
                        f"{result.get('count', 0):,} records"
                    )
                except Exception as exc:
                    log.error(f"  Channel '{ch}' FAILED: {exc}")
                    errors.append((ch, str(exc)))

    # ── POST-CHANNEL: DROP shared ZIP staging table ───────────────────────
    log.info(f"  [POST] All channels finished. Dropping shared ZIP staging table: {zip_staging_table}")
    try:
        _drop_zip_staging_table(zip_staging_table, log)
    except Exception as exc:
        log.warning(f"  [POST] Failed to drop ZIP staging table (non-fatal): {exc}")

    # ── Summary ───────────────────────────────────────────────────────────
    total_records = sum(
        v.get("count", 0) for v in results.values() if isinstance(v, dict)
    )
    summary_lines = [
        f"  {ch}: {v.get('file')} ({v.get('count', 0):,} records)"
        for ch, v in results.items()
        if isinstance(v, dict)
    ]
    summary = (
        f"\n{'=' * 60}\n"
        f"ZIP REQUEST COMPLETE\n"
        f"request_id  : {request_id}\n"
        f"comp_type   : {comp_type}\n"
        f"Channels    : {', '.join(channels_to_run)}\n"
        f"Total recs  : {total_records:,}\n"
        f"ZIP staging : {zip_staging_table} (DROPPED)\n"
        f"Output      : {run_dir}/FINAL_FILES\n"
        + "\n".join(summary_lines)
        + (
            f"\nERRORS ({len(errors)}): " + "; ".join(f"{c}: {e}" for c, e in errors)
            if errors
            else ""
        )
        + f"\n{'=' * 60}"
    )
    log.info(summary)

    if errors:
        send_error_email(
            f"ZIP PARTIAL FAILURE — {len(errors)} channel(s) failed",
            "\n".join(f"{c}: {e}" for c, e in errors),
        )
    else:
        send_success_email(
            f"ZIP REQUEST COMPLETE — {total_records:,} matched",
            [v.get("file") for v in results.values() if isinstance(v, dict)],
            str(run_dir),
        )

    if errors and not results:
        raise RuntimeError(f"All channels failed: {errors}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: zips.py <request_id> <zip_file> <channel> [output_dir]")
        sys.exit(1)
    process_zip_request(
        request_id=int(sys.argv[1]),
        zip_file=sys.argv[2],
        channel=sys.argv[3],
        output_dir=sys.argv[4] if len(sys.argv) > 4 else ".",
    )
