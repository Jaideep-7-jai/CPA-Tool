#!/usr/bin/env python3
"""
AGE STATE Processing - unique request-driven channel execution.
Supports single-channel or all-channel execution, with parallel execution
only when multiple channels are requested.

Output directory layout per run:

    <output_dir>/run_age_<YYYYMMDD_HHMMSS>/
        logs/
            age_<YYYYMMDD_HHMMSS>.log           <- combined log (all channels)
            GREEN_age_<YYYYMMDD_HHMMSS>.log     <- per-channel log
            BLUE_age_<YYYYMMDD_HHMMSS>.log
            ARCAMAX_age_<YYYYMMDD_HHMMSS>.log
            ORANGE_age_<YYYYMMDD_HHMMSS>.log
        FINAL_DIR/
            <client>_Suppression_GREEN_<date>.csv
            <client>_Suppression_BLUE_<date>.csv
            ...  (final files; never deleted)
        GREEN/   <- temp channel working dir; deleted after FTP
        BLUE/
        ARCAMAX/
        ORANGE/

Changes:
  - run dir: run_age_YYYYMMDD_HHMMSS (date+time, not just time)
  - FINAL_DIR: all final output files placed here and kept permanently
  - logs/: combined + per-channel logs, all inside logs/ (no files outside)
  - Temp channel dirs (GREEN/, BLUE/ etc) removed after FTP upload
  - FINAL_DIR and logs/ are never deleted
  - GREEN/BLUE/ARCAMAX/ORANGE: data loaded to S3, then staged into a Snowflake
    TEMPORARY table; DISTINCT email written from that table to path_FINAL
    (one level above s3_path). File is downloaded from path_FINAL for FTP.
  - No row-count deduplication check (removed 100M threshold logic).
"""

import os
import glob
import time
import shutil
import logging
import pymysql
import subprocess
import shlex
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
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

# Module-level logger; replaced per-run by setup_logging().
logger = logging.getLogger("age_processor")


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _make_handler(log_path):
    """Create a FileHandler with the standard formatter."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(str(log_path))
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    )
    return handler


def setup_logging(output_dir, criteria_type):
    """
    Set up the shared 'age_processor' logger with a single combined log file
    under <output_dir>/logs/<criteria_type>_<timestamp>.log.
    """
    log_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(output_dir) / "logs" / f"{criteria_type}_{log_date}.log"

    _logger = logging.getLogger("age_processor")
    _logger.setLevel(logging.INFO)
    _logger.handlers.clear()
    _logger.addHandler(_make_handler(log_file))
    return _logger


def setup_channel_logging(output_dir, channel_name, criteria_type):
    """
    Add a per-channel FileHandler to the shared 'age_processor' logger.
    Returns the handler so the caller can remove it when the channel finishes.
    """
    log_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = (
        Path(output_dir) / "logs" / f"{channel_name.upper()}_{criteria_type}_{log_date}.log"
    )
    handler = _make_handler(log_file)
    _logger = logging.getLogger("age_processor")
    _logger.addHandler(handler)
    return handler


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def get_dob_cutoff(min_age, comp_type):
    if isinstance(min_age, str):
        min_age = int(min_age)
    cutoff_date = date.today() - timedelta(days=365.25 * min_age)
    return cutoff_date.strftime("%Y-%m-%d")


def get_db():
    return pymysql.connect(**DB_CONFIG)


def fetch_request_details(request_id):
    conn = get_db()
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
    except Exception:
        logger.exception("Unable to fetch request details")
        raise
    finally:
        conn.close()


def update_request_status(request_id, status, status_column):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE requests SET {status_column}=%s WHERE id=%s",
                (status, request_id),
            )
        conn.commit()
        logger.info(f"{status_column} updated to {status} for request_id={request_id}")
    except Exception:
        logger.exception(f"Failed updating {status_column}")
        raise
    finally:
        conn.close()


def _build_common_context(request_id, channel_name):
    """
    Build the shared context dict for a channel run.

    S3 layout:
        s3_path       = S3_BASE/<request_type>/<date>/<request_name>/<channel>   <- raw data
        path_FINAL    = S3_BASE/<request_type>/<date>/<request_name>/<channel>_FINAL  <- distinct email output
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
    output_dir     = request_data["output_dir"]

    safe_output_dir = ensure_output_dir(output_dir, criteria_type)

    final_dir = Path(str(safe_output_dir)) / "FINAL_DIR"
    final_dir.mkdir(parents=True, exist_ok=True)

    channel_dir = Path(str(safe_output_dir)) / channel_name.upper()
    channel_dir.mkdir(parents=True, exist_ok=True)

    path_date = datetime.now().strftime("%Y%m%d")

    # Raw S3 path where Snowflake COPY INTO writes gzipped parts
    s3_path = f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}"

    # FINAL S3 path (one level above s3_path folder, suffixed _FINAL).
    # DISTINCT email is written here from the Snowflake temp table.
    path_FINAL = f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}_FINAL"

    output_file = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"

    # Snowflake temporary table name (unique per channel + timestamp to avoid collisions)
    ts_suffix   = datetime.now().strftime("%Y%m%d%H%M%S")
    temp_table  = f"TMP_{channel_name.upper()}_{ts_suffix}"

    return {
        "request_data"  : request_data,
        "client_name"   : client_name,
        "request_type"  : request_type,
        "request_name"  : request_name,
        "criteria_type" : criteria_type,
        "criteria_value": criteria_value,
        "comp_type"     : comp_type,
        "output_dir"    : str(safe_output_dir),
        "final_dir"     : final_dir,
        "channel_dir"   : channel_dir,
        "path_date"     : path_date,
        "s3_path"       : s3_path,
        "path_FINAL"    : path_FINAL,
        "output_file"   : output_file,
        "temp_table"    : temp_table,
    }


def _count_file_lines(file_path):
    """Fast line count using wc -l (avoids loading file into memory)."""
    cmd    = f"wc -l < {shlex.quote(file_path)}"
    result = subprocess.check_output(cmd, shell=True, universal_newlines=True).strip()
    return int(result) if result else 0


def _download_and_combine(s3_path, download_dir, work_dir, output_file, channel_name):
    """
    Download gzipped S3 parts into *download_dir* and concatenate into one
    pipe-delimited file at <work_dir>/<output_file>.
    """
    download_dir = Path(download_dir)
    work_dir     = Path(work_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    run_command(
        ["aws", "s3", "cp", s3_path, str(download_dir), "--recursive", "--quiet"]
    )

    downloaded = sorted(download_dir.glob("data*"))
    if not downloaded:
        raise RuntimeError(
            f"aws s3 cp from {s3_path} downloaded 0 files into {download_dir}. "
            f"Check S3 path and IAM permissions."
        )

    out_path = work_dir / output_file
    if channel_name != "ORANGE":
        run_command(
            f"zcat {shlex.quote(str(download_dir) + '/')}data* | cut -d'|' -f1 > {shlex.quote(str(out_path))}",
        )
    else:
        run_command(
            f"zcat {shlex.quote(str(download_dir) + '/')}data* > {shlex.quote(str(out_path))}",
        )

    run_command(f"rm -f {str(download_dir / 'data*')}", cwd=str(download_dir))


def _load_s3_to_snowflake_temp(s3_path, temp_table, channel_name, criteria_type):
    """
    Stage the raw gzipped S3 parts into a Snowflake TEMPORARY table.

    The temp table schema is always (email STRING) for non-ORANGE channels.
    For ORANGE it is (email_address STRING, account_name STRING).
    The table is TEMPORARY so it is auto-dropped at session end.

    Steps:
      1. CREATE OR REPLACE TEMPORARY TABLE <temp_table> ...
      2. CREATE OR REPLACE STAGE pointing to s3_path
      3. COPY INTO <temp_table> FROM the stage
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    stage_name = f"STG_{temp_table}"

    if channel_name == "ORANGE":
        create_sql = (
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} "
            f"(email_address STRING, account_name STRING);"
        )
        copy_sql = (
            f"COPY INTO {temp_table} "
            f"FROM @{stage_name} "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=0) "
            f"ON_ERROR=CONTINUE;"
        )
    else:
        create_sql = (
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} (email STRING);"
        )
        copy_sql = (
            f"COPY INTO {temp_table} "
            f"FROM (SELECT $1 FROM @{stage_name}) "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=0) "
            f"ON_ERROR=CONTINUE;"
        )

    stage_sql = (
        f"CREATE OR REPLACE STAGE {stage_name} "
        f"URL='{s3_path}/' "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP);"
    )

    combined_sql = f"{create_sql} {stage_sql} {copy_sql}"
    logger.info(f"Loading S3 data into Snowflake temp table: {temp_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", combined_sql])
    logger.info(f"Temp table {temp_table} loaded successfully")


def _write_distinct_email_to_path_FINAL(temp_table, path_FINAL, channel_name):
    """
    Write DISTINCT email from the Snowflake temp table to path_FINAL (S3).

    For non-ORANGE channels: SELECT DISTINCT email
    For ORANGE channel     : SELECT DISTINCT email_address, account_name

    path_FINAL is one level above s3_path (e.g. <channel>_FINAL vs <channel>).
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    if channel_name == "ORANGE":
        select_clause = "DISTINCT email_address, account_name"
    else:
        select_clause = "DISTINCT email"

    sql = (
        f"COPY INTO '{path_FINAL}/' "
        f"FROM (SELECT {select_clause} FROM {temp_table}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
    )

    logger.info(
        f"Writing DISTINCT email from {temp_table} to path_FINAL: {path_FINAL}"
    )
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    logger.info(f"DISTINCT email written to path_FINAL successfully")


def _post_to_ftp(final_dir, path_date, output_file):
    """FTP upload from FINAL_DIR."""
    ftp_cmd = (
        f'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    run_command(ftp_cmd, cwd=str(final_dir))


def _cleanup_channel_dir(channel_dir):
    """Remove the per-channel TEMP working directory after FTP upload.
    FINAL_DIR and logs/ are never touched by this function."""
    try:
        shutil.rmtree(str(channel_dir))
        logger.info(f"Removed temp channel dir: {channel_dir}")
    except Exception as e:
        logger.warning(f"Could not remove temp channel dir {channel_dir}: {e}")


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
# Channel processors
# ---------------------------------------------------------------------------

def process_green_blue(request_id, channel_name):
    """
    GREEN and BLUE channel processor.

    Flow:
      1. COPY INTO s3_path   (raw data written to S3)
      2. Load s3_path -> Snowflake TEMPORARY table
      3. COPY DISTINCT email from temp table -> path_FINAL (S3)
      4. Download from path_FINAL -> combine -> local CSV in channel_dir
      5. FTP upload from FINAL_DIR
    """
    channel_name   = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    ch_handler = setup_channel_logging(ctx["output_dir"], channel_name, ctx["criteria_type"])

    logger.info(f"Started {channel_name} processing for {ctx['criteria_type']} {ctx['comp_type']}")

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]
        channel_dir    = ctx["channel_dir"]
        final_dir      = ctx["final_dir"]
        temp_table     = ctx["temp_table"]
        path_FINAL     = ctx["path_FINAL"]

        if criteria_type == "age":
            condition = f"b.AGE {'>=': if comp_type == 'greater' else '<'} {criteria_value}"
            header    = "a.email,b.age"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            condition = f"b.STATE {'IN' if comp_type == 'include' else 'NOT IN'} ('{states}')"
            header    = "a.email,b.state"
        else:
            raise Exception(f"Unsupported criteria_type {criteria_type}")

        profile_table = (
            "GREEN_LPT.UNIVERSAL_PROFILE" if channel_name == "GREEN" else "INFS_LPT.INFS_PROFILE"
        )

        # Step 1: Write raw data to S3
        sql_copy = (
            f"COPY INTO '{ctx['s3_path']}/' "
            f"FROM (SELECT {header} FROM {profile_table} a "
            f"JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash=b.EMAIL_MD5 "
            f"WHERE {condition}) "
            f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        logger.info(f"Writing raw data to S3: {ctx['s3_path']}")
        run_command(["snowsql", "-c", "datateam1", "-q", sql_copy])

        # Step 2: Load S3 data into Snowflake temp table
        update_request_status(request_id, "Loading to Snowflake", channel_status)
        _load_s3_to_snowflake_temp(ctx["s3_path"], temp_table, channel_name, criteria_type)

        # Step 3: Write DISTINCT email from temp table to path_FINAL
        update_request_status(request_id, "Writing Distinct Emails", channel_status)
        _write_distinct_email_to_path_FINAL(temp_table, path_FINAL, channel_name)

        # Step 4: Download from path_FINAL and combine into local CSV
        update_request_status(request_id, "Combining Data", channel_status)
        download_dir = channel_dir / f"{channel_name}_FINAL_PATH"
        logger.info(f"Downloading distinct email data from path_FINAL: {path_FINAL}")
        _download_and_combine(
            path_FINAL, download_dir, channel_dir, ctx["output_file"], channel_name
        )

        # Move combined file to FINAL_DIR
        tmp_file_path   = channel_dir / ctx["output_file"]
        final_file_path = final_dir / ctx["output_file"]
        shutil.move(str(tmp_file_path), str(final_file_path))
        record_count = _count_file_lines(str(final_file_path))
        logger.info(
            f"{channel_name}: {record_count} distinct emails -> FINAL_DIR/{ctx['output_file']}"
        )

        # Step 5: FTP upload
        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(final_dir, ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info(
            f"{channel_name} processing completed in {elapsed:.2f} seconds | "
            f"final file: FINAL_DIR/{ctx['output_file']}"
        )

        result = _success_result(
            channel_name, ctx["output_file"], str(final_file_path), elapsed, record_count
        )
        _cleanup_channel_dir(channel_dir)
        return result

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception(f"{channel_name} processing failed")
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise
    finally:
        logging.getLogger("age_processor").removeHandler(ch_handler)
        ch_handler.close()


def process_arcamax(request_id):
    """
    ARCAMAX channel processor.

    Flow:
      1. COPY INTO s3_path   (raw data written to S3)
      2. Load s3_path -> Snowflake TEMPORARY table
      3. COPY DISTINCT email from temp table -> path_FINAL (S3)
      4. Download from path_FINAL -> combine -> local CSV in channel_dir
      5. FTP upload from FINAL_DIR
    """
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    ch_handler = setup_channel_logging(ctx["output_dir"], channel_name, ctx["criteria_type"])

    logger.info(f"Started {channel_name} processing for {ctx['criteria_type']} {ctx['comp_type']}")

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]
        channel_dir    = ctx["channel_dir"]
        final_dir      = ctx["final_dir"]
        temp_table     = ctx["temp_table"]
        path_FINAL     = ctx["path_FINAL"]

        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            condition   = (
                f"birthday IS NOT NULL AND TRY_TO_DATE(birthday) "
                f"{'<=' if comp_type == 'greater' else '>='} '{date_cutoff}'"
            )
            header = "email,birthday"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            condition = f"STATE {'IN' if comp_type == 'include' else 'NOT IN'} ('{states}')"
            header    = "email,state"
        else:
            raise Exception(f"Unsupported criteria_type {criteria_type}")

        # Step 1: Write raw data to S3
        sql_copy = (
            f"COPY INTO '{ctx['s3_path']}/' "
            f"FROM (SELECT {header} FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            f"WHERE {condition}) "
            f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        logger.info(f"Writing raw data to S3: {ctx['s3_path']}")
        run_command(["snowsql", "-c", "datateam1", "-q", sql_copy])

        # Step 2: Load S3 data into Snowflake temp table
        update_request_status(request_id, "Loading to Snowflake", channel_status)
        _load_s3_to_snowflake_temp(ctx["s3_path"], temp_table, channel_name, criteria_type)

        # Step 3: Write DISTINCT email from temp table to path_FINAL
        update_request_status(request_id, "Writing Distinct Emails", channel_status)
        _write_distinct_email_to_path_FINAL(temp_table, path_FINAL, channel_name)

        # Step 4: Download from path_FINAL and combine into local CSV
        update_request_status(request_id, "Combining Data", channel_status)
        download_dir = channel_dir / "ARCAMAX_FINAL_PATH"
        logger.info(f"Downloading distinct email data from path_FINAL: {path_FINAL}")
        _download_and_combine(
            path_FINAL, download_dir, channel_dir, ctx["output_file"], channel_name
        )

        # Move combined file to FINAL_DIR
        tmp_file_path   = channel_dir / ctx["output_file"]
        final_file_path = final_dir / ctx["output_file"]
        shutil.move(str(tmp_file_path), str(final_file_path))
        record_count = _count_file_lines(str(final_file_path))
        logger.info(
            f"{channel_name}: {record_count} distinct emails -> FINAL_DIR/{ctx['output_file']}"
        )

        # Step 5: FTP upload
        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(final_dir, ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info(
            f"{channel_name} processing completed in {elapsed:.2f} seconds | "
            f"final file: FINAL_DIR/{ctx['output_file']}"
        )

        result = _success_result(
            channel_name, ctx["output_file"], str(final_file_path), elapsed, record_count
        )
        _cleanup_channel_dir(channel_dir)
        return result

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception(f"{channel_name} processing failed")
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise
    finally:
        logging.getLogger("age_processor").removeHandler(ch_handler)
        ch_handler.close()


def process_orange(request_id):
    """
    ORANGE channel processor.

    Flow:
      1. COPY INTO s3_path   (raw data written to S3)
      2. Load s3_path -> Snowflake TEMPORARY table (email_address, account_name)
      3. COPY DISTINCT email_address, account_name from temp table -> path_FINAL (S3)
      4. Download from path_FINAL -> combine -> local CSV in channel_dir
      5. suppression -> single email-only CSV in FINAL_DIR
         mailing     -> ESP-wise split CSVs packed into a ZIP in FINAL_DIR
      6. FTP upload from FINAL_DIR
    """
    channel_name   = "ORANGE"
    channel_status = "ORANGE_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    ch_handler = setup_channel_logging(ctx["output_dir"], channel_name, ctx["criteria_type"])

    logger.info(f"Started {channel_name} processing for {ctx['criteria_type']} {ctx['comp_type']}")

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]
        request_type   = ctx["request_type"]
        client_name    = ctx["client_name"]
        channel_dir    = ctx["channel_dir"]
        final_dir      = ctx["final_dir"]
        path_date      = ctx["path_date"]
        temp_table     = ctx["temp_table"]
        path_FINAL     = ctx["path_FINAL"]

        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            condition   = f"dob {'<=' if comp_type == 'greater' else '>='} '{date_cutoff}'"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            condition = f"STATE {'IN' if comp_type == 'include' else 'NOT IN'} ('{states}')"
        else:
            raise Exception(f"Unsupported criteria_type {criteria_type}")

        # Step 1: Write raw data to S3
        sql_copy = (
            f"COPY INTO '{ctx['s3_path']}/' "
            f"FROM ("
            f"SELECT a.email_address, b.ACCOUNT_NAME "
            f"FROM ("
            f"SELECT a.FEED_ID, a.email_address "
            f"FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a, "
            f"(SELECT a.email_address, MAX(a.created_at) AS maxdate "
            f"FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a "
            f"WHERE {condition} GROUP BY 1) b "
            f"WHERE a.email_address=b.email_address AND a.created_at=b.maxdate"
            f") a "
            f"JOIN APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 b ON a.FEED_ID=b.FEEDID "
            f"JOIN APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND c ON a.email_address=c.email_address "
            f"JOIN APT_CUSTOM_L90_ORANGE_UNIQ_RESPONDERS_UNIQ_DND d ON a.email_address=d.email"
            f") "
            f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        logger.info(f"Writing raw data to S3: {ctx['s3_path']}")
        run_command(["snowsql", "-c", "datateam1", "-q", sql_copy])

        # Step 2: Load S3 data into Snowflake temp table
        update_request_status(request_id, "Loading to Snowflake", channel_status)
        _load_s3_to_snowflake_temp(ctx["s3_path"], temp_table, channel_name, criteria_type)

        # Step 3: Write DISTINCT email_address, account_name from temp table to path_FINAL
        update_request_status(request_id, "Writing Distinct Emails", channel_status)
        _write_distinct_email_to_path_FINAL(temp_table, path_FINAL, channel_name)

        # Step 4: Download from path_FINAL and combine into local CSV
        update_request_status(request_id, "Combining Data", channel_status)
        raw_combined = f"ORANGE_RAW_{path_date}.csv"
        download_dir = channel_dir / "ORANGE_FINAL_PATH"
        logger.info(f"Downloading distinct email data from path_FINAL: {path_FINAL}")
        _download_and_combine(
            path_FINAL, download_dir, channel_dir, raw_combined, channel_name
        )

        df_final = pd.read_csv(
            str(channel_dir / raw_combined),
            sep="|",
            header=None,
            names=["email_address", "account_name"],
            dtype=str,
        )
        total_count = len(df_final)
        logger.info(f"ORANGE distinct records from path_FINAL: {total_count}")

        update_request_status(request_id, "Posting To FTP", channel_status)

        if request_type.lower() == "suppression":
            output_file      = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"
            suppression_path = final_dir / output_file
            (
                df_final[["email_address"]]
                .drop_duplicates()
                .to_csv(str(suppression_path), index=False, header=False)
            )
            record_count = len(pd.read_csv(str(suppression_path), header=None))
            logger.info(
                f"ORANGE suppression file: {output_file} ({record_count} records) -> FINAL_DIR"
            )
            _post_to_ftp(final_dir, path_date, output_file)
            final_file_path = str(suppression_path)

        else:
            esp_split_dir = channel_dir / "ORANGE_ESP_SPLIT"
            esp_split_dir.mkdir(exist_ok=True)
            esp_names = df_final["account_name"].dropna().unique()
            zip_parts = []
            for esp in esp_names:
                esp_df   = df_final[df_final["account_name"] == esp][["email_address"]].drop_duplicates()
                esp_file = esp_split_dir / f"{client_name}_{request_type}_{esp}_{path_date}.csv"
                esp_df.to_csv(str(esp_file), index=False, header=False)
                zip_parts.append(esp_file)
                logger.info(f"ORANGE ESP split: {esp_file.name} ({len(esp_df)} records)")

            zip_name = f"{client_name}_{request_type}_ORANGE_{path_date}.zip"
            zip_path = final_dir / zip_name
            run_command(
                ["zip", "-j", str(zip_path)] + [str(p) for p in zip_parts]
            )
            logger.info(f"ORANGE mailing ZIP: {zip_name} ({len(zip_parts)} ESPs) -> FINAL_DIR")
            _post_to_ftp(final_dir, path_date, zip_name)
            final_file_path = str(zip_path)
            output_file     = zip_name
            record_count    = total_count

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info(
            f"ORANGE processing completed in {elapsed:.2f} seconds | "
            f"final file: FINAL_DIR/{output_file}"
        )

        result = _success_result(
            channel_name, output_file, final_file_path, elapsed, record_count
        )
        _cleanup_channel_dir(channel_dir)
        return result

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception(f"{channel_name} processing failed")
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise
    finally:
        logging.getLogger("age_processor").removeHandler(ch_handler)
        ch_handler.close()


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

    setup_logging(output_dir, criteria_type)

    channels_to_run = CHANNELS if channel.upper() == "ALL" else [channel.upper()]
    logger.info(
        f"Started request_id={request_id} for channels={','.join(channels_to_run)}"
    )

    for ch in channels_to_run:
        update_request_status(request_id, "Started", f"{ch}_STATUS")

    def process_channel(ch):
        if ch in ("GREEN", "BLUE"):
            return process_green_blue(request_id, ch)
        elif ch == "ARCAMAX":
            return process_arcamax(request_id)
        elif ch == "ORANGE":
            return process_orange(request_id)
        else:
            raise ValueError(f"Unknown channel: {ch}")

    results = {}
    failed  = []

    if len(channels_to_run) == 1:
        ch = channels_to_run[0]
        try:
            results[ch] = process_channel(ch)
        except Exception as e:
            failed.append(ch)
            logger.error(f"FAILED: {e}")
    else:
        with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
            future_map = {executor.submit(process_channel, ch): ch for ch in channels_to_run}
            for future in as_completed(future_map):
                ch = future_map[future]
                try:
                    results[ch] = future.result()
                except Exception as e:
                    failed.append(ch)
                    logger.error(f"FAILED: {e}")
                    raise

    overall_status = "failed" if failed else "completed"
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET overall_status=%s WHERE id=%s",
                (overall_status, request_id),
            )
        conn.commit()
    finally:
        conn.close()

    if not failed:
        send_success_email(
            f"Request {request_id} completed",
            f"Channels: {', '.join(channels_to_run)}\nResults: {results}",
        )

    return results
