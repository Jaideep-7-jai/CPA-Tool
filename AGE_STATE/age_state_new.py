#!/usr/bin/env python3
"""
AGE STATE Processing - unique request-driven channel execution.
Supports single-channel or all-channel execution, with parallel execution
only when multiple channels are requested.

Changes:
  - GREEN/BLUE/ARCAMAX: deduplicate on email column after combining S3 files
  - ORANGE: suppression -> email-only CSV; mailing -> ESP-wise split + ZIP
  - SMART DEDUP: if combined file row-count > 100M, dedup via Snowflake staging
    table (avoids /tmp space exhaustion on servers like GREEN with 278M rows).
    If <= 100M rows, dedup in-process with pandas (fast, no network round-trip).
  - All logs under <output_dir>/logs/ (combined + per-channel subdirs).
  - Output folder: run_age_<HHMMSS>/ timestamped subdir.
  - Per-channel output dirs removed after final file is written to FTP.
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

# Row count threshold above which we use Snowflake staging for dedup instead
# of in-process pandas (to avoid /tmp space exhaustion on large datasets like
# GREEN which can produce 278M+ rows).
LARGE_FILE_ROW_THRESHOLD = 100_000_000

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
    Returns the logger.
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
    Add a per-channel FileHandler to the shared 'age_processor' logger so that
    every message is written to BOTH the combined log and the channel-specific
    log file:

        <output_dir>/logs/<channel>/<criteria_type>_<timestamp>.log

    Returns the channel-specific FileHandler so the caller can remove it when
    the channel finishes (keeping the combined log handler in place).
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

    Output folder layout:

        <output_dir>/run_age_<HHMMSS>/<CHANNEL>/
            e.g. output/TEST_.../run_age_143022/GREEN/

    Each channel writes its final CSV directly into its own channel subdir.
    Temporary download dirs are created as siblings inside the channel subdir.
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

    # ensure_output_dir returns the timestamped run subdir
    safe_output_dir = ensure_output_dir(output_dir, criteria_type)

    # Per-channel subdir — avoids any cross-channel file collision
    channel_dir = Path(str(safe_output_dir)) / channel_name.upper()
    channel_dir.mkdir(parents=True, exist_ok=True)

    path_date   = datetime.now().strftime("%Y%m%d")
    s3_path     = f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}"
    output_file = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"

    return {
        "request_data"  : request_data,
        "client_name"   : client_name,
        "request_type"  : request_type,
        "request_name"  : request_name,
        "criteria_type" : criteria_type,
        "criteria_value": criteria_value,
        "comp_type"     : comp_type,
        "output_dir"    : str(safe_output_dir),
        "channel_dir"   : channel_dir,
        "path_date"     : path_date,
        "s3_path"       : s3_path,
        "output_file"   : output_file,
    }


def _count_file_lines(file_path):
    """Fast line count using wc -l (avoids loading file into memory)."""
    cmd    = f"wc -l < {shlex.quote(file_path)}"
    result = subprocess.check_output(cmd, shell=True, universal_newlines=True).strip()
    return int(result) if result else 0


def _download_and_combine(s3_path, download_dir, channel_dir, output_file, channel_name):
    """
    Download gzipped S3 parts into *download_dir* and concatenate into one
    pipe-delimited file at <channel_dir>/<output_file>.
    """
    download_dir = Path(download_dir)
    channel_dir  = Path(channel_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    channel_dir.mkdir(parents=True, exist_ok=True)

    run_command(
        ["aws", "s3", "cp", s3_path, str(download_dir), "--recursive", "--quiet"]
    )

    downloaded = sorted(download_dir.glob("data*"))
    if not downloaded:
        raise RuntimeError(
            f"aws s3 cp from {s3_path} downloaded 0 files into {download_dir}. "
            f"Check S3 path and IAM permissions."
        )

    out_path = channel_dir / output_file
    if channel_name != "ORANGE":
        run_command(
            f"zcat {shlex.quote(str(download_dir) + '/')}data* | cut -d'|' -f1 > {shlex.quote(str(out_path))}",
        )
    else:
        run_command(
            f"zcat {shlex.quote(str(download_dir) + '/')}data* > {shlex.quote(str(out_path))}",
        )

    run_command(f"rm -f {str(download_dir / 'data*')}", cwd=str(download_dir))


def _get_snowflake_row_count(stage_table):
    """
    Query Snowflake for the exact row count of a staging table.
    Returns the integer count, or raises RuntimeError if the query fails.

    Uses stdout=subprocess.PIPE / stderr=subprocess.PIPE instead of
    capture_output=True to remain compatible with Python 3.6
    (capture_output was added in Python 3.7).
    """
    count_sql = f"SELECT COUNT(*) FROM {stage_table};"
    result = subprocess.run(
        ["snowsql", "-c", "datateam1", "-q", count_sql,
         "-o", "output_format=csv", "-o", "header=false"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to query row count for {stage_table}:\n{result.stderr.strip()}"
        )
    for line in result.stdout.splitlines():
        line = line.strip().strip('"')
        if line.isdigit():
            return int(line)
    raise RuntimeError(
        f"Could not parse row count from SnowSQL output for {stage_table}.\nstdout: {result.stdout!r}"
    )


def _dedup_via_snowflake(file_path, channel_name, s3_path, path_date):
    """
    Dedup strategy for files with > LARGE_FILE_ROW_THRESHOLD rows.

    Steps:
      1. Load combined CSV into a temporary Snowflake staging table.
      2. Assert the staging table has rows (fail fast before COPY OUT).
      3. SELECT DISTINCT email -> COPY OUT to per-channel dedup S3 prefix.
      4. Download part-files; assert at least one CSV landed.
      5. Concatenate part-files into the original file_path using Python.
      6. Clean up Snowflake staging table and S3 dedup prefix.

    Python 3.6 compatibility:
      - subprocess.run uses stdout/stderr=PIPE, not capture_output=True (3.7+)
      - Path.unlink() uses try/except instead of missing_ok=True (3.8+)
    """
    stage_table  = f"APT_ADHOC_DEDUP_STAGING_{channel_name}_{path_date}"
    dedup_s3     = f"{s3_path}/DEDUP_{path_date}/"
    final_dir    = Path(file_path).parent

    dedup_dl_dir = final_dir.resolve() / f"{channel_name}_DEDUP_TMP"
    dedup_dl_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Large file detected for {channel_name} — using Snowflake staging dedup (table: {stage_table})"
    )
    logger.info(f"Snowflake dedup S3 prefix  : {dedup_s3}")
    logger.info(f"Local download directory    : {str(dedup_dl_dir)}")

    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    # 1. Create staging table and load
    run_command(["snowsql", "-c", "datateam1", "-q",
                 f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} (email VARCHAR);"])

    run_command(["snowsql", "-c", "datateam1", "-q",
                 f"PUT file://{file_path} @~/{stage_table}/  OVERWRITE=TRUE AUTO_COMPRESS=TRUE;"])

    run_command(["snowsql", "-c", "datateam1", "-q",
                 f"COPY INTO {stage_table} FROM @~/{stage_table}/ "
                 f"FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=0);"])

    # 2. Assert staging table has rows
    staging_row_count = _get_snowflake_row_count(stage_table)
    logger.info(f"Staging table {stage_table} loaded with {staging_row_count} rows")
    if staging_row_count == 0:
        raise RuntimeError(
            f"Snowflake staging table {stage_table} has 0 rows after COPY INTO. "
            f"Check file format and source file: {file_path}"
        )

    # 3. COPY DISTINCT emails out
    run_command(["snowsql", "-c", "datateam1", "-q",
                 f"COPY INTO '{dedup_s3}' "
                 f"FROM (SELECT DISTINCT email FROM {stage_table}) "
                 f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
                 f"FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER='|') "
                 f"MAX_FILE_SIZE=490000000;"])

    # 4. Download dedup parts
    run_command(["aws", "s3", "cp", dedup_s3, str(dedup_dl_dir), "--recursive", "--quiet"])

    downloaded_csvs = sorted(dedup_dl_dir.glob("*.csv"))
    if not downloaded_csvs:
        raise FileNotFoundError(
            f"Snowflake COPY OUT produced no CSV files in {dedup_dl_dir}. "
            f"Expected files at S3 prefix: {dedup_s3}. "
            f"Check COPY INTO wrote rows and aws s3 cp target matches dedup_dl_dir."
        )

    total_dl_bytes = sum(f.stat().st_size for f in downloaded_csvs)
    logger.info(
        f"Downloaded {len(downloaded_csvs)} CSV part-file(s) from {dedup_s3} "
        f"(total {total_dl_bytes / 1_048_576:.1f} MB)"
    )

    # 5. Concatenate part-files into the original file_path
    with open(file_path, "wb") as out_fh:
        for part in downloaded_csvs:
            with open(str(part), "rb") as in_fh:
                while True:
                    chunk = in_fh.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    out_fh.write(chunk)
    logger.info(f"Reassembled {len(downloaded_csvs)} part-file(s) into {file_path}")

    # 6. Count final rows and cleanup
    record_count = _count_file_lines(file_path)

    for part in downloaded_csvs:
        try:
            part.unlink()
        except FileNotFoundError:
            pass

    try:
        dedup_dl_dir.rmdir()
    except OSError:
        logger.warning(
            f"dedup_dl_dir {str(dedup_dl_dir)} is not empty after cleanup — skipping rmdir"
        )

    run_command(["snowsql", "-c", "datateam1", "-q",
                 f"DROP TABLE IF EXISTS {stage_table}; REMOVE @~/{stage_table}/;"])
    run_command(["aws", "s3", "rm", dedup_s3, "--recursive", "--quiet"])

    logger.info(
        f"Snowflake dedup complete for {channel_name}: {record_count} unique emails in final file"
    )
    return record_count


def _dedup_email_col(file_path, channel_name="", s3_path="", path_date=""):
    """
    Smart dedup dispatcher:
      - Count rows with wc -l (cheap, no memory cost).
      - If row count > LARGE_FILE_ROW_THRESHOLD (100M):
            use Snowflake staging table to do the dedup.
      - Else:
            use pandas in-process dedup (fast for smaller files).
    """
    row_count = _count_file_lines(file_path)
    logger.info(
        f"Dedup check for {file_path}: {row_count} rows (threshold={LARGE_FILE_ROW_THRESHOLD})"
    )

    if row_count > LARGE_FILE_ROW_THRESHOLD:
        if not s3_path or not path_date or not channel_name:
            raise ValueError(
                "_dedup_email_col: s3_path, path_date, and channel_name are "
                "required for large-file Snowflake dedup."
            )
        return _dedup_via_snowflake(file_path, channel_name, s3_path, path_date)
    else:
        df     = pd.read_csv(file_path, sep="|", header=None, dtype=str)
        before = len(df)
        df     = df.drop_duplicates(subset=[0])
        df.to_csv(file_path, sep="|", index=False, header=False)
        after  = len(df)
        logger.info(
            f"Dedup {file_path}: {before} -> {after} unique emails (removed {before - after})"
        )
        return after


def _post_to_ftp(channel_dir, path_date, output_file):
    ftp_cmd = (
        f'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    run_command(ftp_cmd, cwd=str(channel_dir))


def _cleanup_channel_dir(channel_dir):
    """Remove the per-channel output directory after FTP upload."""
    try:
        shutil.rmtree(str(channel_dir))
        logger.info(f"Removed channel dir: {channel_dir}")
    except Exception as e:
        logger.warning(f"Could not remove channel dir {channel_dir}: {e}")


def _success_result(channel_name, channel_dir, output_file, elapsed, record_count=0):
    return {
        "channel" : channel_name,
        "file"    : output_file,
        "status"  : "SUCCESS",
        "elapsed" : elapsed,
        "count"   : record_count,
    }


# ---------------------------------------------------------------------------
# Channel processors
# ---------------------------------------------------------------------------

def process_green_blue(request_id, channel_name):
    """GREEN and BLUE channel processor with smart email deduplication."""
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

        if criteria_type == "age":
            condition = f'''b.AGE {">=" if comp_type == 'greater' else "<"} {criteria_value}'''
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

        sql = (
            f"COPY INTO '{ctx['s3_path']}/' "
            f"FROM (SELECT DISTINCT {header} FROM {profile_table} a "
            f"JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash=b.EMAIL_MD5 "
            f"WHERE {condition}) "
            f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        run_command(["snowsql", "-c", "datateam1", "-q", sql])

        update_request_status(request_id, "Combining Data", channel_status)
        download_dir = channel_dir / f"{channel_name}_OP_PATH"
        _download_and_combine(ctx["s3_path"], download_dir, channel_dir, ctx["output_file"], channel_name)

        final_file_path = str(channel_dir / ctx["output_file"])
        record_count = _dedup_email_col(
            final_file_path,
            channel_name=channel_name,
            s3_path=ctx["s3_path"],
            path_date=ctx["path_date"],
        )

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(channel_dir, ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info(f"{channel_name} processing completed in {elapsed:.2f} seconds")

        result = _success_result(channel_name, channel_dir, ctx["output_file"], elapsed, record_count)
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
    """ARCAMAX channel processor with smart email deduplication."""
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

        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            condition   = (
                f"birthday IS NOT NULL AND TRY_TO_DATE(birthday) {'<=' if comp_type == 'greater' else '>='} '{date_cutoff}'"
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

        sql = (
            f"COPY INTO '{ctx['s3_path']}/' "
            f"FROM (SELECT DISTINCT {header} FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            f"WHERE {condition}) "
            f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        run_command(["snowsql", "-c", "datateam1", "-q", sql])

        update_request_status(request_id, "Combining Data", channel_status)
        download_dir = channel_dir / "ARCAMAX_OP_PATH"
        _download_and_combine(ctx["s3_path"], download_dir, channel_dir, ctx["output_file"], channel_name)

        final_file_path = str(channel_dir / ctx["output_file"])
        record_count = _dedup_email_col(
            final_file_path,
            channel_name=channel_name,
            s3_path=ctx["s3_path"],
            path_date=ctx["path_date"],
        )

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(channel_dir, ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info(f"{channel_name} processing completed in {elapsed:.2f} seconds")

        result = _success_result(channel_name, channel_dir, ctx["output_file"], elapsed, record_count)
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
    - suppression -> single email-only CSV
    - mailing     -> ESP-wise split CSVs packed into a ZIP
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
        path_date      = ctx["path_date"]

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

        sql = (
            f"COPY INTO '{ctx['s3_path']}/' "
            f"FROM ("
            f"SELECT DISTINCT a.email_address, b.ACCOUNT_NAME "
            f"FROM ("
            f"SELECT DISTINCT a.FEED_ID, a.email_address "
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
        run_command(["snowsql", "-c", "datateam1", "-q", sql])

        update_request_status(request_id, "Combining Data", channel_status)
        raw_combined = f"ORANGE_RAW_{path_date}.csv"
        download_dir = channel_dir / "ORANGE_OP_PATH"
        _download_and_combine(ctx["s3_path"], download_dir, channel_dir, raw_combined, channel_name)

        df_final = pd.read_csv(
            str(channel_dir / raw_combined),
            sep="|",
            header=None,
            names=["email_address", "account_name"],
            dtype=str,
        )
        total_count = len(df_final)
        logger.info(f"ORANGE raw records: {total_count}")

        update_request_status(request_id, "Posting To FTP", channel_status)

        if request_type.lower() == "suppression":
            output_file      = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"
            suppression_path = channel_dir / output_file
            (
                df_final[["email_address"]]
                .drop_duplicates()
                .to_csv(str(suppression_path), index=False, header=False)
            )
            run_command(f"rm -f {raw_combined}", cwd=str(channel_dir))
            record_count = len(pd.read_csv(str(suppression_path), header=None))
            logger.info(f"ORANGE suppression file: {output_file} ({record_count} records)")
            _post_to_ftp(channel_dir, path_date, output_file)

        else:
            esp_split_dir = channel_dir / "ORANGE_ESP_SPLIT"
            esp_split_dir.mkdir(exist_ok=True)
            esp_names = df_final["account_name"].drop_duplicates().sort_values().tolist()
            for esp in esp_names:
                df_esp = df_final[df_final["account_name"] == esp][["email_address"]]
                df_esp.to_csv(
                    str(esp_split_dir / f"{esp}_ORANGE_DATA.csv"),
                    index=False,
                    header=False,
                )
                logger.info(f"ESP split: {esp} -> {len(df_esp)} emails")

            output_file = f"{client_name}_{request_type}_{channel_name}_{path_date}.zip"
            zip_file = channel_dir / output_file
            run_command(
                ["zip", "-r", zip_file.name, esp_split_dir.name],
                cwd=str(channel_dir),
            )
            run_command(
                f"rm -rf {esp_split_dir.name} && rm -f {raw_combined}",
                cwd=str(channel_dir),
            )
            record_count = total_count
            logger.info(f"ORANGE mailing ZIP: {output_file} ({record_count} records)")
            _post_to_ftp(channel_dir, path_date, output_file)

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info(f"{channel_name} processing completed in {elapsed:.2f} seconds")

        result = _success_result(channel_name, channel_dir, output_file, elapsed, record_count)
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
# Dispatcher
# ---------------------------------------------------------------------------

def process_channel(request_id, channel_name):
    channel_name = channel_name.upper()
    if channel_name in ["GREEN", "BLUE"]:
        return process_green_blue(request_id, channel_name)
    if channel_name == "ARCAMAX":
        return process_arcamax(request_id)
    if channel_name == "ORANGE":
        return process_orange(request_id)
    raise ValueError(f"Unsupported channel: {channel_name}")


def process_age_state_request(request_id, channel="ALL"):
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found")

    criteria_type  = request_data["criteria_type"]
    criteria_value = request_data["criteria_value"]
    comp_type      = request_data["comp_type"]
    output_dir     = ensure_output_dir(request_data["output_dir"], criteria_type)

    global logger
    logger = setup_logging(str(output_dir), criteria_type)

    selected = channel.upper()
    if selected == "ALL":
        channels_to_run = CHANNELS
    else:
        if selected not in CHANNELS:
            raise ValueError(f"Unsupported channel: {channel}")
        channels_to_run = [selected]

    logger.info(f"Started request_id={request_id} for channels={','.join(channels_to_run)}")
    results = {}

    try:
        if len(channels_to_run) == 1:
            result = process_channel(request_id, channels_to_run[0])
            results[result["channel"]] = result
        else:
            with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
                future_map = {
                    executor.submit(process_channel, request_id, ch): ch
                    for ch in channels_to_run
                }
                for future in as_completed(future_map):
                    result = future.result()
                    results[result["channel"]] = result

        total_records = sum(r.get("count", 0) for r in results.values())

        summary_lines = [
            f"{r['channel']}: {r['file']} | {r.get('count', 0):,} records | {r['status']} | {r['elapsed']:.2f}s"
            for r in results.values()
        ]
        summary = (
            f"SUCCESS - {comp_type.upper()} {criteria_type} {criteria_value}\n"
            f"Output Dir: {str(output_dir)}\n"
            f"Total Records: {total_records:,}\n"
            + "\n".join(summary_lines)
        )
        logger.info(summary)
        send_success_email(
            f"{comp_type.upper()} {criteria_type} {criteria_value} - {total_records:,} RECORDS",
            [],
            str(output_dir),
        )
        return results

    except Exception as e:
        logger.error(f"FAILED: {e}")
        send_error_email(
            f"{comp_type.upper()} {criteria_type} {criteria_value} FAILED",
            str(e),
        )
        raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python age_state_new.py <request_id> [channel|ALL]")

    request_id = int(sys.argv[1])
    channel    = sys.argv[2] if len(sys.argv) > 2 else "ALL"
    process_age_state_request(request_id, channel)
