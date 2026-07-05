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
  - Per-channel log files under logs/<channel>/ subdirectory.
  - Output folder: no top-level FINAL_FILES; each channel writes directly into
    its own run subdir so the parent output folder stays clean.
"""

import os
import glob
import time
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

def _make_handler(log_path: Path) -> logging.FileHandler:
    """Create a FileHandler with the standard formatter."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(str(log_path))
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    )
    return handler


def setup_logging(output_dir: str, criteria_type: str) -> logging.Logger:
    """
    Set up the shared 'age_processor' logger with a single combined log file
    under <output_dir>/logs/<criteria_type>_<timestamp>.log.
    Returns the logger.
    """
    log_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(output_dir) / "logs" / "{}_{}.log".format(criteria_type, log_date)

    _logger = logging.getLogger("age_processor")
    _logger.setLevel(logging.INFO)
    _logger.handlers.clear()
    _logger.addHandler(_make_handler(log_file))
    return _logger


def setup_channel_logging(output_dir: str, channel_name: str, criteria_type: str) -> logging.Logger:
    """
    Add a per-channel FileHandler to the shared 'age_processor' logger so that
    every message is written to BOTH the combined log and the channel-specific
    log file:

        <output_dir>/logs/<channel>/  <criteria_type>_<timestamp>.log

    Returns the channel-specific FileHandler so the caller can remove it when
    the channel finishes (keeping the combined log handler in place).
    """
    log_date   = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file   = (
        Path(output_dir) / "logs" / channel_name.upper()
        / "{}_{}.log".format(criteria_type, log_date)
    )
    handler    = _make_handler(log_file)
    _logger    = logging.getLogger("age_processor")
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


def fetch_request_details(request_id: int) -> Optional[dict]:
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


def update_request_status(request_id: int, status: str, status_column: str):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET {}=%s WHERE id=%s".format(status_column),
                (status, request_id),
            )
        conn.commit()
        logger.info("%s updated to %s for request_id=%s", status_column, status, request_id)
    except Exception:
        logger.exception("Failed updating %s", status_column)
        raise
    finally:
        conn.close()


def _build_common_context(request_id: int, channel_name: str) -> dict:
    """
    Build the shared context dict for a channel run.

    Output folder layout (no top-level FINAL_FILES):

        <output_dir>/<run_subdir>/<CHANNEL>/
            e.g. output/TEST_.../run_age_20260705_123456/GREEN/

    Each channel writes its final CSV directly into its own channel subdir.
    Temporary download dirs are created as siblings inside the channel subdir.
    """
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception("Request ID {} not found".format(request_id))

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
    s3_path     = "{}/{}/{}/{}/{}".format(
        S3_BASE, request_type, path_date, request_name, channel_name
    )
    output_file = "{}_{}_{}_{}.csv".format(
        client_name, request_type, channel_name, path_date
    )

    return {
        "request_data"  : request_data,
        "client_name"   : client_name,
        "request_type"  : request_type,
        "request_name"  : request_name,
        "criteria_type" : criteria_type,
        "criteria_value": criteria_value,
        "comp_type"     : comp_type,
        "output_dir"    : str(safe_output_dir),
        "channel_dir"   : channel_dir,           # NEW: per-channel output dir
        "path_date"     : path_date,
        "s3_path"       : s3_path,
        "output_file"   : output_file,
    }


def _count_file_lines(file_path: str) -> int:
    """Fast line count using wc -l (avoids loading file into memory)."""
    cmd    = "wc -l < {}".format(shlex.quote(file_path))
    result = subprocess.check_output(cmd, shell=True, universal_newlines=True).strip()
    return int(result) if result else 0


def _download_and_combine(s3_path, download_dir, channel_dir, output_file, channel_name):
    """
    Download gzipped S3 parts into *download_dir* and concatenate into one
    pipe-delimited file at <channel_dir>/<output_file>.

    FIX (req #16): download_dir and channel_dir are now explicitly cast to
    Path objects so .mkdir() is always available regardless of caller type.
    Also, aws s3 cp and zcat commands use the resolved absolute paths to
    avoid any cwd-relative ambiguity.
    """
    download_dir = Path(download_dir)
    channel_dir  = Path(channel_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    channel_dir.mkdir(parents=True, exist_ok=True)

    run_command(
        ["aws", "s3", "cp", s3_path, str(download_dir), "--recursive", "--quiet"]
    )

    # Assert at least one data file was downloaded before attempting zcat
    downloaded = sorted(download_dir.glob("data*"))
    if not downloaded:
        raise RuntimeError(
            "aws s3 cp from {} downloaded 0 files into {}. "
            "Check S3 path and IAM permissions.".format(s3_path, download_dir)
        )

    out_path = channel_dir / output_file
    if channel_name != "ORANGE":
        run_command(
            "zcat {}data* | cut -d'|' -f1 > {}".format(
                shlex.quote(str(download_dir) + "/"),
                shlex.quote(str(out_path)),
            ),
        )
    else:
        run_command(
            "zcat {}data* > {}".format(
                shlex.quote(str(download_dir) + "/"),
                shlex.quote(str(out_path)),
            ),
        )

    run_command("rm -f {}".format(str(download_dir / "data*")), cwd=str(download_dir))


def _get_snowflake_row_count(stage_table):
    """
    Query Snowflake for the exact row count of a staging table.
    Returns the integer count, or raises RuntimeError if the query fails.

    Uses stdout=subprocess.PIPE / stderr=subprocess.PIPE instead of
    capture_output=True to remain compatible with Python 3.6
    (capture_output was added in Python 3.7).
    """
    count_sql = "SELECT COUNT(*) FROM {};".format(stage_table)
    result = subprocess.run(
        ["snowsql", "-c", "datateam1", "-q", count_sql,
         "-o", "output_format=csv", "-o", "header=false"],
        stdout=subprocess.PIPE,   # Python 3.6 compatible
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to query row count for {}:\n{}".format(
                stage_table, result.stderr.strip()
            )
        )
    for line in result.stdout.splitlines():
        line = line.strip().strip('"')
        if line.isdigit():
            return int(line)
    raise RuntimeError(
        "Could not parse row count from SnowSQL output for {}.\nstdout: {!r}".format(
            stage_table, result.stdout
        )
    )


def _dedup_via_snowflake(file_path, channel_name, s3_path, path_date):
    """
    Dedup strategy for files with > LARGE_FILE_ROW_THRESHOLD rows.

    Steps:
      1. Load combined CSV into a temporary Snowflake staging table.
      2. Assert the staging table has rows (fail fast before COPY OUT).
      3. SELECT DISTINCT email → COPY OUT to per-channel dedup S3 prefix.
      4. Download part-files; assert at least one CSV landed.
      5. Concatenate part-files into the original file_path using Python.
      6. Clean up Snowflake staging table and S3 dedup prefix.

    Python 3.6 compatibility:
      - subprocess.run uses stdout/stderr=PIPE, not capture_output=True (3.7+)
      - Path.unlink() uses try/except instead of missing_ok=True (3.8+)
    """
    stage_table  = "APT_ADHOC_DEDUP_STAGING_{}_{}".format(channel_name, path_date)
    dedup_s3     = "{}/DEDUP_{}/".format(s3_path, path_date)
    final_dir    = Path(file_path).parent

    # Resolve absolute path — avoids any cwd-relative ambiguity
    dedup_dl_dir = final_dir.resolve() / "{}_DEDUP_TMP".format(channel_name)
    dedup_dl_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Large file detected for %s — using Snowflake staging dedup (table: %s)",
        channel_name, stage_table,
    )
    logger.info("Snowflake dedup S3 prefix  : %s", dedup_s3)
    logger.info("Local download directory    : %s", str(dedup_dl_dir))

    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    # 1. Create staging table and load
    run_command(["snowsql", "-c", "datateam1", "-q",
                 "CREATE OR REPLACE TEMPORARY TABLE {} (email VARCHAR);".format(stage_table)])

    run_command(["snowsql", "-c", "datateam1", "-q",
                 "PUT file://{} @~/{}/  OVERWRITE=TRUE AUTO_COMPRESS=TRUE;".format(
                     file_path, stage_table)])

    run_command(["snowsql", "-c", "datateam1", "-q",
                 "COPY INTO {table} FROM @~/{table}/ "
                 "FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=0);".format(
                     table=stage_table)])

    # 2. Assert staging table has rows
    staging_row_count = _get_snowflake_row_count(stage_table)
    logger.info("Staging table %s loaded with %d rows", stage_table, staging_row_count)
    if staging_row_count == 0:
        raise RuntimeError(
            "Snowflake staging table {} has 0 rows after COPY INTO. "
            "Check file format and source file: {}".format(stage_table, file_path)
        )

    # 3. COPY DISTINCT emails out
    run_command(["snowsql", "-c", "datateam1", "-q",
                 "COPY INTO '{dedup_s3}' "
                 "FROM (SELECT DISTINCT email FROM {table}) "
                 "CREDENTIALS=(AWS_KEY_ID='{key}' AWS_SECRET_KEY='{secret}') "
                 "FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER='|') "
                 "MAX_FILE_SIZE=490000000;".format(
                     dedup_s3=dedup_s3, table=stage_table,
                     key=AWS_KEY_ID, secret=AWS_SECRET_KEY)])

    # 4. Download dedup parts
    run_command(["aws", "s3", "cp", dedup_s3, str(dedup_dl_dir), "--recursive", "--quiet"])

    downloaded_csvs = sorted(dedup_dl_dir.glob("*.csv"))
    if not downloaded_csvs:
        raise FileNotFoundError(
            "Snowflake COPY OUT produced no CSV files in {}. "
            "Expected files at S3 prefix: {}. "
            "Check COPY INTO wrote rows and aws s3 cp target matches dedup_dl_dir.".format(
                dedup_dl_dir, dedup_s3)
        )

    total_dl_bytes = sum(f.stat().st_size for f in downloaded_csvs)
    logger.info(
        "Downloaded %d CSV part-file(s) from %s (total %.1f MB)",
        len(downloaded_csvs), dedup_s3, total_dl_bytes / 1_048_576,
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
    logger.info("Reassembled %d part-file(s) into %s", len(downloaded_csvs), file_path)

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
            "dedup_dl_dir %s is not empty after cleanup — skipping rmdir",
            str(dedup_dl_dir),
        )

    run_command(["snowsql", "-c", "datateam1", "-q",
                 "DROP TABLE IF EXISTS {table}; REMOVE @~/{table}/;".format(table=stage_table)])
    run_command(["aws", "s3", "rm", dedup_s3, "--recursive", "--quiet"])

    logger.info(
        "Snowflake dedup complete for %s: %d unique emails in final file",
        channel_name, record_count,
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
        "Dedup check for %s: %d rows (threshold=%d)",
        file_path, row_count, LARGE_FILE_ROW_THRESHOLD,
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
            "Dedup %s: %d -> %d unique emails (removed %d)",
            file_path, before, after, before - after,
        )
        return after


def _post_to_ftp(channel_dir, path_date, output_file):
    ftp_cmd = (
        'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        '-e "mkdir -p /CPA/{date};cd /CPA/{date};put {file};bye"'
    ).format(date=path_date, file=output_file)
    run_command(ftp_cmd, cwd=str(channel_dir))


def _success_result(channel_name, channel_dir, output_file, elapsed, record_count=0):
    file_path = str(Path(channel_dir) / output_file)
    return {
        "channel"  : channel_name,
        "file"     : output_file,
        "file_path": file_path,
        "status"   : "SUCCESS",
        "elapsed"  : elapsed,
        "count"    : record_count,
    }


# ---------------------------------------------------------------------------
# Channel processors
# ---------------------------------------------------------------------------

def process_green_blue(request_id, channel_name):
    """GREEN and BLUE channel processor with smart email deduplication."""
    channel_name   = channel_name.upper()
    channel_status = "{}_STATUS".format(channel_name)
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    # Per-channel log handler — added on top of the combined handler
    ch_handler = setup_channel_logging(ctx["output_dir"], channel_name, ctx["criteria_type"])

    logger.info("Started %s processing for %s %s", channel_name, ctx["criteria_type"], ctx["comp_type"])

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]
        channel_dir    = ctx["channel_dir"]

        if criteria_type == "age":
            condition = "b.AGE {} {}".format(">='" if comp_type == "greater" else "<", criteria_value)
            header    = "a.email,b.age"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            condition = "b.STATE {} ('{}')" .format(
                "IN" if comp_type == "include" else "NOT IN", states
            )
            header = "a.email,b.state"
        else:
            raise Exception("Unsupported criteria_type {}".format(criteria_type))

        profile_table = (
            "GREEN_LPT.UNIVERSAL_PROFILE" if channel_name == "GREEN" else "INFS_LPT.INFS_PROFILE"
        )

        sql = (
            "COPY INTO '{s3}/' "
            "FROM (SELECT DISTINCT {header} FROM {table} a "
            "JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash=b.EMAIL_MD5 "
            "WHERE {cond}) "
            "CREDENTIALS=(AWS_KEY_ID='{key}' AWS_SECRET_KEY='{secret}') "
            "FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            "FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        ).format(
            s3=ctx["s3_path"], header=header, table=profile_table,
            cond=condition, key=AWS_KEY_ID, secret=AWS_SECRET_KEY,
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        run_command(["snowsql", "-c", "datateam1", "-q", sql])

        update_request_status(request_id, "Combining Data", channel_status)
        download_dir = channel_dir / "{}_OP_PATH".format(channel_name)
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
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, channel_dir, ctx["output_file"], elapsed, record_count)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email("{} Processing Failed".format(channel_name), str(e))
        raise
    finally:
        # Remove per-channel handler so it doesn't bleed into other channels
        logging.getLogger("age_processor").removeHandler(ch_handler)
        ch_handler.close()


def process_arcamax(request_id):
    """ARCAMAX channel processor with smart email deduplication."""
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    ch_handler = setup_channel_logging(ctx["output_dir"], channel_name, ctx["criteria_type"])

    logger.info("Started %s processing for %s %s", channel_name, ctx["criteria_type"], ctx["comp_type"])

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]
        channel_dir    = ctx["channel_dir"]

        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            condition   = (
                "birthday IS NOT NULL AND TRY_TO_DATE(birthday) {} '{}'"
            ).format("<=" if comp_type == "greater" else ">=", date_cutoff)
            header = "email,birthday"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            condition = "STATE {} ('{}')" .format(
                "IN" if comp_type == "include" else "NOT IN", states
            )
            header = "email,state"
        else:
            raise Exception("Unsupported criteria_type {}".format(criteria_type))

        sql = (
            "COPY INTO '{s3}/' "
            "FROM (SELECT DISTINCT {header} FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            "WHERE {cond}) "
            "CREDENTIALS=(AWS_KEY_ID='{key}' AWS_SECRET_KEY='{secret}') "
            "FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            "FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        ).format(
            s3=ctx["s3_path"], header=header,
            cond=condition, key=AWS_KEY_ID, secret=AWS_SECRET_KEY,
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
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, channel_dir, ctx["output_file"], elapsed, record_count)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email("{} Processing Failed".format(channel_name), str(e))
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

    logger.info("Started %s processing for %s %s", channel_name, ctx["criteria_type"], ctx["comp_type"])

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
            condition   = "dob {} '{}'".format(
                "<=" if comp_type == "greater" else ">=", date_cutoff
            )
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states    = "','".join(criteria_value)
            condition = "STATE {} ('{}')" .format(
                "IN" if comp_type == "include" else "NOT IN", states
            )
        else:
            raise Exception("Unsupported criteria_type {}".format(criteria_type))

        sql = (
            "COPY INTO '{s3}/' "
            "FROM ("
            "SELECT DISTINCT a.email_address, b.ACCOUNT_NAME "
            "FROM ("
            "SELECT DISTINCT a.FEED_ID, a.email_address "
            "FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a, "
            "(SELECT a.email_address, MAX(a.created_at) AS maxdate "
            "FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a "
            "WHERE {cond} GROUP BY 1) b "
            "WHERE a.email_address=b.email_address AND a.created_at=b.maxdate"
            ") a "
            "JOIN APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 b ON a.FEED_ID=b.FEEDID "
            "JOIN APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND c ON a.email_address=c.email_address "
            "JOIN APT_CUSTOM_L90_ORANGE_UNIQ_RESPONDERS_UNIQ_DND d ON a.email_address=d.email"
            ") "
            "CREDENTIALS=(AWS_KEY_ID='{key}' AWS_SECRET_KEY='{secret}') "
            "FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
            "FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"
        ).format(
            s3=ctx["s3_path"], cond=condition,
            key=AWS_KEY_ID, secret=AWS_SECRET_KEY,
        )

        start_time = time.time()
        update_request_status(request_id, "Pulling Data", channel_status)
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        run_command(["snowsql", "-c", "datateam1", "-q", sql])

        update_request_status(request_id, "Combining Data", channel_status)
        raw_combined = "ORANGE_RAW_{}.csv".format(path_date)
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
        logger.info("ORANGE raw records: %d", total_count)

        update_request_status(request_id, "Posting To FTP", channel_status)

        if request_type.lower() == "suppression":
            # FIX (req #16): was '{}_{}_{_{}.csv' — mismatched braces caused
            # ValueError: unexpected '{' in field name.
            output_file      = "{}_{}_{}_{}.csv".format(
                client_name, request_type, channel_name, path_date
            )
            suppression_path = channel_dir / output_file
            (
                df_final[["email_address"]]
                .drop_duplicates()
                .to_csv(str(suppression_path), index=False, header=False)
            )
            run_command("rm -f {}".format(raw_combined), cwd=str(channel_dir))
            record_count = len(pd.read_csv(str(suppression_path), header=None))
            logger.info("ORANGE suppression file: %s (%d records)", output_file, record_count)
            _post_to_ftp(channel_dir, path_date, output_file)

        else:
            esp_split_dir = channel_dir / "ORANGE_ESP_SPLIT"
            esp_split_dir.mkdir(exist_ok=True)
            esp_names = df_final["account_name"].drop_duplicates().sort_values().tolist()
            for esp in esp_names:
                df_esp = df_final[df_final["account_name"] == esp][["email_address"]]
                df_esp.to_csv(
                    str(esp_split_dir / "{}_ORANGE_DATA.csv".format(esp)),
                    index=False,
                    header=False,
                )
                logger.info("ESP split: %s -> %d emails", esp, len(df_esp))

            output_file = "{}_{}_{}_{}.zip".format(
                client_name, request_type, channel_name, path_date
            )
            zip_file = channel_dir / output_file
            run_command(
                ["zip", "-r", zip_file.name, esp_split_dir.name],
                cwd=str(channel_dir),
            )
            run_command(
                "rm -rf {} && rm -f {}".format(esp_split_dir.name, raw_combined),
                cwd=str(channel_dir),
            )
            record_count = total_count
            logger.info("ORANGE mailing ZIP: %s (%d records)", output_file, record_count)
            _post_to_ftp(channel_dir, path_date, output_file)

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, channel_dir, output_file, elapsed, record_count)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email("{} Processing Failed".format(channel_name), str(e))
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
    raise ValueError("Unsupported channel: {}".format(channel_name))


def process_age_state_request(request_id, channel="ALL"):
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception("Request ID {} not found".format(request_id))

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
            raise ValueError("Unsupported channel: {}".format(channel))
        channels_to_run = [selected]

    logger.info("Started request_id=%s for channels=%s", request_id, ",".join(channels_to_run))
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

        all_files = [
            r["file_path"] for r in results.values() if Path(r["file_path"]).exists()
        ]
        total_records = sum(r.get("count", 0) for r in results.values())

        summary = (
            "SUCCESS - {} {} {}\nOutput Dir: {}\nTotal Records: {:,}\n".format(
                comp_type.upper(), criteria_type, criteria_value,
                str(output_dir), total_records,
            )
            + "\n".join(
                [
                    "{}: {} | {:,} records | {} | {:.2f}s".format(
                        r["channel"], r["file"], r.get("count", 0),
                        r["status"], r["elapsed"],
                    )
                    for r in results.values()
                ]
            )
        )
        logger.info(summary)
        send_success_email(
            "{} {} {} - {:,} RECORDS".format(
                comp_type.upper(), criteria_type, criteria_value, total_records
            ),
            all_files,
            str(output_dir),
        )
        return results

    except Exception as e:
        logger.error("FAILED: %s", e)
        send_error_email(
            "{} {} {} FAILED".format(comp_type.upper(), criteria_type, criteria_value),
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
