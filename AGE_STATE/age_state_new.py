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
"""

import os
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

logger = logging.getLogger("age_processor")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_logging(output_dir: str, criteria_type: str):
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{criteria_type}_{log_date}.log"

    _logger = logging.getLogger("age_processor")
    _logger.setLevel(logging.INFO)
    _logger.handlers.clear()
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
    _logger.addHandler(handler)
    return _logger


def get_dob_cutoff(min_age, comp_type):
    if isinstance(min_age, str):
        min_age = int(min_age)
    cutoff_date = date.today() - timedelta(days=365.25 * min_age)
    return cutoff_date.strftime("%Y-%m-%d")


def ensure_final_files_dir(output_dir: str) -> str:
    final_dir = Path(output_dir) / "FINAL_FILES"
    final_dir.mkdir(parents=True, exist_ok=True)
    return str(final_dir)


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
                f"UPDATE requests SET {status_column}=%s WHERE id=%s",
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
    final_dir       = ensure_final_files_dir(safe_output_dir)
    path_date       = datetime.now().strftime("%Y%m%d")
    s3_path         = f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}"
    output_file     = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"

    return {
        "request_data"  : request_data,
        "client_name"   : client_name,
        "request_type"  : request_type,
        "request_name"  : request_name,
        "criteria_type" : criteria_type,
        "criteria_value": criteria_value,
        "comp_type"     : comp_type,
        "output_dir"    : safe_output_dir,
        "final_dir"     : final_dir,
        "path_date"     : path_date,
        "s3_path"       : s3_path,
        "output_file"   : output_file,
    }


def _count_file_lines(file_path: str) -> int:
    """Fast line count using wc -l (avoids loading file into memory)."""
    cmd = f"wc -l < {shlex.quote(file_path)}"
    result = subprocess.check_output(cmd, shell=True, text=True).strip()
    return int(result) if result else 0


def _download_and_combine(s3_path: str, download_dir: Path, final_dir: str, output_file: str, channel_name: str):
    """Download gzipped S3 parts and concatenate into one pipe-delimited file."""
    download_dir.mkdir(parents=True, exist_ok=True)
    run_command(["aws", "s3", "cp", s3_path, ".", "--recursive", "--quiet"], cwd=download_dir)
    if channel_name != "ORANGE":
        run_command(
            f"ls data* 1>/dev/null 2>&1 && zcat data* | cut -d'|' -f1 > {final_dir}/{output_file}",
            cwd=download_dir,
        )
    else:
        run_command(
            f"ls data* 1>/dev/null 2>&1 && zcat data* > {final_dir}/{output_file}",
            cwd=download_dir,
       )
    run_command("rm -f data*", cwd=download_dir)


def _dedup_via_snowflake(
    file_path: str,
    channel_name: str,
    s3_path: str,
    path_date: str,
) -> int:
    """
    Dedup strategy for files with > LARGE_FILE_ROW_THRESHOLD rows.

    Steps:
      1. Load the combined CSV into a temporary Snowflake staging table.
      2. Run SELECT DISTINCT email FROM staging_table and COPY OUT back to a
         dedicated S3 dedup prefix.
      3. Download the dedup part-files and reassemble into a single final CSV,
         replacing the original file_path.
      4. Clean up the Snowflake staging table.

    This keeps all heavy sorting/hashing inside Snowflake where /tmp size is
    not a constraint, instead of on the job server.
    """
    stage_table  = f"APT_ADHOC_DEDUP_STAGING_{channel_name}_{path_date}"
    dedup_s3     = f"{s3_path}_DEDUP_{path_date}/"
    final_dir    = str(Path(file_path).parent)
    dedup_dl_dir = Path(final_dir) / f"{channel_name}_DEDUP_TMP"
    dedup_dl_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Large file detected for %s — using Snowflake staging dedup (table: %s)",
        channel_name, stage_table,
    )

    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    # 1. Create staging table and load the combined CSV
    create_sql = (
        f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} (email VARCHAR);"
    )
    run_command(["snowsql", "-c", "datateam1", "-q", create_sql])

    # PUT the file to Snowflake internal stage then COPY INTO table
    put_sql = f"PUT file://{file_path} @~/{stage_table}/ OVERWRITE=TRUE AUTO_COMPRESS=TRUE;"
    run_command(["snowsql", "-c", "datateam1", "-q", put_sql])

    copy_in_sql = (
        f"COPY INTO {stage_table} FROM @~/{stage_table}/ "
        f"FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=0);"
    )
    run_command(["snowsql", "-c", "datateam1", "-q", copy_in_sql])

    # 2. COPY DISTINCT emails back out to S3
    copy_out_sql = (
        f"COPY INTO '{dedup_s3}' "
        f"FROM (SELECT DISTINCT email FROM {stage_table}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER='|') "
        f"MAX_FILE_SIZE=490000000;"
    )
    run_command(["snowsql", "-c", "datateam1", "-q", copy_out_sql])

    # 3. Download dedup parts and reassemble into the original file_path
    run_command(
        ["aws", "s3", "cp", dedup_s3, ".", "--recursive", "--quiet"],
        cwd=dedup_dl_dir,
    )
    run_command(
        f"cat *.csv > {file_path}",
        cwd=dedup_dl_dir,
    )

    # 4. Count final rows and cleanup
    record_count = _count_file_lines(file_path)
    run_command("rm -f *.csv", cwd=dedup_dl_dir)
    dedup_dl_dir.rmdir()

    # Drop staging table and internal stage files
    cleanup_sql = (
        f"DROP TABLE IF EXISTS {stage_table}; "
        f"REMOVE @~/{stage_table}/;"
    )
    run_command(["snowsql", "-c", "datateam1", "-q", cleanup_sql])

    # Remove dedup S3 prefix files
    run_command(
        ["aws", "s3", "rm", dedup_s3, "--recursive", "--quiet"],
    )

    logger.info(
        "Snowflake dedup complete for %s: %d unique emails in final file",
        channel_name, record_count,
    )
    return record_count


def _dedup_email_col(
    file_path: str,
    channel_name: str = "",
    s3_path: str = "",
    path_date: str = "",
) -> int:
    """
    Smart dedup dispatcher:
      - Count rows with wc -l (cheap, no memory cost).
      - If row count > LARGE_FILE_ROW_THRESHOLD (100M):
            use Snowflake staging table to do the dedup (avoids /tmp exhaustion).
      - Else:
            use pandas in-process dedup (fast for smaller files).

    Works for GREEN / BLUE / ARCAMAX where the combined file contains
    email (and optionally a second column).  Only column 0 (email) is
    considered for deduplication.
    """
    row_count = _count_file_lines(file_path)
    logger.info(
        "Dedup check for %s: %d rows (threshold=%d)",
        file_path, row_count, LARGE_FILE_ROW_THRESHOLD,
    )

    if row_count > LARGE_FILE_ROW_THRESHOLD:
        # ------------------------------------------------------------------
        # LARGE FILE PATH: offload dedup to Snowflake to avoid disk/tmp issues
        # ------------------------------------------------------------------
        if not s3_path or not path_date or not channel_name:
            raise ValueError(
                "_dedup_email_col: s3_path, path_date, and channel_name are "
                "required for large-file Snowflake dedup."
            )
        return _dedup_via_snowflake(file_path, channel_name, s3_path, path_date)

    else:
        # ------------------------------------------------------------------
        # SMALL FILE PATH: in-process pandas dedup (fast, no network round-trip)
        # ------------------------------------------------------------------
        df = pd.read_csv(file_path, sep="|", header=None, dtype=str)
        before = len(df)
        df = df.drop_duplicates(subset=[0])   # col 0 = email
        df.to_csv(file_path, sep="|", index=False, header=False)
        after = len(df)
        logger.info(
            "Dedup %s: %d -> %d unique emails (removed %d)",
            file_path, before, after, before - after,
        )
        return after


def _post_to_ftp(final_dir: str, path_date: str, output_file: str):
    ftp_cmd = (
        f'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    run_command(ftp_cmd, cwd=final_dir)


def _success_result(
    channel_name: str, final_dir: str, output_file: str, elapsed: float, record_count: int = 0
) -> dict:
    file_path = str(Path(final_dir) / output_file)
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

def process_green_blue(request_id: int, channel_name: str) -> dict:
    """GREEN and BLUE channel processor with smart email deduplication."""
    channel_name = channel_name.upper()
    if channel_name not in ["GREEN", "BLUE"]:
        raise ValueError("process_green_blue supports only GREEN or BLUE")

    channel_status = f"{channel_name}_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    logger.info("Started %s processing for %s %s", channel_name, ctx["criteria_type"], ctx["comp_type"])

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]

        if criteria_type == "age":
            condition = f"b.AGE {'>=' if comp_type == 'greater' else '<'} {criteria_value}"
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
        output_path = Path(ctx["final_dir"]) / f"{channel_name}_OP_PATH"
        _download_and_combine(ctx["s3_path"], output_path, ctx["final_dir"], ctx["output_file"], channel_name)

        # Smart dedup: large files -> Snowflake, small files -> pandas
        final_file_path = str(Path(ctx["final_dir"]) / ctx["output_file"])
        record_count = _dedup_email_col(
            final_file_path,
            channel_name=channel_name,
            s3_path=ctx["s3_path"],
            path_date=ctx["path_date"],
        )

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(ctx["final_dir"], ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, ctx["final_dir"], ctx["output_file"], elapsed, record_count)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise


def process_arcamax(request_id: int) -> dict:
    """ARCAMAX channel processor with smart email deduplication."""
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    logger.info("Started %s processing for %s %s", channel_name, ctx["criteria_type"], ctx["comp_type"])

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]

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
        output_path = Path(ctx["final_dir"]) / "ARCAMAX_OP_PATH"
        _download_and_combine(ctx["s3_path"], output_path, ctx["final_dir"], ctx["output_file"], channel_name)

        # Smart dedup: large files -> Snowflake, small files -> pandas
        final_file_path = str(Path(ctx["final_dir"]) / ctx["output_file"])
        record_count = _dedup_email_col(
            final_file_path,
            channel_name=channel_name,
            s3_path=ctx["s3_path"],
            path_date=ctx["path_date"],
        )

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(ctx["final_dir"], ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, ctx["final_dir"], ctx["output_file"], elapsed, record_count)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise


def process_orange(request_id: int) -> dict:
    """
    ORANGE channel processor.
    - suppression -> single email-only CSV
    - mailing     -> ESP-wise split CSVs packed into a ZIP
    """
    channel_name   = "ORANGE"
    channel_status = "ORANGE_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    logger.info("Started %s processing for %s %s", channel_name, ctx["criteria_type"], ctx["comp_type"])

    try:
        criteria_type  = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type      = ctx["comp_type"]
        request_type   = ctx["request_type"]
        client_name    = ctx["client_name"]
        final_dir      = ctx["final_dir"]
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

        # Download raw S3 parts into a temp folder and combine
        update_request_status(request_id, "Combining Data", channel_status)
        raw_combined = f"ORANGE_RAW_{path_date}.csv"
        output_path  = Path(final_dir) / "ORANGE_OP_PATH"
        _download_and_combine(ctx["s3_path"], output_path, final_dir, raw_combined, channel_name)

        # Load combined file: columns -> email_address | account_name
        df_final = pd.read_csv(
            str(Path(final_dir) / raw_combined),
            sep="|",
            header=None,
            names=["email_address", "account_name"],
            dtype=str,
        )
        total_count = len(df_final)
        logger.info("ORANGE raw records: %d", total_count)

        update_request_status(request_id, "Posting To FTP", channel_status)

        # ---- suppression vs mailing ----------------------------------------
        if request_type.lower() == "suppression":
            # suppression: unique email-only CSV
            output_file      = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"
            suppression_path = Path(final_dir) / output_file
            (
                df_final[["email_address"]]
                .drop_duplicates()
                .to_csv(suppression_path, index=False, header=False)
            )
            # remove intermediate raw file
            run_command(f"rm -f {raw_combined}", cwd=final_dir)
            record_count = len(pd.read_csv(suppression_path, header=None))
            logger.info("ORANGE suppression file: %s (%d records)", output_file, record_count)

            _post_to_ftp(final_dir, path_date, output_file)

        else:
            # mailing: ESP-wise split + ZIP
            esp_split_dir = Path(final_dir) / "ORANGE_OP_PATH"
            esp_split_dir.mkdir(exist_ok=True)

            esp_names = (
                df_final["account_name"].drop_duplicates().sort_values().tolist()
            )
            for esp in esp_names:
                df_esp = df_final[df_final["account_name"] == esp][["email_address"]]
                df_esp.to_csv(
                    esp_split_dir / f"{esp}_ORANGE_DATA.csv",
                    index=False,
                    header=False,
                )
                logger.info("ESP split: %s -> %d emails", esp, len(df_esp))

            output_file = (
                f"{client_name}_{request_type}_{channel_name}_{path_date}.zip"
            )
            zip_file = Path(final_dir) / output_file
            run_command(
                ["zip", "-r", zip_file.name, esp_split_dir.name],
                cwd=final_dir,
            )
            # cleanup split folder and raw combined file
            run_command(
                f"rm -rf {esp_split_dir.name} && rm -f {raw_combined}",
                cwd=final_dir,
            )
            record_count = total_count
            logger.info("ORANGE mailing ZIP: %s (%d records)", output_file, record_count)

            _post_to_ftp(final_dir, path_date, output_file)
        # --------------------------------------------------------------------

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, final_dir, output_file, elapsed, record_count)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def process_channel(request_id: int, channel_name: str) -> dict:
    channel_name = channel_name.upper()
    if channel_name in ["GREEN", "BLUE"]:
        return process_green_blue(request_id, channel_name)
    if channel_name == "ARCAMAX":
        return process_arcamax(request_id)
    if channel_name == "ORANGE":
        return process_orange(request_id)
    raise ValueError(f"Unsupported channel: {channel_name}")


def process_age_state_request(request_id: int, channel: str = "ALL") -> Dict[str, dict]:
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found")

    criteria_type  = request_data["criteria_type"]
    criteria_value = request_data["criteria_value"]
    comp_type      = request_data["comp_type"]
    output_dir     = ensure_output_dir(request_data["output_dir"], criteria_type)
    final_dir      = ensure_final_files_dir(output_dir)

    global logger
    logger = setup_logging(output_dir, criteria_type)

    selected = channel.upper()
    if selected == "ALL":
        channels_to_run = CHANNELS
    else:
        if selected not in CHANNELS:
            raise ValueError(f"Unsupported channel: {channel}")
        channels_to_run = [selected]

    logger.info("Started request_id=%s for channels=%s", request_id, ",".join(channels_to_run))
    results: Dict[str, dict] = {}

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

        all_files: List[str] = [
            r["file_path"] for r in results.values() if Path(r["file_path"]).exists()
        ]
        total_records = sum(r.get("count", 0) for r in results.values())

        summary = (
            f"SUCCESS - {comp_type.upper()} {criteria_type} {criteria_value}\n"
            f"Final Dir: {final_dir}\n"
            f"Total Records: {total_records:,}\n"
            + "\n".join(
                [
                    f"{r['channel']}: {r['file']} | {r.get('count', 0):,} records "
                    f"| {r['status']} | {r['elapsed']:.2f}s"
                    for r in results.values()
                ]
            )
        )
        logger.info(summary)
        send_success_email(
            f"{comp_type.upper()} {criteria_type} {criteria_value} - {total_records:,} RECORDS",
            all_files,
            output_dir,
        )
        return results

    except Exception as e:
        logger.error("FAILED: %s", e)
        send_error_email(
            f"{comp_type.upper()} {criteria_type} {criteria_value} FAILED", str(e)
        )
        raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python age_state_new.py <request_id> [channel|ALL]")

    request_id = int(sys.argv[1])
    channel    = sys.argv[2] if len(sys.argv) > 2 else "ALL"
    process_age_state_request(request_id, channel)
