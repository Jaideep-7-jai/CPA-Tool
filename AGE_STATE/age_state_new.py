#!/usr/bin/env python3
"""
AGE STATE Processing - unique request-driven channel execution.
Supports single-channel or all-channel execution, with parallel execution
only when multiple channels are requested.
"""

import os
import time
import logging
import pymysql
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

logger = logging.getLogger("age_processor")


def setup_logging(output_dir: str, criteria_type: str):
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{criteria_type}_{log_date}.log"

    logger = logging.getLogger("age_processor")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
    logger.addHandler(handler)
    return logger


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
                    requets_type,
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

    client_name = request_data["client_name"]
    request_type = request_data["requets_type"]
    request_name = request_data["request_name"]
    criteria_type = request_data["criteria_type"]
    criteria_value = request_data["criteria_value"]
    comp_type = request_data["comp_type"]
    output_dir = request_data["output_dir"]

    safe_output_dir = ensure_output_dir(output_dir, criteria_type)
    final_dir = ensure_final_files_dir(safe_output_dir)
    path_date = datetime.now().strftime("%Y%m%d")
    s3_path = f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}"
    output_file = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"

    return {
        "request_data": request_data,
        "client_name": client_name,
        "request_type": request_type,
        "request_name": request_name,
        "criteria_type": criteria_type,
        "criteria_value": criteria_value,
        "comp_type": comp_type,
        "output_dir": safe_output_dir,
        "final_dir": final_dir,
        "path_date": path_date,
        "s3_path": s3_path,
        "output_file": output_file,
    }


def _download_and_combine(s3_path: str, download_dir: Path, final_dir: str, output_file: str):
    download_dir.mkdir(parents=True, exist_ok=True)
    run_command(["aws", "s3", "cp", s3_path, ".", "--recursive", "--quiet"], cwd=download_dir)
    run_command(
        f"ls data* 1>/dev/null 2>&1 && zcat data* > {final_dir}/{output_file}",
        cwd=download_dir,
    )
    run_command("rm -f data*", cwd=download_dir)


def _post_to_ftp(final_dir: str, path_date: str, output_file: str):
    ftp_cmd = (
        f'lftp -u "GreenPub,Zet@Welcome1!" ftp://zxds-ftp-02.bo3.e-dialog.com '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    run_command(ftp_cmd, cwd=final_dir)


def _success_result(channel_name: str, final_dir: str, output_file: str, elapsed: float) -> dict:
    file_path = str(Path(final_dir) / output_file)
    record_count = 0
    try:
        df = pd.read_csv(file_path, sep="|", header=None)
        record_count = len(df)
    except Exception:
        logger.warning("Unable to count records for %s", file_path)

    return {
        "channel": channel_name,
        "file": output_file,
        "file_path": file_path,
        "status": "SUCCESS",
        "elapsed": elapsed,
        "count": record_count,
    }


def process_green_blue(request_id: int, channel_name: str) -> dict:
    channel_name = channel_name.upper()
    if channel_name not in ["GREEN", "BLUE"]:
        raise ValueError("process_green_blue supports only GREEN or BLUE")

    channel_status = f"{channel_name}_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    logger.info(
        "Started %s processing for %s %s",
        channel_name,
        ctx["criteria_type"],
        ctx["comp_type"],
    )

    try:
        criteria_type = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type = ctx["comp_type"]

        if criteria_type == "age":
            condition = f"b.AGE {'>=' if comp_type == 'greater' else '<'} {criteria_value}"
            header = "a.email,b.age"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states = "','".join(criteria_value)
            condition = f"b.STATE {'IN' if comp_type == 'include' else 'NOT IN'} ('{states}')"
            header = "a.email,b.state"
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
        _download_and_combine(ctx["s3_path"], output_path, ctx["final_dir"], ctx["output_file"])

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(ctx["final_dir"], ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, ctx["final_dir"], ctx["output_file"], elapsed)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise


def process_arcamax(request_id: int) -> dict:
    channel_name = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    logger.info(
        "Started %s processing for %s %s",
        channel_name,
        ctx["criteria_type"],
        ctx["comp_type"],
    )

    try:
        criteria_type = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type = ctx["comp_type"]

        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            condition = (
                f"birthday IS NOT NULL AND TRY_TO_DATE(birthday) "
                f"{'<=' if comp_type == 'greater' else '>='} '{date_cutoff}'"
            )
            header = "email,birthday"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states = "','".join(criteria_value)
            condition = f"STATE {'IN' if comp_type == 'include' else 'NOT IN'} ('{states}')"
            header = "email,state"
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
        _download_and_combine(ctx["s3_path"], output_path, ctx["final_dir"], ctx["output_file"])

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(ctx["final_dir"], ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, ctx["final_dir"], ctx["output_file"], elapsed)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise


def process_orange(request_id: int) -> dict:
    channel_name = "ORANGE"
    channel_status = "ORANGE_STATUS"
    update_request_status(request_id, "Started", channel_status)
    ctx = _build_common_context(request_id, channel_name)

    logger.info(
        "Started %s processing for %s %s",
        channel_name,
        ctx["criteria_type"],
        ctx["comp_type"],
    )

    try:
        criteria_type = ctx["criteria_type"]
        criteria_value = ctx["criteria_value"]
        comp_type = ctx["comp_type"]

        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value), comp_type)
            condition = f"dob {'<=' if comp_type == 'greater' else '>='} '{date_cutoff}'"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states = "','".join(criteria_value)
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
        output_path = Path(ctx["final_dir"]) / "ORANGE_OP_PATH"
        _download_and_combine(ctx["s3_path"], output_path, ctx["final_dir"], ctx["output_file"])

        update_request_status(request_id, "Posting To FTP", channel_status)
        _post_to_ftp(ctx["final_dir"], ctx["path_date"], ctx["output_file"])

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status)
        logger.info("%s processing completed in %.2f seconds", channel_name, elapsed)
        return _success_result(channel_name, ctx["final_dir"], ctx["output_file"], elapsed)

    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception("%s processing failed", channel_name)
        send_error_email(f"{channel_name} Processing Failed", str(e))
        raise


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

    criteria_type = request_data["criteria_type"]
    criteria_value = request_data["criteria_value"]
    comp_type = request_data["comp_type"]
    output_dir = ensure_output_dir(request_data["output_dir"], criteria_type)
    final_dir = ensure_final_files_dir(output_dir)

    global logger
    logger = setup_logging(output_dir, criteria_type)

    selected = channel.upper()
    if selected == "ALL":
        channels_to_run = CHANNELS
    else:
        if selected not in CHANNELS:
            raise ValueError(f"Unsupported channel: {channel}")
        channels_to_run = [selected]

    logger.info(
        "Started request_id=%s for channels=%s", request_id, ",".join(channels_to_run)
    )
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
    channel = sys.argv[2] if len(sys.argv) > 2 else "ALL"
    process_age_state_request(request_id, channel)
