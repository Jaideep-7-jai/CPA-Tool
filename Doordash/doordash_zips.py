#!/usr/bin/env python3
"""Doordash ZIP workflow built on the existing ZIPS processing helpers."""

import os
import time
import shutil
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import run_command, send_success_email, send_error_email
from ZIPS.zips import (
    _build_common_context,
    _create_zip_staging_table,
    _download_and_combine,
    _drop_perm_table,
    _drop_zip_staging_table,
    _export_complete_final_file,
    _insert_into_perm_table,
    _load_zips_from_s3,
    _post_to_ftp,
    _query_snowflake,
    _step,
    fetch_request_details,
    process_orange_zip,
    setup_channel_logging,
    setup_main_logging,
    update_request_status,
    update_channel_storage,
)

CHANNELS = ["GREEN", "BLUE", "APPTNESS", "ARCAMAX", "ORANGE"]
COMBINED_CHANNELS = ["GREEN", "BLUE", "APPTNESS", "ARCAMAX"]


def _insert_apptness_into_perm_table(perm_table, zip_staging_table, comp_type, log):
    """Create APPTNESS perm table from APPT_RT_MAIL_REQUESTS_SF."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    kw = "IN" if comp_type == "include" else "NOT IN"
    insert_sql = (
        f"CREATE OR REPLACE TABLE {perm_table} AS "
        f"SELECT EMAILID AS email, "
        f"PARSE_JSON(PROFILEDATAJSON):zipcode::VARCHAR AS ZIP "
        f"FROM APPT_RT_MAIL_REQUESTS_SF "
        f"WHERE PARSE_JSON(PROFILEDATAJSON):zipcode::VARCHAR {kw} "
        f"(SELECT zip_code FROM {zip_staging_table}) "
        f"AND EMAILID IS NOT NULL;"
    )
    log.info(f"  Target table     : {perm_table}")
    log.info(f"  ZIP staging table: {zip_staging_table}")
    log.info(f"  comp_type        : {comp_type} ({kw})")
    log.info("  APPTNESS source  : APPT_RT_MAIL_REQUESTS_SF")
    log.info(f"  INSERT SQL       : {insert_sql}")
    run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
    inserted_rows = _query_snowflake(f"SELECT COUNT(*) FROM {perm_table};", log)
    if inserted_rows >= 0:
        log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
    else:
        log.warning(f"  Could not verify row count for {perm_table} (non-fatal)")
    return inserted_rows


def insert_complete_extract(request_id, channel_name, zip_staging_table, run_dir: Path):
    """Create a non-Orange perm table and export its COMPLETE data to S3."""
    TOTAL_STEPS = 2
    channel_name = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log = setup_channel_logging(run_dir, channel_name)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    perm_table = ctx["perm_table"]

    try:
        start_time = time.time()
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        if channel_name in ("GREEN", "BLUE", "ARCAMAX"):
            inserted_count = _insert_into_perm_table(
                perm_table, channel_name, zip_staging_table, ctx["comp_type"], log
            )
        elif channel_name == "APPTNESS":
            inserted_count = _insert_apptness_into_perm_table(
                perm_table, zip_staging_table, ctx["comp_type"], log
            )
        else:
            raise ValueError(f"Unsupported Doordash extract channel: {channel_name}")

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "status": "NO_DATA", "count": 0,
                "elapsed": time.time() - start_time, "perm_table": None,
            }

        _step(log, 2, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, ctx["path_COMPLETE"], channel_name, log)
        update_channel_storage(request_id, channel_name, ctx["path_COMPLETE"], inserted_count, log)
        elapsed = time.time() - start_time
        update_request_status(request_id, "Complete Data Exported", channel_status, log)
        return {
            "channel": channel_name, "status": "COMPLETE_EXPORTED",
            "count": inserted_count if inserted_count >= 0 else 0,
            "elapsed": elapsed, "perm_table": perm_table,
        }
    except Exception:
        try:
            _drop_perm_table(perm_table, log)
        except Exception:
            log.exception(f"  Failed to clean up {perm_table} after extract failure")
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(f"  {channel_name} CHANNEL (DOORDASH ZIPS) FAILED")
        raise


def _create_combined_outputs(request_id, run_dir: Path, path_date, results, log):
    completed = [
        ch for ch in COMBINED_CHANNELS
        if results.get(ch, {}).get("status") == "COMPLETE_EXPORTED"
    ]
    if not completed:
        return []

    final_files_dir = run_dir / "FINAL_FILES"
    final_files_dir.mkdir(parents=True, exist_ok=True)
    request_data = fetch_request_details(request_id)
    client_name = request_data["client_name"]
    criteria_type = request_data["criteria_type"].title()
    request_type = request_data["request_type"]
    email_name = f"{client_name}_{criteria_type}_{request_type}_EMAIL_{path_date}.csv"
    md5_name = f"{client_name}_{criteria_type}_{request_type}_MD5HASH_{path_date}.csv"
    email_s3 = f"{S3_BASE}/Doordash/{path_date}/{request_data['request_name']}/FINAL_EMAIL"
    md5_s3 = f"{S3_BASE}/Doordash/{path_date}/{request_data['request_name']}/FINAL_ARCAMAX_MD5"

    union_sql = " UNION ".join(
        f"SELECT TRIM(email) AS email FROM {results[ch]['perm_table']} WHERE email IS NOT NULL"
        for ch in completed
    )
    copy_email = (
        f"COPY INTO '{email_s3}/' FROM ({union_sql}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
        f"HEADER=TRUE MAX_FILE_SIZE=490000000;"
    )
    arcamax_table = results.get("ARCAMAX", {}).get("perm_table")
    email_count = 0
    md5_count = 0
    try:
        run_command(["snowsql", "-c", "datateam1", "-q", copy_email])
        email_count = _download_and_combine(
            email_s3, run_dir / "EMAIL_FINAL_DL", run_dir / "EMAIL_FINAL_TMP",
            email_name, "GREEN", log,
        )
        if arcamax_table:
            copy_md5 = (
                f"COPY INTO '{md5_s3}/' FROM (SELECT DISTINCT MD5(LOWER(TRIM(email))) AS md5hash "
                f"FROM {arcamax_table} WHERE email IS NOT NULL) "
                f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
                f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
                f"HEADER=TRUE MAX_FILE_SIZE=490000000;"
            )
            run_command(["snowsql", "-c", "datateam1", "-q", copy_md5])
            md5_count = _download_and_combine(
                md5_s3, run_dir / "MD5_FINAL_DL", run_dir / "MD5_FINAL_TMP",
                md5_name, "GREEN", log,
            )
    except Exception:
        log.exception("  Doordash aggregate export validation failed; source tables retained")
        raise

    for ch in completed:
        _drop_perm_table(results[ch]["perm_table"], log)

    email_dest = final_files_dir / email_name
    shutil.move(str(run_dir / "EMAIL_FINAL_TMP" / email_name), str(email_dest))
    email_ftp_path = _post_to_ftp(final_files_dir, path_date, email_name, log, request_type=request_type)
    shutil.rmtree(str(run_dir / "EMAIL_FINAL_TMP"), ignore_errors=True)
    outputs = [{"channel": "DOORDASH_EMAIL", "file": email_name, "final_file_path": str(email_dest), "status": "SUCCESS", "count": email_count, "s3_path": email_s3, "ftp_path": email_ftp_path}]

    if arcamax_table:
        md5_dest = final_files_dir / md5_name
        shutil.move(str(run_dir / "MD5_FINAL_TMP" / md5_name), str(md5_dest))
        md5_ftp_path = _post_to_ftp(final_files_dir, path_date, md5_name, log, request_type=request_type)
        shutil.rmtree(str(run_dir / "MD5_FINAL_TMP"), ignore_errors=True)
        outputs.append({"channel": "DOORDASH_ARCAMAX_MD5", "file": md5_name, "final_file_path": str(md5_dest), "status": "SUCCESS", "count": md5_count, "s3_path": md5_s3, "ftp_path": md5_ftp_path})
    return outputs


def process_doordash_zip_request(request_id: int, zip_file: str, channel, output_dir: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / f"run_doordash_zips_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log = setup_main_logging(run_dir)

    if isinstance(channel, str):
        channel = [channel]
    channels_to_run = list(CHANNELS) if "ALL" in channel else [ch.upper() for ch in channel if ch.upper() in CHANNELS]
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise RuntimeError(f"Request ID {request_id} was not found in requests")
    if request_data["request_type"] != "Doordash":
        raise RuntimeError(
            f"Request ID {request_id} is a {request_data['request_type']} request. "
            "Doordash jobs must use the ID from the requests table for a Doordash row."
        )

    path_date = datetime.now().strftime("%Y%m%d")
    s3_zip_dir = f"{S3_BASE}/Doordash/ZIPS/{path_date}/staging"
    s3_zip_path = f"{s3_zip_dir}/{os.path.basename(zip_file)}"
    run_command(["aws", "s3", "cp", zip_file, s3_zip_path, "--quiet"])
    zip_staging_table = f"APT_CPA_DOORDASH_ZIPS_STAGING_{ts}"
    _create_zip_staging_table(zip_staging_table, log)
    zip_count = _load_zips_from_s3(zip_staging_table, s3_zip_path, log)
    if zip_count == 0:
        _drop_zip_staging_table(zip_staging_table, log)
        raise RuntimeError("ZIP staging table is empty — no ZIP codes were loaded from the file.")

    def _run_channel(ch):
        if ch == "ORANGE":
            return process_orange_zip(request_id, zip_staging_table, run_dir)
        return insert_complete_extract(request_id, ch, zip_staging_table, run_dir)

    results = {}
    errors = []
    with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
        future_to_ch = {executor.submit(_run_channel, ch): ch for ch in channels_to_run}
        for future in as_completed(future_to_ch):
            ch = future_to_ch[future]
            try:
                results[ch] = future.result()
            except Exception as exc:
                errors.append((ch, str(exc)))
                log.error(f"  Channel '{ch}' FAILED: {exc}")

    combined_outputs = []
    try:
        combined_outputs = _create_combined_outputs(request_id, run_dir, path_date, results, log)
        email_output = next((item for item in combined_outputs if item["channel"] == "DOORDASH_EMAIL"), None)
        md5_output = next((item for item in combined_outputs if item["channel"] == "DOORDASH_ARCAMAX_MD5"), None)
        if email_output or md5_output:
            from ZIPS.zips import get_db_with_retry
            conn = get_db_with_retry(log)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE requests SET DOORDASH_EMAIL_FTP=%s, DOORDASH_EMAIL_FILECOUNT=%s, DOORDASH_MD5HASH_FTP=%s, DOORDASH_MD5HASH_FILECOUNT=%s WHERE id=%s",
                        (
                            email_output.get("ftp_path") if email_output else None,
                            email_output.get("count") if email_output else None,
                            md5_output.get("ftp_path") if md5_output else None,
                            md5_output.get("count") if md5_output else None,
                            request_id,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        for ch in COMBINED_CHANNELS:
            if results.get(ch, {}).get("status") == "COMPLETE_EXPORTED":
                update_request_status(request_id, "Completed", f"{ch}_STATUS", log)
    except Exception as exc:
        errors.append(("COMBINED", str(exc)))
        log.exception("  Combined Doordash output generation failed")

    try:
        _drop_zip_staging_table(zip_staging_table, log)
    except Exception as exc:
        log.warning(f"  Failed to drop ZIP staging table (non-fatal): {exc}")

    total_records = sum(v.get("count", 0) for v in results.values() if isinstance(v, dict))
    if errors:
        error_summary = "\n".join(f"{channel_name}: {error}" for channel_name, error in errors)
        send_error_email("DOORDASH ZIP FAILURE", error_summary)
        raise RuntimeError(f"Doordash request failed:\n{error_summary}")

    files = [
        v["file"] for v in results.values()
        if isinstance(v, dict) and v.get("file")
    ] + [v["file"] for v in combined_outputs]
    send_success_email(
        f"DOORDASH ZIP REQUEST COMPLETE — {total_records:,} matched",
        files,
        str(run_dir),
    )


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: doordash_zips.py <request_id> <zip_file> <channel> [output_dir]")
        sys.exit(1)
    process_doordash_zip_request(
        request_id=int(sys.argv[1]),
        zip_file=sys.argv[2],
        channel=sys.argv[3],
        output_dir=sys.argv[4] if len(sys.argv) > 4 else ".",
    )
