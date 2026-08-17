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
    """Create APPTNESS perm table using a placeholder source query."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    kw = "IN" if comp_type == "include" else "NOT IN"
    insert_sql = (
        f"CREATE OR REPLACE TABLE {perm_table} AS "
        f"SELECT email, zip AS ZIP "
        f"FROM APPTNESS_SAMPLE_TABLE "
        f"WHERE zip {kw} (SELECT zip_code FROM {zip_staging_table});"
    )
    log.info(f"  Target table     : {perm_table}")
    log.info(f"  ZIP staging table: {zip_staging_table}")
    log.info(f"  comp_type        : {comp_type}  ({kw})")
    log.info(f"  INSERT SQL       : {insert_sql}")
    log.info("  Executing CREATE + INSERT via snowsql ...")
    run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
    log.info("  CREATE + INSERT executed successfully")
    inserted_rows = _query_snowflake(f"SELECT COUNT(*) FROM {perm_table};", log)
    if inserted_rows >= 0:
        log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
    else:
        log.warning(f"  Could not verify row count for {perm_table} (non-fatal)")
    return inserted_rows


def insert_complete_extract(request_id, channel_name, zip_staging_table, run_dir: Path):
    """Create a non-Orange perm table and export its COMPLETE data to S3.

    The Doordash aggregate files are built from these perm tables only after
    every selected non-Orange channel has completed this phase.  Keeping the
    tables alive until then avoids reloading the COMPLETE S3 extracts merely
    to build the aggregate outputs.
    """
    TOTAL_STEPS = 2
    channel_name = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log = setup_channel_logging(run_dir, channel_name)
    log.info("=" * 70)
    log.info(f"  {channel_name} CHANNEL (DOORDASH ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    log.info(
        f"  comp_type      = {ctx['comp_type']}\n"
        f"  perm_table     = {ctx['perm_table']}\n"
        f"  path_COMPLETE  = {ctx['path_COMPLETE']}"
    )

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
        update_channel_storage(
            request_id, channel_name, ctx["path_COMPLETE"], inserted_count, log
        )
        elapsed = time.time() - start_time
        update_request_status(request_id, "Complete Data Exported", channel_status, log)
        return {
            "channel": channel_name, "status": "COMPLETE_EXPORTED",
            "count": inserted_count if inserted_count >= 0 else 0,
            "elapsed": elapsed, "perm_table": perm_table,
        }
    except Exception:
        # An unsuccessful extract cannot contribute to the aggregate files.
        # Drop a partially-created table rather than leaving it behind.
        try:
            _drop_perm_table(perm_table, log)
        except Exception:
            log.exception(f"  Failed to clean up {perm_table} after extract failure")
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(f"  {channel_name} CHANNEL (DOORDASH ZIPS) FAILED")
        raise


def _create_combined_outputs(request_id, run_dir: Path, path_date, results, log):
    """Write aggregate email and Arcamax MD5 files, then clean up source tables."""
    completed = [
        ch for ch in COMBINED_CHANNELS
        if results.get(ch, {}).get("status") == "COMPLETE_EXPORTED"
    ]
    if not completed:
        log.info("  Combined Doordash outputs skipped; no non-Orange channels exported data")
        return []

    final_files_dir = run_dir / "FINAL_FILES"
    request_name = _build_common_context(request_id, "GREEN", run_dir)["request_name"]
    email_name = f"Doordash_Green_Blue_Apptness_Arcamax_email_zips_{path_date}.csv"
    md5_name = f"Doordash_Arcamax_md5hash_zips_{path_date}.csv"
    email_s3 = f"{S3_BASE}/Doordash/{path_date}/{request_name}/FINAL_EMAIL"
    md5_s3 = f"{S3_BASE}/Doordash/{path_date}/{request_name}/FINAL_ARCAMAX_MD5"
    union_sql = " UNION ALL ".join(
        f"SELECT email FROM {results[ch]['perm_table']}" for ch in completed
    )
    copy_email = (
        f"COPY INTO '{email_s3}/' FROM (SELECT DISTINCT email FROM ({union_sql}) AS all_channels WHERE email IS NOT NULL) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') HEADER=TRUE MAX_FILE_SIZE=490000000;"
    )
    arcamax_table = results.get("ARCAMAX", {}).get("perm_table")
    try:
        run_command(["snowsql", "-c", "datateam1", "-q", copy_email])
        if arcamax_table:
            copy_md5 = (
                f"COPY INTO '{md5_s3}/' FROM (SELECT DISTINCT MD5(LOWER(TRIM(email))) AS md5hash FROM {arcamax_table} WHERE email IS NOT NULL) "
                f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') HEADER=TRUE MAX_FILE_SIZE=490000000;"
            )
            run_command(["snowsql", "-c", "datateam1", "-q", copy_md5])
    finally:
        # The aggregate data is now in S3; cleanup must precede FTP posting.
        for ch in completed:
            _drop_perm_table(results[ch]["perm_table"], log)

    email_count = _download_and_combine(email_s3, run_dir / "EMAIL_FINAL_DL", run_dir / "EMAIL_FINAL_TMP", email_name, "GREEN", log)
    email_dest = final_files_dir / email_name
    shutil.move(str(run_dir / "EMAIL_FINAL_TMP" / email_name), str(email_dest))
    email_ftp_path = _post_to_ftp(final_files_dir, path_date, email_name, log)
    shutil.rmtree(str(run_dir / "EMAIL_FINAL_TMP"), ignore_errors=True)
    outputs = [{"channel": "DOORDASH_EMAIL", "file": email_name, "final_file_path": str(email_dest), "status": "SUCCESS", "count": email_count, "s3_path": email_s3, "ftp_path": email_ftp_path}]
    if arcamax_table:
        md5_count = _download_and_combine(md5_s3, run_dir / "MD5_FINAL_DL", run_dir / "MD5_FINAL_TMP", md5_name, "GREEN", log)
        md5_dest = final_files_dir / md5_name
        shutil.move(str(run_dir / "MD5_FINAL_TMP" / md5_name), str(md5_dest))
        md5_lines = md5_dest.read_text().splitlines()
        if md5_lines:
            md5_dest.write_text("md5hash\n" + "\n".join(md5_lines[1:]) + ("\n" if len(md5_lines) > 1 else ""))
        md5_ftp_path = _post_to_ftp(final_files_dir, path_date, md5_name, log)
        shutil.rmtree(str(run_dir / "MD5_FINAL_TMP"), ignore_errors=True)
        outputs.append({"channel": "DOORDASH_ARCAMAX_MD5", "file": md5_name, "final_file_path": str(md5_dest), "status": "SUCCESS", "count": md5_count, "s3_path": md5_s3, "ftp_path": md5_ftp_path})
    return outputs


def process_doordash_zip_request(request_id: int, zip_file: str, channel, output_dir: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / f"run_doordash_zips_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log = setup_main_logging(run_dir)
    log.info("=" * 70)
    log.info("  DOORDASH ZIP REQUEST STARTED")
    log.info(f"  request_id : {request_id}")
    log.info(f"  zip_file   : {zip_file}")
    log.info(f"  channel    : {channel}")
    log.info(f"  output_dir : {output_dir}")
    log.info(f"  run_dir    : {run_dir}")
    log.info("=" * 70)

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
            # Orange retains the established ZIPS flow, including FINAL data
            # generation, table cleanup, and its own FTP upload.
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
        combined_outputs = []

    try:
        _drop_zip_staging_table(zip_staging_table, log)
    except Exception as exc:
        log.warning(f"  Failed to drop ZIP staging table (non-fatal): {exc}")

    total_records = sum(v.get("count", 0) for v in results.values() if isinstance(v, dict))
    if errors:
        error_summary = "\n".join(f"{channel_name}: {error}" for channel_name, error in errors)
        send_error_email("DOORDASH ZIP FAILURE", error_summary)
        # Do not let the parent job mark a partial/combined-output failure as
        # completed.  The request stays in progress until this function exits;
        # raising here makes app.run_job persist the final failed status.
        raise RuntimeError(f"Doordash request failed:\n{error_summary}")
    else:
        files = [
            v["file"] for v in results.values()
            if isinstance(v, dict) and v.get("file")
        ] + [v["file"] for v in combined_outputs]
        send_success_email(f"DOORDASH ZIP REQUEST COMPLETE — {total_records:,} matched", files, str(run_dir))


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
