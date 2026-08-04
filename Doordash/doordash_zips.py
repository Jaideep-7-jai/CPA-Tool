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
    _cleanup_channel_tmp,
    _count_file_lines,
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
    _success_result,
    fetch_request_details,
    process_orange_zip,
    setup_channel_logging,
    setup_main_logging,
    update_ftp_path,
    update_request_status,
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


def _process_email_zip_channel(request_id, channel_name, zip_staging_table, run_dir: Path, insert_func):
    TOTAL_STEPS = 7
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
        f"  path_FINAL     = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE  = {ctx['path_COMPLETE']}\n"
        f"  output_file    = {ctx['output_file']}"
    )

    try:
        channel_tmp = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table = ctx["perm_table"]
        start_time = time.time()

        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = insert_func(perm_table, zip_staging_table, ctx["comp_type"], log)
        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            _drop_perm_table(perm_table, log)
            return {"channel": channel_name, "file": None, "final_file_path": None, "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0}

        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, ctx["path_FINAL"], channel_name, log)

        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, ctx["path_COMPLETE"], channel_name, log)

        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)

        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        combined_count = _download_and_combine(ctx["path_FINAL"], channel_tmp / f"{channel_name}_FINAL_DL", channel_tmp, ctx["output_file"], channel_name, log)

        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        src_file = channel_tmp / ctx["output_file"]
        dest_file = final_files_dir / ctx["output_file"]
        shutil.move(str(src_file), str(dest_file))
        record_count = _count_file_lines(str(dest_file))
        log.info(f"  STEP 6 DONE: Moved {src_file.name} -> FINAL_FILES/ | rows: {record_count:,}")

        _step(log, 7, TOTAL_STEPS, f"FTP upload -> /CPA/{ctx['path_date']}/{ctx['output_file']}", channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        ftp_path = _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        update_ftp_path(request_id, channel_name, ftp_path, log)
        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)
        _cleanup_channel_tmp(channel_tmp, log)
        return _success_result(channel_name, ctx["output_file"], str(dest_file), elapsed, record_count)
    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(f"  {channel_name} CHANNEL (DOORDASH ZIPS) FAILED")
        raise


def process_green_blue_doordash(request_id, channel_name, zip_staging_table, run_dir: Path):
    return _process_email_zip_channel(
        request_id,
        channel_name,
        zip_staging_table,
        run_dir,
        lambda perm_table, staging, comp_type, log: _insert_into_perm_table(perm_table, channel_name.upper(), staging, comp_type, log),
    )


def process_apptness(request_id, zip_staging_table, run_dir: Path):
    """APPTNESS Doordash processor with the Green/Blue workflow and placeholder SQL."""
    return _process_email_zip_channel(
        request_id, "APPTNESS", zip_staging_table, run_dir, _insert_apptness_into_perm_table
    )


def process_arcamax_doordash(request_id, zip_staging_table, run_dir: Path):
    return _process_email_zip_channel(
        request_id,
        "ARCAMAX",
        zip_staging_table,
        run_dir,
        lambda perm_table, staging, comp_type, log: _insert_into_perm_table(perm_table, "ARCAMAX", staging, comp_type, log),
    )


def _load_complete_output_to_table(table_name, channel_name, complete_path, log):
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = (
        f"CREATE OR REPLACE TABLE {table_name} (email VARCHAR, zip VARCHAR); "
        f"COPY INTO {table_name} FROM '{complete_path}/' "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' SKIP_HEADER=1 "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') ON_ERROR='CONTINUE' PURGE=FALSE;"
    )
    log.info(f"  Loading {channel_name} COMPLETE output into {table_name} from {complete_path}/")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])


def _create_combined_outputs(request_id, run_dir: Path, path_date, results, log):
    completed = [ch for ch in COMBINED_CHANNELS if results.get(ch, {}).get("status") == "SUCCESS"]
    if completed != COMBINED_CHANNELS:
        log.info("  Combined Doordash outputs skipped; all four email channels did not complete successfully")
        return []

    final_files_dir = run_dir / "FINAL_FILES"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stage_tables = []
    for ch in COMBINED_CHANNELS:
        ctx = _build_common_context(request_id, ch, run_dir)
        table_name = f"APT_CPA_DOORDASH_{ch}_COMPLETE_{ts}"
        _load_complete_output_to_table(table_name, ch, ctx["path_COMPLETE"], log)
        stage_tables.append((ch, table_name))

    combined_table = f"APT_CPA_DOORDASH_COMBINED_{ts}"
    union_sql = " UNION ALL ".join(
        f"SELECT '{ch}' AS channel, email, zip FROM {tbl}" for ch, tbl in stage_tables
    )
    run_command(["snowsql", "-c", "datateam1", "-q", f"CREATE OR REPLACE TABLE {combined_table} AS {union_sql};"])

    email_name = f"Doordash_Green_Blue_Apptness_Arcamax_email_zips_{path_date}.csv"
    md5_name = f"Doordash_Arcamax_md5hash_zips_{path_date}.csv"
    email_s3 = f"{S3_BASE}/Doordash/{path_date}/FINAL_EMAIL"
    md5_s3 = f"{S3_BASE}/Doordash/{path_date}/FINAL_ARCAMAX_MD5"
    copy_email = (
        f"COPY INTO '{email_s3}/' FROM (SELECT DISTINCT email FROM {combined_table} WHERE email IS NOT NULL) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') HEADER=TRUE MAX_FILE_SIZE=490000000;"
    )
    copy_md5 = (
        f"COPY INTO '{md5_s3}/' FROM (SELECT DISTINCT MD5(LOWER(TRIM(email))) AS md5hash FROM {combined_table} WHERE channel='ARCAMAX' AND email IS NOT NULL) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') HEADER=TRUE MAX_FILE_SIZE=490000000;"
    )
    run_command(["snowsql", "-c", "datateam1", "-q", copy_email])
    run_command(["snowsql", "-c", "datateam1", "-q", copy_md5])
    email_count = _download_and_combine(email_s3, run_dir / "EMAIL_FINAL_DL", run_dir / "EMAIL_FINAL_TMP", email_name, "GREEN", log)
    md5_count = _download_and_combine(md5_s3, run_dir / "MD5_FINAL_DL", run_dir / "MD5_FINAL_TMP", md5_name, "GREEN", log)
    email_dest = final_files_dir / email_name
    md5_dest = final_files_dir / md5_name
    shutil.move(str(run_dir / "EMAIL_FINAL_TMP" / email_name), str(email_dest))
    shutil.move(str(run_dir / "MD5_FINAL_TMP" / md5_name), str(md5_dest))
    md5_lines = md5_dest.read_text().splitlines()
    if md5_lines:
        md5_dest.write_text("md5hash\n" + "\n".join(md5_lines[1:]) + ("\n" if len(md5_lines) > 1 else ""))
    _post_to_ftp(final_files_dir, path_date, email_name, log)
    _post_to_ftp(final_files_dir, path_date, md5_name, log)
    for _, tbl in stage_tables:
        _drop_perm_table(tbl, log)
    _drop_perm_table(combined_table, log)
    shutil.rmtree(str(run_dir / "EMAIL_FINAL_TMP"), ignore_errors=True)
    shutil.rmtree(str(run_dir / "MD5_FINAL_TMP"), ignore_errors=True)
    return [
        {"channel": "DOORDASH_EMAIL", "file": email_name, "final_file_path": str(email_dest), "status": "SUCCESS", "count": email_count},
        {"channel": "DOORDASH_ARCAMAX_MD5", "file": md5_name, "final_file_path": str(md5_dest), "status": "SUCCESS", "count": md5_count},
    ]


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
        raise Exception(f"Request ID {request_id} not found in DB")
    comp_type = request_data["comp_type"]
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
        if ch in ("GREEN", "BLUE"):
            return process_green_blue_doordash(request_id, ch, zip_staging_table, run_dir)
        if ch == "APPTNESS":
            return process_apptness(request_id, zip_staging_table, run_dir)
        if ch == "ARCAMAX":
            return process_arcamax_doordash(request_id, zip_staging_table, run_dir)
        if ch == "ORANGE":
            return process_orange_zip(request_id, zip_staging_table, run_dir)
        raise ValueError(f"Unknown channel: {ch}")

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
        send_error_email("DOORDASH ZIP PARTIAL FAILURE", "\n".join(f"{c}: {e}" for c, e in errors))
    else:
        files = [v.get("file") for v in results.values() if isinstance(v, dict)] + [v["file"] for v in combined_outputs]
        send_success_email(f"DOORDASH ZIP REQUEST COMPLETE — {total_records:,} matched", files, str(run_dir))
    if errors and not results:
        raise RuntimeError(f"All channels failed: {errors}")


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
