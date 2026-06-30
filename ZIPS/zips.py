#!/usr/bin/env python3
"""
ZIPS Processing Module
======================
Handles two modes:

  Suppression / Mailing (zips criteria)
  --------------------------------------
  - Upload ZIP codes file → stage in Snowflake table
  - Run include / exclude match per channel
  - Produce same final files as age/state module:
      GREEN   → combined unique email file
      BLUE    → combined unique email file
      ARCAMAX → md5hash file
      ORANGE  → per-ESP email files
  - Each channel writes its own log file under <output_dir>/logs/

  Doordash
  --------
  - No include / exclude – just pull records matching provided ZIP codes
  - Output: zips-matched data only (no suppression/mailing final files)
  - Each channel still writes its own log file
"""

import os
import time
import shutil
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Tuple

from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import (
    run_command, send_success_email, send_error_email,
    get_db_connection, ensure_output_dir, download_combine, load_zip_to_pg
)


# ─── Logging helpers ─────────────────────────────────────────────────────────

def setup_main_logging(output_dir: str) -> logging.Logger:
    """Set up root logger that writes to logs/zips_<ts>.log"""
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"zips_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(log_file)]
    )
    return logging.getLogger("zip_processor")


def get_channel_logger(channel: str, output_dir: str) -> logging.Logger:
    """
    Return a channel-specific logger that writes to
    logs/<CHANNEL>_zips_<timestamp>.log
    """
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{channel.upper()}_zips_{ts}.log"

    logger = logging.getLogger(f"zip_{channel.lower()}")
    logger.setLevel(logging.INFO)
    # Only add handler once
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s'))
        logger.addHandler(fh)
    return logger


# ─── Snowflake / S3 helpers ───────────────────────────────────────────────────

def get_unique_s3_path(base_path, label, ts):
    return f"{base_path}/{ts.strftime('%Y%m%d')}/{label}_{ts.strftime('%Y%m%d_%H%M%S')}"


def get_unique_table_name(prefix, comp_type, channel):
    ts = datetime.now().strftime('%Y%m%d')
    return f"APT_ADHOC_JAIDEEP_ZIP_{prefix}_{comp_type.upper()}_{channel.upper()}_{ts}"


def create_zip_staging_table(table_name: str):
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (zip_code VARCHAR(10), PRIMARY KEY (zip_code));
    """
    run_command(["snowsql", "-c", "datateam1", "-q", sql])


def load_zip_to_snowflake(s3_path: str, table_name: str) -> int:
    copy_sql = f"""
    COPY INTO {table_name}
    FROM '{s3_path}'
    credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
    FILE_FORMAT=(TYPE='CSV' FIELD_DELIMITER=',' SKIP_HEADER=1)
    ON_ERROR='CONTINUE'
    PURGE=TRUE;
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    run_command(["snowsql", "-c", "datateam1", "-q", copy_sql])
    count_sql = f"SELECT COUNT(*) FROM {table_name};"
    result = run_command([
        "snowsql", "-c", "datateam1",
        "-o", "output_format=csv", "-o", "header=false",
        "-o", "timing=false",    "-o", "friendly=false",
        "-q", count_sql
    ])
    return int(result.strip().strip('"'))


def ensure_final_files_dir(output_dir: str) -> str:
    d = Path(output_dir) / "FINAL_FILES"
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


# ─── ZIP condition builder ────────────────────────────────────────────────────

def zip_condition_sql(zip_table: str, zip_col: str, comp_type: str, request_type: str) -> str:
    """
    Build the WHERE clause fragment for ZIP matching.
    - Doordash: always IN (match only)
    - Suppression/Mailing zips: include → IN, exclude → NOT IN
    """
    if request_type == "Doordash" or comp_type == "include":
        return f"{zip_col} IN (SELECT zip_code FROM {zip_table})"
    else:
        return f"{zip_col} NOT IN (SELECT zip_code FROM {zip_table})"


# ─── Per-channel processors ───────────────────────────────────────────────────

def process_green_zip(
    zip_file: str, comp_type: str, request_type: str,
    output_dir: str
) -> Tuple[str, int]:
    logger = get_channel_logger("GREEN", output_dir)
    final_dir = ensure_final_files_dir(output_dir)
    now = datetime.now()
    s3_path = get_unique_s3_path(S3_BASE, "GREEN_ZIP", now)
    label = "DOORDASH" if request_type == "Doordash" else comp_type.upper()

    logger.info(f"GREEN ZIP {label}: {zip_file} started")
    try:
        run_command(["aws", "s3", "cp", zip_file, f"{s3_path}/{os.path.basename(zip_file)}", "--quiet"])

        zip_table = get_unique_table_name("GREEN_ZIP_STAGING", comp_type, "green")
        create_zip_staging_table(zip_table)
        zip_count = load_zip_to_snowflake(s3_path, zip_table)
        logger.info(f"Loaded {zip_count:,} ZIP codes to {zip_table}")

        cond = zip_condition_sql(zip_table, "b.ZIP", comp_type, request_type)

        if request_type == "Doordash":
            output_file = f"GREEN_DOORDASH_ZIPS.csv"
            export_sql = f"""
            COPY INTO '{s3_path}/matched/' FROM (
                SELECT DISTINCT a.email, b.ZIP
                FROM GREEN_LPT.UNIVERSAL_PROFILE a
                JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5
                WHERE {cond}
            )
            credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
            FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|')
            max_file_size=490000000;
            """
        else:
            output_file = f"GREEN_ZIP_{comp_type}.csv"
            export_sql = f"""
            COPY INTO '{s3_path}/matched/' FROM (
                SELECT DISTINCT a.email
                FROM GREEN_LPT.UNIVERSAL_PROFILE a
                JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5
                WHERE {cond}
            )
            credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
            FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|')
            max_file_size=490000000;
            """

        run_command(["snowsql", "-c", "datateam1", "-q", export_sql])
        result_file, record_count = download_combine(f"{s3_path}/matched/", output_file, final_dir)
        logger.info(f"GREEN ZIP {label}: {result_file} ({record_count:,} records)")
        return result_file, record_count

    except Exception as e:
        logger.error(f"GREEN ZIP FAILED: {e}")
        send_error_email(f"GREEN ZIP {label} FAILED", str(e))
        raise


def process_blue_zip(
    zip_file: str, comp_type: str, request_type: str,
    output_dir: str
) -> Tuple[str, int]:
    logger = get_channel_logger("BLUE", output_dir)
    final_dir = ensure_final_files_dir(output_dir)
    now = datetime.now()
    s3_path = get_unique_s3_path(S3_BASE, "BLUE_ZIP", now)
    label = "DOORDASH" if request_type == "Doordash" else comp_type.upper()

    logger.info(f"BLUE ZIP {label}: {zip_file} started")
    try:
        run_command(["aws", "s3", "cp", zip_file, f"{s3_path}/{os.path.basename(zip_file)}", "--quiet"])

        zip_table = get_unique_table_name("BLUE_ZIP_STAGING", comp_type, "BLUE")
        create_zip_staging_table(zip_table)
        zip_count = load_zip_to_snowflake(s3_path, zip_table)
        logger.info(f"Loaded {zip_count:,} ZIP codes to {zip_table}")

        cond = zip_condition_sql(zip_table, "b.ZIP", comp_type, request_type)

        if request_type == "Doordash":
            output_file = f"BLUE_DOORDASH_ZIPS.csv"
            export_sql = f"""
            COPY INTO '{s3_path}/matched/' FROM (
                SELECT DISTINCT a.email, b.ZIP
                FROM INFS_LPT.infs_profile a
                JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5
                WHERE {cond}
            )
            credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
            FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|')
            max_file_size=490000000;
            """
        else:
            output_file = f"BLUE_ZIP_{comp_type}.csv"
            export_sql = f"""
            COPY INTO '{s3_path}/matched/' FROM (
                SELECT DISTINCT a.email
                FROM INFS_LPT.infs_profile a
                JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5
                WHERE {cond}
            )
            credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
            FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|')
            max_file_size=490000000;
            """

        run_command(["snowsql", "-c", "datateam1", "-q", export_sql])
        result_file, record_count = download_combine(f"{s3_path}/matched/", output_file, final_dir)
        logger.info(f"BLUE ZIP {label}: {result_file} ({record_count:,} records)")
        return result_file, record_count

    except Exception as e:
        logger.error(f"BLUE ZIP FAILED: {e}")
        send_error_email(f"BLUE ZIP {label} FAILED", str(e))
        raise


def process_arcamax_zip(
    zip_file: str, comp_type: str, request_type: str,
    output_dir: str
) -> Tuple[str, int]:
    logger = get_channel_logger("ARCAMAX", output_dir)
    final_dir = ensure_final_files_dir(output_dir)
    label = "DOORDASH" if request_type == "Doordash" else comp_type.upper()

    logger.info(f"ARCAMAX ZIP {label}: {zip_file} started")
    try:
        arcamax_config = get_db_connection('arcamax')
        zip_table = get_unique_table_name("DATA", comp_type, "ARCAMAX")
        load_count = load_zip_to_pg(zip_file, zip_table, arcamax_config)
        logger.info(f"Loaded {load_count:,} ZIP codes to {zip_table}")

        if request_type == "Doordash":
            # Return md5hash + ZIP for Doordash
            filename = f"ARCAMAX_DOORDASH_ZIPS.csv"
            output_file = f"{final_dir}/{filename}"
            sql_query = (
                f"SELECT DISTINCT email_md5hash, zip FROM apt_custom_arcamax_customer_table_aamir "
                f"WHERE ZIP IN (SELECT zip_code FROM {zip_table})"
            )
        else:
            # Suppression/Mailing: md5hash file, include/exclude
            filename = f"ARCAMAX_ZIP_{comp_type}.csv"
            output_file = f"{final_dir}/{filename}"
            cond = zip_condition_sql(zip_table, "ZIP", comp_type, request_type)
            sql_query = (
                f"SELECT DISTINCT email_md5hash FROM apt_custom_arcamax_customer_table_aamir "
                f"WHERE {cond}"
            )

        cmd = [
            "psql", "-U", "datateam", "-d", arcamax_config['db'],
            "-h", arcamax_config['host'], "-qtAX", "-c", sql_query
        ]
        start = time.time()
        with open(output_file, "w") as f:
            run_command(cmd, cwd=None, stdout=f)
        elapsed = time.time() - start
        logger.info(f"ARCAMAX query executed in {elapsed:.1f}s")

        record_count = int(run_command(f"wc -l < {output_file}", shell=True).strip())

        zip_name = f"{output_file}.zip"
        zip_path = Path(zip_name)
        shutil.make_archive(
            base_name=zip_path.with_suffix('').as_posix(),
            format='zip',
            root_dir=final_dir,
            base_dir=filename
        )
        Path(output_file).unlink(missing_ok=True)
        logger.info(f"ARCAMAX ZIP {label}: {zip_name} ({record_count:,} records)")
        return zip_name, record_count

    except Exception as e:
        logger.error(f"ARCAMAX ZIP FAILED: {e}")
        send_error_email(f"ARCAMAX ZIP {label} FAILED", str(e))
        raise


def process_orange_zip(
    zip_file: str, comp_type: str, request_type: str,
    output_dir: str
) -> Tuple[str, int]:
    logger = get_channel_logger("ORANGE", output_dir)
    final_dir = ensure_final_files_dir(output_dir)
    label = "DOORDASH" if request_type == "Doordash" else comp_type.upper()

    logger.info(f"ORANGE ZIP {label}: {zip_file} started")
    try:
        orange_config = get_db_connection('orange')
        mt2_config    = get_db_connection('mt2')
        zip_table = get_unique_table_name("DATA", comp_type, "ORANGE")
        load_count = load_zip_to_pg(zip_file, zip_table, orange_config)
        logger.info(f"Loaded {load_count:,} ZIP codes to {zip_table}")

        s3_path = get_unique_s3_path(S3_BASE, "ORANGE_ZIP", datetime.now())
        cond = zip_condition_sql(zip_table, "ZIP", comp_type, request_type)

        # Step 1: ESP mapping from MySQL
        esp_file = str(Path(output_dir) / "esp_details.csv")
        mysql_cmd = [
            "mysql", mt2_config['db'], "-h", mt2_config['host'],
            "-u", mt2_config['user'], f"-p{mt2_config['password']}",
            "-A", "-ss", "-e",
            "SELECT DISTINCT a.feed_id, b.account_name "
            "FROM mt2_data.esp_data_exports a, mt2_data.esp_accounts b "
            "WHERE a.esp_account_id = b.id"
        ]
        esp_output = run_command(mysql_cmd, cwd=output_dir).replace("\t", "|")
        Path(esp_file).write_text(esp_output)
        logger.info(f"ESP mapping: {len(esp_output.splitlines())} rows")

        # Step 2: PSQL exports from Orange
        trans_file   = str(Path(output_dir) / "orange_email_zip.csv")
        profile_file = str(Path(output_dir) / "orange_profile.csv")

        if request_type == "Doordash":
            # For Doordash: pull email + ZIP
            trans_sql = (
                f"SELECT email_address, feed_id, ZIP "
                f"FROM apt_custom_orange_transaction_dnd "
                f"WHERE email_address IS NOT NULL AND ZIP IN (SELECT zip_code FROM {zip_table})"
            )
        else:
            trans_sql = (
                f"SELECT email_address, feed_id "
                f"FROM apt_custom_orange_transaction_dnd "
                f"WHERE email_address IS NOT NULL AND {cond}"
            )

        run_command([
            "psql", "-U", "datateam", "-h", orange_config['host'],
            "-d", orange_config['db'],
            "-c", f"\\COPY ({trans_sql}) TO '{trans_file}' WITH (FORMAT CSV, HEADER)",
            "-qAtX"
        ])

        profile_sql = "SELECT email_address FROM apt_custom_orange_profile_email_dnd WHERE email_address IS NOT NULL"
        run_command([
            "psql", "-U", "datateam", "-h", orange_config['host'],
            "-d", orange_config['db'],
            "-c", f"\\COPY ({profile_sql}) TO '{profile_file}' WITH (FORMAT CSV, HEADER)",
            "-qAtX"
        ])

        # Step 3: Upload to S3
        for src in [esp_file, trans_file, profile_file]:
            run_command(["aws", "s3", "cp", src, f"{s3_path}/{os.path.basename(src)}", "--quiet"])

        # Step 4: Snowflake stage + export per-ESP
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        table_esp     = get_unique_table_name("esp_details",   comp_type, "orange")
        table_email   = get_unique_table_name("email_zip",     comp_type, "orange")
        table_profile = get_unique_table_name("PROFILE_DND",   comp_type, "orange")

        if request_type == "Doordash":
            email_cols   = "email VARCHAR, feedid VARCHAR, zip VARCHAR"
            select_final = (
                f"SELECT DISTINCT a.email, b.account_name, a.zip "
                f"FROM {table_email} a, {table_esp} b "
                f"WHERE a.feedid = b.feedid "
                f"AND a.email NOT IN (SELECT email_address FROM {table_profile})"
            )
        else:
            email_cols   = "email VARCHAR, feedid VARCHAR"
            select_final = (
                f"SELECT DISTINCT a.email, b.account_name "
                f"FROM {table_email} a, {table_esp} b "
                f"WHERE a.feedid = b.feedid "
                f"AND a.email NOT IN (SELECT email_address FROM {table_profile})"
            )

        snowflake_sqls = [
            f"DROP TABLE IF EXISTS {table_esp};",
            f"CREATE TABLE {table_esp} (feedid VARCHAR, account_name VARCHAR);",
            f"DROP TABLE IF EXISTS {table_email};",
            f"CREATE TABLE {table_email} ({email_cols});",
            f"DROP TABLE IF EXISTS {table_profile};",
            f"CREATE TABLE {table_profile} (email_address VARCHAR);",
            f"COPY INTO {table_esp} FROM '{s3_path}/esp_details.csv' "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE='CSV' FIELD_DELIMITER='|') ON_ERROR='CONTINUE';",
            f"COPY INTO {table_email} FROM '{s3_path}/orange_email_zip.csv' "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE='CSV') ON_ERROR='CONTINUE';",
            f"COPY INTO {table_profile} FROM '{s3_path}/orange_profile.csv' "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE='CSV') ON_ERROR='CONTINUE';",
            f"COPY INTO '{s3_path}/ORANGE_FINAL_DATA/' FROM ({select_final}) "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|') "
            f"max_file_size=4900000000;",
        ]

        sql_file = Path(output_dir) / "run_orange.sql"
        sql_file.write_text("\n".join(snowflake_sqls))
        run_command(["snowsql", "-c", "datateam1", "-f", str(sql_file)])

        # Step 5: Download + split per-ESP
        if request_type == "Doordash":
            final_name = f"ORANGE_DOORDASH_ZIPS.csv"
        else:
            final_name = f"ORANGE_ZIP_{comp_type}.csv"

        run_command([
            "aws", "s3", "cp", f"{s3_path}/ORANGE_FINAL_DATA/",
            ".", "--recursive", "--quiet"
        ], cwd=final_dir)
        run_command(f"ls data* 1>/dev/null 2>&1 && zcat data* > {final_name}", cwd=final_dir)
        run_command("rm -f data*", cwd=final_dir)

        if request_type == "Doordash":
            col_names = ["email", "account_name", "zip"]
        else:
            col_names = ["email", "account_name"]

        df_final = pd.read_csv(
            str(Path(final_dir) / final_name),
            names=col_names, delimiter='|'
        )
        esp_names   = df_final['account_name'].drop_duplicates().sort_values().tolist()
        total_count = len(df_final)
        logger.info(f"ORANGE Total: {total_count:,} records across {len(esp_names)} ESPs")

        output_path = Path(final_dir) / "ORANGE_OP_PATH"
        output_path.mkdir(exist_ok=True)

        for esp in esp_names:
            df_esp = df_final[df_final['account_name'] == esp][['email']]
            df_esp.to_csv(output_path / f"{esp}_ORANGE_DATA.csv", index=False, header=False)

        if request_type == "Doordash":
            zip_out_name = "ORANGE_DOORDASH_ZIPS.zip"
        else:
            zip_out_name = f"ORANGE_ZIP_{comp_type}.zip"

        zip_out = Path(final_dir) / zip_out_name
        run_command(
            ["zip", "-r", zip_out.name, output_path.name],
            cwd=str(final_dir)
        )
        run_command(f"rm -rf {output_path.name} && rm -f {final_name}", cwd=final_dir)
        logger.info(f"ORANGE ZIP {label}: {zip_out_name} ({total_count:,} records)")
        return zip_out_name, total_count

    except Exception as e:
        logger.error(f"ORANGE ZIP FAILED: {e}")
        send_error_email(f"ORANGE ZIP {label} FAILED", str(e))
        raise


# ─── Channel dispatcher ───────────────────────────────────────────────────────

CHANNEL_PROCESSORS = {
    "GREEN":   process_green_zip,
    "BLUE":    process_blue_zip,
    "ARCAMAX": process_arcamax_zip,
    "ORANGE":  process_orange_zip,
}


def process_zip_request(
    request_type: str,
    zip_file: str,
    comp_type: str,
    channel: str,
    output_dir: str,
):
    """
    Main entry point called by main.py.

    request_type : 'Suppression' | 'Mailing' | 'Doordash'
    zip_file     : absolute path to uploaded ZIP codes file
    comp_type    : 'include' | 'exclude'  (Doordash always uses include internally)
    channel      : 'ALL' | 'GREEN' | 'BLUE' | 'ORANGE' | 'ARCAMAX'
    output_dir   : base output directory
    """
    main_logger = setup_main_logging(output_dir)
    main_logger.info(f"=== ZIP REQUEST ===")
    main_logger.info(f"Type       : {request_type}")
    main_logger.info(f"Zip file   : {zip_file}")
    main_logger.info(f"Comp type  : {comp_type}")
    main_logger.info(f"Channel    : {channel}")
    main_logger.info(f"Output dir : {output_dir}")

    # Doordash always forces include
    effective_comp = "include" if request_type == "Doordash" else comp_type

    safe_output_dir = ensure_output_dir(output_dir, effective_comp)

    # Determine which channels to run
    if channel == "ALL":
        channels_to_run = list(CHANNEL_PROCESSORS.keys())
    else:
        channels_to_run = [channel.upper()]

    results = {}
    errors  = []

    for ch in channels_to_run:
        if ch not in CHANNEL_PROCESSORS:
            main_logger.warning(f"Unknown channel '{ch}', skipping.")
            continue
        try:
            main_logger.info(f"--- Starting channel: {ch} ---")
            result_file, record_count = CHANNEL_PROCESSORS[ch](
                zip_file=zip_file,
                comp_type=effective_comp,
                request_type=request_type,
                output_dir=safe_output_dir,
            )
            results[ch] = {"file": result_file, "count": record_count}
            main_logger.info(f"--- {ch} completed: {record_count:,} records ---")
        except Exception as e:
            main_logger.error(f"--- {ch} FAILED: {e} ---")
            errors.append((ch, str(e)))

    total_records = sum(v['count'] for v in results.values())
    summary_lines = [f"  {ch}: {v['file']} ({v['count']:,} records)" for ch, v in results.items()]
    summary = (
        f"\n{'='*60}\n"
        f"ZIP REQUEST COMPLETE\n"
        f"Type       : {request_type}\n"
        f"Comp type  : {effective_comp}\n"
        f"Channels   : {', '.join(channels_to_run)}\n"
        f"Total recs : {total_records:,}\n"
        f"Output     : {safe_output_dir}/FINAL_FILES\n"
        + "\n".join(summary_lines)
        + (f"\nERRORS ({len(errors)}): " + "; ".join(f"{c}: {e}" for c, e in errors) if errors else "")
        + f"\n{'='*60}"
    )
    main_logger.info(summary)

    if errors:
        send_error_email(
            f"ZIP {request_type} PARTIAL FAILURE",
            "\n".join(f"{c}: {e}" for c, e in errors)
        )
    else:
        send_success_email(
            f"ZIP {request_type} - {total_records:,} MATCHED",
            [v['file'] for v in results.values()],
            safe_output_dir
        )

    if errors and not results:
        raise RuntimeError(f"All channels failed: {errors}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 5:
        print("Usage: zips.py <request_type> <zip_file> <comp_type> <channel> <output_dir>")
        sys.exit(1)
    process_zip_request(
        request_type=sys.argv[1],
        zip_file=sys.argv[2],
        comp_type=sys.argv[3],
        channel=sys.argv[4],
        output_dir=sys.argv[5],
    )
