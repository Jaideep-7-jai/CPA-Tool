
#!/usr/bin/env python3
"""
ZIP Processing - Load ZIP → Snowflake Table → Match → Export
Handles huge ZIP files by streaming directly to temp S3 staging
"""

import os
import gzip
import time
import shutil
import logging
import pandas as pd
from typing import Tuple, List
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
import boto3
from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import (
    run_command, send_success_email, send_error_email,
    get_db_connection, ensure_output_dir, download_combine, load_zip_to_pg
)

def setup_logging(output_dir):
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"zip_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(log_file)]
    )
    return logging.getLogger("zip_processor")

def ensure_final_files_dir(output_dir: str) -> str:
    """Create final directory if missing"""
    final_dir = Path(output_dir) / "FINAL_FILES"
    final_dir.mkdir(parents=True, exist_ok=True)
    return str(final_dir)


def get_unique_s3_path(base_path, criteria, comp_type, timestamp):
    """Unique S3 path per run"""
    pathdate = datetime.now().strftime('%Y%m%d')
    pathdate1 = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{base_path}/{pathdate}/{criteria}_{comp_type}_{pathdate1}"

def get_unique_table_name(prefix, comp_type, channel):
    """Unique staging table name"""
    timestamp = datetime.now().strftime('%Y%m%d')
    return f"APT_ADHOC_JAIDEEP_ZIP_{prefix}_{comp_type}_{channel}_{timestamp}"


def create_zip_staging_table(table_name: str):
    """Create Snowflake staging table for ZIP data"""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    sql_commands = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        zip_code VARCHAR(10),
        PRIMARY KEY (zip_code)
    );
    """

    run_command(["snowsql", "-c", "datateam1", "-q", sql_commands])

def load_zip_to_snowflake(s3_path: str, table_name: str) -> int:
    """Load ZIP data from S3 to Snowflake staging table"""
    logger = logging.getLogger("zip_processor")

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

    # Get row count
    count_sql = f"SELECT COUNT(*) FROM {table_name};"
    count_result = run_command(["snowsql","-c", "datateam1","-o", "output_format=csv","-o", "header=false","-o", "timing=false","-o", "friendly=false","-q", count_sql])
    row_count = int(count_result.strip().strip('"'))
    logging.info(f"✅ Loaded {row_count:,} ZIP records to {table_name}")

    return row_count

def get_file_count(filename: str) -> int:
    """Get line count of file"""
    try:
        return int(run_command(f"wc -l < {filename}", shell=True))
    except:
        return 0


def process_green_zip(zip_file: str, comp_type: str, output_dir: str) -> Tuple[str, int]:
    """Process GREEN channel ZIP matching"""
    logger = logging.getLogger("zip_processor")
    final_dir = ensure_final_files_dir(output_dir)
    s3_path = get_unique_s3_path(S3_BASE, "GREEN_ZIP", comp_type, datetime.now())
    #zip_file='/datateam/PFM_CUSTOM_SCRIPTS/CPA_MAILING_SUPP_REQ/zips.csv'
    logging.info(f"✅ GREEN ZIP {comp_type}: {zip_file} started")

    try:
        # 1. Stream ZIP to S3 staging
        run_command(["aws", "s3", "cp", zip_file, f"{s3_path}/{os.path.basename(zip_file)}", "--quiet"])

        # 2. Create unique staging table
        zip_table = get_unique_table_name("GREEN_ZIP_STAGING", comp_type, "green")
        create_zip_staging_table(zip_table)

        # 3. Load ZIP data to Snowflake
        zip_count = load_zip_to_snowflake(s3_path, zip_table)

        zip_condition = f"b.ZIP in (select zip_code from {zip_table})" if comp_type == "include" else f"b.ZIP not in (select zip_code from {zip_table})"

        # 5. Export matched results
        output_file = f"GREEN_ZIP_{comp_type}.csv"
        export_sql = f"""
        COPY INTO '{s3_path}/matched/' FROM (select distinct a.email FROM GREEN_LPT.UNIVERSAL_PROFILE a
        JOIN APT_CUSTOM_GREEN_REA_DATA_DND b where a.md5hash=b.EMAIL_MD5 and {zip_condition})
        credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
        FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='"')
        max_file_size=490000000;
        """

        run_command(["snowsql", "-c", "datateam1", "-q", export_sql])

        # 6. Download and zip final results
        zip_file_final, record_count = download_combine(f"{s3_path}/matched/", output_file, final_dir)

        logging.info(f"✅ GREEN ZIP {comp_type}: {zip_file_final} ({record_count:,} matched records)")
        return zip_file_final, record_count

    except Exception as e:
        logging.error(f"💥 GREEN ZIP FAILED: {e}")
        send_error_email(f"GREEN ZIP {comp_type} FAILED", str(e))
        raise

def process_blue_zip(zip_file: str, comp_type: str, output_dir: str) -> Tuple[str, int]:
    """Process BLUE channel ZIP matching"""
    logger = logging.getLogger("zip_processor")
    final_dir = ensure_final_files_dir(output_dir)
    s3_path = get_unique_s3_path(S3_BASE, "BLUE_ZIP", comp_type, datetime.now())
    #zip_file='/datateam/PFM_CUSTOM_SCRIPTS/CPA_MAILING_SUPP_REQ/zips.csv'
    logging.info(f"✅ BLUE ZIP {comp_type}: {zip_file} started")

    try:
        # 1. Stream ZIP to S3 staging
        run_command(["aws", "s3", "cp", zip_file, f"{s3_path}/{os.path.basename(zip_file)}", "--quiet"])

        # 2. Create unique staging table
        zip_table = get_unique_table_name("BLUE_ZIP_STAGING", comp_type, "BLUE")
        create_zip_staging_table(zip_table)

        # 3. Load ZIP data to Snowflake
        zip_count = load_zip_to_snowflake(s3_path, zip_table)

        zip_condition = f"b.ZIP in (select zip_code from {zip_table})" if comp_type == "include" else f"b.ZIP not in (select zip_code from {zip_table})"

        # 5. Export matched results
        output_file = f"BLUE_ZIP_{comp_type}.csv"
        export_sql = f"""
        COPY INTO '{s3_path}/matched/' FROM (select distinct a.email FROM INFS_LPT.infs_profile a
        JOIN APT_CUSTOM_GREEN_REA_DATA_DND b where a.md5hash=b.EMAIL_MD5 and {zip_condition})
        credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
        FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='"')
        max_file_size=490000000;
        """

        run_command(["snowsql", "-c", "datateam1", "-q", export_sql])

        # 6. Download and zip final results
        zip_file_final, record_count = download_combine(f"{s3_path}/matched/", output_file, final_dir)

        logging.info(f"✅ BLUE ZIP {comp_type}: {zip_file_final} ({record_count:,} matched records)")
        return zip_file_final, record_count
        logging.info(f"📧 BLUE ZIP {comp_type} ended")
    except Exception as e:
        logging.error(f"💥 BLUE ZIP FAILED: {e}")
        send_error_email(f"BLUE ZIP {comp_type} FAILED", str(e))
        raise


def process_arcamax_zip(zip_file: str, comp_type: str, output_dir: str) -> Tuple[str, int]:
    """Process ARCAMAX channel ZIP matching"""
    logger = logging.getLogger("zip_processor")
    final_dir = ensure_final_files_dir(output_dir)

    try:
        logging.info(f"✅ ARCAMAX ZIP {comp_type}: {zip_file} started")
        filename = f"ARCAMAX_ZIP_{comp_type}.csv"
        output_file = f"{final_dir}/{filename}"

        arcamax_config = get_db_connection('arcamax')
        zip_table = get_unique_table_name("DATA", comp_type, "ARCAMAX")
        load_count = load_zip_to_pg(zip_file, zip_table, arcamax_config)

        zip_condition = f"ZIP in (select zip_code from {zip_table})" if comp_type == "include" else f"ZIP not in (select zip_code from {zip_table})"

        cmd = [
            "psql", "-U", "datateam", "-d", arcamax_config['db'],
            "-h", arcamax_config['host'], "-qtAX", "-c",
            f"select distinct email from apt_custom_arcamax_customer_table_aamir where {zip_condition}"
        ]

        start = time.time()
        with open(output_file, "w") as f:
            run_command(cmd, cwd=None, stdout=f)
        elapsed = time.time() - start

        logging.info(f"⏱ Query executed in {elapsed:.1f}s")
        count_cmd = f"wc -l < {output_file}"
        record_count = int(run_command(count_cmd))

        start = time.time()
        zip_name = f"{output_file}.zip"
        logging.info(f"✅ ARCAMAX: {zip_name} ({record_count:,} records)")
        zip_path = Path(zip_name)
        shutil.make_archive(base_name=zip_path.with_suffix('').as_posix(),
                            format='zip',
                            root_dir=final_dir,
                            base_dir=filename)

        if not zip_path.exists():
            raise RuntimeError("Zip file not created")
        elapsed = time.time() - start
        logging.info(f"⏱ Zipped file in {elapsed:.1f}s")

        size = zip_path.stat().st_size
        logging.info(f"📦 {zip_name}: {size/1e6:.1f}MB")

        output_file_path = Path(output_file)
        if output_file_path.exists():
            output_file_path.unlink()

        return zip_name, record_count
        logging.info(f"📧 ARCAMAX ZIP {comp_type} ended")
    except Exception as e:
        logger.error(f"💥 ARCAMAX ZIP FAILED: {e}")
        send_error_email(f"ARCAMAX ZIP {comp_type} FAILED", str(e))
        raise


def process_orange_zip(zip_file: str, comp_type: str, output_dir: str) -> Tuple[str, int]:
    """Process ORANGE channel ZIP matching (complex with ESP mapping)"""
    logger = logging.getLogger("zip_processor")
    final_dir = ensure_final_files_dir(output_dir)

    logging.info(f"✅ ORANGE ZIP {comp_type}: {zip_file}started")
    try:
        filename = f"ORANGE_ZIP_{comp_type}.csv"
        output_file = f"{final_dir}/{filename}"

        orange_config = get_db_connection('orange')
        zip_table = get_unique_table_name("DATA", comp_type, "ORANGE")
        load_count = load_zip_to_pg(zip_file, zip_table, orange_config)

        s3_path = get_unique_s3_path(S3_BASE, "ORANGE", comp_type, datetime.now())
        # Step 1: MySQL ESP
        mt2_config = get_db_connection('mt2')
        esp_file = f"{output_dir}/esp_details.csv"
        mysql_cmd = [
            "mysql", mt2_config['db'], "-h", mt2_config['host'],
            "-u", mt2_config['user'], f"-p{mt2_config['password']}", "-A", "-ss", "-e",
            "select distinct a.feed_id,b.account_name from mt2_data.esp_data_exports a, "
            "mt2_data.esp_accounts b where a.esp_account_id=b.id"
            ]

        start = time.time()
        output = run_command(mysql_cmd, cwd=output_dir)
        output = output.replace("\t", "|")
        esp_path = Path(output_dir) / "esp_details.csv"
        esp_path.write_text(output)
        logging.info(f"💾 {esp_path} ({len(output.splitlines())} rows)")

        elapsed = time.time() - start
        logging.info(f"⏱ Query + formatting executed in {elapsed:.1f}s")

        # Step 2-3: PSQL exports
        orange_config = get_db_connection('orange')
        trans_file = f"{output_dir}/orange_email_age.csv"
        profile_file = f"{output_dir}/orange_profile.csv"

        zip_condition = f"ZIP in (select zip_code from {zip_table})" if comp_type == "include" else f"ZIP not in (select zip_code from {zip_table})"
        trans_sql = f"""SELECT email_address,feed_id from apt_custom_orange_transaction_dnd
                    where email_address is not null and dob is not null and dob<>'NULL'
                    and {zip_condition}"""

        run_command([
            "psql", "-U", "datateam", "-h", orange_config['host'],
            "-d", orange_config['db'], "-c", f"\\COPY ({trans_sql}) TO '{trans_file}' WITH (FORMAT CSV, HEADER)",
            "-qAtX"
        ])

        profile_sql = "select email_address from apt_custom_orange_profile_email_dnd where email_address is not null"
        run_command([
            "psql", "-U", "datateam", "-h", orange_config['host'],
            "-d", orange_config['db'], "-c", f"\\COPY ({profile_sql}) TO '{profile_file}' WITH (FORMAT CSV, HEADER)",
            "-qAtX"
        ])

        #Step 4: S3 upload
        for src_file in [esp_file, trans_file, profile_file]:
            run_command(["aws", "s3", "cp", src_file, f"{s3_path}/{os.path.basename(src_file)}", "--quiet"])

        #Step 5: UNIQUE Snowflake tables
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

        table_esp = get_unique_table_name("esp_details", comp_type, "orange")
        table_email = get_unique_table_name("email_age", comp_type, "orange")
        table_profile = get_unique_table_name("PROFILE_DND", comp_type, "orange")

        snowflake_sqls = [
            f"drop table if exists {table_esp};",
            f"create table {table_esp} (feedid varchar,account_name varchar);",
            f"drop table if exists {table_email};",
            f"create table {table_email} (email varchar,feedid varchar);",
            f"drop table if exists {table_profile};",
            f"CREATE TABLE {table_profile} (email_address VARCHAR);",

            # COPY INTO unique tables
            f"COPY INTO {table_esp} from '{s3_path}/esp_details.csv' "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE='CSV' FIELD_DELIMITER='|') ON_ERROR='CONTINUE';",

            f"COPY INTO {table_email} from '{s3_path}/orange_email_age.csv' "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE='CSV') ON_ERROR='CONTINUE';",

            f"COPY INTO {table_profile} from '{s3_path}/orange_profile.csv' "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE='CSV') ON_ERROR='CONTINUE';",

            # Final UNIQUE export
            f"COPY INTO {s3_path}/ORANGE_FINAL_DATA/ from (select distinct a.email,b.account_name "
            f"from {table_email} a, {table_esp} b "
            f"where a.feedid=b.feedid and email not in (select email_address from {table_profile})) "
            f"credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}') "
            f"FILE_FORMAT=(TYPE = CSV COMPRESSION=GZIP FIELD_DELIMITER = '|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') max_file_size=4900000000;"
        ]

        sql_file = Path(output_dir) / "run.sql"
        with open(sql_file, "w") as f:
            f.write("\n".join(snowflake_sqls))

        cmd = f"snowsql -c datateam1 -f {sql_file}"
        run_command(cmd)

        # Download final
        final_orange = f"ORANGE_AGE_{comp_type}.csv"
        run_command(["aws", "s3", "cp", f"{s3_path}/ORANGE_FINAL_DATA/", ".", "--recursive", "--quiet"],cwd=final_dir)
        run_command(f"ls data* 1> /dev/null 2>&1 && zcat data* > {final_orange}", cwd=final_dir)
        run_command("rm -f data*", cwd=final_dir)

        df_final = pd.read_csv(f"{final_dir}/{final_orange}", names=["email", "account_name"],delimiter='|')
        esp_names = df_final['account_name'].drop_duplicates().sort_values().tolist()
        total_count=len(df_final)
        logging.info(f"📊 ORANGE Total: {total_count:,} records")

        output_path = Path(final_dir) / "ORANGE_OP_PATH"
        output_path.mkdir(exist_ok=True)

        for esp in esp_names:
            df_esp = df_final[df_final['account_name'] == esp][['email']]
            df_esp.to_csv(output_path / f"{esp}_ORANGE_DATA.csv",
                        index=False, header=False)

        zipfinal_orange = f"ORANGE_AGE_{comp_type}.zip"

        # Zip all
        zip_file = Path(final_dir) / zipfinal_orange
        input_folder = Path(final_dir) / "ORANGE_OP_PATH"

        run_command(["zip", "-r", zip_file.name, input_folder.name],cwd=str(Path(final_dir)))
        zip_filename = zipfinal_orange
        run_command(f"rm -rf {input_folder.name} && rm -f {final_orange}", cwd=final_dir)
        logging.info(f"✅ ORANGE: {zip_filename} ({total_count:,} records)")

        return zip_filename, total_count



        output_file = f"ORANGE_ZIP_{comp_type}.csv"
        zip_file_final, record_count = download_combine(f"{s3_path}/matched/", output_file, final_dir)

        logger.info(f"✅ ORANGE ZIP {comp_type}: {zip_file_final} ({record_count:,} matched records)")
        return zip_file_final, record_count

    except Exception as e:
        logger.error(f"💥 ORANGE ZIP FAILED: {e}")
        send_error_email(f"ORANGE ZIP {comp_type} FAILED", str(e))
        raise

def process_zip_all_channels(zip_file: str, comp_type: str, output_dir: str):
    """Main orchestrator for ALL channels ZIP processing"""
    logger = setup_logging(output_dir)
    results = {}

    try:
        safe_output_dir = ensure_output_dir(output_dir,comp_type)
        logger.info(f"🎯 ZIP File: {zip_file}")
        logger.info(f"🎯 Output: {safe_output_dir}")
        logger.info(f"🎯 Comp Type: {comp_type}")

        # Process all channels
        green_file, green_count = process_green_zip(zip_file, comp_type, safe_output_dir)
        results[green_file] = green_count
        
        blue_file, blue_count = process_blue_zip(zip_file, comp_type, safe_output_dir)
        results[blue_file] = blue_count

        arcamax_file, arcamax_count = process_arcamax_zip(zip_file, comp_type, safe_output_dir)
        results[arcamax_file] = arcamax_count

        orange_file, orange_count = process_orange_zip(zip_file, comp_type, safe_output_dir)
        results[orange_file] = orange_count

        total_records = sum(results.values())
        all_files = list(results.keys())

        summary = f"""
        🎉 SUCCESS - ZIP {comp_type.upper()}
        📁 {safe_output_dir}/FINAL_FILES
        📦 Input ZIP: {Path(zip_file).name}
        📊 TOTAL MATCHED: {total_records:,} RECORDS ({len(results)} files)

        BREAKDOWN:
        """ + "\n".join([f"  📄 {f}: {c:,}" for f,c in results.items()])

        logger.info(summary)
        send_success_email(f"ZIP {comp_type.upper()} - {total_records:,} MATCHED", all_files, safe_output_dir)

    except Exception as e:
        logger.error(f"💥 ZIP PROCESSING FAILED: {e}")
        send_error_email(f"ZIP {comp_type.upper()} FAILED", str(e))
        raise

if __name__ == "__main__":
    import sys
    process_zip_all_channels(sys.argv[1], sys.argv[2], sys.argv[3])


