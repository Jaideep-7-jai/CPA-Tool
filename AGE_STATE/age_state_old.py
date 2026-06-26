#!/usr/bin/env python3
"""
AGE STATE Processing - UNIQUE TABLES + Dynamic DOB for ALL Channels
"""
import os
import gzip
import time
import shutil
import pymysql
import logging
import pandas as pd
from typing import Tuple
from pathlib import Path
from datetime import date, datetime, timedelta
from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import (
    run_command, download_combine, send_success_email, send_error_email,
    get_db_connection, ensure_output_dir)


DB_CONFIG = {
    "host": "zds-prod-jbdb3-vip.bo3.e-dialog.com",
    "user": "techuser",
    "password": "tech12#$",
    "database": "CUST_TECH_DB",
    "charset": "utf8mb4",
    "autocommit": True,
}


def setup_logging(output_dir,criteria_type):
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_date=datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{criteria_type}_{log_date}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.FileHandler(log_file)]
    )
    return logging.getLogger("age_processor")



def get_file_count(filename: str) -> int:
    """Get line count of file"""
    try:
        return int(run_command(f"wc -l < {filename}", shell=True))
    except:
        return 0

def get_dob_cutoff(min_age, comp_type):
    if isinstance(min_age, str):
        min_age = int(min_age)
    """Dynamic DOB cutoff for ALL channels"""
    cutoff_date = date.today() - timedelta(days=365.25 * min_age)
    return cutoff_date.strftime('%Y-%m-%d')



def get_unique_s3_path(base_path, criteria, value, comp_type):
    """Unique S3 path per run"""
    pathdatee=datetime.now().strftime('%Y%m%d')
    return f"{base_path}/{pathdatee}/{criteria}_{comp_type}_{value}"



def ensure_final_files_dir(output_dir: str) -> str:
    """Create final directory if missing"""
    final_dir = Path(output_dir) / "FINAL_FILES"
    final_dir.mkdir(parents=True, exist_ok=True)
    return str(final_dir)

def get_unique_table_name(prefix, criteria_type, comp_type):
    """Unique table names per run"""
    tabledatee=datetime.now().strftime('%Y%m%d')
    return f"APT_ADHOC_{prefix}_{criteria_type}_{comp_type}_{tabledatee}"



def get_db():
    return pymysql.connect(**DB_CONFIG)


def fetch_request_details(request_id):
    """Fetch request details from requests table"""
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("""
                SELECT id, client_name, requets_type, requets_type, criteria_type, criteria_value, comp_type, output_dir FROM requests WHERE id=%s""", (request_id,))
            return cur.fetchone()
    except Exception as e:
        logger.exception("Unable to fetch request details")
        raise
    finally:
        conn.close()



def update_request_status(request_id, status, status_column):
    """Update GREEN_STATUS / BLUE_STATUS / ORANGE_STATUS / ARCAMAX_STATUS"""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f""" UPDATE requests SET {status_column}=%s WHERE id=%s """, (status, request_id))
        conn.commit()

        logger.info(f"{status_column} updated to " f"{status} for request_id={request_id}")

    except Exception as e:
        logger.exception(f"Failed updating {status_column}")
        raise
    finally:
        conn.close()



def process_green_blue(request_id,channel_name):
    """Green & Blue Requestor"""
    
    channel_status=f"{channel_name}_STATUS"
    update_request_status(request_id, "Started", channel_status)
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
    
    final_dir = ensure_final_files_dir(output_dir)
    
    path_date = datetime.now().strftime("%Y%m%d")
    s3_path = (f"{S3_BASE}/"f"{request_type}/"f"{path_date}/"f"{request_name}/"f"{channel_name}")
    
    logger.info(f"Started {channel_name} Processing " f"{criteria_type} {comp_type}")
    try:
        if criteria_type == "age":
            condition = (f"b.AGE " f"{'>=' if comp_type == 'greater' else '<'} " f"{criteria_value}")
            header = "a.email,b.age"
        
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
                states = "','".join(criteria_value)
            condition = (f"b.STATE " f"{'IN' if comp_type == 'include' else 'NOT IN'} " f"('{states}')")
            header = "a.email,b.state"
        
        else:
            raise Exception(f"Unsupported criteria_type " f"{criteria_type}")
    
        output_file_date = datetime.now().strftime("%Y%m%d")
        output_file = (f"{client_name}_"f"{request_type}_"f"{channel_name}_"f"{output_file_date}.csv")
        
        PROFILETABLENAME= "GREEN_LPT.UNIVERSAL_PROFILE" if channel_name == 'GREEN' else "INFS_LPT.INFS_PROFILE"
        
        sql = f"""COPY INTO '{s3_path}/' FROM (SELECT DISTINCT {header} FROM {PROFILETABLENAME} a JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash=b.EMAIL_MD5 WHERE {condition}) CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"""
    
        logger.info(sql)
        start_time = time.time()
    
        update_request_status(request_id,"Pulling Data",channel_status)
    
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        run_command(["snowsql","-c","datateam1","-q",sql])
    
        output_path = (Path(final_dir)/ f"{channel_name}_OP_PATH")
        output_path.mkdir(parents=True,exist_ok=True)
    
        update_request_status(request_id,"Combining Data",channel_status)
    
        run_command(["aws","s3","cp",s3_path,".","--recursive","--quiet"],cwd=output_path)
        run_command(f"ls data* 1>/dev/null 2>&1 && " f"zcat data* > "f"{final_dir}/{output_file}",cwd=output_path)
        run_command("rm -f data*",cwd=output_path)
    
        update_request_status(request_id,"Posting To FTP",channel_status)
    
        ftp_cmd = (f'lftp -u' f'"GreenPub,Zet@Welcome1!"' f'ftp://zxds-ftp-02.bo3.e-dialog.com' f'-e "mkdir -p /CPA/{path_date};' f'cd /CPA/{path_date};' f'put {output_file};' f'bye"')
    
        run_command(ftp_cmd,cwd=final_dir)
        elapsed = (time.time() - start_time)
    
        update_request_status(request_id,"Completed",channel_status)
    
        logger.info(f"{channel_name} processing completed "f"in {elapsed:.2f} seconds")
        return {"channel": channel_name,"file": output_file,"status": "SUCCESS","elapsed": elapsed}
    
    except Exception as e:
        update_request_status(request_id,"Failed",channel_status)
        logger.exception(f"{channel_name} processing failed")
        send_error_email(f"{channel_name} Processing Failed",str(e))
        raise



def process_arcamax(request_id):
    """ARCAMAX Request Processor"""
    
    channel_name = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    update_request_status(request_id,"Started",channel_status)
    
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception("Request ID {request_id} not found")
    
    client_name = request_data["client_name"]
    request_type = request_data["requets_type"]
    request_name = request_data["request_name"]
    criteria_type = request_data["criteria_type"]
    criteria_value = request_data["criteria_value"]
    comp_type = request_data["comp_type"]
    output_dir = request_data["output_dir"]
    
    final_dir = ensure_final_files_dir(output_dir)
    
    path_date = datetime.now().strftime("%Y%m%d")
    s3_path = (f"{S3_BASE}/"f"{request_type}/"f"{path_date}/"f"{request_name}/"f"{channel_name}")
    
    logger.info(f"Started {channel_name} Processing "f"{criteria_type} {comp_type}")
    
    try:
        if criteria_type == "age":
            date_cutoff = get_dob_cutoff(int(criteria_value),comp_type)
    
            condition = f"""birthday IS NOT NULL AND TRY_TO_DATE(birthday){'<=' if comp_type == 'greater' else '>='}'{date_cutoff}'"""
            header = "email,birthday"
        elif criteria_type == "state":
            if isinstance(criteria_value, str):
                criteria_value = [x.strip() for x in criteria_value.split(",")]
            states = "','".join(criteria_value)
    
            condition = (f"STATE "f"{'IN' if comp_type == 'include' else 'NOT IN'} "f"('{states}')")
            header = "email,state"
        else:
            raise Exception(f"Unsupported criteria_type "f"{criteria_type}")
    
        output_file_date = datetime.now().strftime("%Y%m%d")
        output_file = (f"{client_name}_"f"{request_type}_"f"{channel_name}_"f"{output_file_date}.csv")
    
        sql = f"""COPY INTO '{s3_path}/' FROM (SELECT DISTINCT {header} FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE WHERE {condition}) CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') MAX_FILE_SIZE=490000000;"""
    
        logger.info(sql)
    
        start_time = time.time()
        update_request_status(request_id,"Pulling Data",channel_status)
    
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        run_command(["snowsql","-c","datateam1","-q",sql])
    
        output_path = (Path(final_dir)/ "ARCAMAX_OP_PATH")
        output_path.mkdir(parents=True,exist_ok=True)
    
        update_request_status(request_id, "Combining Data", channel_status)
    
        run_command(
            ["aws", "s3", "cp", s3_path, ".", "--recursive", "--quiet"], cwd=output_path)
        run_command(f"ls data* 1>/dev/null 2>&1 && " f"zcat data* > " f"{final_dir}/{output_file}", cwd=output_path)
        run_command("rm -f data*", cwd=output_path)
    
        update_request_status(request_id, "Posting To FTP", channel_status)
    
        ftp_cmd = (f'lftp -u' f'"GreenPub,Zet@Welcome1!"' f'ftp://zxds-ftp-02.bo3.e-dialog.com' f'-e "mkdir -p /CPA/{path_date};' f'cd /CPA/{path_date};' f'put {output_file};' f'bye"')
    
        run_command(ftp_cmd, cwd=final_dir)
        elapsed = (time.time() - start_time)
    
        update_request_status(request_id, "Completed", channel_status)
        logger.info(f"{channel_name} processing completed" f"in {elapsed:.2f} seconds")
        return {"channel": channel_name, "file": output_file, "status": "SUCCESS", "elapsed": elapsed}
    
    except Exception as e:
        update_request_status(request_id, "Failed", channel_status)
        logger.exception(f"{channel_name} processing failed")
        send_error_email(f"{channel_name} Processing Failed",str(e))
        raise




def process_orange(criteria_type, criteria_value, comp_type, output_dir) -> Tuple[str, int]:
    logger = logging.getLogger("age_processor")
    final_dir = ensure_final_files_dir(output_dir)
    #s3_prefix = "s3://temporary-data/JAIDEEP/ADHOC/CPA_SUPP_MALNG_REQ/ORANGE_PROFILES/"
    s3_prefix = get_unique_s3_path(S3_BASE, "ORANGE", criteria_type, comp_type)
    logging.info(f"S3 PREFIX USED: {s3_prefix}")
    try:
        logging.info(f"Sarted Orange Processing for :: {criteria_type} {comp_type}")
        logging.info(f"FILES DIR :: {final_dir}")

        if criteria_type == 'age':
            date_cutoff = get_dob_cutoff(criteria_value, comp_type)
            dob_condition = f"dob {'<=' if comp_type == 'greater' else '>='} '{date_cutoff}'"
            trans_sql = f"""SELECT email_address,feed_id from apt_custom_orange_transaction_dnd
            where email_address is not null and dob is not null and dob<>'NULL'
            and {dob_condition}"""
        else:
            requiredstates="','".join(criteria_value)
            condition = f"STATE {'in' if comp_type=='include' else 'not in'} ('{requiredstates}')"
            trans_sql = f"""SELECT email_address,feed_id from apt_custom_orange_transaction_dnd
            where email_address is not null and {condition}"""


        sql = f"""
            COPY INTO '{s3_path}/'
            FROM (select distinct a.email_address,b.ACCOUNT_NAME from (select distinct a.FEED_ID,a.email_address from APT_CUSTOM_ORANGE_TRANSACTION_DND a,(select a.email_address,max(a.created_at) as maxdate from APT_CUSTOM_ORANGE_TRANSACTION_DND a where {condition} group by 1) b where a.email_address=b.email_address and a.created_at=b.maxdate) a join APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 b on a.FEED_ID=b.FEEDID join APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND c on a.email_address=c.email_address join APT_CUSTOM_L90_ORANGE_UNIQ_RESPONDERS_UNIQ_DND d on a.email_address=d.email)
            credentials=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}')
            FILE_FORMAT = (
                TYPE = CSV
                COMPRESSION = GZIP
                FIELD_DELIMITER = '|'
                FIELD_OPTIONALLY_ENCLOSED_BY='\"'
            )
            max_file_size = 490000000;
        """
        run_command(["snowsql","-c", "datateam1","-q", sql])

        # Download final
        final_orange = f"ORANGE_{criteria_type}_{comp_type}.csv"
        run_command(["aws", "s3", "cp", f"{s3_prefix}/ORANGE_FINAL_DATA/", ".", "--recursive", "--quiet"],cwd=final_dir)
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
            df_esp.to_csv(output_path / f"{esp}_ORANGE_DATA.csv",index=False, header=False)

        output_file_datee=datetime.now().strftime('%Y%m%d')
        zipfinal_orange = f"ORANGE_AGE_{criteria_type}_{comp_type}_{output_file_datee}.zip"

        # Zip all
        zip_file = Path(final_dir) / zipfinal_orange
        input_folder = Path(final_dir) / "ORANGE_OP_PATH"

        run_command(["zip", "-r", zip_file.name, input_folder.name],cwd=str(Path(final_dir)))
        zip_filename = zipfinal_orange

        run_command(f"rm -rf {input_folder.name} && rm -f {final_orange}", cwd=final_dir)
        logging.info(f"✅ ORANGE: {zip_filename} ({total_count:,} records)")

        return zip_filename, total_count

    except Exception as e:
        logging.error(f"💥 ORANGE {comp_type} {criteria_type} FAILED: {e}")
        send_error_email(f"ORANGE {comp_type} {criteria_type} FAILED", str(e))
        raise





def process_age_state_all_channels(criteria_type, criteria_value, comp_type, output_dir):
    logger = setup_logging(output_dir,criteria_type)
    results = {}

    try:
        safe_output_dir = ensure_output_dir(output_dir,criteria_type)
        final_dir = ensure_final_files_dir(safe_output_dir)
        logger.info(f"🎯 Statred: {criteria_type} request ")
        logger.info(f"🎯 Output: {safe_output_dir}")
        logger.info(f"🎯 Final files: {final_dir}")

        green_file, green_count = process_green(criteria_type, criteria_value, comp_type, safe_output_dir)
        results[green_file] = green_count

        blue_file, blue_count = process_blue(criteria_type, criteria_value, comp_type, safe_output_dir)
        results[blue_file] = blue_count

        arcamax_file, arcamax_count = process_arcamax(criteria_type, criteria_value, comp_type, safe_output_dir)
        results[arcamax_file] = arcamax_count

        #orange_file, orange_count = process_orange(criteria_type, criteria_value, comp_type, safe_output_dir)
        #results[orange_file] = orange_count
        #all_files = list(results.keys())

        total_files = len([f for f in all_files if Path(f).exists()])

        total_records = sum(results.values())
        summary = f"""
        🎉 SUCCESS - {comp_type.upper()} {criteria_type} {criteria_value}
        📁 {final_dir}
        📊 TOTAL: {total_records:,} RECORDS ({len(results)} files)

        FILES:
        """ + "\n".join([f"  📄 {f}: {c:,}" for f,c in results.items()])

        logger.info(summary)
        send_success_email(f"{comp_type.upper()} {criteria_type} {criteria_value} - {total_records:,} RECORDS", all_files, safe_output_dir)

    except Exception as e:
        logger.error(f"💥 FAILED: {e}")
        send_error_email(f"{comp_type.upper()} {criteria_type} {criteria_value} FAILED", str(e))
        raise

if __name__ == "__main__":
    import sys
    process_age_state_all_channels(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4])



