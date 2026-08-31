#!/usr/bin/env python3
"""
CPA Tool - Entry point
Routes incoming requests to the correct processing module:
  age / state  -> AGE_STATE/age_state.py  (process_age_state_request)
  zips         -> ZIPS/zips.py  (Suppression / Mailing)
  doordash     -> Doordash/doordash_zips.py
"""

import argparse
import os
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

_ALL_REQUEST_TYPES = {"Suppression", "Mailing", "Doordash"}


def _safe_filename_part(value):
    value = str(value or "").strip()
    return re.sub(r"[\\/\s]+", "_", value)


def _configure_output_naming_and_ftp(module):
    """Apply common final-file naming and FTP request-type subfolders."""
    if getattr(module, "_CPA_OUTPUT_CONTRACT_CONFIGURED", False):
        return

    original_build = module._build_common_context
    original_post = module._post_to_ftp
    context_by_dir = {}

    def _build_common_context_with_contract(*args, **kwargs):
        ctx = original_build(*args, **kwargs)
        request_data = ctx["request_data"]
        channel_name = str(args[1] if len(args) > 1 else kwargs.get("channel_name", "")).upper()
        client_name = _safe_filename_part(request_data["client_name"])
        criteria_type = _safe_filename_part(request_data["criteria_type"].title())
        request_type = _safe_filename_part(request_data["request_type"])
        path_date = ctx["path_date"]
        extension = Path(ctx["output_file"]).suffix or ".csv"

        ctx["output_file"] = (
            f"{client_name}_{criteria_type}_{request_type}_"
            f"{channel_name}_{path_date}{extension}"
        )
        context_by_dir[str(ctx["final_files_dir"])] = {
            "client_name": client_name,
            "criteria_type": criteria_type,
            "request_type": request_type,
            "channel_name": channel_name,
        }
        return ctx

    def _post_to_ftp_with_contract(final_files_dir, path_date, output_file, log):
        final_files_dir = Path(final_files_dir)
        context = context_by_dir.get(str(final_files_dir))
        request_type = context["request_type"] if context else None
        if request_type not in _ALL_REQUEST_TYPES:
            request_type = next(
                (item for item in _ALL_REQUEST_TYPES
                 if f"_{item.lower()}_" in f"_{str(output_file).lower()}_"),
                "Suppression",
            )

        desired_name = output_file
        lower_name = str(output_file).lower()
        if context and context["request_type"] == "Doordash":
            if "md5hash" in lower_name:
                channel_token = "MD5HASH"
            elif "email" in lower_name:
                channel_token = "EMAIL"
            else:
                channel_token = context["channel_name"] or "DOORDASH"
            extension = Path(str(output_file)).suffix or ".csv"
            desired_name = (
                f"{context['client_name']}_{context['criteria_type']}_"
                f"{context['request_type']}_{channel_token}_{path_date}{extension}"
            )

        source_path = final_files_dir / output_file
        target_path = final_files_dir / desired_name
        if source_path != target_path and source_path.exists():
            if target_path.exists():
                target_path.unlink()
            source_path.rename(target_path)

        return original_post(
            final_files_dir, f"{path_date}/{request_type}", desired_name, log
        )

    module._build_common_context = _build_common_context_with_contract
    module._post_to_ftp = _post_to_ftp_with_contract
    module._CPA_OUTPUT_CONTRACT_CONFIGURED = True


def _configure_doordash_apptness(module):
    """Use APPT_RT_MAIL_REQUESTS_SF as the APPTNESS ZIP source."""
    if getattr(module, "_CPA_APPTNESS_CONFIGURED", False):
        return

    def _insert_apptness_into_perm_table(perm_table, zip_staging_table, comp_type, log):
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = module.SNOWSQL_PASSPHRASE
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
        module.run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
        inserted_rows = module._query_snowflake(
            f"SELECT COUNT(*) FROM {perm_table};", log
        )
        if inserted_rows >= 0:
            log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
        else:
            log.warning(f"  Could not verify row count for {perm_table} (non-fatal)")
        return inserted_rows

    module._insert_apptness_into_perm_table = _insert_apptness_into_perm_table
    module._CPA_APPTNESS_CONFIGURED = True


def parse_args():
    parser = argparse.ArgumentParser(description="CPA Tool - request processor")
    parser.add_argument("--request-type", required=True,
                        choices=["Suppression", "Mailing", "Doordash"])
    parser.add_argument("--criteria-type", required=True,
                        choices=["age", "state", "zips"])
    parser.add_argument("--comp-type", required=True,
                        choices=["greater", "less", "include", "exclude"])
    parser.add_argument("--channel", required=True,
                        choices=["ALL", "GREEN", "BLUE", "ORANGE", "ARCAMAX", "APPTNESS"],
                        action="append", dest="channels")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--age", type=int, default=None)
    parser.add_argument("--states", nargs="+", default=None)
    parser.add_argument("--zip-file", default=None)
    parser.add_argument("--request-id", type=int, default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    criteria = args.criteria_type.lower()
    req_type = args.request_type
    channels = list(dict.fromkeys(args.channels))
    if "ALL" in channels:
        channels = ["ALL"]

    if criteria in ("age", "state"):
        import AGE_STATE.age_state as age_state_module
        _configure_output_naming_and_ftp(age_state_module)
        if args.request_id is None:
            print("[ERROR] --request-id is required for age/state criteria", file=sys.stderr)
            sys.exit(1)
        age_state_module.process_age_state_request(
            request_id=args.request_id,
            channel=channels,
        )

    elif criteria == "zips":
        if req_type == "Doordash":
            import ZIPS.zips as zips_module
            _configure_output_naming_and_ftp(zips_module)
            import Doordash.doordash_zips as doordash_module
            _configure_output_naming_and_ftp(doordash_module)
            _configure_doordash_apptness(doordash_module)
            processor = doordash_module.process_doordash_zip_request
        else:
            import ZIPS.zips as zips_module
            _configure_output_naming_and_ftp(zips_module)
            processor = zips_module.process_zip_request

        if args.request_id is None:
            print("[ERROR] --request-id is required for zips criteria", file=sys.stderr)
            sys.exit(1)
        processor(
            request_id=args.request_id,
            zip_file=args.zip_file,
            channel=channels,
            output_dir=args.output_dir,
        )
    else:
        print(f"[ERROR] Unknown criteria type: {criteria}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
