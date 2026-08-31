#!/usr/bin/env python3
"""ZIPS processing module with client-based filenames and request-type FTP folders."""

import importlib.util
from pathlib import Path

_MODULE_DIR = Path(__file__).resolve().parent
_LEGACY_PATH = _MODULE_DIR / "_legacy_zips.py"
_SPEC = importlib.util.spec_from_file_location("cpa_zips_legacy", str(_LEGACY_PATH))
_LEGACY = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_LEGACY)

_REQUEST_TYPE_BY_FINAL_DIR = {}
_ORIGINAL_BUILD_COMMON_CONTEXT = _LEGACY._build_common_context
_ORIGINAL_POST_TO_FTP = _LEGACY._post_to_ftp


def _safe_filename_part(value):
    return str(value or "").strip().replace("/", "_").replace("\\", "_").replace(" ", "_")


def _build_common_context(request_id, channel_name, run_dir):
    ctx = _ORIGINAL_BUILD_COMMON_CONTEXT(request_id, channel_name, run_dir)
    request_data = ctx["request_data"]
    client_name = _safe_filename_part(request_data["client_name"])
    criteria_type = _safe_filename_part(request_data["criteria_type"].title())
    request_type = _safe_filename_part(request_data["request_type"])
    channel = str(channel_name).upper()
    path_date = ctx["path_date"]
    extension = Path(ctx["output_file"]).suffix or ".csv"

    ctx["output_file"] = (
        f"{client_name}_{criteria_type}_{request_type}_{channel}_{path_date}{extension}"
    )
    _REQUEST_TYPE_BY_FINAL_DIR[str(ctx["final_files_dir"])] = request_type
    return ctx


def _post_to_ftp(final_files_dir, path_date, output_file, log, request_type=None):
    request_type = request_type or _REQUEST_TYPE_BY_FINAL_DIR.get(
        str(Path(final_files_dir)), "Suppression"
    )
    ftp_path_date = f"{path_date}/{request_type}"
    return _ORIGINAL_POST_TO_FTP(
        final_files_dir, ftp_path_date, output_file, log
    )


_LEGACY._build_common_context = _build_common_context
_LEGACY._post_to_ftp = _post_to_ftp

globals().update(vars(_LEGACY))
