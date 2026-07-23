#!/usr/bin/env python3
"""
CPA Tool – Entry point
Routes incoming requests to the correct processing module:
  age / state  → AGE_STATE/age_state_new.py  (process_age_state_request)
  zips         → ZIPS/zips.py  (Suppression / Mailing)
  doordash     → ZIPS/zips.py  (Doordash mode)
"""

import argparse
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))


def parse_args():
    parser = argparse.ArgumentParser(description="CPA Tool – request processor")
    parser.add_argument("--request-type",  required=True,
                        choices=["Suppression", "Mailing", "Doordash"],
                        help="Type of request")
    parser.add_argument("--criteria-type", required=True,
                        choices=["age", "state", "zips"],
                        help="Criteria type")
    parser.add_argument("--comp-type",     required=True,
                        choices=["greater", "less", "include", "exclude"],
                        help="Comparison / inclusion type")
    # Accept one or more channel values: --channel GREEN --channel ORANGE
    # or a single value like --channel ALL
    parser.add_argument("--channel",       required=True,
                        choices=["ALL", "GREEN", "BLUE", "ORANGE", "ARCAMAX"],
                        action="append",
                        dest="channels",
                        help="Channel(s) to process. Repeat flag for multiple: --channel GREEN --channel ORANGE")
    parser.add_argument("--output-dir",    required=True,
                        help="Output directory")
    # Criteria-specific args
    parser.add_argument("--age",           type=int, default=None)
    parser.add_argument("--states",        nargs="+", default=None)
    parser.add_argument("--zip-file",      default=None,
                        help="Path to uploaded ZIP codes file")
    parser.add_argument("--request-id",    type=int, default=None,
                        help="DB request ID (required for age/state and zips processors)")
    return parser.parse_args()


def main():
    args = parse_args()

    criteria = args.criteria_type.lower()
    req_type = args.request_type

    # Normalise: if ALL is present, treat as ["ALL"]; otherwise deduplicate
    channels = list(dict.fromkeys(args.channels))  # preserve order, deduplicate
    if "ALL" in channels:
        channels = ["ALL"]

    if criteria in ("age", "state"):
        from AGE_STATE.age_state import process_age_state_request
        if args.request_id is None:
            print("[ERROR] --request-id is required for age/state criteria", file=sys.stderr)
            sys.exit(1)
        process_age_state_request(
            request_id=args.request_id,
            channel=channels,
        )

    elif criteria == "zips":
        from ZIPS.zips import process_zip_request
        if args.request_id is None:
            print("[ERROR] --request-id is required for zips criteria", file=sys.stderr)
            sys.exit(1)
        process_zip_request(
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
