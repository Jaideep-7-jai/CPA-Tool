#!/usr/bin/env python3
"""
CPA Tool – Entry point
Routes incoming requests to the correct processing module:
  age / state  → AGE_STATE/age_state_new.py
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
    parser.add_argument("--channel",       required=True,
                        choices=["ALL", "GREEN", "BLUE", "ORANGE", "ARCAMAX"],
                        help="Channel to process")
    parser.add_argument("--output-dir",    required=True,
                        help="Output directory")
    # Criteria-specific args
    parser.add_argument("--age",           type=int, default=None)
    parser.add_argument("--states",        nargs="+", default=None)
    parser.add_argument("--zip-file",      default=None,
                        help="Path to uploaded ZIP codes file")
    return parser.parse_args()


def main():
    args = parse_args()

    criteria = args.criteria_type.lower()
    req_type = args.request_type

    if criteria in ("age", "state"):
        from AGE_STATE.age_state_new import process_age_state
        process_age_state(
            request_type=req_type,
            criteria=criteria,
            comp_type=args.comp_type,
            channel=args.channel,
            age=args.age,
            states=args.states,
            output_dir=args.output_dir,
        )

    elif criteria == "zips":
        from ZIPS.zips import process_zip_request
        process_zip_request(
            request_type=req_type,
            zip_file=args.zip_file,
            comp_type=args.comp_type,
            channel=args.channel,
            output_dir=args.output_dir,
        )

    else:
        print(f"[ERROR] Unknown criteria type: {criteria}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
