import json
import sys
import os
from AGE_STATE import process_age_state_all_channels
from config import SNOWSQL_PASSPHRASE


def load_config(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")

    with open(path) as f:
        return json.load(f)


def validate(config):
    required_fields = ["criteria", "comp", "value", "output_dir", "user", "client", "request_type"]

    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required field: {field}")

    criteria = config["criteria"]
    comp = config["comp"]
    value = config["value"]

    if criteria not in ["age", "state"]:
        raise ValueError("criteria must be 'age' or 'state'")

    if criteria == "age":
        if comp not in ["greater", "less"]:
            raise ValueError("comp must be greater/less for age")
        if not isinstance(value, int):
            raise ValueError("value must be integer for age")

    if criteria == "state":
        if comp not in ["include", "exclude"]:
            raise ValueError("comp must be include/exclude for state")
        if not isinstance(value, list):
            raise ValueError("value must be list for state")


def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <config.json>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        config = load_config(config_path)
        validate(config)

        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

        process_age_state_all_channels(
            config["criteria"],
            config["value"],
            config["comp"],
            config["output_dir"],
            config["user"],
            config["client"],
            config["request_type"]
        )

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
