#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")

# test data
sys.argv = ["test1.py", "data"]

# test scaffolding
from partition_tweets import parse_command_line

json_files = parse_command_line()
print("There are %d JSON files, estimated %.1fGB." % (
        len(json_files), len(json_files) / 20),
      file=sys.stderr)

# Expected output:
# There are 3 JSON files, estimated 0.1GB.

