#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")

# test data
sys.argv[1:] = ["data"]

# test scaffolding
from partition_tweets import parse_command_line

output, json_files = parse_command_line()
for file in json_files:
    print(file)

