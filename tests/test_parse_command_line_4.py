#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")

# test scaffolding
from partition_tweets import parse_command_line

args = parse_command_line()
for file in args.sources:
    print(file)

