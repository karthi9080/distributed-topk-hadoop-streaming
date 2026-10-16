#!/usr/bin/env python3

import sys
import io
import re
import os

# Handle any non-UTF-8 bytes in the log files
sys.stdin = io.TextIOWrapper(
    sys.stdin.buffer,
    encoding="utf-8",
    errors="replace"
)

# Get the time period supplied by the user
START_HOUR = int(os.environ.get("START_HOUR", "0"))
END_HOUR = int(os.environ.get("END_HOUR", "24"))

# First field = IP address or Web address
# Timestamp contains [dd/Mon/yyyy:HH:mm:ss
pattern = re.compile(
    r'^(?P<address>\S+).*?\[\d{1,2}/[A-Za-z]{3}/\d{4}:(?P<hour>\d{2}):'
)

for line in sys.stdin:

    match = pattern.search(line)

    if match:

        address = match.group("address")
        hour = int(match.group("hour"))

        # Example:
        # 5-8 means 05:00 <= time < 08:00
        if START_HOUR <= hour < END_HOUR:
            print(f"{address}\t1")
