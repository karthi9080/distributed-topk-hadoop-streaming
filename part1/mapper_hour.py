#!/usr/bin/env python3

import sys
import io
import re

sys.stdin = io.TextIOWrapper(
    sys.stdin.buffer,
    encoding="utf-8",
    errors="replace"
)

pattern = re.compile(
    r'^(?P<address>\S+).*?\[\d{1,2}/[A-Za-z]{3}/\d{4}:(?P<hour>\d{2}):'
)

for line in sys.stdin:
    match = pattern.search(line)

    if match:
        hour = match.group("hour")
        address = match.group("address")

        # Composite Hadoop key = hour + address
        print(f"{hour}|{address}\t1")
