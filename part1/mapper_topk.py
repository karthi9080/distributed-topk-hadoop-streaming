#!/usr/bin/env python3

import sys

# Input from Job 1:
# hour    address    count
#
# Example:
# 00      128.159.129.170    18

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 3:
        continue

    hour, address, count = parts

    try:
        count = int(count)
    except ValueError:
        continue

    # Keep hour as the key.
    # Send address and count to reducer.
    print(f"{hour}\t{address}\t{count}")
