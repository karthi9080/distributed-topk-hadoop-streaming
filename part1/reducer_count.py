#!/usr/bin/env python3

import sys

current_key = None
total = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 2:
        continue

    key, count = parts

    try:
        count = int(count)
    except ValueError:
        continue

    if key == current_key:
        total += count
    else:
        if current_key is not None:
            hour, address = current_key.split("|", 1)
            print(f"{hour}\t{address}\t{total}")

        current_key = key
        total = count

if current_key is not None:
    hour, address = current_key.split("|", 1)
    print(f"{hour}\t{address}\t{total}")
