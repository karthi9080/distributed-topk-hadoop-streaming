#!/usr/bin/env python3

import sys

current_address = None
total = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 2:
        continue

    address, count = parts

    try:
        count = int(count)
    except ValueError:
        continue

    if address == current_address:
        total += count
    else:
        if current_address is not None:
            print(f"{current_address}\t{total}")

        current_address = address
        total = count

if current_address is not None:
    print(f"{current_address}\t{total}")
