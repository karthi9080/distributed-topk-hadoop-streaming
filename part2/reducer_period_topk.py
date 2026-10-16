#!/usr/bin/env python3

import sys
import os
import heapq

try:
    K = int(os.environ.get("TOP_K", "5"))
except ValueError:
    K = 5

heap = []

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

    item = (count, address)

    if len(heap) < K:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)

results = sorted(heap, key=lambda x: (-x[0], x[1]))

rank = 1

for count, address in results:
    print(f"{rank}\t{address}\t{count}")
    rank += 1
