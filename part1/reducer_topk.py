#!/usr/bin/env python3

import sys
import os
import heapq

# K is supplied by the user when the Hadoop job is run
try:
    K = int(os.environ.get("TOP_K", "5"))
except ValueError:
    K = 5

current_hour = None
heap = []


def output_top_k(hour, heap):
    # Sort highest count first.
    results = sorted(heap, key=lambda x: (-x[0], x[1]))

    rank = 1

    for count, address in results:
        print(f"{hour}\t{rank}\t{address}\t{count}")
        rank += 1


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

    # We reached a new hour
    if current_hour is not None and hour != current_hour:
        output_top_k(current_hour, heap)
        heap = []

    current_hour = hour

    item = (count, address)

    if len(heap) < K:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


# Output final hour
if current_hour is not None:
    output_top_k(current_hour, heap)
