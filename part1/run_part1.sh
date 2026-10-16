#!/bin/bash

# ==========================================
# Part 1 - Top K Hosts Per Hour
# ==========================================

HADOOP_HOME="/home/exouser/hadoop-3.4.0"
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar"

echo "======================================"
echo "   Part 1 - Top K Hosts Per Hour"
echo "======================================"
echo

# Choose input file
echo "Choose input file:"
echo "1) access.log"
echo "2) access2.log"
echo
read -p "Enter choice (1 or 2): " CHOICE

case "$CHOICE" in
    1)
        INPUT="/user/exouser/input/access.log"
        FILE_NAME="access.log"
        ;;
    2)
        INPUT="/user/exouser/input/access2.log"
        FILE_NAME="access2.log"
        ;;
    *)
        echo "Error: Please enter 1 or 2."
        exit 1
        ;;
esac

# Ask user for K
read -p "Enter K value: " K

# Validate K
if ! [[ "$K" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: K must be a positive integer."
    exit 1
fi

# Separate output directories for each input file
COUNT_OUTPUT="/user/exouser/part1-counts-${CHOICE}"
TOPK_OUTPUT="/user/exouser/part1-topk-${CHOICE}"

echo
echo "Input file : $FILE_NAME"
echo "K          : $K"
echo

# Record start time
START_TIME=$(date +%s)

# ==========================================
# JOB 1
# ==========================================

echo "--------------------------------------"
echo "Job 1: Counting requests per host/hour"
echo "--------------------------------------"

hdfs dfs -rm -r -f "$COUNT_OUTPUT" >/dev/null 2>&1

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -files mapper_hour.py,reducer_count.py \
    -mapper mapper_hour.py \
    -reducer reducer_count.py \
    -input "$INPUT" \
    -output "$COUNT_OUTPUT"

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: Job 1 failed."
    exit 1
fi

echo
echo "Job 1 completed successfully."
echo

# ==========================================
# JOB 2
# ==========================================

echo "--------------------------------------"
echo "Job 2: Finding Top $K hosts per hour"
echo "--------------------------------------"

hdfs dfs -rm -r -f "$TOPK_OUTPUT" >/dev/null 2>&1

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.reduce.env="TOP_K=$K" \
    -files mapper_topk.py,reducer_topk.py \
    -mapper mapper_topk.py \
    -reducer reducer_topk.py \
    -input "$COUNT_OUTPUT/part-*" \
    -output "$TOPK_OUTPUT"

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: Job 2 failed."
    exit 1
fi

# Record end time
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo
echo "======================================"
echo "   Top $K Hosts Per Hour"
echo "   Input: $FILE_NAME"
echo "======================================"
echo

hdfs dfs -cat "$TOPK_OUTPUT/part-*"

echo
echo "======================================"
echo "Part 1 completed successfully."
echo "Input file     : $FILE_NAME"
echo "K value        : $K"
echo "Execution time : $ELAPSED seconds"
echo "Output         : $TOPK_OUTPUT"
echo "======================================"
