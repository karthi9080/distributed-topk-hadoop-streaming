#!/bin/bash

# ==========================================
# Part 2 - Top K Hosts for a Time Period
# ==========================================

HADOOP_HOME="/home/exouser/hadoop-3.4.0"
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar"

echo "======================================"
echo " Part 2 - Top K Hosts for Time Period"
echo "======================================"
echo

# ------------------------------------------
# Choose input file
# ------------------------------------------

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

# ------------------------------------------
# Get K
# ------------------------------------------

read -p "Enter K value: " K

if ! [[ "$K" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: K must be a positive integer."
    exit 1
fi

# ------------------------------------------
# Get TimePeriod
# Example: 5-8 means 05:00 to 08:00
# ------------------------------------------

read -p "Enter TimePeriod (example 5-8): " TIME_PERIOD

if ! [[ "$TIME_PERIOD" =~ ^([0-9]|1[0-9]|2[0-3])-([1-9]|1[0-9]|2[0-4])$ ]]; then
    echo "Error: Use format START-END, for example 5-8."
    exit 1
fi

START_HOUR="${TIME_PERIOD%-*}"
END_HOUR="${TIME_PERIOD#*-}"

# Convert values such as 05 to decimal safely
START_HOUR=$((10#$START_HOUR))
END_HOUR=$((10#$END_HOUR))

if [ "$START_HOUR" -ge "$END_HOUR" ]; then
    echo "Error: Start hour must be smaller than end hour."
    exit 1
fi

# ------------------------------------------
# HDFS output directories
# ------------------------------------------

COUNT_OUTPUT="/user/exouser/part2-counts-${CHOICE}"
TOPK_OUTPUT="/user/exouser/part2-topk-${CHOICE}"

echo
echo "Input file  : $FILE_NAME"
echo "K           : $K"
printf "Time Period : %02d:00 - %02d:00\n" "$START_HOUR" "$END_HOUR"
echo

# Start timer
START_TIME=$(date +%s)

# ==========================================
# JOB 1
# Filter selected period and count hosts
# ==========================================

echo "--------------------------------------"
echo "Job 1: Counting hosts in time period"
echo "--------------------------------------"

hdfs dfs -rm -r -f "$COUNT_OUTPUT" >/dev/null 2>&1

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.map.env="START_HOUR=$START_HOUR,END_HOUR=$END_HOUR" \
    -files mapper_period.py,reducer_period_count.py \
    -mapper mapper_period.py \
    -reducer reducer_period_count.py \
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
# Find Top K
# ==========================================

echo "--------------------------------------"
echo "Job 2: Finding Top $K hosts"
echo "--------------------------------------"

hdfs dfs -rm -r -f "$TOPK_OUTPUT" >/dev/null 2>&1

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.reduce.env="TOP_K=$K" \
    -files reducer_period_topk.py \
    -mapper /bin/cat \
    -reducer reducer_period_topk.py \
    -input "$COUNT_OUTPUT/part-*" \
    -output "$TOPK_OUTPUT"

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: Job 2 failed."
    exit 1
fi

# Stop timer
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# ==========================================
# Display results
# ==========================================

echo
echo "======================================"
echo "             RESULTS"
echo "======================================"
echo
echo "Input file  : $FILE_NAME"
echo "K           : $K"
printf "Time Period : %02d:00 - %02d:00\n" "$START_HOUR" "$END_HOUR"
echo
echo "Rank    Address    Visits"
echo "--------------------------------------"

hdfs dfs -cat "$TOPK_OUTPUT/part-*"

echo
echo "======================================"
echo "Part 2 completed successfully."
echo "Input file     : $FILE_NAME"
echo "K value        : $K"
printf "Time Period    : %02d:00 - %02d:00\n" "$START_HOUR" "$END_HOUR"
echo "Execution time : $ELAPSED seconds"
echo "Output         : $TOPK_OUTPUT"
echo "======================================"
