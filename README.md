# Distributed Top-K Web Log Analytics with Hadoop Streaming

A distributed web-log analytics system built with **Python, Hadoop
Streaming, HDFS, YARN, Bash, Linux, and Jetstream2**. The project
processes large web-server logs across a three-node Hadoop cluster and
identifies the most frequently occurring IP addresses or hostnames
either **per hour** or across a **user-defined time window**.

The implementation uses a two-stage MapReduce design: distributed
counting first computes exact host frequencies, then a bounded
**min-heap** selects the Top-K results with `O(K)` memory after
aggregation.

## Project Highlights

-   Built and configured a **3-node Hadoop 3.4.0 cluster** on
    Jetstream2.
-   Processed **2.5M+ web-server log records** across two datasets.
-   Implemented distributed analytics using **Python-based Hadoop
    Streaming**.
-   Designed two Top-K workflows:
    -   hourly Top-K host analysis;
    -   Top-K host analysis over a configurable time interval.
-   Used **HDFS replication factor 2** across two worker nodes.
-   Used **YARN** for distributed resource management and MapReduce
    execution.
-   Implemented **bounded min-heap Top-K selection** instead of sorting
    every candidate.
-   Added **mapper-side time filtering** to reduce unnecessary shuffle
    traffic.
-   Reduced the tested time-window workload from 1,569,898 input records
    to 106,260 emitted records before aggregation.
-   Configured private cluster networking, passwordless SSH, and UFW
    firewall restrictions.
-   Validated distributed results using local pipelines, independent
    computation, Hadoop counters, and cross-workflow comparisons.
-   Measured end-to-end execution times between **67 and 85 seconds** on
    small 3 GB cloud instances.

------------------------------------------------------------------------

## Architecture

``` text
                         Jetstream2
                             |
                    Three-Node Hadoop Cluster
                             |
          +------------------+------------------+
          |                  |                  |
     node-master        node-worker1       node-worker2
          |                  |                  |
       NameNode            DataNode           DataNode
  SecondaryNameNode       NodeManager        NodeManager
   ResourceManager
          |
          +---------------- HDFS ----------------+
                             |
                     Web Server Logs
                             |
                  Python Hadoop Streaming
                             |
                  +----------+----------+
                  |                     |
            MapReduce Job 1       MapReduce Job 2
            Parse + Count         Top-K Selection
                  |                     |
                  +----------+----------+
                             |
                       Ranked Hosts
```

The master coordinates HDFS metadata and YARN scheduling. The two
workers provide distributed storage and execute MapReduce containers.

------------------------------------------------------------------------

## Technology Stack

  Area                     Technology
  ------------------------ ------------------------------------------------------
  Cloud Infrastructure     Jetstream2, Exosphere
  Distributed Processing   Hadoop 3.4.0, MapReduce, Hadoop Streaming
  Distributed Storage      HDFS
  Resource Management      YARN
  Programming              Python 3, Bash
  Operating System         Ubuntu Linux
  Networking               Private subnet, SSH
  Security                 UFW firewall
  Algorithms               Aggregation, bounded min-heap Top-K
  Validation               Unix pipelines, Python verification, Hadoop counters

------------------------------------------------------------------------

## Repository Structure

``` text
distributed-topk-hadoop-streaming/
├── README.md
├── part1/
│   ├── run_part1.sh
│   ├── mapper_hour.py
│   ├── reducer_count.py
│   ├── mapper_topk.py
│   └── reducer_topk.py
├── part2/
│   ├── run_part2.sh
│   ├── mapper_period.py
│   ├── reducer_period_count.py
│   └── reducer_period_topk.py
└── results/
    ├── part1_access.log_results.txt
    ├── part1_access2.log_results.txt
    ├── part2_access.log_results.txt
    └── part2_access2.log_results.txt
```

The implementation is organized into `part1/` and `part2/` for
compatibility with the original code structure. Conceptually, these
directories represent the **Hourly Top-K Pipeline** and the
**Time-Window Top-K Pipeline**.

------------------------------------------------------------------------

## Dataset

The system was tested on two web-server access-log datasets.

  ---------------------------------------------------------------------------
  Dataset                      Records         Approx. Size Characteristics
  --------------- -------------------- -------------------- -----------------
  `access.log`               1,569,898               160 MB Multiple
                                                            dates/hours;
                                                            hostnames and IP
                                                            addresses

  `access2.log`              1,000,000               231 MB IP addresses; all
                                                            records occur in
                                                            hour 12
  ---------------------------------------------------------------------------

The raw datasets are intentionally **not included in this repository**.

Both files are expected to be stored in HDFS under:

``` text
/user/exouser/input/
```

The log parser extracts:

1.  the first whitespace-delimited token as the visitor IP/hostname;
2.  the hour from the bracketed timestamp.

Input is decoded with replacement behavior for invalid UTF-8 bytes so
malformed byte sequences do not terminate mapper tasks.

------------------------------------------------------------------------

## Analytics Pipeline 1: Hourly Top-K Hosts

The hourly workflow identifies the **K most frequently occurring hosts
independently for each hour** represented in the log.

### Job 1 --- Distributed Counting

`mapper_hour.py` parses each record and emits a composite key:

``` text
hour|host    1
```

For example:

``` text
05|example.host.com    1
```

Hadoop groups identical `(hour, host)` keys. `reducer_count.py` then
sums the values and produces:

``` text
hour    host    count
```

This stage calculates the exact number of requests generated by every
host during every hour.

### Job 2 --- Top-K Selection

`mapper_topk.py` reorganizes the aggregated records so all host counts
belonging to the same hour are grouped together.

`reducer_topk.py` maintains a **min-heap containing at most K
`(count, host)` candidates** for each hour. When a stronger candidate
arrives, it replaces the smallest candidate currently in the heap.

The final output format is:

``` text
HOUR    RANK    HOST    COUNT
```

Example:

``` text
05    1    news.dfrc.nasa.gov       791
05    2    www-relay.pa-x.dec.com   200
05    3    130.134.64.17            144
```

For `access.log` with `K=3`, the pipeline processed **1,569,898
records**, generated **123,591 unique `(hour, host)` counts**, and
returned **72 ranked results across 24 hours**.

------------------------------------------------------------------------

## Analytics Pipeline 2: Time-Window Top-K Hosts

The time-window workflow identifies the Top-K hosts across a
user-selected interval.

A period such as:

``` text
5-8
```

uses start-inclusive/end-exclusive semantics:

``` text
05:00 <= timestamp < 08:00
```

Therefore, hours 05, 06, and 07 are included.

### Job 1 --- Filter and Count

`mapper_period.py` applies the time filter before emitting data:

``` text
START_HOUR <= hour < END_HOUR
```

Matching records are emitted as:

``` text
host    1
```

`reducer_period_count.py` then calculates the exact request count for
each host across the complete selected period.

Filtering in the mapper is important because irrelevant records never
enter the shuffle phase.

For `access.log` and the `5-8` interval:

``` text
Original records:      1,569,898
Records after filter:    106,260
Percentage retained:        6.8%
```

This means more than 93% of the records were removed before aggregation
for this workload.

### Job 2 --- Global Top-K Selection

The second job uses an identity mapper (`/bin/cat`) and
`reducer_period_topk.py`.

The reducer maintains one global bounded min-heap and returns:

``` text
RANK    HOST    COUNT
```

For `access.log`, `K=3`, and `5-8`, the result was:

    Rank Host                         Requests
  ------ -------------------------- ----------
       1 `edams.ksc.nasa.gov`              991
       2 `www-relay.pa-x.dec.com`          813
       3 `news.dfrc.nasa.gov`              791

------------------------------------------------------------------------

## Algorithm Design

### Why Two MapReduce Jobs?

Top-K ranking cannot be performed correctly until the **complete count
for each host** is known.

The workflow therefore separates the problem into:

``` text
Raw records
    |
    v
Exact distributed aggregation
    |
    v
Host counts
    |
    v
Top-K selection
    |
    v
Ranked output
```

Trying to select Top-K hosts directly from individual log records could
discard a host before all of its requests have been aggregated.

### Bounded Min-Heap

After aggregation, the Top-K reducers keep only K candidates in memory.

For `M` aggregated candidates:

``` text
Time:   O(M log K)
Memory: O(K)
```

This is more memory-efficient than storing and sorting every host count.

The heap uses `(count, host)` tuples for deterministic behavior when
visit counts are equal. Final selected hosts are displayed by descending
count and then ascending hostname.

### Mapper-Side Filtering

The time-window workflow filters records as early as possible.

This reduces:

-   mapper output;
-   shuffle traffic;
-   reducer input;
-   intermediate storage.

In the measured `5-8` experiment, Job 1 shuffle traffic was
approximately **2.4 MB**, compared with approximately **41 MB** for the
unfiltered hourly workflow.

------------------------------------------------------------------------

## Cloud Infrastructure

The project was deployed on three small Jetstream2 virtual machines.

  -----------------------------------------------------------------------
  Node                    Hadoop Role             Resources
  ----------------------- ----------------------- -----------------------
  `node-master`           NameNode,               1 vCPU, 3 GB RAM, 40 GB
                          SecondaryNameNode,      
                          ResourceManager         

  `node-worker1`          DataNode, NodeManager   1 vCPU, 3 GB RAM, 40 GB

  `node-worker2`          DataNode, NodeManager   1 vCPU, 3 GB RAM, 40 GB
  -----------------------------------------------------------------------

Hadoop inter-node communication uses a **private cloud subnet**. Exact
historical IP addresses are intentionally omitted from this public
repository.

### HDFS

The cluster uses:

``` xml
<property>
  <name>dfs.replication</name>
  <value>2</value>
</property>
```

With two DataNodes, this stores each HDFS block on both workers.

### YARN

YARN resource limits were tuned for the small 3 GB instances:

``` text
NodeManager memory:             2048 MB
Maximum allocation:            2048 MB
Minimum allocation:             512 MB
Map container:                  512 MB
Reduce container:               512 MB
ApplicationMaster:              512 MB
```

Cluster health can be checked with:

``` bash
hdfs dfsadmin -report
yarn node -list
```

A healthy deployment should report two live DataNodes and two running
NodeManagers.

------------------------------------------------------------------------

## Networking and Security

The cluster was configured with a deny-by-default firewall policy.

The intended model is:

``` text
Trusted administrator IP  ---> SSH (22/tcp)
Private cluster subnet    ---> Hadoop node-to-node traffic
All other inbound traffic ---> Denied
```

Example UFW configuration:

``` bash
sudo ufw allow from <TRUSTED_PUBLIC_IP> to any port 22 proto tcp
sudo ufw allow from <PRIVATE_CLUSTER_SUBNET>
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw enable
```

Hadoop web interfaces should **not** be exposed directly to the public
Internet.

When browser access is required, use SSH port forwarding:

``` bash
ssh \
  -L 8088:node-master:8088 \
  -L 9870:node-master:9870 \
  <USER>@<MASTER_PUBLIC_IP>
```

Then access:

``` text
http://localhost:8088   # YARN ResourceManager
http://localhost:9870   # HDFS NameNode
```

Passwordless SSH between the master and workers is used for Hadoop
service management.

------------------------------------------------------------------------

## Hadoop Configuration

### `core-site.xml`

``` xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://node-master:9000</value>
  </property>
</configuration>
```

### `hdfs-site.xml`

``` xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
</configuration>
```

### `yarn-site.xml`

``` xml
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>node-master</value>
  </property>

  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>

  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>2048</value>
  </property>
</configuration>
```

### Worker Nodes

``` text
node-worker1
node-worker2
```

The complete Hadoop environment may require additional MapReduce
classpath and container environment settings depending on the
installation location.

------------------------------------------------------------------------

## Loading Data into HDFS

Create the HDFS input directory:

``` bash
hdfs dfs -mkdir -p /user/exouser/input
```

Upload the logs:

``` bash
hdfs dfs -put access.log /user/exouser/input/
hdfs dfs -put access2.log /user/exouser/input/
```

Verify:

``` bash
hdfs dfs -ls -h /user/exouser/input
```

------------------------------------------------------------------------

## Running the Project

The Python and shell scripts must be executable:

``` bash
chmod +x part1/*.py part1/*.sh
chmod +x part2/*.py part2/*.sh
```

The wrapper scripts expect the relevant Python files to be available in
their working directory because Hadoop Streaming distributes them using
`-files`.

### Hourly Top-K

``` bash
cd part1
./run_part1.sh
```

The script asks for:

``` text
Input dataset
K value
```

Example:

``` text
Dataset: access.log
K: 3
```

Output:

``` text
HOUR    RANK    HOST    COUNT
```

### Time-Window Top-K

``` bash
cd part2
./run_part2.sh
```

The script asks for:

``` text
Input dataset
K value
TimePeriod
```

Example:

``` text
Dataset: access.log
K: 3
TimePeriod: 5-8
```

Output:

``` text
RANK    HOST    COUNT
```

Intervals that wrap past midnight, such as `22-2`, are rejected by the
current implementation.

------------------------------------------------------------------------

## Experimental Results

The final implementation was evaluated on both datasets and several
parameter combinations.

  Workflow            Dataset           K Period        Execution Time
  ------------------- --------------- --- ----------- ----------------
  Hourly Top-K        `access.log`      3 All hours               84 s
  Hourly Top-K        `access2.log`     3 All hours               85 s
  Time-Window Top-K   `access.log`      3 5-8                     77 s
  Time-Window Top-K   `access2.log`     4 2-3                     67 s
  Time-Window Top-K   `access2.log`     3 12-13                   85 s

These are single-run wall-clock measurements on small three-node cloud
infrastructure and should be interpreted as experimental observations
rather than formal benchmarks.

### Selected Results

**`access.log`, Time Window 5-8, K=3**

``` text
1    edams.ksc.nasa.gov        991
2    www-relay.pa-x.dec.com    813
3    news.dfrc.nasa.gov        791
```

**`access.log`, Hour 05, K=3**

``` text
1    news.dfrc.nasa.gov        791
2    www-relay.pa-x.dec.com    200
3    130.134.64.17             144
```

**`access2.log`, Hour 12, K=3**

``` text
1    94.136.152.94             2
2    96.122.55.104             2
3    99.216.207.36             2
```

Full saved outputs are available in `results/`.

------------------------------------------------------------------------

## Correctness Validation

The implementation was validated at multiple levels.

### Local Pipeline Testing

Individual mapper/reducer chains can be tested without Hadoop:

``` bash
head -10000 access.log \
  | ./mapper_hour.py \
  | sort \
  | ./reducer_count.py \
  | head
```

This makes parser and aggregation problems easier to isolate before
distributed execution.

### Record-Count Validation

For `access.log`, the hourly mapper processed all **1,569,898 input
records**, confirming that valid log records were not silently lost
during parsing.

### Independent Computation

Final Hadoop results were compared with an independent single-machine
computation.

### Cross-Workflow Validation

The time-window results were also reconstructed from hourly counts.

For example:

``` text
www-relay.pa-x.dec.com
Hour 05: 200
Hour 06: 258
Hour 07: 355
-------------------
Total:   813
```

This exactly matches the `5-8` time-window result.

`access2.log` provides another useful validation case because every
record occurs during hour 12. Therefore:

``` text
Hourly Top-K at hour 12
        ==
Time-Window Top-K for 12-13
```

The two workflows produced matching results.

### Edge Cases

The implementation also handles:

-   valid periods containing no records;
-   K values larger than the number of available hosts;
-   deterministic ordering for tied counts;
-   invalid time ranges;
-   malformed input bytes.

For `access2.log`, the `2-3` interval correctly completed with an empty
result instead of failing.

------------------------------------------------------------------------

## Performance Observations

The final runs completed in approximately **67--85 seconds**.

The relatively narrow runtime range indicates that, on this small
cluster, total execution time is strongly influenced by:

-   Hadoop job startup;
-   YARN container startup;
-   input scanning;
-   HDFS I/O;
-   shuffle setup.

The final output itself is very small, so output size has little impact
on total runtime.

Mapper-side filtering in the time-window pipeline substantially reduces
intermediate data, but fixed distributed-system overhead remains.

------------------------------------------------------------------------

## Challenges and Troubleshooting

Building the project exposed several practical Hadoop deployment issues.

### Hadoop Streaming JAR Not Found

**Symptom**

``` text
ClassNotFoundException: -files
```

**Cause**

The Hadoop Streaming JAR variable was not correctly resolved.

**Fix**

``` bash
export STREAMING_JAR=$(find "$HADOOP_HOME" -name 'hadoop-streaming-3.4.0.jar' | head -1)
```

### `MRAppMaster` Class Not Found

**Symptom**

``` text
Could not find or load main class ... MRAppMaster
```

**Cause**

MapReduce libraries were not correctly available inside YARN containers.

**Resolution**

Configured the MapReduce application classpath and passed
`HADOOP_MAPRED_HOME` into the ApplicationMaster, mapper, and reducer
environments.

### `YarnChild` Class Not Found

The MapReduce classpath originally depended on an environment variable
that was not expanded as expected inside the container.

The fix was to use explicit Hadoop MapReduce library paths in
`mapred-site.xml`.

### XML Configuration Failure

A leading whitespace character before:

``` xml
<?xml version="1.0"?>
```

caused the NodeManager to reject the XML configuration.

The file was corrected, synchronized to the worker nodes, and the
affected services were restarted.

### Resource Pressure

Each VM had only 3 GB RAM. Desktop/background services consumed
resources required by Hadoop.

Unnecessary graphical and background services were stopped, and the
machines were configured for a lighter multi-user environment.

### Local `BrokenPipeError`

Using `head` at the end of a local Unix pipeline can close the pipe
before upstream Python processes finish writing.

This is harmless for quick inspection; full validation can instead
redirect output to a file.

------------------------------------------------------------------------

## Design Trade-Offs

### Single Final Reducer

The final Top-K stage uses one reducer so that one process sees all
candidates required for an exact ranking.

This simplifies correctness but introduces a scalability limitation.

For significantly larger datasets, the architecture could be extended
to:

1.  calculate local Top-K candidates across multiple reducers;
2.  merge only those candidates in a final global stage.

### No Combiner

The counting stages could potentially use combiners to reduce mapper
output before shuffle.

The current implementation keeps the pipeline simple and explicit, but
adding combiners would be a natural optimization for larger workloads.

### Time Zones

The parser uses the hour written directly in each log record and does
not normalize timestamps across UTC offsets.

A production implementation could parse timestamps into a common
timezone before grouping.

------------------------------------------------------------------------

## Future Improvements

Potential extensions include:

-   add combiners to reduce network shuffle;
-   use multiple reducers for first-stage counting;
-   implement hierarchical/local Top-K aggregation;
-   support time windows that cross midnight;
-   normalize timestamps to UTC;
-   support configurable HDFS input/output paths;
-   add automated unit and integration tests;
-   containerize the local development environment;
-   expose job configuration through command-line arguments rather than
    interactive prompts;
-   benchmark scaling with larger clusters and datasets;
-   export structured JSON/CSV results for downstream dashboards or
    APIs.

------------------------------------------------------------------------

## Key Takeaways

This project demonstrates an end-to-end distributed data-processing
workflow rather than only a standalone MapReduce algorithm.

The implementation combines:

``` text
Cloud VM provisioning
        ↓
Linux networking and security
        ↓
Hadoop / HDFS / YARN configuration
        ↓
Distributed data ingestion
        ↓
Python log parsing
        ↓
MapReduce aggregation
        ↓
Heap-based Top-K selection
        ↓
Correctness validation
        ↓
Performance analysis
```

The project highlights an important distributed-systems principle:
efficient analytics requires considering not only algorithmic
complexity, but also **data movement, shuffle volume, storage
replication, container startup overhead, memory constraints, network
security, and failure/debugging behavior**.

------------------------------------------------------------------------

## Author

**Karthikeyan Sundara Anandam**

Data Science \| Distributed Systems \| Cloud Computing \| Data
Engineering \| AI/ML
