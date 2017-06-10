# cassandra-config
Configuration Optimizer for Cassandra

Cassandra Config is a command line tool to help test and optimize important paramaters for read/write performance in your cassandra.yaml file.  The current paramters tested are `concurrent_reads`, `concurrent_writes`, `read_request_timeout_in_ms`, `compaction_throughput_mb_per_sec`, `concurrent_counter_writes`.  A gradient descent algorithm is used to test different combinations of paratmeters and optimzie their values.  The script is currently set up to optimize the `AverageLatency (us)` Read latency but this can be changed easily by modifying the script.

The script can be run like
```
python cassandra-config.py --ycsb_path /scripts/ycsb-0.12.0 \
              --yaml_path /etc/cassandra/cassandra.yaml \
              --ycsb_workload b
```
where ycsb_path is the path to [ycsb](https://github.com/brianfrankcooper/YCSB), yaml_path is the path to your cassandra.yaml configuration file, and ycsb_workload is the workload you would like to test.  A description of workloads can be found [here](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads).
