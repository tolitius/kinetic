# 0.1.7

* **breaking**: CloudWatch metrics are now disabled by default (`MetricsLevel/NONE`)
  - previously the KCL default (`DETAILED`) was used and metrics were published to CloudWatch
  - to keep the previous behavior, pass `:metrics-level "DETAILED"` to `start-consumer`
* add `:metrics-level` consumer config
  - accepts `"NONE"` / `"SUMMARY"` / `"DETAILED"` as a string (case insensitive), a keyword (e.g. `:summary`), or a `MetricsLevel`
* add `make test`

# 0.1.6

* enable checkpointing on
  - shard end
  - shutdown request

# 0.1.5

* add support to:
  - consumer multiple streams with a single consumer
  - consumer multiple streams with multiple consumers

# 0.1.2

* default "`:delete-leases?`" consumer config to false
* update amazon-kinesis-client to `2.5.1`
* force deleting leases on `TRIM_HORIZON` and `AT_TIMESTAMP` initial positions
