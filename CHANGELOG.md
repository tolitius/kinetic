# 0.1.8

* add support for consuming a stream by its ARN (single stream mode): `{:streams [{:arn "arn:aws:kinesis:..."}]}`
  - enables cross account stream consumption
  - multi stream mode does not support `:arn` yet and will throw if any stream has one
* update amazon-kinesis-client `2.5.2` => `2.7.4`
  - needed for ARN support: ARN only makes it all the way down to `ListShards` / `GetShardIterator` / `GetRecords` starting `2.5.3`
  - this changes some transitive deps: AWS SDK `2.33.0`, protobuf `4.x`, no more AWS SDK v1

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
