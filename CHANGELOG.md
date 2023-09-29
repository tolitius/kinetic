# 0.1.5

* add support to:
  - consumer multiple streams with a single consumer
  - consumer multiple streams with multiple consumers

# 0.1.2

* default "`:delete-leases?`" consumer config to false
* update amazon-kinesis-client to `2.5.1`
* force deleting leases on `TRIM_HORIZON` and `AT_TIMESTAMP` initial positions
