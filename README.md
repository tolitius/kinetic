# kinetic

aws kinesis client based on [amazon 2.x api](https://github.com/awslabs/amazon-kinesis-client)

[![<! release](https://img.shields.io/badge/dynamic/json.svg?label=release&url=https%3A%2F%2Fclojars.org%2Fcom.tolitius%2Fkinetic%2Flatest-version.json&query=version&colorB=blue)](https://github.com/tolitius/kinetic/releases)
[![<! clojars>](https://img.shields.io/clojars/v/com.tolitius/kinetic.svg)](https://clojars.org/com.tolitius/kinetic)

- [consumer](#consumer)
  - [initial position](#initial-position)
  - [leases](#leases)
  - [stopping consumer](#stopping-consumer)
  - [multiple streams](#multiple-streams)
  - [credentials](#credentials)
- [license](#license)

```clojure
$ make repl

=> (require '[kinetic.consumer :as k])
```

## consumer

```clojure

=> (def consumer
     (k/start-consumer {:streams [{:name "milky-way.solar.pluto"}]
                        :application-name "hubble"
                        :consume k/echo}))
```

[echo](https://github.com/tolitius/kinetic/blob/2cdde9a3ca55ec3f2b6a2a4aaa1b6924f454d7f8/src/kinetic/consumer.clj#L190-L198) :point_up_2:
here is a sample function that decodes (UTF-8) and echos all the records it consumes from a kinesis stream

in order to do something more useful provide a function as a value of the "`:consume`" key<br/>
that takes a batch of kinesis records in [this format](https://github.com/tolitius/kinetic/blob/2cdde9a3ca55ec3f2b6a2a4aaa1b6924f454d7f8/src/kinetic/consumer.clj#L85-L94).

### initial position

by default a kinesis consumer will start consuming records from a previously recorded "`checkpoint`" that is stored in something that is called a "`lease`".

in order to start consuming from a custom place in a stream use a `:start-from` key:

```clojure
=> (def consumer
     (k/start-consumer {:streams [{:name "milky-way.solar.pluto"
                                   :start-from {:position :trim-horizon}}]
                        :application-name "hubble"
                        :consume k/echo}))
```

you would see this new "initial position" in logs when the shard is initialized:

```java
[ShardRecordProcessor-0000] INFO  kinetic.consumer - initializing shard shardId-000000000000 at sequence {SequenceNumber: TRIM_HORIZON,SubsequenceNumber: 0}
```

possible values are (as [per amazon's api](https://github.com/awslabs/amazon-kinesis-client/blob/0c5042dadf794fe988438436252a5a8fe70b6b0b/amazon-kinesis-client/src/main/java/software/amazon/kinesis/common/InitialPositionInStreamExtended.java#L36-L39)):

* `{:position :trim-horizon}`: start at the earliest sequence number available for your application
* `{:position :latest}`: start at the latest sequence number available on the stream
* `{:position :at-timestamp :timestamp <date>}`: start at a certain timestamp. the timestamp value is `java.util.Date`

for example:

```clojure
=> (import '[java.util Date]
           '[java.time Instant]
           '[java.time.temporal ChronoUnit])

=> (def yesterday                             ;; to start consuming from
     (-> (Instant/now)
         (.minus 1 ChronoUnit/DAYS)
         (Date/from)))

=> (def consumer
     (k/start-consumer {:streams [{:name "milky-way.solar.pluto"
                                   :start-from {:position :at-timestamp
                                                :timestamp yesterday}}]
                        :application-name "hubble"
                        :consume k/echo}))
```

you'll see a different starting sequence number in the logs:

```java
[ShardRecordProcessor-0000] INFO
  kinetic.consumer - initializing shard shardId-000000000000
                     at sequence {SequenceNumber: AT_TIMESTAMP,SubsequenceNumber: 0}
```

and the records will start arriving from yesterday until the latest one on the stream.

### leases

```clojure
=> (k/show-leases consumer)

;; [#object[software.amazon.kinesis.leases.Lease 0x764d54a0
;; "Lease(leaseKey=shardId-000000000000,
;;        leaseOwner=d498fa4c-12b5-45b3-a82f-9025e396f952,
;;        leaseCounter=124,
;;        concurrencyToken=null,
;;        lastCounterIncrementNanos=null,
;;        checkpoint={SequenceNumber: 50638743107472231712790482650536060104711379819100635138,
;;                    SubsequenceNumber: 0},
;;        pendingCheckpoint=null,
;;        pendingCheckpointState=null,
;;        isMarkedForLeaseSteal=false,
;;        ownerSwitchesSinceCheckpoint=0,
;;        parentShardIds=[],
;;        childShardIds=[],
;;        hashKeyRangeForLease=HashKeyRangeForLease(startingHashKey=0,
;;                                                  endingHashKey=340282366920938463463374607431768211455))"]]
```

### stopping consumer

```clojure
=> (k/stop-consumer consumer))
```

### multiple streams

an example of a single consumer that consumes from multiple streams:

```clojure
=> (def consumer
     (k/start-consumer {:streams [{:name "milky-way.solar.pluto"
                                   :start-from {:position :trim-horizon}}   ;; <= ":start-from" is optional
                                  {:name "milky-way.solar.mars"}]
                        :application-name "hubble"
                        :consume k/echo}))
```

for a consumer to consume a single stream, but _track_ it in a multi stream mode:

```clojure
=> (def consumer
     (k/start-consumer {:streams [{:name "milky-way.solar.pluto"
                                   :start-from {:position :trim-horizon}}]
                        :application-name "hubble"
                        :multi-stream? true
                        :consume k/echo}))
```

"`multi-stream?`" is only needed in case a single stream is provided _and_ it needs to be tracked as a multi stream in case other consumers, apps, instances use the same application name.

kinesis stream leases are _tracked_ differently between a non multi and multi stream modes.

in a case of a single stream (the default) mode leases are tracked _with no stream intel_ in the "lease key":

```java
[[software.amazon.kinesis.leases.Lease
                                 Lease(leaseKey=shardId-000000000000, ... )
                                 checkpoint={SequenceNumber: 4964...3442, SubsequenceNumber: 0}]]
```

in the case of a multi stream mode, when either:
* more than one stream is provided in the config<br/>
OR
* the explicit "`multi-stream?`" is set in the config

leases include stream intel in trackers, and use a different underlying object ([MultiStreamLease](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/MultiStreamLease.java)) to store and parse / read leases:

```java
[[software.amazon.kinesis.leases.MultiStreamLease
                                 Lease(leaseKey=123243:milky-way.solar.pluto:1:shardId-000000000000, ... )
                                 checkpoint={SequenceNumber: 4923...3941, SubsequenceNumber: 0}]
[software.amazon.kinesis.leases.MultiStreamLease
                                 Lease(leaseKey=123243:milky-way.solar.mars:1:shardId-000000000000, ... )
                                 checkpoint={SequenceNumber: 4826...3870, SubsequenceNumber: 0}]]

```

these 👆 are entries / records kinesis client stores and reads to/from a single dynambodb lease table that is named after the "`:application-name`" config param.

one thing to keep in mind: 👉 multi stream entries **cannot** coexist with a single stream (no stream intel) entries
because internally AWS client uses _one_ type of a lease object to parse them.

### credentials

AWS credentials will be either picked up via the [regular AWS means](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)<br/>
or can be explicitly provided to kinetic consumer:

```clojure
=> (def consumer
     (k/start-consumer {:streams [{:name "milky-way.solar.pluto"
                                   :start-from {:position :trim-horizon}}]
                        :application-name "hubble"
                        :creds {:access-key-id     "AK..ZZ"          ;; <= via a "creds" map
                                :secret-access-key "z0.........0m"}
                        :consume k/echo}))
```

## license

Copyright © 2023 tolitius

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
