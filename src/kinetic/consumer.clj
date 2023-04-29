(ns kinetic.consumer
  ; (:require [])
  (:import [java.util Date]
           [java.time Instant]
           [java.time.temporal ChronoUnit]

           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
           [software.amazon.awssdk.services.kinesis KinesisAsyncClient]
           [software.amazon.kinesis.common ConfigsBuilder
                                           InitialPositionInStreamExtended
                                           KinesisClientUtil
                                           StreamIdentifier]
           [software.amazon.kinesis.coordinator Scheduler]
           [software.amazon.kinesis.processor ShardRecordProcessor
                                              ShardRecordProcessorFactory
                                              SingleStreamTracker]
           [software.amazon.kinesis.retrieval KinesisClientRecord
                                              polling.PollingConfig]
           ))

(defn record-checkpoint []
  )

(defn start-from []
  ;; delete the lease if starting from anything by LATEST or recorcded checkpoint
  ;; e.g. AT_TIMESTAMP, TIME_HORIZON, etc.
  )

(defn make-config []
  )

(defn make-scheduler []
  )

(defn make-kinesis-client [{:keys [app-name
                                   stream-name
                                   region]}]
  )

(defn start-consumer []
  )

(defn stop-consumer []
  )
