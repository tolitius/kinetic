(ns kinetic.consumer
  (:require [kinetic.aws :as aws])
  (:import [java.util UUID]
           [software.amazon.kinesis.common ConfigsBuilder
                                           InitialPositionInStream
                                           InitialPositionInStreamExtended
                                           StreamIdentifier]
           [software.amazon.kinesis.coordinator Scheduler]
           [software.amazon.kinesis.processor ShardRecordProcessor
                                              ShardRecordProcessorFactory
                                              SingleStreamTracker]
           [software.amazon.kinesis.retrieval KinesisClientRecord
                                              polling.PollingConfig]))

(defn make-initial-position [{:keys [position
                                     timestamp] :as at}]
  (case position
    :latest       (-> InitialPositionInStream/LATEST
                      InitialPositionInStreamExtended/newInitialPosition)

    ;; (!) for the below to have an effect, delete the existing lease
    :time-horizon (-> InitialPositionInStream/TRIM_HORIZON
                      InitialPositionInStreamExtended/newInitialPosition)
    :at-timestamp (InitialPositionInStreamExtended/newInitialPositionAtTimestamp timestamp)

    ;; TODO: tap into the software.amazon.kinesis.retrieval.IteratorBuilder
    ; :at_sequence_number
    ; :after_sequence_number
    (throw (RuntimeException. (str "initial position is not supported: " at)))))

(defn single-stream-tracker [{:keys [stream-name
                                     start-from]}]
  (let [stream-identifier (StreamIdentifier/singleStreamInstance stream-name)]
    (if-not start-from
      (SingleStreamTracker. stream-identifier)
      (SingleStreamTracker. stream-identifier
                            (make-initial-position start-from)))))

(defn make-config
  ;; TODO: rewrite via
  [{:keys [stream-name
           start-from
           application-name
           kinesis-client
           dynamo-client
           cloud-watch-client
           record-processor]}]


                   ;; TODO: add multistream if/when needed
  {:config-builder (ConfigsBuilder. (single-stream-tracker stream-name start-from)
                                    application-name
                                    kinesis-client
                                    dynamo-client
                                    cloud-watch-client
                                    (UUID/randomUUID)
                                    record-processor)
   :polling-config (PollingConfig. stream-name kinesis-client)})

(defn make-scheduler [{:keys [config-builder
                              polling-config]}]

  ;; TODO: add more params beyond the ConfigsBuilder defaults
  (Scheduler. (.checkpointConfig config-builder)
              (.coordinatorConfig config-builder)
              (.leaseManagementConfig config-builder)
              (.lifecycleConfig config-builder)
              (.metricsConfig config-builder)
              (.processorConfig config-builder)
              (-> config-builder
                  .retrievalConfig
                  (.retrievalSpecificConfig polling-config))))

(defn record-checkpoint []
  )

(defn shard-record-processor [])


(defn start-consumer []
  )

(defn stop-consumer []
  )
