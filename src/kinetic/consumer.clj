(ns kinetic.consumer
  (:require [clojure.tools.logging :as log]
            [kinetic.tools :as t]
            [kinetic.aws :as aws])
  (:import [java.util UUID]
           [java.nio.charset StandardCharsets]
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

(defn make-utf8-decoder []
  (let [decoder (-> (StandardCharsets/UTF_8)
                    .newDecoder)]
    (fn [{:keys [data]}]
      (-> data
          (.decode decoder)
          .toString))))

(defn shard-record-processor [consume
                              checkpoint-every-ms]
  (let [shard-id (atom "was not initialized with the shard id")]
    (reify ShardRecordProcessor

      (initialize
        [this init-with]
        "invoked by the Amazon Kinesis Client Library before data records are delivered to the ShardRecordProcessor instance
         (via 'processRecords').

         args: 'init-with' provides information related to initialization"
        (let [sid (.shardId init-with)
              sequence-number (.extendedSequenceNumber init-with)]
          (reset! shard-id sid)
          (log/infof "initializing shard %s at sequence %s"
                     @shard-id
                     sequence-number)))

      (processRecords
        [this records]
        "process data records.
         the Amazon Kinesis Client Library will invoke this method to deliver data records to the application.
         upon fail over, the new instance will get records with sequence number > checkpoint position for each partition key.

         args: 'records' provides the records to be processed as well as information and capabilities related to them (eg checkpointing)."
        (consume records)
        ;; TODO: (when checkpoint-every-ms ...)
        (record-checkpoint (.checkpointer records)))

      (leaseLost
        [this lost-lease]
        "called when the lease that tied to this record processor has been lost.
         once the lease has been lost the record processor can no longer checkpoint.

         args: 'lost-lease' access to functions and data related to the loss of the lease.
               currently this has no functionality.")

      (shardEnded
        [this completed-shard]
        "called when the shard that this record process is handling has been completed.
         once a shard has been completed no further records will ever arrive on that shard.

         when this is called the record processor must call 'record-checkpoint'
         otherwise an exception will be thrown and the all child shards of this shard will not make progress.

         args: 'completed-shard' provides access to a checkpointer method for completing processing of the shard."
        (log/infof "%s shard is completed. recording a checkpoint.."
                   @shard-id)
        (-> completed-shard .checkpointer .checkpoint))

      (shutdownRequested
        [this shutdown-request]
        "called when the Scheduler has been requested to shutdown.
         this is called while the record processor still holds the lease so checkpointing is possible.
         once this method has completed the lease for the record processor is released,
              and 'leaseLost' will be called at a later time.

         args: 'shutdown-request' provides access to a checkpointer (RecordProcessorCheckpointer)
               allowing a record processor to checkpoint before the shutdown is completed."

        (log/infof "scheduler for the shard %s is shutting down. recording a checkpoint.."
                   @shard-id)
        (-> shutdown-request .checkpointer .checkpoint)))))


(defn start-consumer []
  )

(defn stop-consumer []
  )
