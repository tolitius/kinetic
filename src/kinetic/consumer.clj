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
    :trim-horizon (-> InitialPositionInStream/TRIM_HORIZON
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
           record-processor-factory] :as opts}]


                   ;; TODO: add multistream if/when needed
  {:config-builder (ConfigsBuilder. (single-stream-tracker opts)
                                    application-name
                                    kinesis-client
                                    dynamo-client
                                    cloud-watch-client
                                    (-> (UUID/randomUUID) str)
                                    record-processor-factory)
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

(defn record-checkpoint [checkpointer]
  (let [last-sequence (.lastCheckpointValue checkpointer)
        current-sequence (.largestPermittedCheckpointValue checkpointer)]
    (log/infof "recording a checkpoint at sequence: %s (last recorded sequence was %s)"
               current-sequence last-sequence)
    (.checkpoint checkpointer)))

(defn ->record [^KinesisClientRecord record]
  {:sequence-number                (.sequenceNumber record)
   :approximate-arrival-timestamp  (.approximateArrivalTimestamp record)
   :data                           (.data record)
   :partitionKey                   (.partitionKey record)
   :encryption-type                (.encryptionType record)
   :sub-sequence-number            (.subSequenceNumber record)
   :explicit-hash-key              (.explicitHashKey record)
   :aggregated                     (.aggregated record)
   :schema                         (.schema record)})

(defn make-utf8-decoder []
  (let [decoder (-> (StandardCharsets/UTF_8)
                    .newDecoder)]
    (fn [{:keys [data]}]
      (->> data
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
        [this batch]
        "process data records.
         the Amazon Kinesis Client Library will invoke this method to deliver data records to the application.
         upon fail over, the new instance will get records with sequence number > checkpoint position for each partition key.

         args: 'records' provides the records to be processed as well as information and capabilities related to them (eg checkpointing)."
        (consume (->> batch .records (mapv ->record)))
        ;; TODO: (when checkpoint-every-ms ...)
        (record-checkpoint (.checkpointer batch)))

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
        ;; (-> completed-shard .checkpointer .checkpoint)
        )

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
        ;; (-> shutdown-request .checkpointer .checkpoint)
        ))))

(defn shard-record-processor-factory [consume
                                      checkpoint-every-ms]
  (reify ShardRecordProcessorFactory
    (shardRecordProcessor [this]
      (shard-record-processor consume checkpoint-every-ms))))

(defn validate-consumer-args [{:keys [stream-name
                                      application-name
                                      consume] :as args}]
  (when-not (or stream-name
                application-name
                consume)
    (throw (RuntimeException.
             (str "kinesis consumer needs at least these 3 pieces to start: stream name, application name and a consume function. "
                  args))))
  (when-not (fn? consume)
    (throw (RuntimeException.
             (str "'consume' needs to be a _function_ that is capable of taking a batch of kinesis records: "
                  args)))))

(defn echo
  "echo consume function.
   expects records to be UTF-8 decoded"
  [records]
  (let [decode (make-utf8-decoder)]
    (doseq [record records]
      (log/infof "got a record: %s, data: %s\n"
                 record
                 (decode record)))))

(defn show-leases [consumer]
  (-> consumer :scheduler .leaseRefresher .listLeases))

(defn delete-all-leases [{:keys [scheduler]}]
  (log/infof "deleting leases for \"%s\" application"
             (.applicationName scheduler))
  (-> scheduler .leaseRefresher .deleteAll))

(defn prep-initial-position
  "if there is an existing lease, by default, kinesis client will ignore an initial position that is provided.
   this deletes existing leases for this app (a.k.a. consumer group) in case there is a position provided"
  [scheduler
   start-from
   delete-leases?]
  (when-let [{:keys [position]} start-from]
    (when (or delete-leases?
              ;; in order for :trim-horizon and :at-timestamp initial position to take effect
              ;; leases need to be deleted
              (#{:trim-horizon
                 :at-timestamp} position))
      (log/infof "about to delete leases, initial position \"%s\""
                 start-from)
      (delete-all-leases {:scheduler scheduler}))))

(defn start-consumer [{:keys [stream-name
                              application-name
                              creds               ;; in a form of {:access-key-id "foo" :secret-access-key "bar"}
                              region
                              consume
                              start-from
                              delete-leases?
                              checkpoint-every-ms
                              config]             ;; TODO: use this placeholder to exand on the ConfigsBuilder defaults
                       :or {delete-leases? false}
                       :as opts}]
  (validate-consumer-args opts)
  (let [config-args (merge opts
                           {:kinesis-client (aws/make-kinesis-client opts)
                            :dynamo-client (aws/make-dynamo-db-client opts)
                            :cloud-watch-client (aws/make-cloud-watch-client opts)
                            :record-processor-factory (shard-record-processor-factory consume
                                                                                      checkpoint-every-ms)})
        scheduler (-> (make-config config-args)
                      make-scheduler)]
    (prep-initial-position scheduler
                           start-from
                           delete-leases?)
    {:scheduler scheduler
     :executor (t/start-within-thread scheduler)}))

(defn stop-consumer [{:keys [scheduler
                             executor]}]
  ;; NOTE: current "expected" :( error on graceful shutdown: https://github.com/awslabs/amazon-kinesis-client/issues/914
  ;; (.startGracefulShutdown scheduler)
  (try (.shutdown scheduler)
       (catch Exception e
         (log/warn "could not cleanly shutdown the scheduler for"
                   (.applicationName scheduler)
                   "application due to" e)))
  (try (.shutdown executor)
       (catch Exception e
         (log/warn "could not cleanly shutdown the executor for"
                   (.applicationName scheduler)
                   "application due to" e))))
