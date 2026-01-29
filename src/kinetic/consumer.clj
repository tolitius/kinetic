(ns kinetic.consumer
  (:require [clojure.tools.logging :as log]
            [kinetic.tools :as t]
            [kinetic.tracker :as tracker]
            [kinetic.aws :as aws])
  (:import [java.util UUID]
           [java.time Duration]
           [java.nio.charset StandardCharsets]
           [software.amazon.kinesis.common ConfigsBuilder
                                           InitialPositionInStream
                                           InitialPositionInStreamExtended
                                           StreamIdentifier
                                           StreamConfig]
           [software.amazon.kinesis.coordinator Scheduler]
           [software.amazon.kinesis.leases Lease]
           [software.amazon.kinesis.processor ShardRecordProcessor
                                              ShardRecordProcessorFactory
                                              FormerStreamsLeasesDeletionStrategy
                                              FormerStreamsLeasesDeletionStrategy$AutoDetectionAndDeferredDeletionStrategy
                                              StreamTracker
                                              SingleStreamTracker]
           [software.amazon.kinesis.retrieval KinesisClientRecord
                                              polling.PollingConfig]))

(defn make-config
  [{:keys [streams
           multi-stream?        ;; in case we need a multi stream tracking
           aws-account-number
           application-name
           kinesis-client
           dynamo-client
           cloud-watch-client
           record-processor-factory] :as opts}]
  (let [multi? (or multi-stream?
                   (> (count streams) 1))]
    {:config-builder (ConfigsBuilder. (tracker/make-stream-tracker opts)
                                      application-name
                                      kinesis-client
                                      dynamo-client
                                      cloud-watch-client
                                      (-> (UUID/randomUUID) str)
                                      record-processor-factory)
     :polling-config (if multi?
                       (PollingConfig. kinesis-client)
                       (PollingConfig. (-> streams first :name)
                                       kinesis-client))}))

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

        (log/infof "%s shard is completed. recording a checkpoint.." @shard-id)
        (try
          (-> completed-shard .checkpointer .checkpoint)
          (catch Exception e
            (log/errorf e "failed to checkpoint at shard end for %s" @shard-id)
            (throw e))))

      (shutdownRequested
        [this shutdown-request]
        "called when the Scheduler has been requested to shutdown.
         this is called while the record processor still holds the lease so checkpointing is possible.
         once this method has completed the lease for the record processor is released,
              and 'leaseLost' will be called at a later time.

         args: 'shutdown-request' provides access to a checkpointer (RecordProcessorCheckpointer)
               allowing a record processor to checkpoint before the shutdown is completed."

        (log/infof "scheduler for the shard %s is shutting down. recording a checkpoint.." @shard-id)
        (try
          (-> shutdown-request .checkpointer .checkpoint)
          (catch Exception e
            (log/errorf e "failed to checkpoint at shutdown for %s" @shard-id)))))))

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

(defn make-lease [lease-key]
  (doto
    (Lease.)
    (.leaseKey lease-key)))

(defn find-leases-for-stream [scheduler
                              stream]
  (let [stream-identifier (if (tracker/multi-stream? scheduler)
                            (tracker/multi-stream-identifier stream)
                            (tracker/single-stream-identifier stream))]
    (-> scheduler
        .leaseRefresher
        (.listLeasesForStream stream-identifier))))

(defn delete-lease [scheduler
                    lease]
  (log/infof "deleting lease: %s for %s application"
             (.leaseKey lease)
             (.applicationName scheduler))
  (-> scheduler
      .leaseRefresher
      (.deleteLease lease)))

(defn delete-leases-for-stream [scheduler
                                stream]
  (log/infof "deleting leases for stream \"%s\", application \"%s\""
             stream
             (.applicationName scheduler))
  (->> stream
       (find-leases-for-stream scheduler)
       (mapv (partial delete-lease scheduler))))

(defn delete-all-leases [scheduler]
  (log/infof "deleting leases for \"%s\" application"
             (.applicationName scheduler))
  (-> scheduler .leaseRefresher .deleteAll))

(defn prep-initial-position
  "if there is an existing lease, by default, kinesis client will ignore an initial position that is provided.
   this deletes existing leases for this app (a.k.a. consumer group) in case there is a position provided"
  [scheduler
   aws-account-number
   {:keys [start-from
           delete-leases?] :as stream}]
  (when-let [{:keys [position]} start-from]
    (when (or delete-leases?
              ;; in order for :trim-horizon and :at-timestamp initial position to take effect
              ;; leases need to be deleted
              (#{:trim-horizon
                 :at-timestamp} position))
      (log/infof "about to delete a leases for %s"
                 stream)
      (delete-leases-for-stream scheduler
                                (assoc stream :aws-account-number
                                              aws-account-number)))))

(defn start-consumer [{:keys [application-name
                              aws-account-number
                              streams
                              creds               ;; in a form of {:access-key-id "foo" :secret-access-key "bar"}
                              region
                              consume
                              checkpoint-every-ms
                              config]             ;; TODO: use this placeholder to exand on the ConfigsBuilder defaults
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
    (mapv (partial prep-initial-position scheduler
                                         aws-account-number)
          streams)
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
