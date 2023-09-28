(ns kinetic.tracker
  (:require [clojure.tools.logging :as log]
            [kinetic.tools :as t])
  (:import [java.time Duration]
           [software.amazon.kinesis.common ConfigsBuilder
                                           InitialPositionInStream
                                           InitialPositionInStreamExtended
                                           StreamIdentifier
                                           StreamConfig]
           [software.amazon.kinesis.processor FormerStreamsLeasesDeletionStrategy
                                              FormerStreamsLeasesDeletionStrategy$AutoDetectionAndDeferredDeletionStrategy
                                              StreamTracker
                                              SingleStreamTracker
                                              MultiStreamTracker]))

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

;; single stream tracker

(defn single-stream-tracker [{:keys [name
                                     start-from]}]

  (log/infof "making a single stream tracker for %s stream"
             name)

  (let [stream-identifier (StreamIdentifier/singleStreamInstance name)]
    (if-not start-from
      (SingleStreamTracker. stream-identifier)
      (SingleStreamTracker. stream-identifier
                            (make-initial-position start-from)))))

;; multiple stream tracker

(defn make-stream-id
  "
  stream identifier format: <accountId>:<streamName>:<creationEpoch>

  source: https://github.com/awslabs/amazon-kinesis-client/blob/7899820cb17eefb1eed4a4d0fbc3b86f2da9f2d3/amazon-kinesis-client/src/main/java/software/amazon/kinesis/common/StreamIdentifier.java#L48-L53
  "
  [aws-account-number
   {:keys [name
           epoch]}]
  (str (or aws-account-number "0")
       ":"
       name
       ":"
       (or epoch 1)))

(defn add-multi-stream-identifier [aws-account-number
                             id]
  (assoc id :identifier
            (StreamIdentifier/multiStreamInstance (make-stream-id aws-account-number
                                                                  id))))

(defn make-stream-config [{:keys [identifier
                                  start-from] :as opts}]
  (StreamConfig. identifier
                 (make-initial-position start-from)))

(defn multi-stream-tracker
  "
  {:aws-account-id 42
   :streams [{:name        \"milky-way:solar:pluto\"
              :start-from  {:position :trim-horizon}
              :epoc        1}
             {:name        \"milky-way:solar:mars\"
              :start-from  {:position :trim-horizon}
              :epoc        42}]}
  "
  [{:keys [aws-account-number
           streams]}]

  (log/infof "making a multiple streams tracker for %s streams"
             (mapv :name streams))

  (let [stream-configs (->> streams
                            (mapv (partial add-multi-stream-identifier aws-account-number))
                            (mapv make-stream-config))]
    ; (reify StreamTracker
    (reify MultiStreamTracker   ;; TODO: switch back to StreamTracker once MultiStreamTracker is fully depricated
      (streamConfigList [this]
        stream-configs)
      (formerStreamsLeasesDeletionStrategy [this]
        (proxy [FormerStreamsLeasesDeletionStrategy$AutoDetectionAndDeferredDeletionStrategy] []
          (waitPeriodToDeleteFormerStreams []
            (Duration/ZERO))))
      (isMultiStream [this]
        true))))

(defn make-stream-tracker [{:keys [streams
                                   multi-stream?]
                            :as opts}]
  (case (count streams)
    0 (throw (RuntimeException. "please provide at least one stream name in a list of \"streams\""))
    1 (if-not multi-stream?
        (single-stream-tracker (first streams))
        (multi-stream-tracker opts))
    (multi-stream-tracker opts)))
