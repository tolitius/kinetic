(ns kinetic.tracker-test
  (:require [clojure.test :refer [deftest is]]
            [kinetic.consumer :as consumer]
            [kinetic.tracker :as tracker])
  (:import [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
           [software.amazon.awssdk.services.kinesis KinesisAsyncClient]
           [software.amazon.kinesis.processor ShardRecordProcessorFactory]))

(def arn "arn:aws:kinesis:us-west-2:123456789012:stream/events")

(deftest stream-name-from-arn
  (is (= "events" (tracker/stream-name-from-arn arn))))

(deftest single-stream-identifier-arn-vs-name
  (let [by-arn (tracker/single-stream-identifier {:arn arn})
        by-name (tracker/single-stream-identifier {:name "events"})]
    (is (= arn (str (.get (.streamArnOptional by-arn)))))
    (is (= "events" (.streamName by-arn)))
    (is (false? (.isPresent (.streamArnOptional by-name))))
    (is (= "events" (.streamName by-name)))))

(deftest make-config-polling-name-for-arn-stream
  (let [region (Region/of "us-west-2")
        clients [(-> (KinesisAsyncClient/builder) (.region region) .build)
                 (-> (DynamoDbAsyncClient/builder) (.region region) .build)
                 (-> (CloudWatchAsyncClient/builder) (.region region) .build)]
        [kinesis-client dynamo-client cloud-watch-client] clients
        processor-factory (reify ShardRecordProcessorFactory
                            (shardRecordProcessor [_] nil))]
    (try
      (let [config (consumer/make-config
                     {:streams [{:arn arn}]
                      :application-name "application"
                      :kinesis-client kinesis-client
                      :dynamo-client dynamo-client
                      :cloud-watch-client cloud-watch-client
                      :record-processor-factory processor-factory})]
        (is (= "events" (.streamName (:polling-config config)))))
      (finally (doseq [client clients] (.close client))))))
