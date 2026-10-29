(ns kinetic.consumer-test
  (:require [clojure.test :refer [deftest is testing]]
            [kinetic.consumer :as consumer])
  (:import [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
           [software.amazon.awssdk.services.kinesis KinesisAsyncClient]
           [software.amazon.kinesis.metrics MetricsLevel]
           [software.amazon.kinesis.processor ShardRecordProcessorFactory]))

(defn make-config [metrics-level]
  (let [region (Region/of "us-east-1")
        clients [(-> (KinesisAsyncClient/builder)
                     (.region region)
                     (.build))
                 (-> (DynamoDbAsyncClient/builder)
                     (.region region)
                     (.build))
                 (-> (CloudWatchAsyncClient/builder)
                     (.region region)
                     (.build))]
        [kinesis-client dynamo-client cloud-watch-client] clients
        processor-factory (reify ShardRecordProcessorFactory
                            (shardRecordProcessor [_]
                              nil))]
    (try
      (consumer/make-config {:streams [{:name "stream"}]
                             :application-name "application"
                             :kinesis-client kinesis-client
                             :dynamo-client dynamo-client
                             :cloud-watch-client cloud-watch-client
                             :metrics-level metrics-level
                             :record-processor-factory processor-factory})
      (finally
        (doseq [client clients]
          (.close client))))))

(deftest metrics-level-conversion
  (testing "metrics are disabled when no level is supplied"
    (is (= MetricsLevel/NONE
           (consumer/->metrics-level nil))))
  (testing "supported levels accept strings and keywords"
    (is (= MetricsLevel/SUMMARY
           (consumer/->metrics-level "summary")))
    (is (= MetricsLevel/DETAILED
           (consumer/->metrics-level :detailed))))
  (testing "an existing KCL level is passed through"
    (is (identical? MetricsLevel/SUMMARY
                    (consumer/->metrics-level MetricsLevel/SUMMARY))))
  (testing "unsupported values fail clearly"
    (is (try
          (consumer/->metrics-level 42)
          false
          (catch IllegalArgumentException _
            true)))))

(deftest make-config-metrics-level
  (testing "the configuration boundary defaults to NONE"
    (is (= MetricsLevel/NONE
           (.metricsLevel (:metrics-config (make-config nil))))))
  (testing "the configuration boundary preserves explicit opt-in"
    (is (= MetricsLevel/SUMMARY
           (.metricsLevel (:metrics-config (make-config "SUMMARY")))))
    (is (= MetricsLevel/DETAILED
           (.metricsLevel (:metrics-config (make-config :detailed)))))))

