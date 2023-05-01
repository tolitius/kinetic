(ns kinetic.aws
  ; (:require [])
  (:import [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
           [software.amazon.awssdk.services.kinesis KinesisAsyncClient]
           [software.amazon.kinesis.common KinesisClientUtil]))

(defn make-region
  ([]
   (make-region {}))
  ([{:keys [region]
     :or {region "us-east-1"}}]
   (Region/of region)))

(defn make-dynamo-db-client [{:keys [region]}]
  (-> (DynamoDbAsyncClient/builder)
      (.region (make-region region))
      .build))

(defn make-cloud-watch-client [{:keys [region]}]
  (-> (CloudWatchAsyncClient/builder)
      (.region (make-region region))
      .build))

(defn make-kinesis-client [{:keys [region]}]
  (-> (KinesisAsyncClient/builder)
      (.region (make-region region))
      KinesisClientUtil/createKinesisAsyncClient))
