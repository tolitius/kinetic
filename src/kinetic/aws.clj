(ns kinetic.aws
  ; (:require [])
  (:import [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    AwsCredentialsProvider]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
           [software.amazon.awssdk.services.kinesis KinesisAsyncClient]
           [software.amazon.kinesis.common KinesisClientUtil]))

(defn make-creds [{:keys [access-key-id
                          secret-access-key]}]
  (AwsBasicCredentials/create access-key-id
                              secret-access-key))

(defn make-creds-provider [creds]
  (reify AwsCredentialsProvider
    (resolveCredentials [_]
      (make-creds creds))))

(defn make-region
  ([]
   (make-region {}))
  ([{:keys [region]
     :or {region "us-east-1"}}]
   (Region/of region)))

(defn make-dynamo-db-client [{:keys [region
                                     creds]}]
  (cond-> (DynamoDbAsyncClient/builder)
    :region  (.region (make-region region)) ;; region should be there
    creds    (.credentialsProvider (make-creds-provider creds))
    :and     .build))

(defn make-cloud-watch-client [{:keys [region
                                       creds]}]
  (cond-> (CloudWatchAsyncClient/builder)
    :region  (.region (make-region region)) ;; region should be there
    creds    (.credentialsProvider (make-creds-provider creds))
    :and     .build))

(defn make-kinesis-client [{:keys [region
                                   creds]}]
  (cond-> (KinesisAsyncClient/builder)
          :region (.region (make-region region))
          creds   (.credentialsProvider (make-creds-provider creds))
          :and    KinesisClientUtil/createKinesisAsyncClient))
