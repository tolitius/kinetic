(ns kinetic.aws
  ; (:require [])
  (:import [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    AwsCredentialsProvider]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
           [software.amazon.awssdk.services.dynamodb.model DescribeTableRequest
                                                           DeleteTableRequest
                                                           CreateTableRequest
                                                           KeySchemaElement
                                                           KeyType
                                                           AttributeDefinition
                                                           ScalarAttributeType
                                                           BillingMode]
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
   (make-region "us-east-1"))
  ([region]
   (Region/of (or region "us-east-1"))))

(defn make-dynamo-db-client [{:keys [region
                                     creds]}]

  (cond-> (DynamoDbAsyncClient/builder)
    :region  (.region (make-region region)) ;; region should be there
    creds    (.credentialsProvider (make-creds-provider creds))
    :and     .build))

(defn describe-table [dynamo-db-client
                      {:keys [table-name]}]
  (let [request (-> (DescribeTableRequest/builder)
                    (.tableName table-name)
                    .build)]
    (-> dynamo-db-client
        (.describeTable request)
        .get)))

(defn create-table [dynamo-db-client
                    {:keys [table-name
                            key-schema
                            attribute-definitions
                            billing-mode]

                     ;;TODO: make key schema and attribute definitions plural
                     :or {key-schema {:key-type (KeyType/HASH)
                                      :key-attribute "leaseKey"}
                          attribute-definitions {:name "leaseKey"
                                                 :type (ScalarAttributeType/S)}
                          billing-mode (BillingMode/PAY_PER_REQUEST)}}]
  (let [request (-> (CreateTableRequest/builder)
                    (.tableName table-name)
                    (.keySchema [(-> (KeySchemaElement/builder)
                                     (.attributeName (:key-attribute key-schema))
                                     (.keyType (:key-type key-schema))
                                     .build)])
                    (.attributeDefinitions [(-> (AttributeDefinition/builder)
                                                (.attributeName (:name attribute-definitions))
                                                (.attributeType (:type attribute-definitions))
                                                .build)])
                    (.billingMode billing-mode)
                    .build)]
    (-> dynamo-db-client
        (.createTable request)
        .get)))

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
