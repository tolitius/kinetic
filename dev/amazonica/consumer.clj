;; amazonica is based on the old (pre 2.0) aws kinesis client

(ns kinetic.consumer
  (:require [amazonica.aws.kinesis :as k]
            [amazonica.core :as aws])
  (:import [java.util Date]
           [java.time Instant]
           [java.time.temporal ChronoUnit]))

(def yesterday                     ;; to start consuming from
   (-> (Instant/now)
       (.minus 1 ChronoUnit/DAYS)
       (Date/from)))

(defn login
  ([]
   (login {}))
  ([{:keys [kid secret region]
     :or {kid (System/getenv "AWS_ACCESS_KEY_ID")
          secret (System/getenv "AWS_SECRET_KEY")
          region "us-east-1"}}]
   (when-not (and kid secret)
     (throw (RuntimeException. (str "can't find aws access key or/and secret. "
                                    "pass them in or export AWS_ACCESS_KEY_ID and AWS_SECRET_KEY as env variables"))))
   (aws/defcredential kid secret region)
   (keyword region)))

(defn make-worker [args]
  (let [[^Worker worker uuid] (k/worker args)]
    {:worker worker :uuid uuid}))

(defn consume [events]
  (doseq [{:keys [data
                  sequence-number
                  partition-key] :as event} events]
    (println "raw event => "event)
    (println "data => " (update event :data #(String. %)))))

(defn start-consumer [app stream]
  (let [{:keys [worker uuid] :as w} (make-worker {:app app
                                                  :stream stream
                                                  :checkpoint true  ;; default to disabled checkpointing, can still force
                                                                    ;; a checkpoint by returning true from the processor function
                                                  ;; :initial-position-in-stream "TRIM_HORIZON"
                                                  :initial-position-in-stream-date yesterday
                                                  :processor consume})]
    (println "starting kinesis worker #" uuid)
    (future (.run worker))
    w))

(defn stop-consumer [{:keys [uuid worker]}]
  (println "stopping the worker" uuid)
  (.shutdown worker))
