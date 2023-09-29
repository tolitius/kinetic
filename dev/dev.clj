(ns dev
  (:require [clojure.pprint :refer [pprint]]
            [clojure.repl :refer :all]
            [kinetic.tools :refer :all]
            [kinetic.aws :refer :all]
            [kinetic.consumer :refer :all])
  (:import [java.util Date]
           [java.time Instant]
           [java.time.temporal ChronoUnit]))

(def yesterday                             ;; to start consuming from
     (-> (Instant/now)
         (.minus 1 ChronoUnit/DAYS)
         (Date/from)))

;; (!) single stream and multi stream trackers / leases can't coexist
;;     because the lease entries are read into a different objects: Lease vs. MultiStreamLease
;;     so the lease table need to be empty in case a switch from single to multi is needed

(defn consumer []
  (start-consumer {:streams [{:name "milky-way:solar:pluto"
                              :start-from {:position :trim-horizon}
                              ; :start-from {:position :latest}
                              }
                             {:name "milky-way:solar:mars"
                              ; :delete-leases? true
                              :start-from {:position :at-timestamp
                                           :timestamp yesterday}
                              }]
                   ; :multi-stream? true              ;; <= only needed if there is a single stream that needs to run as a multi
                   :application-name "hubble"
                   ; :creds {:access-key-id "AK..ZZ"
                   ;         :secret-access-key "z0.........0m"}
                   :consume echo}))

(comment
  (def c (consumer))

  (show-leases c)

  (delete-all-leases c)

  (stop-consumer c))
