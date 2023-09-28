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

(defn consumer []
  (start-consumer {:streams [{:name "lagoon-nebula"
                              ; :start-from {:position :at-timestamp
                              ;              :timestamp yesterday}
                              :start-from {:position :trim-horizon}
                              ; :start-from {:position :latest}
                              ; :start-from {:position :latest}
                              }]
                   :application-name "hubble"
                   ; :delete-leases? true
                   ; :creds {:access-key-id "AK..ZZ"
                   ;         :secret-access-key "z0.........0m"}
                   :consume echo}))

(comment
  (def c (consumer))

  (show-leases c)

  (delete-all-leases c)

  (stop-consumer c))
