(ns dev
  (:require [clojure.pprint :refer [pprint]]
            [clojure.repl :refer :all]
            [kinetic.tools :refer :all]
            [kinetic.aws :refer :all]
            [kinetic.consumer :refer :all]))

(defn consumer []
  (start-consumer {:stream-name "lagoon-nebula"
                   :application-name "hubble"
                   :start-from {:position :trim-horizon}
                   :consume echo}))

(comment
  (def c (consumer))

  (show-leases c)

  (delete-all-leases c)

  (stop-consumer c))
