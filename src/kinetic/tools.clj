(ns kinetic.tools
  (:import [java.util.concurrent Executors]))

(defn start-within-thread [fun]
  (let [executor (Executors/newSingleThreadExecutor)]
    (.submit executor fun)
    executor))
