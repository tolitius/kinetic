(ns kinetic.tools
  (:import [java.util.concurrent Executors]))

(defn start-within-thread [fun]
  (let [executor (Executors/newSingleThreadExecutor)]
    (.submit executor fun)
    executor))

(defn private-field [obj field]
  (let [m (.. obj getClass (getDeclaredField field))]
    (. m (setAccessible true))
    (. m (get obj))))
