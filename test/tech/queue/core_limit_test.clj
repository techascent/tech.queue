(ns tech.queue.core-limit
  (:require [tech.queue.core-limit :as core-limit]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as c]))


(deftest core-limit
  (let [active-test-cores (atom 0)
        max-test-cores (atom 0)
        core-count 8
        data-sequence (repeatedly 1000 #(+ 1 (rand-int (- core-count 1))))
        mgr (c/start (core-limit/fifo-mgr :system-core-count core-count))]

    (try
      (let [results (->> data-sequence
                         (pmap (fn [core-count]
                                 (core-limit/with-cores mgr core-count
                                   (swap! active-test-cores (partial + core-count)))))
                         vec)]
        (is (<= @(get mgr :max-active-cores) core-count))
        (is (>= @(get mgr :min-released-cores) 0)))
      (finally
        (c/stop mgr)))))


(defn sleep-test
  "A test that can only be visually inspected"
  []
  (let [core-count 8
        mgr (c/start (core-limit/fifo-mgr :system-core-count core-count))
        event-list (atom [])
        indexes
        (->> (range 12)
             (pmap (fn [idx]
                     (core-limit/with-cores mgr 3
                       (swap! event-list conj {:idx idx
                                               :state :running})
                       (Thread/sleep 1000)
                       (swap! event-list conj {:idx idx
                                               :state :stopping}))

                     idx))
             vec)]
    @event-list))
