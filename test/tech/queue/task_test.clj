(ns tech.queue.task-test
  (:require [tech.queue.task :as task]
            [tech.queue.filesystem :as qf]
            [tech.queue.worker :as qw]
            [tech.io.temp-file :as temp-file]
            [clojure.test :refer :all]
            [tech.queue.worker-test :as worker-test]
            [com.stuartsierra.component :as c]
            [tech.queue.resource-limit :as resource-limit]))


(defn msg->processor
  [context]
  (task/forward-queue-processor
   #(worker-test/process-queue-data
     (:score context)
     %)
   :resource-map-fn (constantly {:froodles 3})))


(defn test-process-item
  [queue resource-mgr]
  (testing "Testing that basic processing works"
    (let [resource-mgr (when resource-mgr
                         (c/start resource-mgr))
          score-atom (atom 0)
          amounts [1 5 8 10 21 4]
          _  (doseq [amt amounts]
               (task/put! queue #'msg->processor {:amount amt} {}))
          worker (c/start
                  (qw/worker :incrementor (task/task-processor {:score score-atom})
                             queue
                             (assoc (worker-test/threads-or-resource-mgr resource-mgr)
                                    :heavy-logging? true)))]
      (try
        (worker-test/delay-till-empty queue)
        (is (= @score-atom (reduce + amounts)))
        (finally
          (c/stop worker)
          (when resource-mgr
            (c/stop resource-mgr)))))))


(deftest process-item
  (worker-test/with-queue-provider
    (test-process-item queue nil)))


(deftest process-item-res-mgr
  (worker-test/with-queue-provider
    (test-process-item queue (resource-limit/resource-manager
                              {:initial-resources {:froodles 10}}))))
