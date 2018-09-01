(ns tech.queue.worker-test
  (:require [clojure.test :refer :all]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as c]
            [tech.queue.worker :as qw]
            [tech.queue.protocols :as q]
            [tech.queue.filesystem :as qf]
            [clojure.core.async :as async]
            [tech.queue.resource-limit :as resource-limit]
            [taoensso.timbre :as log])
  (:import [java.util UUID]))


(defn process-queue-data
  [*score msg]
  (swap! *score + (:amount msg))
  ;;simulate timeouts for things
  (Thread/sleep 100)
  {:status :success})


(defrecord BasicAtomProcessor [log-ctx *score *retire-list ready?]
  q/QueueProcessor
  (msg->log-context [_ _] log-ctx)
  (msg-ready? [_ _] ready?)
  (process! [_ msg]
    (process-queue-data *score msg)
    ;;(log/info (str (:amount msg)))
    {:status :success})
  (retire! [_ msg result] (swap! *retire-list conj [msg result]))

  q/PResourceLimit
  (resource-map [_ msg init-res]
    {:froodles 3}))


(defn create-basic-atom-processor
  [& {:keys [ready?]
      :or {ready? true}}]
  (->BasicAtomProcessor {} (atom 0) (atom []) ready?))



(defmacro with-queue-provider
  [& body]
  `(let [temp-dir# (str "/tmp/" (UUID/randomUUID) "/")]
     (try
       (fs/mkdir temp-dir#)
       (let [queue-provider# (qf/provider temp-dir# {})
             ~'queue (q/get-or-create-queue! queue-provider# :incrementor {})]
         ~@body)
       (finally
         (fs/delete-dir temp-dir#)))))


(defn delay-till-empty
  [queue]
  (let [result (async/alt!!
                 (async/timeout 20000) :timeout
                 (async/thread
                   (->> (repeatedly #(do (Thread/sleep 200) (q/stats queue {})))
                        (map :in-flight)
                        (take-while #(> % 0))
                        last)) :success)]
    (is (= result :success))))


(defn threads-or-resource-mgr
  [resource-mgr]
  (merge {:message-retry-period 60
          :retry-delay-seconds 1}
         (if resource-mgr
           {:resource-mgr resource-mgr}
           {:thread-count 4})))


(defn test-process-item
  [queue resource-mgr]
  (testing "Testing that basic processing works"
    (let [resource-mgr (when resource-mgr
                         (c/start resource-mgr))
          processor (create-basic-atom-processor)
          amounts [5 1 8 10 21 4]
          _  (doseq [amt amounts]
               (q/put! queue {:amount amt} {}))
          worker (c/start
                  (qw/worker :incrementor processor queue
                             (threads-or-resource-mgr resource-mgr)))]
      (try
        (delay-till-empty queue)
        (is (= @(get processor :*score) (reduce + amounts)))
        (finally
          (c/stop worker)
          (when resource-mgr
            (c/stop resource-mgr)))))))


(deftest process-item
  (with-queue-provider
    (test-process-item queue nil)))


(deftest process-item-res-mgr
  (with-queue-provider
    (test-process-item queue (resource-limit/resource-manager
                              {:initial-resources {:froodles 10}}))))


(defn test-msg-lifetime
  [queue resource-mgr]
  (testing "Testing that a bad message gets caught appropriately"
    (let [resource-mgr (when resource-mgr
                         (c/start resource-mgr))
          processor (create-basic-atom-processor)
          amounts [1 5 8 :b 21 4]
          _ (doseq [amt amounts]
              (q/put! queue {:amount amt} {}))
          worker (c/start
                  (qw/worker :incrementor processor queue (merge
                                                           (threads-or-resource-mgr
                                                            resource-mgr)
                                                           {:retry-delay-seconds 1
                                                            :message-retry-period 3})))]
      (try
        (delay-till-empty queue)
        ;;Due to lifetime limits (1 second isn't much time), it is possible that
        ;;one of the valid messages gets dropped.
        (is (<= @(get processor :*score) (reduce + (filter number? amounts))))
        (is (= {:amount :b
                :status :retired}
               (let [[msg stat] (first @(get processor :*retire-list))]
                 {:amount (:amount msg)
                  :status (:status stat)})))
        (finally
          (c/stop worker)
          (when resource-mgr
            (c/stop resource-mgr)))))))


(deftest msg-lifetime
  (with-queue-provider
    (test-msg-lifetime queue nil)))


(deftest msg-lifetime-res-mgr
  (with-queue-provider
    (test-msg-lifetime queue (resource-limit/resource-manager {:initial-resources
                                                               {:froodles 10}}))))


(defn test-msg-not-ready-timeout
  [queue resource-mgr]
  (testing "Testing message that aren't ready eventually get removed"
    (let [resource-mgr (when resource-mgr
                         (c/start resource-mgr))
          amounts [1 5 8 :b 21 4]
          _ (doseq [amt amounts]
              (q/put! queue {:amount amt} {}))
          processor (create-basic-atom-processor :ready? false)
          worker (c/start
                  (qw/worker :incrementor processor queue (merge
                                                           (threads-or-resource-mgr
                                                            resource-mgr)
                                                           {:retry-delay-seconds 1
                                                            :message-retry-period 3})))]
      (try
        (delay-till-empty queue)
        (is (= @(get processor :*score) 0))
        (is (= (count amounts)
               (count @(get processor :*retire-list))))
        (is (every? #(= :retired %)
                    (map (comp :status second) @(get processor :*retire-list))))
        (finally
          (c/stop worker)
          (when resource-mgr
            (c/stop resource-mgr)))))))


(deftest msg-not-ready-timeout
  (with-queue-provider
    (test-msg-not-ready-timeout queue nil)))


(deftest msg-not-ready-timeout-resource-mgr
  (with-queue-provider
    (test-msg-not-ready-timeout queue (resource-limit/resource-manager {:initial-resources
                                                                        {:froodles 10}}))))
