(ns tech.queue.worker-test
  (:require [clojure.test :refer :all]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as c]
            [tech.queue.worker :as qw]
            [tech.queue.protocols :as q]
            [tech.queue.filesystem :as qf]
            [clojure.core.async :as async])
  (:import [java.util UUID]))


(defrecord BasicAtomProcessor [log-ctx *score *retire-list ready?]
  q/QueueProcessor
  (msg->log-context [_ _] log-ctx)
  (msg-ready? [_ _] ready?)
  (process! [_ msg]
    (try
      (swap! *score + (:amount msg))
      (catch Throwable e
        (throw e)))
    {:status :success})
  (retire! [_ msg result] (swap! *retire-list conj [msg result])))


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


(defn- delay-till-empty
  [queue]
  (let [result (async/alt!!
                 (async/timeout 6000) :timeout
                 (async/thread
                   (->> (repeatedly #(do (Thread/sleep 200) (q/stats queue {})))
                        (map :in-flight)
                        (take-while #(> % 0))
                        last)) :success)]
    (is (= result :success))))


(defn test-process-item
  [queue]
  (testing "Testing that basic processing works"
    (let [processor (create-basic-atom-processor)
          worker (c/start
                  (qw/worker :incrementor processor queue {:thread-count 4}))
          amounts [1 5 8 10 21 4]]
      (try
        (doseq [amt amounts]
          (q/put! queue {:amount amt} {}))
        (delay-till-empty queue)
        (is (= @(get processor :*score) (reduce + amounts)))
        (finally
          (c/stop worker))))))


(deftest process-item
  (with-queue-provider
    (test-process-item queue)))


(defn test-msg-lifetime
  [queue]
  (testing "Testing that a bad message gets caught appropriately"
    (let [processor (create-basic-atom-processor)
          worker (c/start
                  (qw/worker :incrementor processor queue {:retry-delay-seconds 1
                                                           :message-retry-period 1
                                                           :thread-count 4}))
          amounts [1 5 8 :b 21 4]]
      (try
        (doseq [amt amounts]
          (q/put! queue {:amount amt} {}))
        (delay-till-empty queue)
        ;;Due to lifetime limits (1 second isn't much time), it is possible that
        ;;one of the valid messages gets dropped.
        (is (<= @(get processor :*score) (reduce + (filter number? amounts))))
        (is (= {:amount :b
                :status :error}
               (let [[msg stat] (first @(get processor :*retire-list))]
                 {:amount (:amount msg)
                  :status (:status stat)})))
        (finally
          (c/stop worker))))))


(deftest msg-lifetime
  (with-queue-provider
    (test-msg-lifetime queue)))


(defn test-msg-not-ready-timeout
  [queue]
  (testing "Testing message that aren't ready eventually get removed"
    (let [processor (create-basic-atom-processor :ready? false)
          worker (c/start
                  (qw/worker :incrementor processor queue {:retry-delay-seconds 1
                                                           :message-retry-period 1
                                                           :thread-count 4}))
          amounts [1 5 8 :b 21 4]]
      (try
        (doseq [amt amounts]
          (q/put! queue {:amount amt} {}))
        (delay-till-empty queue)
        (is (= @(get processor :*score) 0))
        (is (= (count amounts)
               (count @(get processor :*retire-list))))
        (is (every? #(= :not-ready? %)
                    (map (comp :status second) @(get processor :*retire-list))))
        (finally
          (c/stop worker))))))


(deftest msg-not-ready-timeout
  (with-queue-provider
    (test-msg-not-ready-timeout queue)))
