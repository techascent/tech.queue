(ns tech.queue.worker
  (:require [com.stuartsierra.component :as c]
            [clojure.core.async :as a]
            [taoensso.timbre :as log]
            [clojure.core.async.impl.protocols :as async-protocols]
            [think.parallel.core :as parallel]
            [tech.queue.core-limit :as core-limit]
            [tech.queue.logging :as logging]
            [tech.queue.protocols :as q]
            [tech.queue.time :as time])
  (:import [java.time Duration]
           [java.util Date]))


(defn- loop-read-from-queue
  [service-name queue write-channel timeout-ms control-channel]
  (logging/merge-context
   {:service service-name}
   (loop []
     (try
       (let [next-value (q/take! queue)]
         (when-not (= :timeout next-value)
           (a/>!! write-channel next-value)))
       (catch Throwable e
         (log/error e)))
     (if-not (or (async-protocols/closed? write-channel)
                 (async-protocols/closed? control-channel))
       (recur)
       (do
         (a/close! write-channel)
         (log/warn :queue-feeder-exit))))))


(defn date-difference-seconds
  ^long [^Date previous-date ^Date current-date]
  (long
   (->
    (Duration/between
     (.toInstant previous-date)
     (.toInstant current-date))
    (.getSeconds))))


(defn worker-type
  [worker]
  (cond
    (:thread-count worker)
    :thread-limited
    (:core-mgr worker)
    :core-limited))

(defmulti worker-process-item
  (fn [worker & args]
    (worker-type worker)))


(defn- handle-processed-msg
  [{:keys [processor queue name
           message-retry-period
           retry-delay-seconds
           core-count] :as worker} next-item-task result]
  (let [msg (:msg result)
        status (:status result)]
    (if (or (= status :not-ready?)
            (= status :error))
      ;;We didn't process the callback
      (let [msg-birthdate (q/msg->birthdate queue msg)
            time-alive (date-difference-seconds msg-birthdate (Date.))]
        (if (< time-alive message-retry-period)
          (a/thread
            (do (a/<!! (a/timeout (* 1000 retry-delay-seconds)))
                (q/put! queue msg)
                (q/complete! queue next-item-task)))
          (do
            (log/warn (format "Dropping message: birthdate %s, time-alive: %s, queue ttl seconds: %s"
                              msg-birthdate time-alive message-retry-period))

            (try
              ;;If we fail to retire the message then we have to requeue it.  This essentially
              ;;means that we have to fix the failure mode somehow.  We simply cannot lose
              ;;messages due to failure if they should be retired
              (q/retire! processor msg result)
              (q/complete! queue next-item-task)
              (catch Throwable e
                ;;Note the task won't be completed.  This means that we will get back around to
                ;;it.
                (log/error e))))))
      (do
        (log/info :processing-success)
        (q/complete! queue next-item-task)))))


(defmethod worker-process-item :thread-limited
  [{:keys [processor queue name
           message-retry-period
           retry-delay-seconds
           core-count] :as worker} next-item-task]
  (let [msg (q/task->msg queue next-item-task)]
    (logging/merge-context
     (q/msg->log-context processor msg)
     (let [result (if (q/msg-ready? processor msg)
                    (try
                      (q/process! processor msg)
                      (catch Throwable e
                        (log/error e)
                        {:status :error
                         :error e
                         :msg msg}))
                    {:status :not-ready?
                     :msg msg})
           ;;Overshadow msg potentially.  This allows the processing function to do something
           ;;like attempt a callback to a remote system.  This can fail, but we want to keep
           ;;track of callback attempts
           result (assoc result :msg (or (:msg result) msg))]
       (handle-processed-msg worker next-item-task result)))))


(defmethod worker-process-item :core-limited
  [{:keys [processor queue name
           message-retry-period
           retry-delay-seconds
           core-mgr] :as worker} next-item-task]
  (let [msg (q/task->msg queue next-item-task)]
    (logging/merge-context
     (q/msg->log-context processor msg)
     (if-let [ready? (q/msg-ready? processor msg)]
       (let [core-count (min (q/core-count processor msg)
                             (:system-core-count core-mgr))]
         ;;Block here until we get the green light
         (core-limit/request-cores core-mgr core-count)
         (future
           (try
             (let [result (try
                            (q/process! processor msg)
                            (catch Throwable e
                              (log/error e)
                              {:status :error
                               :error e
                               :msg msg}))
                   result (assoc result :msg (or (:msg result) msg))]
               (handle-processed-msg worker next-item-task result))
             (finally
               (core-limit/release-cores core-mgr core-count)))))
       (handle-processed-msg worker next-item-task {:status :not-ready?
                                                    :msg msg})))))


(defn- process-item-sequence
  [worker next-item-ch ctl-ch]
  (logging/merge-context
   {:service (:service worker)}
   (loop []
     (try
       (->> (parallel/async-channel-to-lazy-seq next-item-ch)
            (parallel/queued-pmap (or (get worker :thread-count) 1)
                                  (partial worker-process-item worker))
            dorun)
       (catch Throwable e
         (log/error e)))
     (if-not (or (async-protocols/closed? next-item-ch)
                 (async-protocols/closed? ctl-ch))
       (recur)
       (log/warn :queue-worker-exit)))))


(defrecord Worker [service processor-fn! queue thread-count
                   timeout-ms message-retry-period retry-delay-seconds
                   msg->ctx-fn core-limit
                   core-mgr]
  c/Lifecycle
  (start [this]
    (if-not (contains? this :ctl-ch)
      (let [ctl-ch (a/chan)
            next-item-ch (a/chan)
            take-thread (a/thread
                          (loop-read-from-queue service
                                                queue next-item-ch
                                                timeout-ms ctl-ch))
            processor-thread (a/thread (process-item-sequence this next-item-ch ctl-ch))]
        (assoc this
               ::ctl-ch     ctl-ch
               ::take-thread take-thread
               ::processor-thread processor-thread))
      this))

  (stop [this]
    (when (contains? this ::ctl-ch)
      (a/close! (::ctl-ch this))
      (a/<!! (::take-thread this))
      (a/<!! (::processor-thread this)))
    (dissoc this ::ctl-ch ::take-thread ::processor-thread)))


(defn- cpu-count
  []
  (. (Runtime/getRuntime) availableProcessors))


(def worker-defaults
  {:message-retry-period (time/hours->seconds 2)
   :retry-delay-seconds (time/minutes->seconds 5)})
