(ns tech.queue.worker
  (:require [com.stuartsierra.component :as c]
            [clojure.core.async :as a]
            [taoensso.timbre :as log]
            [clojure.core.async.impl.protocols :as async-protocols]
            [think.parallel.core :as parallel]
            [tech.queue.resource-limit :as resource-limit]
            [tech.queue.logging :as logging]
            [tech.queue.protocols :as q]
            [tech.queue.time :as time]))


(defn- loop-read-from-queue
  [service-name queue write-channel timeout-ms control-channel]
  (logging/merge-context
   {:service service-name}
   (loop []
     (try
       (let [next-value (q/take! queue {})]
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


(defn worker-type
  [worker]
  (cond
    (:thread-count worker)
    :thread-limited
    (:resource-mgr worker)
    :resource-limited))

(defmulti worker-process-item
  (fn [worker & args]
    (worker-type worker)))


(defn preprocess-msg
  [{:keys [message-retry-period
           queue
           processor] :as worker}
   next-item-task msg]
  (let [msg-birthdate (time/msg->birthdate msg)
        time-alive (try (time/seconds-since msg-birthdate)
                        (catch Throwable e nil))]
    (if (and time-alive
             (>= time-alive message-retry-period))
      (do
        (q/complete! queue next-item-task {})
        (log/warn (format "Dropping message: birthdate %s, time-alive: %s, queue ttl seconds: %s"
                          msg-birthdate time-alive message-retry-period))
        (try
          (q/retire! processor msg {:msg msg
                                    :status :retired})
          (catch Throwable e
            (log/error e)))
        nil)
      {:status (if (try
                     (q/msg-ready? processor msg)
                     (catch Throwable e
                       (log/error e)
                       false))
                 :ready
                 :not-ready)
       :msg msg})))


(defn- handle-processed-msg
  [{:keys [processor
           queue
           retry-delay-seconds]
    :as worker} next-item-task result]
  (let [msg (:msg result)
        status (:status result)]
    (if (= status :success)
      (do
        (log/info :processing-success)
        (q/complete! queue next-item-task {}))
      (a/thread
        (a/<!! (a/timeout (* 1000 retry-delay-seconds)))
        (q/put! queue msg {})
        (q/complete! queue next-item-task {})))))


(defmethod worker-process-item :thread-limited
  [{:keys [processor queue name
           message-retry-period
           retry-delay-seconds
           core-count] :as worker} next-item-task]
  (let [msg (q/task->msg queue next-item-task)]
    (logging/merge-context
     (q/msg->log-context processor msg)
     (when-let [preprocess-result (preprocess-msg worker next-item-task msg)]
       (let [{:keys [msg status]} preprocess-result
             result (if (= status :ready)
                      (try
                        (q/process! processor msg)
                        (catch Throwable e
                          (log/error e)
                          {:status :error
                           :error e
                           :msg msg}))
                      preprocess-result)]
         (handle-processed-msg worker next-item-task result))))))


(defmethod worker-process-item :resource-limited
  [{:keys [processor queue name
           message-retry-period
           retry-delay-seconds
           resource-mgr] :as worker} next-item-task]
  (let [msg (q/task->msg queue next-item-task)]
    (logging/merge-context
     (q/msg->log-context processor msg)
     (let [preprocess-result (preprocess-msg worker next-item-task msg)
           res-map (q/resource-map processor msg (:initial-resources resource-mgr))]
       (when preprocess-result
         (try
           (if (and (= (:status preprocess-result) :ready)
                    (= :success (resource-limit/request-resources!
                                 resource-mgr
                                 res-map
                                 100)))
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
                   (resource-limit/release-resources! resource-mgr res-map))))
             (handle-processed-msg worker next-item-task preprocess-result))
           (catch Throwable e
             (log/error e)
             (handle-processed-msg worker next-item-task {:status :error
                                                          :error e
                                                          :msg msg}))))))))


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


(defrecord Worker [service processor queue thread-count
                   timeout-ms message-retry-period retry-delay-seconds
                   core-limit resource-mgr]
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


(defn worker
  [service processor queue {:keys [thread-count
                                   resource-mgr
                                   message-retry-period
                                   retry-delay-seconds]
                            :as args}]
  (when (and thread-count resource-mgr)
    (throw (ex-info "Either thread count or core limit must be in effect" {:thread-count thread-count
                                                                           :resource-mgr resource-mgr})))
  (when-not (or thread-count resource-mgr)
    (throw (ex-info "Either thread count or core limit must be in effect; neither is defined" {})))
  (map->Worker (merge worker-defaults
                      {:service service
                       :processor processor
                       :queue queue}
                      args)))
