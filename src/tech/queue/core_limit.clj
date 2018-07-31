(ns tech.queue.core-limit
  "Limit the number of processes running via a system where they have to request
their resources before they start.  If the system doesn't have them, everything blocks."
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as c]
            [taoensso.timbre :as log]))


(def ^:dynamic *machine-core-count* (.availableProcessors (Runtime/getRuntime)))


(defprotocol PCoreManager
  (request-cores [mgr core-count]
    "potentially blocking call, returns when cores can be satisfied")
  (release-cores [mgr core-count]
    "nonblocking returns cores to the source"))


(defmacro with-cores
  [mgr core-count & body]
  `(do
     (request-cores ~mgr ~core-count)
     (try
       ~@body
       (finally
         (release-cores ~mgr ~core-count)))))


(defn- add-msg
  "debug hook"
  [msg & args]
  nil)


(defrecord FIFOCoreManager [system-core-count request-chan notify-chan active-cores
                            max-active-cores min-released-cores]
  PCoreManager
  (request-cores [mgr core-count]
    (add-msg "request-cores begin" @active-cores core-count)
    (when-not request-chan
      (throw (ex-info "Uninitialized" {})))
    (let [result-chan (async/chan)]
      ;;The put request returns false when it fails.
      (when-let [result (async/>!! request-chan {:core-count core-count
                                                 :result-chan result-chan})]
        (let [error (async/<!! result-chan)]
          (when error
            (throw (ex-info "thread interrupted"
                            {:error error}))))))
    (add-msg "request-cores end" @active-cores core-count ))

  (release-cores [mgr core-count]
    (add-msg "release cores begin" @active-cores core-count)
    (swap! min-released-cores min
           (swap! active-cores #(- % core-count)))
    (async/>!! notify-chan 1)
    (add-msg "release cores end" @active-cores core-count))


  c/Lifecycle
  (start [this]
    (if-not (:thread this)
      (let [request-chan (async/chan)
            notify-chan (async/chan)
            active-cores (atom 0)
            thread (async/thread
                     (loop []
                       (let [timeout-chan (async/timeout 50)
                             continue?
                             (try
                               ;;Loop exits if this request chan closes.  Else it always recurs.
                               (let [value (async/alt!!
                                             request-chan ([result] result)
                                             timeout-chan :timeout
                                             notify-chan :notify)]
                                 (cond
                                   (= value :timeout)
                                   true
                                   (= value :notify)
                                   true
                                   :else
                                   (when value
                                     (let [{:keys [core-count result-chan]} value]
                                       (add-msg "grab-cores" @active-cores core-count)
                                       (loop [active-count (+ core-count @active-cores)]
                                         (if (<= active-count system-core-count)
                                           (do
                                             (swap! max-active-cores max
                                                    (swap! active-cores (partial + core-count)))
                                             (async/close! result-chan)
                                             (add-msg "grabbed" @active-cores core-count)
                                             true)
                                           (do
                                             (async/<!! notify-chan)
                                             (add-msg "notified" @active-cores core-count)
                                             (recur (+ core-count @active-cores)))))))))
                               (catch Throwable e
                                 (log/error e)
                                 (Thread/sleep 2000)
                                 true))]
                         (when continue?
                           (recur))))
                     (log/warn "Queueing core exiting")
                     (loop [item (async/<!! request-chan)]
                       (when item
                         (async/>!! (:result-chan item) (ex-info "core exiting" {}))
                         (recur (async/<!! request-chan)))))]
        (assoc this
               :request-chan request-chan
               :notify-chan notify-chan
               :active-cores active-cores
               :thread thread))
      this))
  (stop [this]
    (if (:thread this)
      (do
        (async/close! request-chan)
        (async/close! notify-chan)
        ;;Wait for thread exit
        (async/<!! (:thread this))
        (dissoc this
                :request-chan
                :notify-chan
                :active-cores
                :thread))
      this)))


(defn fifo-mgr
  [& {:keys [system-core-count]
      :or {system-core-count *machine-core-count*}}]
  (map->FIFOCoreManager {:system-core-count (long system-core-count)
                         :max-active-cores (atom 0)
                         :min-released-cores (atom 0)}))
