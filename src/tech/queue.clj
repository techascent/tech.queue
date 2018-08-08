(ns tech.queue
  (:require [tech.io.url :as url]
            [tech.queue.protocols :as q-proto]
            [tech.queue.providers :as providers]
            [tech.queue.worker :as worker]
            [tech.queue.time :as time]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [think.parallel.core :as parallel]))

(def ^:dynamic *static-queue-options* {})


(def ^:dynamic *url-parts->queue*
  (fn [url-parts options]
    (let [path (:path url-parts)
          provider (q-proto/url-parts->provider url-parts
                                                (merge *static-queue-options*  options))]
      (q-proto/get-or-create-queue! provider
                                    (keyword (last path))
                                    (merge *static-queue-options* options)))))

(defmacro ^:private lookup-provider
  [url options & body]
  `(let [~'url-parts (url/url->parts ~url)
         ~'queue (*url-parts->queue* ~'url-parts ~options)]
     ~@body))


(defn put!
  "Place a msg in a queue"
  [url msg & [options]]
  (lookup-provider
   url options
   (when-not (map? msg)
     (throw (ex-info "Queue messages must be maps" {:msg msg})))
   (q-proto/put! queue msg options)))

(defn take!
  "Retrive a task from a queue"
  [url & [options]]
  (lookup-provider
   url options
   (q-proto/take! queue options)))

(defn task->msg
  "Conversion of a task to a message"
  [url task & [options]]
  (lookup-provider
   url options
   (q-proto/task->msg queue task)))

(defn msg->birthdate
  "Queues always place birthdates on messages if they are not already on them."
  [url msg & [options]]
  (time/msg->birthdate msg))

(defn complete!
  "Complete a task.  Tasks, once taken, are only temporarily invisible.
In order to remove a task from a queue it must be successfully completed."
  [url task & [options]]
  (lookup-provider
   url options
   (q-proto/complete! queue task options)))

(defn stats
  "Retrieve information about the queue.  Returns a map containing:
:in-flight (required, integer) - Number of items in flight in the queue."
  [url & [options]]
  (lookup-provider
   url options
   (q-proto/stats queue options)))

(defn delete-queue!
  "Delete a queue.  Not supported across all queue providers."
  [url & [options]]
  (let [url-parts (url/url->parts url)
        path (:path url)
        provider (q-proto/url-parts->provider url-parts
                                              (merge *static-queue-options*  options))]
    (q-proto/delete-queue! provider (keyword (last path)) options)))


(defn queue->task-seq-details
  "Convert a queue to a task sequence.  Processors must call complete! on each task
in order to remove it from the queue."
  [url {:keys [receive-message-wait-time-seconds]
        :or {receive-message-wait-time-seconds 10}
        :as options}]
  (lookup-provider
   url options
   (let [input-chan (async/chan)
         reader-thread (async/thread
                         (try
                           (->> (repeatedly #(let [take-result (take! url options)]
                                               (when-not (= :timeout take-result)
                                                 (async/>!! input-chan take-result))
                                               (async-protocols/closed? input-chan)))
                                ;;Take while true, then terminate thread
                                ;;Note that an exception will terminate the take thread.  This is by design.
                                (take-while not)
                                last)
                           (catch Throwable e
                             (println e)
                             (log/error e)))
                         (log/warn "queue thread exiting"))
         input-seq (parallel/async-channel-to-lazy-seq input-chan)]
     {:shutdown-fn (fn []
                     (async/close! input-chan)
                     (async/<!! reader-thread))
      :input-seq input-seq})))


(defn pmap-queue
  "Create an infinite sequence of results of reading from the queue.  Tasks that fail to process
will be reprocessed according to the visibility timeout member of options.  See protocols.clj.

Result will be infinite unless the queue itself gets deleted."
  [url num-threads processing-fn & [options]]
  (let [{:keys [shutdown-fn input-seq]} (queue->task-seq-details url options)
        process-fn (fn [task]
                     (try
                       (let [result (processing-fn (task->msg url task options))]
                         (complete! url task options)
                         result)
                       (catch Throwable e
                         (shutdown-fn)
                         (throw e))))]
    (parallel/queued-pmap num-threads process-fn input-seq)))


(defn seq->queue!
  "Place a sequence into a queue.  Blocking operation, exceptions are propagated to
callers."
  [url options data-seq]
  (->> data-seq
       (map #(put! url % options))
       dorun)
  :ok)
