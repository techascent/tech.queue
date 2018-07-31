(ns tech.queue.filesystem
  (:require [durable-queue :as durable]
            [com.stuartsierra.component :as c]
            [me.raynes.fs :as fs]
            [tech.queue.protocols :as q])
  (:import [java.util Date UUID]))


(defn- queue-name-kwd->queue-filename
  [queue-name-kwd]
  (-> (name queue-name-kwd)
      (.replace "-" "_")))


(defrecord DurableQueue [queue-obj queue-name queue-options]
  q/QueueProtocol
  (put! [this msg options]
    (durable/put! queue-obj queue-name (merge {::q/birthdate (Date.)}
                                               msg)))
  (take! [this options]
    (durable/take! queue-obj queue-name
             (* 1000 (get queue-options
                          :receive-message-wait-time-seconds))
             :timeout))
  (task->msg [this task options] @task)
  (msg->birthdate [this msg] (::q/birthdate msg))
  (complete! [this task options]
    (durable/complete! task))
  (stats [this options]
    (-> queue-obj
        durable/stats
        (get (queue-name-kwd->queue-filename queue-name))
        ((fn [queue-stats]
           ;;If the queue has never seen any data then it will return an empty map.

           {:in-flight (if (:enqueued queue-stats)
                         (- (:enqueued queue-stats)
                            (:completed queue-stats))
                         0)})))))


(defrecord DurableQueueProvider [queue-directory queue-obj *queues default-options]
  q/QueueProvider
  (get-or-create-queue! [this queue-name create-options]
    (if-let [retval (get @*queues queue-name)]
      retval
      (do
        (swap! *queues assoc queue-name (->DurableQueue queue-obj queue-name
                                                        (merge default-options
                                                               create-options)))
        (get @*queues queue-name))))

  (delete-queue! [this queue-name]))


(defn provider
  [queue-directory options]
  (->DurableQueueProvider queue-directory
                          (durable/queues queue-directory)
                          (atom {})
                          (merge q/default-create-options options)))


;;Deletes the queue directory on shutdown.
(defrecord TemporaryDurableQueueProvider [temp-dir]
  c/Lifecycle
  (start [this] this)

  (stop [this]
    (when (:temp-dir this)
      (fs/delete-dir (:temp-dir this))
      (dissoc this :provider)))

  q/QueueProvider
  (get-or-create-queue! [this queue-name create-options]
    (q/get-or-create-queue! (get this :provider) queue-name create-options))

  (delete-queue! [this queue-name]
    (q/delete-queue! (get this :provider) queue-name)))


(defn temp-provider
  [& {:keys [temp-dir-stem]
      :or {temp-dir-stem "/tmp/test-queues/"}
      :as options}]
  (let [temp-dir (str temp-dir-stem (UUID/randomUUID) "/")]
    (fs/mkdir temp-dir)
    (assoc (->TemporaryDurableQueueProvider temp-dir)
           :provider (provider temp-dir options))))
