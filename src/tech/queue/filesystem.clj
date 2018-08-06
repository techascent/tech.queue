(ns tech.queue.filesystem
  (:require [durable-queue :as durable]
            [com.stuartsierra.component :as c]
            [me.raynes.fs :as fs]
            [tech.queue.protocols :as q]
            [tech.io.temp-file :as temp-file]
            [tech.io.url :as url])
  (:import [java.util Date UUID]))


(defn- queue-name-kwd->queue-filename
  [queue-name-kwd]
  (-> (name queue-name-kwd)
      (.replace "-" "_")))


(defrecord DurableQueue [queue-obj queue-name default-options]
  q/QueueProtocol
  (put! [this msg options]
    (durable/put! queue-obj queue-name (merge {::q/birthdate (Date.)}
                                               msg)))
  (take! [this options]
    (durable/take! queue-obj queue-name
                   (* 1000 (get (merge default-options options)
                                :receive-message-wait-time-seconds))
                   :timeout))
  (task->msg [this task] @task)
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

  (delete-queue! [this queue-name options] (throw (ex-info "Unimplemented" {}))))


(defn provider
  [queue-directory options]
  (->DurableQueueProvider queue-directory
                          (durable/queues queue-directory)
                          (atom {})
                          (merge q/default-create-options options)))


;;Deletes the queue directory on shutdown.
(defrecord TemporaryDurableQueueProvider [temp-dir default-options]
  c/Lifecycle
  (start [this]
    (if (:started? this)
      this
      (do
        (fs/mkdirs temp-dir)
        (assoc this
               :started? true
               :provider (provider temp-dir default-options)))))


  (stop [this]
    (if-not (:started? this)
      this
      (do
        (fs/delete-dir temp-dir)
        (dissoc this :started? :provider))))

  q/QueueProvider
  (get-or-create-queue! [this queue-name options]
    (q/get-or-create-queue! (get this :provider) queue-name (merge default-options options)))

  (delete-queue! [this queue-name options]
    (q/delete-queue! (get this :provider) queue-name (merge default-options options))))


(defn temp-provider
  [& {:keys [temp-dir]
      :as options}]
  (let [temp-dir (or temp-dir (-> (temp-file/random-file-url)
                                  url/url->parts
                                  url/parts->file-path))]
    (->TemporaryDurableQueueProvider temp-dir options)))
