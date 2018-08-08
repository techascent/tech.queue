(ns tech.queue.sqs
  "Provider for amazon sqs"
  (:require [amazonica.aws.sqs :as sqs]
            [tech.queue.protocols :as q-proto]
            [clojure.string :as s]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as c]
            [taoensso.timbre :as log]
            [tech.queue.time :as time])
  (:import [java.util Date UUID]
           [com.amazonaws.services.sqs.model QueueDoesNotExistException]))


(defn- keyword->camel-case
  [normal-kwd]
  (let [kwd-name (name normal-kwd)
        parts (s/split kwd-name #"-")]
    (->> parts
         (map (fn [^String part]
                (str (.toUpperCase (.substring part 0 1))
                     (.substring part 1))))
         (s/join)
         keyword)))

(def cred-map {:tech.aws/access-key :access-key
               :tech.aws/secret-key :secret-key
               :tech.aws/session-token :session-token
               :tech.aws/endpoint :endpoint})

(def cred-map-keys (set (keys cred-map)))

(def queue-att-keys (set (keys q-proto/default-create-options)))


(defn- filter-keys
  [key-set data-map]
  (when-let [item-seq (->> data-map
                           (filter (comp key-set first))
                           seq)]
    (into {} item-seq)))


(defn- remap-keys
  [remap-map data]
  (when data
    (->> data
         (map (fn [[k v]]
                [(remap-map k) v]))
         (into {}))))


(defn- call-aws-fn
  [fn options & args]
  (if-let [creds (->> (filter-keys cred-map-keys options)
                      (remap-keys cred-map))]
    (do
      (apply fn creds args))
    (apply fn args)))


(defrecord SQSQueue [default-options queue-url]
  q-proto/QueueProtocol
  (put! [this msg options]
    (call-aws-fn sqs/send-message (merge default-options options)
                 :queue-url queue-url
                 :message-body (pr-str (time/add-birthdate msg))))
  (take! [this options]
    (if-let [msg (-> (call-aws-fn sqs/receive-message (merge default-options options)
                                  :queue-url queue-url
                                  :max-number-of-messages 1)
                     :messages
                     first)]
      msg
      :timeout))
  (task->msg [this task] (edn/read-string (:body task)))
  (complete! [this task options]
    (call-aws-fn sqs/delete-message (merge default-options options)
                 (assoc task :queue-url queue-url)))
  (stats [this options]
    (let [attributes (call-aws-fn sqs/get-queue-attributes (merge default-options options)
                                  :queue-url queue-url
                                  :attribute-names ["All"])
          get-att #(Integer/parseInt
                      (get attributes %))]
          {:in-flight (+ (get-att :ApproximateNumberOfMessagesNotVisible)
                         (get-att :ApproximateNumberOfMessages))})))


(defn- get-or-create-queue!
  [queue-name options]
  (:queue-url
   (try
     (call-aws-fn sqs/get-queue-url options queue-name)
     (catch QueueDoesNotExistException e
       (call-aws-fn sqs/create-queue options
                         :queue-name queue-name
                         :attributes (->> (merge q-proto/default-create-options
                                                 options)
                                          (filter-keys queue-att-keys)
                                          (map (fn [[k v]]
                                                 [(keyword->camel-case k) v]))
                                          (into {})))))))

(defn- queue-name->real-name
  [queue-prefix name-kwd]
  (str queue-prefix (name name-kwd)))


(defrecord SQSQueueProvider [queue-prefix *queues default-options]
  q-proto/QueueProvider
  (get-or-create-queue! [this queue-name options]
    (let [real-queue-name (queue-name->real-name queue-prefix queue-name)]
      (if-let [retval (get @*queues queue-name)]
        retval
        (let [queue-url (get-or-create-queue! real-queue-name
                                              (merge default-options
                                                     options))
              retval (->SQSQueue default-options queue-url)]
          (swap! *queues assoc queue-name retval)
          retval))))
  (delete-queue! [this queue-name options]
    (call-aws-fn sqs/delete-queue (merge default-options options)
                 (queue-name->real-name queue-prefix queue-name))))


(defn provider
  "Using a queue prefix allows you to easily setup IAM roles and restrict
  environments to a subset of queues (ones that start with prefix)."
  [queue-prefix options]
  (->SQSQueueProvider queue-prefix (atom {}) options))


(defrecord TempSQSQueueProvider [src-provider queue-set-atom]
  c/Lifecycle
  (start [this]
    (assoc this :started? true))
  (stop [this]
    (log/info (str "STOPPING TEMP QUEUE" @queue-set-atom " " (:started? this)))
    (when (:started? this)
      (doseq [queue-name @queue-set-atom]
        (try
          (log/info (str "Deleting queue - " queue-name))
          (q-proto/delete-queue! src-provider queue-name {})
          ;;Ignore this error.  If we fire up multiple systems based on this provider
          ;;then stop will get called multiple times leading to us trying to delete
          ;;the same sqs queue multiple times.
          (catch Throwable e
            (println "queue delete failed-most likely not a problem" queue-name)
            nil)))
      (reset! queue-set-atom #{}))
    (dissoc this :started?))

  q-proto/QueueProvider
  (get-or-create-queue! [this queue-name options]
    (let [retval (q-proto/get-or-create-queue! src-provider queue-name options)]
      (swap! queue-set-atom conj queue-name)
      retval))

  (delete-queue! [this queue-name options]
    (q-proto/delete-queue! src-provider queue-name options)
    (swap! queue-set-atom disj queue-name)))


(defn temp-provider
  [src-provider]
  (->TempSQSQueueProvider src-provider (atom #{})))
