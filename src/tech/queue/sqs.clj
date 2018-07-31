(ns tech.queue.sqs
  "Provider for amazon sqs"
  (:require [amazonica.aws.sqs :as sqs]
            [tech.queue.protocols :as q-proto]
            [clojure.string :as s]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as c])
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

(def cred-map-keys #{:tech.aws/access-key :tech.aws/secret-key
                     :tech.aws/session-token :tech.aws/endpoint})

(def queue-att-keys (set (keys q-proto/default-create-options)))


(defn- filter-keys
  [key-set data-map]
  (when-let [item-seq (->> data-map
                           (filter (comp key-set first))
                           seq)]
    (into {} item-seq)))

(defn- call-aws-fn
  [fn options & args]
  (if-let [creds (filter-keys cred-map-keys options)]
    (apply fn creds args)
    (apply fn args)))


(defrecord SQSQueue [default-options queue-url]
  q-proto/QueueProtocol
  (put! [this msg ]
    (call-aws-fn sqs/send-message )
    (sqs/send-message {:tech.aws/endpoint endpoint}
                      :queue-url queue-url
                      :message-body (pr-str (merge {::q-proto/birthdate (Date.)}
                                                   msg))))
  (take! [this]
    (if-let [msg (-> (sqs/receive-message {:tech.aws/endpoint endpoint}
                                          :queue-url queue-url
                                          :max-number-of-messages 1)
                     :messages
                     first)]
      msg
      :timeout))
  (task->msg [this task] (edn/read-string (:body task)))
  (msg->birthdate [this task]
    (::q-proto/birthdate task))
  (complete! [this task]
    (sqs/delete-message {:tech.aws/endpoint endpoint} (assoc task :queue-url queue-url)))
  (stats [_]
    (let [attributes (sqs/get-queue-attributes {:tech.aws/endpoint endpoint}
                                               :queue-url queue-url
                                               :attribute-names ["All"])
          get-att #(Integer/parseInt
                      (get attributes %))]
          {:in-flight (+ (get-att :ApproximateNumberOfMessagesNotVisible)
                         (get-att :ApproximateNumberOfMessages))})))


(defn- get-or-create-queue!
  [queue-name region create-options]
  (:queue-url
   (try
     (sqs/get-queue-url {:tech.aws/endpoint region} queue-name)
     (catch QueueDoesNotExistException e
       (sqs/create-queue {:tech.aws/endpoint region}
                         :queue-name queue-name
                         :attributes (->> (merge q-proto/default-create-options
                                                 create-options)
                                          (map (fn [[k v]]
                                                 [(keyword->camel-case k) v]))
                                          (into {})))))))

(defn- queue-name->real-name
  [queue-prefix name-kwd]
  (str queue-prefix (name name-kwd)))


(defrecord SQSQueueProvider [queue-prefix region *queues options]
  q-proto/QueueProvider
  (get-or-create-queue! [this queue-name create-options]
    (let [real-queue-name (queue-name->real-name queue-prefix queue-name)]
      (if-let [retval (get @*queues queue-name)]
        retval
        (let [queue-url (get-or-create-queue! real-queue-name region (merge options
                                                                            create-options))
              retval (->SQSQueue region queue-url)]
          (swap! *queues assoc queue-name retval)
          retval))))
  (delete-queue! [this queue-name]
    (sqs/delete-queue {:tech.aws/endpoint region} (queue-name->real-name queue-prefix queue-name))))


(defn provider
  [& {:keys [queue-prefix region queues]
      :or {queue-prefix "dev-"
           region "us-west-2"
           queues {}}}]
  (->SQSQueueProvider queue-prefix region (atom queues)))


(defrecord TempSQSQueue [provider create-options]
  c/Lifecycle
  (start [this]
    (let [queue-name (keyword (str (UUID/randomUUID)))]
      (assoc this
             :queue (q-proto/get-or-create-queue! provider queue-name create-options)
             :queue-name queue-name)))
  (stop [this]
    (when-let [queue (get this :queue)]
      (q-proto/delete-queue! provider (:queue-name this)))
    (dissoc this :queue :queue-name))

  q-proto/QueueProtocol
  (put! [this msg] (q-proto/put! (:queue this) msg))
  (take! [this] (q-proto/take! (:queue this)))
  (task->msg [this task] (q-proto/task->msg (:queue this) task))
  (msg->birthdate [this task] (q-proto/msg->birthdate (:queue this) task))
  (complete! [this task] (q-proto/complete! (:queue this) task))
  (stats [this] (q-proto/stats (:queue this))))


(defrecord TempSQSQueueProvider []
  c/Lifecycle
  (start [this] this)
  (stop [this]
    (when (:temp-queue-prefix this)
      (let [provider (get this :provider)]
        (doseq [queue-name (keys @(get provider :*queues))]
          (try
            (q-proto/delete-queue! provider queue-name)
            ;;Ignore this error.  If we fire up multiple systems based on this provider
            ;;then stop will get called multiple times leading to us trying to delete
            ;;the same sqs queue multiple times.
            (catch Throwable e
              (println "queue delete failed-most likely not a problem" queue-name)
              nil))))))

  q-proto/QueueProvider
  (get-or-create-queue! [this queue-name create-options]
    (q-proto/get-or-create-queue! (get this :provider) queue-name create-options))

  (delete-queue! [this queue-name]
    (q-proto/delete-queue! (get this :provider) queue-name)))


(defn temp-provider
  [& {:keys [prefix region]
      :or {prefix "dev-temp-queues"
           region "us-west-2"}}]
  (let [temp-queue-prefix (str prefix "-" (UUID/randomUUID) "-")]
      (assoc (->TempSQSQueueProvider)
             :temp-queue-prefix temp-queue-prefix
             :provider (provider :queue-prefix temp-queue-prefix
                                 :region region))))
