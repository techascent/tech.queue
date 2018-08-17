(ns tech.queue.task
  "Generic things that can be executed on the queue.  Uses keyword dispatch"
  (:require [tech.queue.protocols :as q]))


(defn keyword->fn
  [kwd]
  (require (symbol (namespace kwd)))
  (resolve (symbol (namespace kwd) (name kwd))))


(defn msg->task-object
  [context msg]
  ((keyword->fn (:tech.queue.task/msg->obj msg)) context))


(defn var->keyword
  "Given a var, make a keyword that points to it"
  [var]
  (let [{:keys [ns name]} (meta var)]
    (keyword (clojure.core/name
              (clojure.core/ns-name ns))
             (clojure.core/name name))))


(defn add-processor-to-msg
  [task-obj-constructor-var msg]
  (assoc msg :tech.queue.task/msg->obj
         (var->keyword task-obj-constructor-var)))


;;Forward everything to something created via the message
(defrecord TaskQueueProcessor [dispatch-fn context]
  q/QueueProcessor
  (msg->log-context [this msg]
    (q/msg->log-context (dispatch-fn context msg) msg))
  (msg-ready? [this msg]
    (q/msg-ready? (dispatch-fn context msg) msg))
  (process! [this msg]
    (q/process! (dispatch-fn context msg) msg))
  (retire! [this msg last-attempt-result]
    (q/retire! (dispatch-fn context msg) msg last-attempt-result))

  q/PResourceLimit
  (resource-map [this msg initial-res]
    (q/resource-map (dispatch-fn context msg) msg initial-res)))


(defn task-processor
  [context & {:keys [dispatch-fn]
              :or {dispatch-fn msg->task-object}}]
  (->TaskQueueProcessor dispatch-fn context))


(defn put!
  [queue constructor-var msg options]
  (q/put! queue (add-processor-to-msg constructor-var msg) options))


(defrecord ForwardQueueProcessor [process-fn log-context-fn ready-fn retire-fn
                                  resource-map-fn]
  q/QueueProcessor
  (msg->log-context [this msg]
    (if log-context-fn
      (log-context-fn msg)
      {}))
  (msg-ready? [this msg]
    (if ready-fn
      (ready-fn msg)
      true))
  (process! [this msg]
    (process-fn msg)
    {:status :success
     :msg msg})
  (retire! [this msg last-attempt-result]
    (when retire-fn
      (retire-fn msg)))

  q/PResourceLimit
  (resource-map [this msg initial-res-map]
    (if resource-map-fn
      (resource-map-fn msg initial-res-map)
      {})))


(defn forward-queue-processor
  [process-fn & {:keys [log-context-fn ready-fn retire-fn
                        resource-map-fn]}]
  (->ForwardQueueProcessor process-fn log-context-fn ready-fn retire-fn
                           resource-map-fn))
