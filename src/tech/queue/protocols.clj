(ns tech.queue.protocols
  (:require [tech.queue.time :as qt]))


(defprotocol QueueProvider
  (get-or-create-queue! [this queue-name options])
  (delete-queue! [this queue-name options]))


(defprotocol QueueProtocol
  (put! [this msg options])
  (take! [this options]
    "take should return the value :timeout *if* the receive has timed out.
Else it should return the next task in the queue")
  (task->msg [this task])
  (complete! [this task options])
  (stats [this options]))


(def default-create-options
  {;;length of time to delay delivery of message in queue.
   :delay-seconds 0 ;;seconds

   :maximum-message-size 262144 ;;256KiB
   ;;Time the message is retained
   :message-retention-period (qt/days->seconds 4)
   ;;length of time a receive message waits if not specified
   :receive-message-wait-time-seconds 0 ;;seconds
   ;;Time the message is invisible during processing
   :visibility-timeout 30 ;;seconds
   })


(defprotocol QueueProcessor
  (msg->log-context [this msg]
    "Given a message return a map of context that should be included with every log message")
  (msg-ready? [this msg]
    "Boolean as to whether the system is ready to process this message.  Defaults to (constantly
    true)")
  (process! [this msg]
    "Process this message.  Errors are captured and logged and the message will be retried until
    its ttl is up.
This must return a map that contains:
{:status - one of :error, :not-ready? :success
 :msg - an updated message.
}")
  (retire! [this msg last-attempt-result]
    "Return a message.  If the last time the system attempted to process this message then the
    error returned
is captured in last-exception.
last-attempt-result:
{
 :status - :not-ready? or :error
 :error - last error returned due to processing
}"))


(defprotocol PResourceLimit
  (resource-map [this msg initial-resource-map]
    "Resources this task needs.
Map may be empty, or may contain whatever resources are defined in the
resource manager.
Default resources would be (resource-limit/default-resource-map):
{:num-cores (integer)
 :system-memory-MB (integer}"))


(defmulti url-parts->provider (fn [url-parts options]
                                (:protocol url-parts)))
