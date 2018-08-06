(ns tech.queue.logging
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [clojure.pprint]))

(s/def ::log-context
  (fn [item]
    (every? (complement coll?) (vals item))))


(defmacro merge-context
  [context-map & body]
  `(let [validation-error# (try
                             (log/log-and-rethrow-errors
                               (s/assert ::log-context ~context-map))
                             nil
                             (catch Throwable e#
                               (clojure.pprint/pprint {"####Error merging context####"
                                                       e#})
                               e#))]
     ;;If the logging context has a validation error, just do the body without the context
     ;;Else use the validated context map as the context.
     (if validation-error#
       (do
         ~@body)
       (log/with-context (merge log/*context* ~context-map)
         ~@body))))
