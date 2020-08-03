(ns tech.queue.logging
  (:require [clojure.tools.logging :as log]))



(defmacro merge-context
  "TODO - figure out how to add structured logging to log4j setups."
  [context-data & body]
  `(do ~@body))
