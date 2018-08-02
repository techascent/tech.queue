(ns tech.queue.resource-limit
  "It is not uncommon for a machine that is running heterogeneous tasks
to have different tasks take vastly different amounts of resources.
Some may use a lot of ram but no cpu, etc.
Resource limit is a generic way to limit the amount of resources the
system can use assuming they can be statically defined before a particular
task is finished.

On our case resources are represented by a number and as long as
subtracting that number from the system's resources returns a result
above or equal to zero we assume that those resources are available.

A resource map can contain many entries of keyword->integer.
  At manager creation time a similar map is provided indicating the initial state."
  (:require [com.stuartsierra.component :as c]
            [clojure.core.async :as async])
  (:import [oshi SystemInfo]))


(defprotocol PResourceManager
  (request-resources! [mgr resource-map options]
    "potentially blocking call, returns when resources can be satisfied")
  (release-resources! [mgr resource-map options]
    "nonblocking returns resources to the source"))


(defn jvm-resources
  []
  {:num-cores (.availableProcessors (Runtime/getRuntime))
   :jvm-free-space (.maxMemory (Runtime/getRuntime))})

(defn system-resources
  []
  (let [si (SystemInfo.)
        hw (.getHardware si)
        gm (.getMemory hw)]
    {:system-free-space (.getTotal gm)}))


(defn megabyte->byte
  [^long meg]
  (* meg 0x100000))


(defn gigabyte->byte
  [^long gig]
  (-> (* gig 1024)
      megabyte->byte))


(defn default-resource-map
  [& {:keys [unavailable-system-mem]
      :or {unavailable-system-mem (gigabyte->byte 1)}}]
  (let [system-map (system-resources)
        jvm-map (jvm-resources)
        system-mem (long (:system-free-space system-map))]
    {:num-cores (:num-cores jvm-map)
     :system-memory (max 0 (- system-mem unavailable-system-mem (:jvm-free-space jvm-map)))}))


(defmacro with-resources
  [mgr resource-map & body]
  `(do
     (request-cores ~mgr ~resource-map)
     (try
       ~@body
       (finally
         (release-cores ~mgr ~resource-map)))))


(defn- release-resource-map
  "Adds resources back to the current resource map"
  [current-resources resource-map]
  (reduce (fn [current-resources [res-name res-amount]]
            (update current-resources res-name #(+ res-amount %)))
          current-resources
          resource-map))


(defn- request-resource-map
  "Request resources from map.  Returns nil upon failure."
  [current-resources resource-map]
  (when (every? #(>= (- (get current-resources (first %))
                        (second %))
                     0)
                (seq resource-map))
    (reduce (fn [current-resources [res-name res-amount]]
              (update current-resources res-name #(- % res-amount)))
            current-resources
            resource-map)))


(defrecord ResourceManager [initial-resources request-chan notify-chan
                            current-resources*
                            max-resources*
                            min-resources*]
  PResourceManager
  (request-resources! [mgr resource-map options]
    (when-not request-chan
      (throw (ex-info "Uninitialized" {})))
    (let [result-chan (async/chan)]
      ;;The put request returns false when it fails.
      (when-let [result (async/>!! request-chan {:resource-map resource-map
                                                 :result-chan result-chan})]
        (let [error (async/<!! result-chan)]
          (when error
            (throw (ex-info "thread interrupted"
                            {:error error})))))))

  (release-resources! [mgr resource-map options]
    (swap! min-released-cores min
           (swap! active-cores #(- % core-count)))
    (async/>!! notify-chan 1))

  )
