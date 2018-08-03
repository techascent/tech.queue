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
            [clojure.core.async :as async]
            [taoensso.timbre :as log])
  (:import [oshi SystemInfo]
           [java.util.concurrent Semaphore]))


(defprotocol PResourceManager
  (request-resources! [mgr resource-map]
    "potentially blocking call, returns when resources can be satisfied")
  (release-resources! [mgr resource-map]
    "nonblocking returns resources to the source"))


(defn jvm-resources
  []
  {:num-cores (.availableProcessors (Runtime/getRuntime))
   :jvm-free-space-MB (/ (.maxMemory (Runtime/getRuntime))
                         0x100000)})

(defn system-resources
  []
  (let [si (SystemInfo.)
        hw (.getHardware si)
        gm (.getMemory hw)]
    {:system-free-space-MB (quot (.getTotal gm)
                                 0x100000)}))


(defn megabyte->byte
  [^long meg]
  (* meg 0x100000))


(defn gigabyte->megabyte
  [^long gig]
  (* gig 1024))


(defn default-resource-map
  [& {:keys [unavailable-system-mem]
      :or {unavailable-system-mem (gigabyte->megabyte 1)}}]
  (let [system-map (system-resources)
        jvm-map (jvm-resources)
        system-mem (long (:system-free-space-MB system-map))]
    {:num-cores (:num-cores jvm-map)
     :system-memory-MB (max 0 (- system-mem unavailable-system-mem
                                 (:jvm-free-space-MB jvm-map)))}))


(defmacro with-resources
  [mgr resource-map & body]
  `(do
     (request-resources! ~mgr ~resource-map)
     (try
       ~@body
       (finally
         (release-resources! ~mgr ~resource-map)))))


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


(defn- store-resources
  [boundary-map
   current-resources
   op]
  (->> current-resources
       (reduce (fn [boundary-map [map-key map-val]]
                 (let [existing (get boundary-map map-key)]
                   (assoc boundary-map map-key
                          (if existing
                            (op existing map-val)
                            map-val))))
               boundary-map)))


(defn- store-min-resources
  [min-resources current-resources]
  (store-resources min-resources current-resources min))


(defn- store-max-resources
  [max-resources current-resources]
  (store-resources max-resources current-resources max))


(defn- check-resource-amounts!
  [initial-resources resource-map]
  (when-let [invalid-resources
             (->> resource-map
                  (filter (fn [[k v]]
                            (when-not (or (contains? initial-resources k)
                                          (> v (get initial-resources k))))))
                  seq)]
    (throw (ex-info "Invalid resources, either not specified initially or
larger than initial amounts"
                    {:initial-resources initial-resources
                     :resource-request resource-map}))))

(defn- map-semaphore-action!
  [action current-resources resource-map]
  (->> resource-map
       (map (fn [[k v]]
              (when-not (>= (int v) 0)
                (throw (ex-info "Resource count greater than zero"
                                {:initial resource-map
                                 :key k
                                 :value v})))
              (action (get current-resources k) (int v))))
       dorun)
  :ok)


(defrecord ResourceManager [initial-resources
                            current-resources]
  PResourceManager
  (request-resources! [mgr resource-map]
    (check-resource-amounts! initial-resources resource-map)
    (map-semaphore-action! #(.acquire ^Semaphore %1 %2) current-resources resource-map)
    :ok)

  (release-resources! [mgr resource-map]
    (map-semaphore-action! #(.release ^Semaphore %1 %2) current-resources resource-map)
    :ok)


  c/Lifecycle
  (start [this]
    (if-not (:current-resources this)
      (assoc this :current-resources
             (->> initial-resources
                  (map (fn [[k v]]
                         (when-not (>= (int v) 0)
                           (throw (ex-info "Value out of range" {:initial initial-resources
                                                                 :key k
                                                                 :value v})))
                         [k (Semaphore. (int v) true)]))
                  (into {})))
      this))
  (stop [this]
    (dissoc this :current-resources)))

(defn resource-manager
  [{:keys [initial-resources]
    :or {initial-resources (default-resource-map)}}]
  (map->ResourceManager {:initial-resources initial-resources}))
