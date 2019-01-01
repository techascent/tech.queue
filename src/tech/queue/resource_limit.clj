(ns tech.queue.resource-limit
  "It is not uncommon for a machine that is running heterogeneous tasks
to have different tasks take vastly different amounts of resources.
Some may use a lot of ram but no cpu, etc.
Resource limit is a generic way to limit the amount of resources the
system can use assuming they can be statically defined for the task.

In our case resources are represented by an integer and as long as
subtracting that number from the system's resources returns a result
above or equal to zero we assume that those resources are available.

A resource map can contain many entries of keyword->integer.

At manager creation time a similar map is provided indicating the initial state."
  (:require [com.stuartsierra.component :as c]
            [clojure.core.async :as async]
            [taoensso.timbre :as log])
  (:import [java.lang.management ManagementFactory]))


(defprotocol PResourceManager
  (request-resources! [mgr resource-map timeout-ms]
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
  {:system-free-space-MB (-> (ManagementFactory/getOperatingSystemMXBean)
                             (.getTotalPhysicalMemorySize)
                             (quot 0x100000)) })


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
  [mgr resource-map timeout & body]
  `(do
     (if (= :success (request-resources! ~mgr ~resource-map ~timeout))
       (try
         ~@body
         (finally
           (release-resources! ~mgr ~resource-map)))
       (throw (ex-info "Timeout" {:timeout ~timeout})))))


(defn- check-resource-amounts!
  [initial-resources resource-map]
  (when-let [invalid-resources
             (->> resource-map
                  (remove (fn [[k v]]
                            (and (contains? initial-resources k)
                                 (number? v)
                                 (number? (get initial-resources k))
                                 (<= v (get initial-resources k)))))
                  seq)]
    (throw (ex-info "Invalid resources, either not specified initially or
larger than initial amounts"
                    {:initial-resources initial-resources
                     :resource-request resource-map}))))


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

(defn- loop-release-resources
  [current-resources* min-resources* resource-map notify-chan]
  (loop [current-res @current-resources*]
    (if (compare-and-set! current-resources*
                          current-res
                          (release-resource-map current-res resource-map))
      (do
        (swap! min-resources* store-min-resources current-res)
        (when notify-chan
          (async/>!! notify-chan :released))
        :ok)
      (recur @current-resources*))))


(defn- resource-thread-loop
  [current-resources* min-resources* request-chan notify-chan]
  (loop []
    (let [continue?
          (try
            ;;Loop exits if this request chan closes.  Else it always recurs.
            (let [value (async/alt!!
                          request-chan ([result] result)
                          (async/timeout 50) :timeout
                          notify-chan :notify)]
              (cond
                (= value :timeout)
                true
                (= value :notify)
                true
                :else
                (when value
                  (let [{:keys [resource-map result-chan status-atom]} value]
                    ;;Inner loop exits when resources may be successfully allocated
                    ;;else spins waiting for notification that resources were
                    ;;released.
                    (loop [current-resources @current-resources*
                           current-status @status-atom]
                      (if (= current-status :waiting)
                        (let [request-result (request-resource-map
                                              current-resources
                                              resource-map)]
                          (if (and request-result
                                   (compare-and-set! current-resources*
                                                     current-resources
                                                     request-result))
                            (do
                              (let [successful? (compare-and-set! status-atom :waiting
                                                                  :success)]
                                (async/close! result-chan)
                                (when-not successful?
                                    ;;Timeout on request thread happened
                                    (loop-release-resources current-resources*
                                                            min-resources*
                                                            resource-map
                                                            nil))))
                            (do
                              ;;If we reset the status atom but failed to get resources
                              ;;then we reset it back.  Only set it back, however, if
                              ;;it's current status is satisfied.  It could be :timeout
                              (async/<!! notify-chan)
                              (recur @current-resources* @status-atom)))))))
                  true)))
            (catch Throwable e
              (log/error e)
              (Thread/sleep 2000)
              true))]
      (when continue?
        (recur))))
  (log/warn "resource limiter thread exit")
  (loop [item (async/<!! request-chan)]
    (when item
      (async/>!! (:result-chan item) (ex-info "resource limit thread exit" {}))
      (recur (async/<!! request-chan)))))


;; There is an implementation of the resource manager that uses a map of semaphores.
;; This may seem simpler in that it avoids the need for the resource thread.
;; It is, however, impossible to guarantee FIFO behavior in that case as each semaphore
;; is grabbed in a granular fashion instead of atomically in a transaction.  Without FIFO
;; you do run into the very possible case of starvation.  Thus we move to a more complex
;; implementation that attempts to guarantee transactional semantics and strict FIFO
;; behavior.
(defrecord ResourceManager [initial-resources request-chan notify-chan
                            current-resources*
                            min-resources*]
  PResourceManager
  (request-resources! [mgr resource-map timeout-ms]
    (when-not request-chan
      (throw (ex-info "Uninitialized" {})))
    (check-resource-amounts! initial-resources resource-map)
    (let [result-chan (async/chan)
          status-atom (atom :waiting)
          timeout-chan (async/timeout timeout-ms)
          go-chan (async/go
                    ;;The put request returns false when it fails.
                    (if-let [result (async/>! request-chan {:resource-map resource-map
                                                            :result-chan result-chan
                                                            :status-atom status-atom})]
                      (async/<! result-chan)
                      :request-chan-closed))
          result (async/alt!!
                   go-chan ([value] value)
                   timeout-chan :timeout)]
      (cond
        (= result :timeout)
        ;;The request thread *just* completed
        (if (compare-and-set! status-atom :waiting :timeout)
          (do
            (async/close! result-chan)
            :timeout)
          (do
            (let [val @status-atom]
              (when-not (= val :success)
                (throw (ex-info "Unexpected status:"
                                {:status val}))))
            :success))
        ;;The request thread completed within the timeout
        (nil? result)
        :success
        ;;The request thread indicated exit or error.
        :else
        (throw (ex-info "Request failed" {:result result})))))

  (release-resources! [mgr resource-map]
    (loop-release-resources current-resources* min-resources* resource-map notify-chan))


  c/Lifecycle
  (start [this]
    (if-not (:thread this)
      (let [request-chan (async/chan)
            notify-chan (async/chan)
            current-res* (atom initial-resources)
            min-resources* (atom {})
            thread (async/thread
                     (resource-thread-loop current-res*
                                           min-resources*
                                           request-chan notify-chan))]
        (assoc this
               :request-chan request-chan
               :notify-chan notify-chan
               :current-resources* current-res*
               :min-resources* min-resources*
               :thread thread))
      this))
  (stop [this]
    (if (:thread this)
      (do
        (async/close! request-chan)
        (async/close! notify-chan)
        ;;Wait for thread exit
        (async/<!! (:thread this))
        (dissoc this
                :request-chan
                :notify-chan
                :active-cores
                :thread
                :current-resources*
                :min-resources*))
      this)))


(defn resource-manager
  [{:keys [initial-resources]
    :or {initial-resources (default-resource-map)}}]
  (map->ResourceManager {:initial-resources initial-resources}))
