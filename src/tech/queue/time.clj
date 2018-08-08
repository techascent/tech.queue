(ns tech.queue.time
  (:import [java.util Date]
           [java.time Duration]))


(defn minutes->seconds
  ^long [minutes]
  (long (* (double minutes) 60)))


(defn hours->seconds
  ^long [hrs]
  (long
   (Math/round
    (* (double hrs) 3600))))

(defn days->seconds
  ^long [days]
  (hours->seconds
   (* (double days) 24)))


(defn add-birthdate
  [msg]
  (merge (merge {:tech.queue.protocols/birthdate (Date.)}
                msg)))


(defn msg->birthdate
  [msg]
  (or (:tech.queue.protocols/birthdate msg)
      (:fsof.components.queue/birthdate msg)))


(defn seconds-since
  [previous-date & [current-date]]
  (long
   (->
    (Duration/between
     (.toInstant previous-date)
     (.toInstant ^Date (or current-date (Date.))))
    (.getSeconds))))
