(ns tech.queue.time)


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
