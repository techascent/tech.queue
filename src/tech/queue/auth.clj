(ns tech.queue.auth
  (:require [tech.queue.protocols :as q]
            [tech.io.auth :as io-auth]))



(defrecord AuthQueue [request-credentials-fn src-queue]
  q/QueueProtocol
  (put! [this msg options]
    (q/put! src-queue msg (merge (request-credentials-fn) options)))
  (take! [this options]
    (q/take! src-queue (merge (request-credentials-fn) options)))
  (task->msg [this task]
    (q/task->msg src-queue task))
  (complete! [this task options]
    (q/complete! src-queue task (merge (request-credentials-fn) options)))
  (stats [this options]
    (q/stats src-queue (merge (request-credentials-fn) options))))



(defrecord AuthProvider [re-request-time-ms
                         request-credentials-fn
                         src-cred-fn
                         src-provider]
  q/QueueProvider
  (get-or-create-queue! [this queue-name options]
    (->AuthQueue request-credentials-fn
                 (q/get-or-create-queue! src-provider queue-name
                                         (merge (request-credentials-fn) options))))
  (delete-queue! [this queue-name options]
    (q/delete-queue! src-provider queue-name (merge (request-credentials-fn) options))))


(defn provider
  "You need to call com.stuartsierra.component/start on this to enable the credential request system."
  [cred-fn src-provider {:keys [
                                re-request-time-ms ;;How long to wait for a credential request before signalling error.
                                ]
                         :or {
                              ;;Save credentials for 20 minutes
                              re-request-time-ms (* 20 60 1000)}}]
  (let [cred-atom (atom {})
        request-cred-fn #(io-auth/get-credentials re-request-time-ms cred-fn cred-atom)]
    (->AuthProvider re-request-time-ms
                    request-cred-fn cred-fn src-provider)))


(defn vault-aws-auth-provider
  [vault-path src-provider options]
  (provider #(io-auth/get-vault-aws-creds vault-path options)
            src-provider options))
