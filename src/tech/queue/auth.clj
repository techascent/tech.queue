(ns tech.queue.auth
  (:require [tech.queue.protocols :as q]
            [tech.io.auth :as io-auth]
            [com.stuartsierra.component :as c]))



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



(defrecord AuthProvider [cred-request-timeout-ms
                         re-request-time-ms
                         request-credentials-fn
                         src-cred-fn
                         src-provider]
  q/QueueProvider
  (get-or-create-queue! [this queue-name options]
    (->AuthQueue request-credentials-fn
                 (q/get-or-create-queue! src-provider queue-name
                                         (merge (request-credentials-fn) options))))
  (delete-queue! [this queue-name options]
    (q/delete-queue! src-provider queue-name (merge (request-credentials-fn) options)))

  c/Lifecycle
  (start [this]
    (io-auth/start-auth-provider this))

  (stop [this]
    (io-auth/stop-auth-provider this)))


(defn provider
  "You need to call com.stuartsierra.component/start on this to enable the credential request system."
  [cred-fn src-provider {:keys [cred-request-timeout-ms ;;How long credentials last
                                re-request-time-ms ;;How long to wait for a credential request before signalling error.
                                ]
                         :or {cred-request-timeout-ms 10000
                              ;;Save credentials for 20 minutes
                              re-request-time-ms (* 20 60 1000)}}]
  (->AuthProvider cred-request-timeout-ms re-request-time-ms
                  nil cred-fn src-provider))


(defn vault-aws-auth-provider
  [vault-path src-provider options]
  (require 'tech.vault-clj.core)
  (provider #(io-auth/get-vault-aws-creds vault-path) src-provider options))
