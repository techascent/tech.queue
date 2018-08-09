(ns tech.queue.providers
  (:require [tech.queue.protocols :as q-proto]
            [me.raynes.fs :as fs]
            [tech.io.url :as url]
            [tech.config.core :as config]
            [com.stuartsierra.component :as c]))


(def filesystem-provider
  (memoize
   (fn [dirname options]
     (fs/mkdirs dirname)
     (require 'tech.queue.filesystem)
     ((resolve 'tech.queue.filesystem/provider) dirname options))))


(def sqs-provider
  (memoize
   (fn [queue-prefix options]
     (require 'tech.queue.sqs)
     (let [sqs-provider ((resolve 'tech.queue.sqs/provider) queue-prefix
                         (assoc options
                                :tech.aws/endpoint
                                (config/get-config :tech-aws-endpoint)))]
       (if (config/get-config :tech-queue-sqs-vault-auth)
         (do
           (require 'tech.queue.auth)
           (-> ((resolve 'tech.queue.auth/vault-aws-auth-provider)
                (config/get-config :tech-vault-aws-path)
                sqs-provider
                options)
               c/start))
         sqs-provider)))))


(defmethod q-proto/url-parts->provider :file
  [url-parts options]
  (when-not (> (count (:path url-parts)) 1)
    (throw (ex-info "File queue urls must have a path greater than 1 part"
                    url-parts)))
  (filesystem-provider (url/string-seq->file-path (butlast (:path url-parts))) options))


(defmethod q-proto/url-parts->provider :sqs
  [url-parts options]
  (when-not (= (count (:path url-parts)) 2)
    (throw (ex-info "SQS queue urls must have a path of exactly 2"
                    url-parts)))
  (sqs-provider (first (:path url-parts)) options))
