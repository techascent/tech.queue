(ns tech.queue
  (:require [tech.io.url :as url]
            [tech.queue.protocols :as q-proto]
            [tech.queue.providers :as providers]))


(def ^:dynamic *url-parts->queue*
  (fn [url-parts options]
    (let [path (:path url-parts)
          provider (q-proto/url-parts->provider url-parts {})]
      (q-proto/get-or-create-queue! provider
                                    (keyword (last path))
                                    options))))

(defmacro ^:private lookup-provider
  [url options & body]
  `(let [~'url-parts (url/url->parts ~url)
         ~'queue (*url-parts->queue* ~'url-parts ~options)]
     ~@body))


(defn put!
  [url msg & {:as options}]
  (lookup-provider
   url options
   (q-proto/put! queue msg options)))

(defn take!
  [url & {:as options}]
  (lookup-provider
   url options
   (q-proto/take! queue options)))

(defn task->msg
  [url task & {:as options}]
  (lookup-provider
   url options
   (q-proto/task->msg queue task)))

(defn msg->birthdate
  [url msg & {:as options}]
  (lookup-provider
   url options
   (q-proto/msg->birthdate queue msg)))

(defn complete!
  [url task & {:as options}]
  (lookup-provider
   url options
   (q-proto/complete! queue task options)))

(defn stats
  [url & {:as options}]
  (lookup-provider
   url options
   (q-proto/stats queue options)))
