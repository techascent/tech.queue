(ns tech.queue-test
  (:require [tech.queue :as q]
            [clojure.test :refer :all]
            [tech.io.temp-file :as temp-file]
            [tech.io.uuid :as uuid]))


(deftest process-queue
  (let [src-seq (->> (range 100)
                     (map (partial hash-map :value)))
        update-fn #(update % :value (partial + 10))
        dest-seq (mapv update-fn src-seq)]
    (temp-file/with-temp-dir
      temp-dir
      (let [queue-url (str "file://" temp-dir "/" (uuid/random-uuid-str))]
        (q/seq->queue! queue-url {} src-seq)
        (is (set dest-seq)
            (->> (q/pmap-queue queue-url 10 update-fn)
                 ;;Take is important as the sequence never ends
                 (take (count src-seq))
                 set))))))
