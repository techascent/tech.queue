(ns tech.queue.resource-limit-test
  (:require [tech.queue.resource-limit :as rl]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as c]))


(deftest resource-limit
  (let [manager (c/start (rl/resource-manager {:initial-resources
                                               {:froodles 10
                                                :boodles 150}}))
        count-atom (atom 0)
        max-atom (atom 0)]
    (try
      (let [number-range 1000
            result (->> (range number-range)
                        (pmap (fn [idx]
                                (rl/with-resources manager {:froodles 1
                                                            :boodles 50}
                                  (let [items (swap! count-atom inc)]
                                    (swap! max-atom max items)
                                    (Thread/sleep 10)
                                    (swap! count-atom dec))
                                  (inc idx))))
                        (apply +))
            answer (->> (range number-range)
                        (map inc)
                        (apply +))]
        (is (= answer result))
        (is (<= 3 @max-atom)))
      (finally
        (c/stop manager)))))