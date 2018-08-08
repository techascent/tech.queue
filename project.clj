(defproject techascent/tech.queue "0.1.9-SNAPSHOT"
  :description "Queue abstraction with bindings at least to filesystem and amazon sqs."
  :url "http://github.com:tech-ascent/tech.queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [techascent/tech.io "0.1.15"]
                 [com.amazonaws/aws-java-sdk-sqs "1.11.341"]
                 [thinktopic/think.parallel "0.3.7"]
                 [factual/durable-queue "0.1.6" :exclusions [com.taoensso/nippy]]
                 [com.github.dblock/oshi-core "3.4.0"]]

  :plugins [[lein-environ "1.1.0"]]

  :profiles {:dev {:dependencies [[techascent/vault-clj "0.2.17"]]
                   :env {:tech-queue-sqs-vault-auth "true"
                         :tech-queue-vault-aws-path "aws/sts/core"}
                   :jvm-opts ["-Xmx4g"]}
             :test {:dependencies [[techascent/vault-clj "0.2.17"]]
                    :env {:tech-queue-sqs-vault-auth "true"
                          :tech-queue-vault-aws-path "aws/sts/core"}
                    :jvm-opts ["-Xmx4g"]}})
