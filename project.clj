(defproject techascent/tech.queue "2.0"
  :description "Queue abstraction with bindings at least to filesystem and amazon sqs."
  :url "http://github.com:tech-ascent/tech.queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-environ "1.1.0"]
            [lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :repositories {"releases"  {:url "s3p://techascent.jars/releases/"
                              :no-auth true
                              :sign-releases false}}
  :profiles {:dev {:lein-tools-deps/config {:resolve-aliases [:test]}
                   :env {:tech-queue-sqs-vault-auth "true"
                         :tech-queue-vault-aws-path "aws/sts/core"}
                   :jvm-opts ["-Xmx4g"]}})
