(defproject tech.queue "0.1.0-SNAPSHOT"
  :description "Queue abstraction with bindings at least to filesystem and amazon sqs."
  :url "http://github.com:tech-ascent/tech.queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [amazonica "0.3.127"
                  :exclusions [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor
                               com.fasterxml.jackson.core/jackson-core
                               com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client
                               com.amazonaws/dynamodb-streams-kinesis-adapter]]
                 [com.amazonaws/aws-java-sdk-core "1.11.341"]
                 [com.amazonaws/aws-java-sdk-sqs "1.11.341"]
                 [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor "2.9.0"]
                 [com.fasterxml.jackson.core/jackson-databind "2.9.0"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.clojure/core.async "0.3.465"]
                 [com.stuartsierra/component "0.3.2"]
                 [cheshire "5.8.0"]
                 [thinktopic/think.parallel "0.3.7"]
                 [factual/durable-queue "0.1.6" :exclusions [com.taoensso/nippy
                                                             primitive-math]]])
