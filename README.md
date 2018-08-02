# tech.queue

[![Clojars Project](https://clojars.org/techascent/tech.queue/latest-version.svg)](https://clojars.org/techascent/tech.queue)

Simple queue abstraction.  Setup a queue/worker abstraction that will survive crashes and machine reboots.

Choose either filesystem or sqs to back your queues.

```clojure
(q/put! "file://tmp/queue-a" {:data 1})

(let [task (q/take! "file://temp/queue-a")]
  (println (q/task->msg "file://temp/queue-a" task))
  (q/complete! "file://temp/queue-a" task))
```

Also implemented is a worker abstraction that works with the component library and enables robust, 
high volume processing of tasks in a system environment.

## Examples

Please see [queue-test](test/tech/queue-test.clj).

For using the worker abstraction, please see [worker-test](test/tech/queue/worker_test.clj).

## License

Copyright © 2018 TechAscent, LLC.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
