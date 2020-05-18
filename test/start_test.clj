(ns start-test
  (:require [clojure.test :refer :all]
            [start])
  (:import [org.apache.activemq ActiveMQSslConnectionFactory]))

(defn with-test-jms-connection
  "CALL with a local JMS connection for testing."
  [call]
  (let [url     "vm://localhost?broker.persistent=false"
        factory (new ActiveMQSslConnectionFactory url)
        queue   "test.queue"]
    (with-open [connection (.createQueueConnection factory)]
      (call connection queue))))

(deftest produce-consume-message
  "Test we could produce a message and consume it."
  (letfn [(produce-consume [connection queue]
            (start/produce connection queue "test-message" {"test" "OK"})
            (start/consume connection queue))]
    (if-let [msg (with-test-jms-connection produce-consume)]
      (is (= "test-message" (.getText msg)))
      (is false))))
