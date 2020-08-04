(ns ptc.unit.start-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn  :as edn]
            [clojure.walk :as walk]
            [ptc.start    :as start]
            [ptc.util.jms :as jms])
  (:import [org.apache.activemq ActiveMQSslConnectionFactory]))

;; Local testing for ActiveMQ
;; https://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection
;;
(defn with-test-jms-connection
  "CALL with a local JMS connection for testing."
  [call]
  (let [url     "vm://localhost?broker.persistent=false"
        factory (new ActiveMQSslConnectionFactory url)
        queue   "test.queue"]
    (with-open [connection (.createQueueConnection factory)]
      (call connection queue))))

(def message
  "Example JMS message for testing."
  (edn/read-string (slurp "test/data/good-jms.edn")))

(deftest produce-consume-message
  (let [text "consume"
        properties (::jms/Properties (jms/encode message))]
    (letfn [(produce-consume [connection queue]
              (start/produce connection queue text properties)
              (start/consume connection queue))]
      (if-let [msg (with-test-jms-connection produce-consume)]
        (testing "Message is not nil and can be properly consumed"
          (is (= text (.getText msg)))
          (is (apply = (map ::jms/Properties [message (jms/ednify msg)]))))
        (is false)))))

(deftest produce-peek-message
  (let [text "peek"
        properties (::jms/Properties (jms/encode message))]
    (letfn [(produce-peek [connection queue]
              (start/produce connection queue text properties)
              (start/peek-message connection queue))]
      (if-let [msg (with-test-jms-connection produce-peek)]
        (testing "Message is not nil and can be properly consumed"
          (is (= text (.getText msg)))
          (is (apply = (map ::jms/Properties [message (jms/ednify msg)]))))
        (is false)))))
