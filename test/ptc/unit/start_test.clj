(ns ptc.unit.start-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn  :as edn]
            [clojure.walk :as walk]
            [ptc.start    :as start])
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
  (edn/read-string (slurp "test/data/test_msg.edn")))

(deftest produce-consume-message
  (let [message-text  (prn-str             (:headers message))
        message-props (walk/stringify-keys (:properties message))]
    (letfn [(produce-consume [connection queue]
              (start/produce connection queue message-text message-props)
              (start/consume connection queue))]
      (if-let [msg (with-test-jms-connection produce-consume)]
        (testing "Message is not nil and can be properly consumed"
          (is (= (:headers message)
                 (edn/read-string (.getText msg))))
          (doseq [[k v] (.getProperties msg)]
            (is (= (get (:properties message) (keyword k))
                   v))))
        (is false)))))

(deftest produce-peek-message
  (let [message-text  (prn-str             (:headers message))
        message-props (walk/stringify-keys (:properties message))]
    (letfn [(produce-peek [connection queue]
              (start/produce connection queue message-text message-props)
              (start/peek-message connection queue))]
      (if-let [msg (with-test-jms-connection produce-peek)]
        (testing "Message is not nil and can be properly consumed"
          (is (= (:headers message)
                 (edn/read-string (.getText msg))))
          (doseq [[k v] (.getProperties msg)]
            (is (= (get (:properties message) (keyword k))
                   v))))
        (is false)))))
