(ns start-test
  (:require [clojure.test :refer :all]
            [clojure.edn :as edn]
            [start])
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

(def jms-message
  "An example JMS message for testing."
  {:headers    {:message-id "ID:gotc-jenkins-slave01.broadinstitute.org-12345-123456789101-10:1:1:1:1"}
   :properties {:launchSingleSampleCloudWorkflow false
                :project "Test-Project"
                :dataType "WGS"
                :analysisVersion "1"
                :sampleAlias "Test-Alias"
                :clinicalDataDeliveryTest false}})
(comment
  (edn/read-string (prn-str jms-message)))

(deftest produce-consume-message
  "Test we could produce a message and consume it."
  (letfn [(produce-consume [connection queue]
            (start/produce connection queue "test-message" {"test" "OK"})
            (start/consume connection queue))]
    (if-let [msg (with-test-jms-connection produce-consume)]
      (is (= "test-message" (.getText msg)))
      (is false))))
