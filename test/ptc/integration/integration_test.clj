(ns ptc.integration.integration-test
  (:require [clojure.test  :refer [deftest is testing]]
            [clojure.edn   :as edn]
            [clojure.walk  :as walk]
            [ptc.start     :as start]
            [ptc.util.gcs  :as gcs]
            [ptc.util.jms  :as jms])
  (:import [org.apache.activemq ActiveMQSslConnectionFactory]
           (java.util UUID)))

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

(def bucket
  "Storage bucket for running ptc.integration test with."
  "broad-gotc-dev-zero-test")

(defn cleanup-object-test
  "Clean up the testing bucket after the FILE-test."
  [file]
  (doseq [object (map :name (gcs/list-objects bucket))]
    (when (= (:name file) object)
      (gcs/delete-object bucket object))))

(deftest integration
  (let [prefix (str "test/" (UUID/randomUUID))
        properties (::jms/Properties (jms/encode message))]
    (letfn [(task [_]
              (try
                (testing "end-to-end: "
                  (testing "upload a file to the bucket"
                    (let [upload (gcs/upload-file "deps.edn" bucket prefix)]
                      (is (= prefix (:name upload)))
                      (is (= bucket (:bucket upload)))
                      (is (= [upload] (gcs/list-objects bucket prefix))))))
                (finally (cleanup-object-test prefix)))
              false)
            (flow [connection queue]
              (start/produce connection queue "text" properties)
              (start/listen-and-consume-from-queue task connection queue))]
      (testing "Message is not nil and can be properly read"
        (if-let [msg (with-test-jms-connection flow)]
          (is (= message (select-keys msg [::jms/Properties])))
          (is false))))))

(deftest peeking
  (let [properties (::jms/Properties (jms/encode message))]
    (letfn [(task [message] (is message) false)]
      (with-test-jms-connection
        (fn [connection queue]
          (testing "Message given to task isn't nil"
            (start/produce connection queue "text" properties)
            (start/listen-and-consume-from-queue task connection queue))
          (testing "The message was only peeked and can still be consumed"
            (let [msg (jms/ednify (start/consume connection queue))]
              (testing "Message is not nil and can be properly read"
                (is (= message (select-keys msg [::jms/Properties])))))))))))
