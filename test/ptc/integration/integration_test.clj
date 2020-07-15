(ns ptc.integration.integration-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [clojure.walk :as walk]
            [ptc.start :as start]
            [ptc.util.gcs    :as gcs])
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

(def testing-bucket
  "Storage bucket for running ptc.integration test with."
  "broad-gotc-dev-zero-test")

(defn cleanup-object-test
  "Clean up the testing bucket after the FILE-test."
  [file]
  (doseq [object (map :name (gcs/list-objects testing-bucket))]
    (when (= (:name file) object)
      (gcs/delete-object testing-bucket object))))

(defn lazy-contains? [coll x]
  (some #(= x %) coll))

(deftest integration
  (let [object    {:name "test/junk" :contentType "text/plain"}
        test-file "deps.edn"
        message-text  (prn-str             (:headers message))
        message-props (walk/stringify-keys (:properties message))]
    (letfn [(custom-task [message]
              (try
                (testing "end-to-end: "
                  (testing "upload a file to the bucket"
                    (let [result (gcs/upload-file test-file testing-bucket (:name object))]
                      (is (= object (select-keys result (keys object))))
                      (is (= testing-bucket (:bucket result)))))
                  (testing "uploaded object can be found with list-objects"
                    (let [result (gcs/list-objects testing-bucket)]
                      (is (= true (lazy-contains? (map (fn [x] (select-keys x (keys object))) result) object))))))
                (finally (cleanup-object-test object)))
              ;; return false to break the loop
              false)]
      (if-let [parsed-msg (with-test-jms-connection
                            (fn [connection queue]
                              (start/produce connection queue message-text message-props)
                              (start/listen-and-consume-from-queue connection queue custom-task)))]
        (testing "Message is not nil and can be properly read"
          (is (= message-text
                 (:headers parsed-msg)))
          (doseq [[k v] (:properties parsed-msg)]
            (is (= (get message-props k)
                   v))))
        (is false)))))

(deftest peeking
  (let [message-text (prn-str (:headers message))
        message-props (walk/stringify-keys (:properties message))]
    (letfn [(custom-task [message]
              (testing "Message given to task isn't nil"
                (is (not (nil? message))))
              ;; return false to break the loop (as if the task failed)
              false)]
      (with-test-jms-connection
        (fn [connection queue]
          (start/produce connection queue message-text message-props)
          (start/listen-and-consume-from-queue connection queue custom-task)
          (testing "The message was only peeked and can still be consumed"
            (let [message (start/parse-message (start/consume connection queue))]
              (testing "Message is not nil and can be properly read"
                (is (= message-text
                       (:headers message)))
                (doseq [[k v] (:properties message)]
                  (is (= (get message-props k)
                         v)))))))))))