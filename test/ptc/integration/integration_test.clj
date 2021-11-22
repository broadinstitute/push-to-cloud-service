(ns ptc.integration.integration-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [ptc.start :as start]
            [ptc.tools.gcs :as gcs-tools]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.environment :as env]
            [ptc.util.gcs :as gcs]
            [ptc.util.jms :as jms])
  (:import (java.util UUID)))

(def bucket
  "Storage bucket for running ptc.integration test with."
  "broad-gotc-dev-wfl-ptc-test-outputs")

(deftest integration
  (let [prefix     (str "test/" (UUID/randomUUID))
        properties (::jms/Properties (jms/encode @jms-tools/good-jms-message))]
    (letfn [(task [_ _]
              (testing "upload a file to the bucket"
                (let [object (str prefix "/deps.edn")]
                  (try
                    (gcs-tools/upload-file "deps.edn" bucket object)
                    (gcs/list-objects bucket object)
                    (finally (gcs-tools/delete-object bucket object)))))
              false)                    ; to break out from the loop
            (flow [connection queue]
              (start/produce connection queue "text" properties)
              (start/listen-and-consume-from-queue task connection queue))]
      (testing "Message is not nil and can be properly read"
        (if-let [msg (jms-tools/call-with-test-connection "queue" flow)]
          (is (= @jms-tools/good-jms-message
                 (select-keys msg [::jms/Properties])))
          (is false))))))

(deftest peeking
  (let [properties (::jms/Properties (jms/encode @jms-tools/good-jms-message))]
    (letfn [(task [message _] (is message) false)]
      (jms-tools/call-with-test-connection
       "queue"
       (fn [connection queue]
         (testing "Message given to task isn't nil"
           (start/produce connection queue "text" properties)
           (start/listen-and-consume-from-queue task connection queue))
         (testing "The message was only peeked and can still be consumed"
           (let [msg (jms/ednify (start/consume connection queue))]
             (testing "Message is not nil and can be properly read"
               (is (= @jms-tools/good-jms-message
                      (select-keys msg [::jms/Properties])))))))))))

(deftest forwarding
  (testing "forward a bad message to the dead-letter queue"
    (let [queue (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_QUEUE_NAME")
          dlq   (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME")
          msg   (-> "./test/data/bad-jms.edn" slurp edn/read-string
                    (update-in [::jms/Properties :payload :workflow]
                               assoc :analysisCloudVersion
                               (rand-int Integer/MAX_VALUE)))
          props (-> msg jms/encode ::jms/Properties)]
      (letfn [(handle-or-dlq-just-once [jms connection]
                (#'start/handle-or-dlq jms connection)
                false)
              (unbot [msg] (dissoc msg ::jms/Headers :brokerOutTime))
              (ok? [left right] (and (not-any? nil? [left right])
                                     (= left right)))]
        (with-open [connection (start/create-queue-connection
                                "vm://localhost?broker.persistent=false")]
          (start/produce connection queue "text" props)
          (start/listen-and-consume-from-queue handle-or-dlq-just-once
                                               connection queue)
          (let [peeked   (jms/ednify (start/peek-message connection dlq))
                consumed (jms/ednify (start/consume      connection dlq))]
            (is (apply ok? (map ::jms/Properties [msg peeked])))
            (is (apply ok? (map unbot [peeked consumed])))))))))
