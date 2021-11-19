(ns ptc.integration.integration-test
  (:require [clojure.test :refer [deftest is testing]]
            [ptc.start :as start]
            [ptc.tools.gcs :as gcs-tools]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.environment :as env]
            [ptc.util.gcs :as gcs]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc])
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

(comment (clojure.test/test-vars [#'test-dead-letter-queue]))

(deftest test-dead-letter-queue
  (testing "a bad message winds up in the dead-letter queue"
    (let [dlq      (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME")
          workflow (jms-tools/queue-one-jms-message "./test/data/bad-jms.edn")
          prefix   (env/getenv-or-throw "PTC_BUCKET_URL")]
      (jms-tools/with-queue-connection
        (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SERVER_URL")
        (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME")
        (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SECRET_PATH")
        (fn [connection queue]
          (let [peeked (start/peek-message connection queue)]
            (misc/trace peeked))))
      (is false))))
