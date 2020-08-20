(ns ptc.integration.integration-test
  (:require [clojure.test :refer [deftest is testing]]
            [ptc.start :as start]
            [ptc.tools.gcs :as gcs]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.jms :as jms])
  (:import (java.util UUID)))

(def bucket
  "Storage bucket for running ptc.integration test with."
  "broad-gotc-dev-zero-test")

(deftest integration
  (let [prefix     (str "test/" (UUID/randomUUID))
        properties (::jms/Properties (jms/encode @jms-tools/good-jms-message))]
    (letfn [(task [_]
              (try
                (testing "end-to-end: "
                  (testing "upload a file to the bucket"
                    (let [upload (gcs/upload-file "deps.edn" bucket prefix)]
                      (is (= prefix (:name upload)))
                      (is (= bucket (:bucket upload)))
                      (is (= [upload] (gcs/list-objects bucket prefix))))))
                (finally (gcs/delete-object bucket prefix)))
              false)
            (flow [connection queue]
              (start/produce connection queue "text" properties)
              (start/listen-and-consume-from-queue task connection queue))]
      (testing "Message is not nil and can be properly read"
        (if-let [msg (jms-tools/with-test-queue-connection flow)]
          (is (= @jms-tools/good-jms-message (select-keys msg [::jms/Properties])))
          (is false))))))

(deftest peeking
  (let [properties (::jms/Properties (jms/encode @jms-tools/good-jms-message))]
    (letfn [(task [message] (is message) false)]
      (jms-tools/with-test-queue-connection
        (fn [connection queue]
          (testing "Message given to task isn't nil"
            (start/produce connection queue "text" properties)
            (start/listen-and-consume-from-queue task connection queue))
          (testing "The message was only peeked and can still be consumed"
            (let [msg (jms/ednify (start/consume connection queue))]
              (testing "Message is not nil and can be properly read"
                (is (= @jms-tools/good-jms-message (select-keys msg [::jms/Properties])))))))))))
