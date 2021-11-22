(ns ptc.unit.jms-test
  "Test some of ptc.util.jms."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [ptc.start :as start]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.gcs :as gcs]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]))

(def read-jms-message (comp edn/read-string slurp))

(deftest use-cloud-idat-paths
  (testing "WFL is able to find cloud idat paths if they exist."
    (let [jms (read-jms-message "./test/data/reprocessing-jms.edn")
          workflow (get-in jms [::jms/Properties :payload :workflow])
          prefix "gs://broad-gotc-dev-wfl-ptc-test-inputs"]
      (is (#'jms/find-input-or-throw prefix workflow :greenIDatPath)))))

(deftest test-message-id-equality
  (let [msg       (edn/read-string (slurp "test/data/test_msg.edn"))
        different (edn/read-string (slurp "test/data/test_msg_diff.edn"))
        same      (edn/read-string (slurp "test/data/test_msg_same.edn"))]
    (testing "message ID equality"
      (testing "true with no arguments"
        (is (jms/message-ids-equal?)))
      (testing "true with one argument"
        (is (jms/message-ids-equal? msg)))
      (testing "test_msg equal to itself"
        (is (jms/message-ids-equal? msg msg)))
      (testing "test_msg equal to a different message with same ID"
        (is (jms/message-ids-equal? msg same)))
      (testing "test_msg not equal to different message with different ID"
        (is (not (jms/message-ids-equal? msg different))))
      (testing "not equal even if only one argument isn't"
        (is (not (jms/message-ids-equal? msg same different)))))))
