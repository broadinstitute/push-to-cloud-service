(ns ptc.unit.jms-test
  "Test some of ptc.util.jms."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [ptc.start :as start]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.environment :as env]
            [ptc.util.gcs :as gcs]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]))

(def read-jms-message (comp edn/read-string slurp))

(deftest use-on-prem-idat-paths
  (testing "WFL uses the on-prem idat files if they do not exist in the cloud."
    (let [jms (read-jms-message "./test/data/good-jms.edn")
          workflow (get-in jms [::jms/Properties :payload :workflow])
          {:keys [cloudGreenIdatPath cloudRedIdatPath]} workflow
          push (::jms/push jms/wfl-keys->jms-keys)]
      (is (not (gcs/gcs-object-exists? cloudGreenIdatPath)))
      (is (not (gcs/gcs-object-exists? cloudRedIdatPath)))
      (is (= (:green_idat_cloud_path push) :greenIDatPath))
      (is (= (:red_idat_cloud_path   push) :redIDatPath)))))

(deftest use-cloud-idat-paths
  (testing "WFL is able to find cloud idat paths if they exist."
    (let [jms (read-jms-message "./test/data/reprocessing-jms.edn")
          workflow (get-in jms [::jms/Properties :payload :workflow])
          prefix "gs://broad-gotc-dev-wfl-ptc-test-inputs"]
      (is (#'jms/find-input-or-throw prefix workflow :greenIDatPath)))))

(comment (clojure.test/test-vars [#'test-dead-letter-queue]))

(deftest test-dead-letter-queue
  (testing "a bad message winds up in the dead-letter queue"
    (let [dlq (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME")
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

(comment (clojure.test/test-vars [#'test-message-id-equality]))

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
      (testing "test_msg equal to a different message with same properties"
        (is (jms/message-ids-equal? msg same)))
      (testing "test_msg not equal to different message with different properties"
        (is (not (jms/message-ids-equal? msg different))))
      (testing "not equal even if only one argument isn't"
        (is (not (jms/message-ids-equal? msg same different)))))))
