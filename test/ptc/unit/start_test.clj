(ns ptc.unit.start-test
  "Test JMS message peek and consume functions."
  (:require [clojure.test :refer [deftest is testing]]
            [ptc.start :as start]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.jms :as jms]))

(deftest produce-consume-message
  (let [text       "consume"
        properties (::jms/Properties (jms/encode @jms-tools/good-jms-message))]
    (letfn [(produce-consume [connection queue]
              (start/produce connection queue text properties)
              (start/consume connection queue))]
      (if-let [msg (jms-tools/with-test-queue-connection produce-consume)]
        (testing "Message is not nil and can be properly consumed"
          (is (= text (.getText msg)))
          (is (apply = (map ::jms/Properties [@jms-tools/good-jms-message (jms/ednify msg)]))))
        (is false)))))

(deftest produce-peek-message
  (let [text       "peek"
        properties (::jms/Properties (jms/encode @jms-tools/good-jms-message))]
    (letfn [(produce-peek [connection queue]
              (start/produce connection queue text properties)
              (start/peek-message connection queue))]
      (if-let [msg (jms-tools/with-test-queue-connection produce-peek)]
        (testing "Message is not nil and can be properly consumed"
          (is (= text (.getText msg)))
          (is (apply = (map ::jms/Properties [@jms-tools/good-jms-message (jms/ednify msg)]))))
        (is false)))))
