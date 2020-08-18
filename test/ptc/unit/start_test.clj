(ns ptc.unit.start-test
  (:require [clojure.test :refer [deftest is testing]]
            [ptc.start :as start]
            [ptc.util.jms :as jms]
            [ptc.tools.jms :refer [with-test-queue-connection message]]))

(deftest produce-consume-message
  (let [text       "consume"
        properties (::jms/Properties (jms/encode @message))]
    (letfn [(produce-consume [connection queue]
              (start/produce connection queue text properties)
              (start/consume connection queue))]
      (if-let [msg (with-test-queue-connection produce-consume)]
        (testing "Message is not nil and can be properly consumed"
          (is (= text (.getText msg)))
          (is (apply = (map ::jms/Properties [@message (jms/ednify msg)]))))
        (is false)))))

(deftest produce-peek-message
  (let [text       "peek"
        properties (::jms/Properties (jms/encode @message))]
    (letfn [(produce-peek [connection queue]
              (start/produce connection queue text properties)
              (start/peek-message connection queue))]
      (if-let [msg (with-test-queue-connection produce-peek)]
        (testing "Message is not nil and can be properly consumed"
          (is (= text (.getText msg)))
          (is (apply = (map ::jms/Properties [@message (jms/ednify msg)]))))
        (is false)))))
