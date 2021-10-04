(ns ptc.integration.jms-test
  (:require [clojure.data :as data]
            [clojure.test :refer [deftest is testing]]
            [ptc.start :as start]
            [ptc.tools.gcs :as gcs]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]))

(deftest push-notification-for-jms
  (let [path     [::jms/Properties :payload :workflow]
        bad      (jms-tools/fix-paths "./test/data/bad-jms.edn")
        good     (jms-tools/fix-paths "./test/data/good-jms.edn")
        missing  (-> good (data/diff bad) first (get-in path) keys first
                     (->> (str (var-get #'jms/missing-keys-message) ".*"))
                     re-pattern)
        workflow (get-in good path)]
    (gcs/with-temporary-gcs-folder folder
      (jms-tools/with-test-queue-connection
        (fn [connection queue]
          (testing "a BAD message"
            (start/produce connection queue
                           "BAD" (::jms/Properties (jms/encode bad)))
            (let [msg (start/consume connection queue)]
              (is (thrown-with-msg? IllegalArgumentException missing
                                    (jms/handle-message folder (jms/ednify msg))))
              (is (empty? (->> folder
                               gcs/parse-gs-url
                               (apply gcs/list-objects))))))
          (testing "a GOOD message"
            (start/produce connection queue
                           "GOOD" (::jms/Properties (jms/encode good)))
            (let [msg    (start/consume connection queue)
                  [params ptc] (jms/handle-message folder (jms/ednify msg))
                  {:keys [notifications]} (gcs/gcs-edn ptc)
                  {:keys [::jms/chip ::jms/push]} jms/wfl-keys->jms-keys
                  push   (-> jms/wfl-keys->jms-keys ::jms/push
                             (->> (merge chip))
                             keys
                             (->> (apply juxt)))
                  inputs (remove nil? (push (first notifications)))
                  pushed (into [params ptc] inputs)
                  gcs    (gcs/list-gcs-folder folder)]
              (is (= (set pushed) (set gcs)))
              (is (= (jms/jms->params workflow) (gcs/gcs-cat params))))))))))
