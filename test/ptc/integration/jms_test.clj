(ns ptc.integration.jms-test
  (:require [clojure.data :as data]
            [clojure.test :refer [deftest is testing]]
            [ptc.start :as start]
            [ptc.tools.gcs :as gcs]
            [ptc.tools.jms :as jms-tools]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc])
  (:import [java.util UUID]))

(def gcs-test-bucket
  "Throw test files in this bucket."
  "broad-gotc-dev-wfl-ptc-test-outputs")

(defmacro with-temporary-gcs-folder
  "
  Create a temporary folder in GCS-TEST-BUCKET for use in BODY.
  The folder will be deleted after execution transfers from BODY.

  Example
  -------
    (with-temporary-gcs-folder uri
      ;; use temporary folder at `uri`)
      ;; <- temporary folder deleted
  "
  [uri & body]
  `(let [name# (str "ptc-test-" (UUID/randomUUID))
         ~uri (misc/gs-url gcs-test-bucket name#)]
     (try
       ~@body
       (finally
         (->>
          (gcs/list-objects gcs-test-bucket name#)
          (run! (comp (partial gcs/delete-object gcs-test-bucket) :name)))))))

(deftest push-notification-for-jms
  (let [path     [::jms/Properties :payload :workflow]
        bad      (jms-tools/fix-paths "./test/data/bad-jms.edn")
        good     (jms-tools/fix-paths "./test/data/good-jms.edn")
        missing  (-> good (data/diff bad) first (get-in path) keys first
                     (->> (str jms/missing-keys-message ".*"))
                     re-pattern)
        workflow (get-in good path)]
    (with-temporary-gcs-folder folder
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
                  push   (-> workflow jms/wfl-keys->jms-keys-for ::jms/push
                             (->> (merge chip))
                             keys
                             (->> (apply juxt)))
                  inputs (remove nil? (push (first notifications)))
                  pushed (into [params ptc] inputs)
                  gcs    (gcs/list-gcs-folder folder)]
              (is (= (set pushed) (set gcs)))
              (is (= (jms/jms->params workflow) (gcs/gcs-cat params))))))))))
