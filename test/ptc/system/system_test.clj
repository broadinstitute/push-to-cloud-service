(ns ptc.system.system-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [ptc.start :as start]
            [ptc.tools.cromwell :as cromwell]
            [ptc.tools.wfl :as wfl]
            [ptc.util.jms :as jms]
            [ptc.integration.jms-test :as jms-test])
  (:import [java.util UUID]))

(def environment
  (or (System/getenv "environment") "dev"))

(def bucket
  (or (System/getenv "ptc_bucket_name") "gs://dev-aou-arrays-input"))

(def cromwell-url
  (if (= environment "prod")
    "https://cromwell-aou.gotc-prod.broadinstitute.org"
    "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org"))

(def wfl-url
  (if (= environment "prod")
    "https://aou-wfl.gotc-dev.broadinstitute.org"
    "https://dev-wfl.gotc-dev.broadinstitute.org"))

(def jms-message
  (edn/read-string (slurp "./test/data/plumbing-test-jms-dev.edn")))

(defn timeout
  "Timeout FUNCTION after MILLISECONDS."
  [milliseconds function]
  (let [f (future (function))
        return (deref f milliseconds ::timed-out)]
    (when (= return ::timed-out)
      (future-cancel f))
    return))

(defn queue-message-placeholder
  "Upload files to FOLDER in MESSAGE using a test jms connection."
  [folder message]
  (jms-test/with-test-jms-connection
    (fn [connection queue]
      (start/produce connection queue
                     "GOOD" (::jms/Properties (jms/encode message)))
      (let [msg (start/consume connection queue)
            [params ptc] (jms/handle-message folder msg)]
        (print "Pushed message")))))

(deftest test-end-to-end
  (let [chipwell-barcode (str (UUID/randomUUID))
        message (assoc-in jms-message
                          [::jms/Properties :payload :workflow :chipWellBarcode] chipwell-barcode)
        analysis-version (get-in message [::jms/Properties :payload :workflow :analysisCloudVersion])
        workflow (get-in message [::jms/Properties :payload :workflow])
        cloud-prefix (jms/cloud-prefix bucket workflow)
        push (-> jms/notification-keys->jms-keys
                 ((juxt ::jms/chip ::jms/push))
                 (->> (apply merge))
                 keys
                 (->> (apply juxt)))]
    ;(jms-test/queue-messages 1 message environment)
    (queue-message-placeholder bucket message)
    (testing "Files are uploaded to the input bucket"
      (let [params (str cloud-prefix "/params.txt")
            ptc (str cloud-prefix "/ptc.json")
            {:keys [notifications] :as request} (jms-test/gcs-edn ptc)
            pushed (push (first notifications))
            gcs (jms-test/list-gcs-folder cloud-prefix)
            union (set/union (set gcs) (set pushed))
            diff (set/difference (set gcs) (set pushed))]
        (is (== (count pushed) (count (set pushed))))
        (is (== (count gcs) (count (set gcs))))
        (is (== (count union) (count (set gcs))))
        (is (== 2 (count diff)))
        (is (= diff (set [params ptc])))
        (is (= (jms/jms->params workflow) (jms-test/gcs-cat params)))))
    (testing "Cromwell workflow is started by WFL"
      (let [workflow-id (timeout 180000 #(wfl/wait-for-workflow-creation wfl-url chipwell-barcode analysis-version))]
        (is (not= workflow-id :ptc.system.system-test/timed-out))
        (is (uuid? (UUID/fromString workflow-id)))
        (testing "Cromwell workflow succeeds"
          (let [workflow-timeout 1800000
                result (timeout workflow-timeout #(cromwell/wait-for-workflow-complete cromwell-url workflow-id))]
            (is (= result "Succeeded"))))))))
