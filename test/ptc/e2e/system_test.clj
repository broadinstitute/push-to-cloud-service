(ns ptc.e2e.system-test
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [ptc.tools.cromwell :as cromwell]
            [ptc.tools.gcs :as gcs]
            [ptc.tools.wfl :as wfl]
            [ptc.tools.utils :as utils]
            [ptc.util.jms :as jms]
            [ptc.tools.jms :as jms-tools])
  (:import [java.lang Integer]
           [java.util UUID]))

(def environment
  (keyword (or (System/getenv "ENVIRONMENT") "dev")))

(def bucket
  (or (System/getenv "PTC_BUCKET_NAME") "gs://dev-aou-arrays-input"))

(def cromwell-url
  (if (= environment :prod)
    "https://cromwell-aou.gotc-prod.broadinstitute.org"
    "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org"))

(def wfl-url
  (if (= environment :prod)
    "https://aou-wfl.gotc-prod.broadinstitute.org"
    "https://dev-wfl.gotc-dev.broadinstitute.org"))

(def jms-message
  (edn/read-string (slurp "./test/data/plumbing-test-jms-dev.edn")))

(defn timeout
  "Timeout FUNCTION after MILLISECONDS."
  [milliseconds function]
  (let [f      (future (function))
        return (deref f milliseconds ::timed-out)]
    (when (= return ::timed-out)
      (future-cancel f))
    return))

(deftest test-end-to-end
  (let [analysis-version (rand-int Integer/MAX_VALUE)
        message          (assoc-in jms-message
                                   [::jms/Properties :payload :workflow :analysisCloudVersion] analysis-version)
        chipwell-barcode (get-in message [::jms/Properties :payload :workflow :chipWellBarcode])
        workflow         (get-in message [::jms/Properties :payload :workflow])
        cloud-prefix     (jms/cloud-prefix bucket workflow)]
    (jms-tools/queue-messages 1 environment message)
    (testing "Files are uploaded to the input bucket"
      (let [params (str cloud-prefix "/params.txt")
            ptc    (str cloud-prefix "/ptc.json")
            _      (timeout 180000 #(gcs/wait-for-files-in-bucket cloud-prefix [ptc]))
            ; Dodge rarely observed race condition where `cat` errors even though `ls` shows the file
            _      (.sleep TimeUnit/SECONDS 1)
            {:keys [notifications]} (gcs/gcs-edn ptc)
            pushed (utils/pushed-files (first notifications) params)
            gcs    (timeout 180000 #(gcs/wait-for-files-in-bucket cloud-prefix pushed))]
        (is (not= gcs ::timed-out) "Timeout waiting for files to upload")
        (let [diff (set/difference (set gcs) (set pushed))]
          (is (= diff (set [ptc])) "Files in bucket do not match expected files")
          (is (= (jms/jms->params workflow) (gcs/gcs-cat params))))))
    (testing "Cromwell workflow is started by WFL"
      (let [workflow-id (timeout 180000 #(wfl/wait-for-workflow-creation wfl-url chipwell-barcode analysis-version))]
        (is (not= workflow-id ::timed-out) "Timeout waiting for workflow creation")
        (is (uuid? (UUID/fromString workflow-id)) "Workflow id is not a valid UUID")
        (testing "Cromwell workflow succeeds"
          (let [workflow-timeout 1800000
                result           (timeout workflow-timeout #(cromwell/wait-for-workflow-complete cromwell-url workflow-id))]
            (is (= result "Succeeded") "Cromwell workflow failed")))))))
