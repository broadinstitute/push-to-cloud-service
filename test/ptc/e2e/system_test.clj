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

(def jms-url
  (keyword (or (System/getenv "JMS_URL") "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616")))

(def queue
  (keyword (or (System/getenv "QUEUE") "wfl.broad.pushtocloud.enqueue.dev")))

(def vault-path
  (keyword (or (System/getenv "VAULT_PATH") "secret/dsde/gotc/dev/activemq/logins/zamboni")))

(def bucket
  (or (System/getenv "PTC_BUCKET_URL") "gs://dev-aou-arrays-input"))

(def cromwell-url
  (or (System/getenv "CROMWELL_URL") "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org"))

(def wfl-url
  (or (System/getenv "WFL_URL") "https://dev-wfl.gotc-dev.broadinstitute.org"))

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
    (jms-tools/queue-messages 1 jms-url queue vault-path message)
    (testing "Files are uploaded to the input bucket"
      (let [params      (str cloud-prefix "/params.txt")
            ptc-file    (str cloud-prefix "/ptc.json")
            ptc-present (timeout 360000 #(gcs/wait-for-files-in-bucket [ptc-file]))]
        (is (not= ptc-present ::timed-out) "Timed out waiting for ptc.json to upload")
        (let [{:keys [notifications]} (gcs/gcs-edn ptc-file)
              expected-files          (utils/pushed-files (first notifications) params)
              expected-present        (timeout 180000 #(gcs/wait-for-files-in-bucket expected-files))]
          (is (not= expected-present ::timed-out) "Timed out waiting for expected files to upload")
          (is (= (jms/jms->params workflow) (gcs/gcs-cat params))))))
    (testing "Cromwell workflow is started by WFL"
      (let [workflow-id (timeout 180000 #(wfl/wait-for-workflow-creation wfl-url chipwell-barcode analysis-version))]
        (is (not= workflow-id ::timed-out) "Timeout waiting for workflow creation")
        (is (uuid? (UUID/fromString workflow-id)) "Workflow id is not a valid UUID")
        (testing "Cromwell workflow succeeds"
          (let [workflow-timeout 1800000
                result (timeout workflow-timeout #(cromwell/wait-for-workflow-complete cromwell-url workflow-id))]
            (is (= result "Succeeded") "Cromwell workflow failed")))))))
