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

(def zamboni-activemq-server-url
  (or (System/getenv "ZAMBONI_ACTIVEMQ_SERVER_URL") "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616"))

(def zamboni-activemq-queue-name
  (or (System/getenv "ZAMBONI_ACTIVEMQ_QUEUE_NAME") "wfl.broad.pushtocloud.enqueue.dev"))

(def zamboni-activemq-secret-path
  (or (System/getenv "ZAMBONI_ACTIVEMQ_SECRET_PATH") "secret/dsde/gotc/dev/activemq/logins/zamboni"))

(def ptc-bucket-url
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
    (when (= ::timed-out return)
      (future-cancel f))
    return))

(deftest test-end-to-end
  (let [analysis-version (rand-int Integer/MAX_VALUE)
        message          (assoc-in jms-message
                                   [::jms/Properties :payload :workflow :analysisCloudVersion] analysis-version)
        chipwell-barcode (get-in message [::jms/Properties :payload :workflow :chipWellBarcode])
        workflow         (get-in message [::jms/Properties :payload :workflow])
        cloud-prefix     (jms/cloud-prefix ptc-bucket-url workflow)]
    (jms-tools/queue-messages 1 zamboni-activemq-server-url zamboni-activemq-queue-name zamboni-activemq-secret-path message)
    (testing "Files are uploaded to the input bucket"
      (let [params      (str cloud-prefix "/params.txt")
            ptc-file    (str cloud-prefix "/ptc.json")
            ptc-present (timeout 360000 #(gcs/wait-for-files-in-bucket [ptc-file]))]
        (is (not= ::timed-out ptc-present) "Timed out waiting for ptc.json to upload")
        (let [{:keys [notifications]} (gcs/gcs-edn ptc-file)
              expected-files          (utils/pushed-files (first notifications) params)
              expected-present        (timeout 180000 #(gcs/wait-for-files-in-bucket expected-files))]
          (is (not= expected-present ::timed-out) "Timed out waiting for expected files to upload")
          (is (= (gcs/gcs-cat params) (jms/jms->params workflow))))))
    (testing "Cromwell workflow is started by WFL"
      (let [workflow-id (timeout 180000 #(wfl/wait-for-workflow-creation wfl-url chipwell-barcode analysis-version))]
        (is (not= ::timed-out workflow-id ) "Timeout waiting for workflow creation")
        (is (uuid? (UUID/fromString workflow-id)) "Workflow id is not a valid UUID")
        (testing "Cromwell workflow succeeds"
          (let [workflow-timeout 1800000
                result (timeout workflow-timeout #(cromwell/wait-for-workflow-complete cromwell-url workflow-id))]
            (is (= "Succeeded" result) "Cromwell workflow failed")))))))
