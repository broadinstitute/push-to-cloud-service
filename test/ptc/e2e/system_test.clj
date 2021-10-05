(ns ptc.e2e.system-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [ptc.tools.cromwell :as cromwell]
            [ptc.tools.gcs :as gcs]
            [ptc.tools.wfl :as wfl]
            [ptc.tools.utils :as utils]
            [ptc.util.environment :as env]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]
            [ptc.tools.jms :as jms-tools])
  (:import [java.lang Integer]
           [java.util UUID]))

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
                                   [::jms/Properties
                                    :payload :workflow
                                    :analysisCloudVersion] analysis-version)
        chipwell-barcode (get-in message [::jms/Properties
                                          :payload :workflow
                                          :chipWellBarcode])
        workflow         (get-in message [::jms/Properties
                                          :payload :workflow])
        cloud-prefix     (jms/cloud-folder (env/getenv-or-throw "PTC_BUCKET_URL")
                                           workflow)]
    (jms-tools/queue-messages
     message 1
     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SERVER_URL")
     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_QUEUE_NAME")
     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SECRET_PATH"))
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
      (let [workflow-id (timeout 180000
                                 #(wfl/wait-for-workflow-creation
                                   (env/getenv-or-throw "WFL_URL")
                                   chipwell-barcode analysis-version))]
        (is (not= ::timed-out workflow-id) "Timeout waiting for workflow creation")
        (is (uuid? (UUID/fromString workflow-id)) "Workflow id is not a valid UUID")
        (testing "Cromwell workflow succeeds"
          (let [workflow-timeout 3600000
                result (timeout workflow-timeout
                                #(cromwell/wait-for-workflow-complete
                                  (env/getenv-or-throw "CROMWELL_URL")
                                  workflow-id))]
            (is (= "Succeeded" result) "Cromwell workflow failed")))))))

(deftest test-dead-letter-queue
  (testing "a bad message winds up in the dead-letter queue"
    (let [version (rand-int Integer/MAX_VALUE)
          path    [::jms/Properties :payload :workflow]
          message (-> jms-message
                      (get-in path)
                      (assoc  :analysisCloudVersion version)
                      (dissoc :chipWellBarcode)
                      (->> (assoc-in jms-message path)))]
      (jms-tools/queue-messages
       message 1
       (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SERVER_URL")
       (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_QUEUE_NAME")
       (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SECRET_PATH")))))

(comment
  (clojure.test/test-vars [#'test-dead-letter-queue])
  )
