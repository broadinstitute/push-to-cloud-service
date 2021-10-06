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
  (:import [java.util UUID]))

(defn timeout
  "Timeout FUNCTION after MINUTES."
  ([] ::timed-out)
  ([seconds function]
   (let [cancel (timeout)
         ff     (future (function))
         result (deref ff (* 60 1000 seconds) cancel)]
     (when (= cancel result)
       (future-cancel ff))
     result)))

(defn queue-jms-message
  "Queue a new JMS message and return its :workflow part."
  []
  (let [properties [::jms/Properties :payload :workflow]
        version    (rand-int Integer/MAX_VALUE)
        message    (edn/read-string
                    (slurp "./test/data/plumbing-test-jms-dev.edn"))
        workflow   (get-in message properties)
        result     (assoc workflow :analysisCloudVersion version)]
    (jms-tools/queue-messages
     (assoc-in message properties result) 1
     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SERVER_URL")
     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_QUEUE_NAME")
     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SECRET_PATH"))
    result))

(deftest test-end-to-end
  (let [{:keys [analysisCloudVersion chipWellBarcode] :as workflow}
        (queue-jms-message)
        prefix (partial jms/in-cloud-folder
                        (env/getenv-or-throw "PTC_BUCKET_URL") workflow)]
    (testing "Files are uploaded to the input bucket"
      (let [params   (prefix "params.txt")
            ptc-file (prefix "ptc.json")]
        (is (not= (timeout) (timeout 6 #(gcs/wait-for-files [ptc-file])))
            "Timed out waiting for ptc.json to upload")
        (let [{:keys [notifications]} (gcs/gcs-edn ptc-file)]
          (is (not= (timeout) (timeout 3 #(-> notifications first
                                              (utils/pushed-files params)
                                              gcs/wait-for-files)))
              "Timed out waiting for expected files to upload")
          (is (= (gcs/gcs-cat params) (jms/jms->params workflow))))))
    (testing "Cromwell workflow is started by WFL"
      (let [workflow-id (timeout 3 #(wfl/wait-for-workflow-creation
                                     (env/getenv-or-throw "WFL_URL")
                                     chipWellBarcode analysisCloudVersion))]
        (is (not= (timeout) workflow-id)
            "Timeout waiting for workflow creation")
        (is (uuid? (UUID/fromString workflow-id))
            "Workflow id is not a valid UUID")
        (testing "Cromwell workflow succeeds"
          (let [result (timeout 60 #(cromwell/wait-for-workflow-complete
                                     (env/getenv-or-throw "CROMWELL_URL")
                                     workflow-id))]
            (is (= "Succeeded" result) "Cromwell workflow failed")))))))

(deftest test-dead-letter-queue
  (testing "a bad message winds up in the dead-letter queue"
    (let [workflow (queue-jms-message)
          barcode  (:chipWellBarcode workflow)
          prefix   (env/getenv-or-throw "PTC_BUCKET_URL")])
    (is false)))

(comment (clojure.test/test-vars [#'test-dead-letter-queue]))
