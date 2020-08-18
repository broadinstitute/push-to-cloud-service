(ns ptc.system.system-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [ptc.tools.cromwell :as cromwell]
            [ptc.tools.gcs :as gcs]
            [ptc.tools.wfl :as wfl]
            [ptc.util.jms :as jms]
            [ptc.integration.jms-test :as jms-test])
  (:import [java.util UUID]
           [java.lang Integer]))

(def environment
  (or (System/getenv "ENVIRONMENT") "dev"))

(def bucket
  (or (System/getenv "PTC_BUCKET_NAME") "gs://dev-aou-arrays-input"))

(def cromwell-url
  (if (= environment "prod")
    "https://cromwell-aou.gotc-prod.broadinstitute.org"
    "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org"))

(def wfl-url
  (if (= environment "prod")
    "https://aou-wfl.gotc-prod.broadinstitute.org"
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

(deftest test-end-to-end
  (let [analysis-version (rand-int Integer/MAX_VALUE)
        message (assoc-in jms-message
                          [::jms/Properties :payload :workflow :analysisCloudVersion] analysis-version)
        chipwell-barcode (get-in message [::jms/Properties :payload :workflow :chipWellBarcode])
        workflow (get-in message [::jms/Properties :payload :workflow])
        cloud-prefix (jms/cloud-prefix bucket workflow)
        push (-> jms/notification-keys->jms-keys
                 ((juxt ::jms/chip ::jms/push))
                 (->> (apply merge))
                 keys
                 (->> (apply juxt)))]
    (jms-test/queue-messages 1 environment message)
    (testing "Files are uploaded to the input bucket"
      (let [params (str cloud-prefix "/params.txt")
            ptc (str cloud-prefix "/ptc.json")
            wait (timeout 180000 #(gcs/wait-for-files-in-bucket cloud-prefix [ptc]))
            {:keys [notifications] :as request} (gcs/gcs-edn ptc)
            pushed (conj (push (first notifications)) params)
            gcs (timeout 180000 #(gcs/wait-for-files-in-bucket cloud-prefix pushed))]
        (is (== (count pushed) (count (set pushed))))
        (is (not= gcs :ptc.system.system-test/timed-out))
        (let [union (set/union (set gcs) (set pushed))
              diff (set/difference (set gcs) (set pushed))]
          (is (== (count gcs) (count (set gcs))))
          (is (== (count union) (count (set gcs))))
          (is (== 1 (count diff)))
          (is (= diff (set [ptc])))
          (is (= (jms/jms->params workflow) (gcs/gcs-cat params))))))
    (testing "Cromwell workflow is started by WFL"
      (let [workflow-id (timeout 180000 #(wfl/wait-for-workflow-creation wfl-url chipwell-barcode analysis-version))]
        (is (not= workflow-id :ptc.system.system-test/timed-out))
        (is (uuid? (UUID/fromString workflow-id)))
        (testing "Cromwell workflow succeeds"
          (let [workflow-timeout 1800000
                result (timeout workflow-timeout #(cromwell/wait-for-workflow-complete cromwell-url workflow-id))]
            (is (= result "Succeeded"))))))))
