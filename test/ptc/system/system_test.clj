(ns ptc.system.system-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]
            [clj-http.client :as client]
            [ptc.start :as start]
            [ptc.util.cromwell :as cromwell]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]
            [ptc.integration.jms-test :as jms-test])
  (:import [java.util UUID]))

(def environment
  (or (System/getenv "environment") "dev"))

(def bucket
  "gs://dev-aou-arrays-input")

(def cromwell-url
  "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org")

(def wfl-url
  "https://workflow-launcher.gotc-dev.broadinstitute.org")

(defn get-aou-workloads
  "Query WFL for AllOfUsArrays workloads"
  [wfl-url]
  (let [auth-header (misc/get-auth-header!)
        response (client/get (str wfl-url "/api/v1/workload")
                             {:headers auth-header})]
    (letfn [(array-workload? [workload]
              (= (:pipeline workload) "AllOfUsArrays"))]
      (->> (:body response)
           (misc/parse-json-string)
           (filter array-workload?)))))

(defn is-aou-workflow?
  [chipwell-barcode analysis-version workflow]
  (and (= (:chip_well_barcode workflow) chipwell-barcode)
       (= (:analysis_version_number workflow) analysis-version)))

(defn get-workflow-id
  [wfl-url chipwell-barcode analysis-version]
  (remove nil? (for [workload (get-aou-workloads wfl-url)]
                 (let [workflows (:workflows workload)]
                   (if (seq workflows)
                     (->> workflows
                          (filter #(is-aou-workflow? chipwell-barcode analysis-version %))
                          (first)
                          (:uuid)))))))

(defn wait-for-workflow-creation
  [wfl-url chipwell-barcode analysis-version]
  (loop [wfl-url wfl-url
         chipwell-barcode chipwell-barcode
         analysis-version analysis-version]
    (let [seconds 15
          workflow-id (get-workflow-id wfl-url chipwell-barcode analysis-version)]
      (if (nil? (first workflow-id))
        (do (print "Sleeping %s seconds" seconds)
            (misc/sleep-seconds seconds)
            (recur wfl-url chipwell-barcode analysis-version))
        (first (get-workflow-id wfl-url chipwell-barcode analysis-version))))))

(defn timeout
  [milliseconds function]
  (let [f (future (function))
        return (deref f milliseconds ::timed-out)]
    (when (= return ::timed-out)
      (future-cancel f))
    return))

(defn queue-message-placeholder
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
        message (assoc-in (jms-test/fix-paths "./test/data/good-jms.edn")
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
    (let [ptc (str cloud-prefix "/ptc.json")
          {:keys [notifications] :as request} (jms-test/gcs-edn ptc)
          pushed (push (first notifications))
          gcs (jms-test/list-gcs-folder bucket)
          union (set/union (set gcs) (set pushed))
          diff (set/difference (set gcs) (set pushed))]
      (is (== (count pushed) (count (set pushed))))
      (is (== (count gcs) (count (set gcs))))
      (is (== (count union) (count (set gcs))))
      (is (== 2 (count diff)))
      ;(is (= diff (set [params ptc])))
      ;(is (= (jms/jms->params workflow) (gcs-cat params)))
      (let [workflow-id (timeout 300000 #(wait-for-workflow-creation wfl-url chipwell-barcode analysis-version))]
        (println (str "Found workflow: " workflow-id))
        (let [workflow-timeout 1800000
              result (timeout workflow-timeout #(cromwell/wait-for-workflow-complete cromwell-url workflow-id))]
          (is (= result "Succeeded")))))))

(comment
  (test-end-to-end))
