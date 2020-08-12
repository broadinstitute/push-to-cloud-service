(ns ptc.integration.system-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-http.client :as client]
            [ptc.start :as start]
            [ptc.util.cromwell :as cromwell]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]
            [ptc.integration.jms-test :as jms-test])
  (:import [java.util UUID]))

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
        (do (log/infof "Sleeping %s seconds" seconds)
            (misc/sleep-seconds seconds)
            (recur wfl-url chipwell-barcode analysis-version))
        (get-workflow-id wfl-url chipwell-barcode analysis-version)))))


(defn queue-message-placeholder
  [folder message]
  (let [push (-> jms/notification-keys->jms-keys
                 ((juxt ::jms/chip ::jms/push))
                 (->> (apply merge))
                 keys
                 (->> (apply juxt)))]
    (jms-test/with-test-jms-connection
      (fn [connection queue]
        (start/produce connection queue
                       "GOOD" (::jms/Properties (jms/encode message)))
      (let [msg (start/consume connection queue)
            [params ptc] (jms/handle-message folder msg)
            gcs (jms-test/list-gcs-folder folder)]
        (print "Pushed message")
        (print ptc)
        (print gcs))))))

(defn end-to-end-test
  [env bucket cromwell-url wfl-url]
  ; create JMS test message with paths to test files on prem
  (let [chipwell-barcode (str (UUID/randomUUID))
        analysis-version 1
        message (assoc-in (jms-test/fix-paths "./test/data/good-jms.edn")
                          [::jms/Properties :payload :workflow :chipWellBarcode] chipwell-barcode)]
    ;(jms-test/queue-messages 1 message env)
    (queue-message-placeholder bucket message)

    ;(let [workflow-id (first (get-workflow-id wfl-url chipwell-barcode analysis-version))]
    ;(cromwell/wait-for-workflow-complete cromwell-url workflow-id)
    ))

(comment (end-to-end-test "dev"))
(defn -main
  [& args]
  (let [env "dev"
        bucket "gs://dev-aou-arrays-input"
        cromwell-url "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org"
        wfl-url "https://workflow-launcher.gotc-dev.broadinstitute.org"]
    (end-to-end-test env bucket cromwell-url wfl-url)))