(ns ptc.tools.wfl
  "Utility functions for WFL."
  (:require [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [ptc.util.misc     :as misc])
  (:import [java.util.concurrent TimeUnit]))

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
  "Check if a WORKFLOW has a specific CHIPWELL-BARCODE and ANALYSIS-VERSION."
  [chipwell-barcode analysis-version workflow]
  (and (= (:chip_well_barcode workflow) chipwell-barcode)
       (= (:analysis_version_number workflow) analysis-version)))

(defn get-aou-workflow-ids
  "Get the AllOfUsArrays workflow started in WFL-URL by its CHIPWELL-BARCODE and ANALYSIS-VERSION."
  [wfl-url chipwell-barcode analysis-version]
  (remove nil? (for [workload (get-aou-workloads wfl-url)]
                 (let [workflows (:workflows workload)]
                   (when (seq workflows)
                     (->> workflows
                          (filter #(is-aou-workflow? chipwell-barcode analysis-version %))
                          (first)
                          (:uuid)))))))

(defn wait-for-workflow-creation
  "Wait for a workflow with CHIPWELL-BARCODE and ANALYSIS-VERSION to appear in an AllOfUsArrays workload in WFL-URL."
  [wfl-url chipwell-barcode analysis-version]
  (let [seconds 15]
    (loop []
      (let [workflow-ids (get-aou-workflow-ids wfl-url chipwell-barcode analysis-version)]
        (if (seq workflow-ids)
          (first workflow-ids)
          (do (log/infof "Sleeping %s seconds" seconds)
              (.sleep TimeUnit/SECONDS seconds)
              (recur)))))))
