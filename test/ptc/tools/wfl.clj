(ns ptc.tools.wfl
  "Utility functions for WFL."
  (:require [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [ptc.util.misc     :as misc]))

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

(defn get-workflow-id
  "Get the AllOfUsArrays workflow started in WFL-URL by its CHIPWELL-BARCODE and ANALYSIS-VERSION."
  [wfl-url chipwell-barcode analysis-version]
  (remove nil? (for [workload (get-aou-workloads wfl-url)]
                 (let [workflows (:workflows workload)]
                   (if (seq workflows)
                     (->> workflows
                          (filter #(is-aou-workflow? chipwell-barcode analysis-version %))
                          (first)
                          (:uuid)))))))

(defn wait-for-workflow-creation
  "Wait for a workflow with CHIPWELL-BARCODE and ANALYSIS-VERSION to appear in an AllOfUsArrays workload in WFL-URL."
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
