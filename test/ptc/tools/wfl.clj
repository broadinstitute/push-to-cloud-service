(ns ptc.tools.wfl
  "Utility functions for WFL."
  (:require [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [ptc.util.misc     :as misc]))

(defn get-aou-workloads
  "Return the AllOfUsArrays workloads from WFL at WFL-URL."
  [wfl-url]
  (letfn [(aou? [workload] (= (:pipeline workload) "AllOfUsArrays"))]
    (-> (str wfl-url "/api/v1/workload")
      (client/get {:headers (misc/get-auth-header!)})
      :body misc/parse-json-string
      (->> (filter aou?)))))

(defn aou-uuid
  "Return a semipredicate that returns nil or the UUID of WORKFLOW when
  it has CHIPWELL-BARCODE ANALYSIS-VERSION-NUMBER."
  [chipwell-barcode analysis-version-number]
  (let [match? (juxt :chip_well_barcode :analysis_version_number)]
    (fn [workflow] (when (= [chipwell-barcode analysis-version-number]
                           (match? workflow))
                     (:uuid workflow)))))

(defn get-aou-workflow-ids
  "Return UUIDs of workflows at WFL-URL with CHIPWELL-BARCODE and
  ANALYSIS-VERSION-NUMBER."
  [wfl-url chipwell-barcode analysis-version-number]
  (let [match? (aou-uuid chipwell-barcode analysis-version-number)]
    (->> wfl-url
      get-aou-workloads
      (mapcat :workflows)
      (keep match?))))

(defn wait-for-workflow-creation
  "Wait for a workflow with CHIPWELL-BARCODE and ANALYSIS-VERSION-NUMBER
  to appear in an AllOfUsArrays workload in WFL-URL."
  [wfl-url chipwell-barcode analysis-version-number]
  (letfn [(fetch! [] (get-aou-workflow-ids
                       wfl-url chipwell-barcode analysis-version-number))]
    (let [seconds 15]
      (loop [ids (fetch!)]
        (if (empty? ids)
          (do (log/infof "Sleeping %s seconds" seconds)
              (misc/sleep-seconds seconds)
              (recur (fetch!)))
          (first ids))))))
