(ns ptc.tools.cromwell
  "Utility functions for Cromwell."
  (:require [clj-http.client :as client]
            [ptc.util.misc :as misc]
            [ptc.tools.gcs :as gcs]))

(defn status
  "Status of the workflow with ID at CROMWELL-URL."
  [cromwell-url id]
  (let [auth-header (gcs/get-auth-header!)
        response    (client/get (str cromwell-url "/api/workflows/v1/" id "/status")
                      {:headers auth-header})]
    (->> (:body response)
      (misc/parse-json-string)
      (:status))))

(defn query
  "Query for a workflow with ID at CROMWELL-URL."
  [cromwell-url id]
  (let [auth-header (gcs/get-auth-header!)
        response    (client/get (str cromwell-url "/api/workflows/v1/" id "/query")
                      {:headers auth-header})]
    (->> (:body response)
      (misc/parse-json-string)
      (:results))))
