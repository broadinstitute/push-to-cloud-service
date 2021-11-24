(ns ptc.tools.cromwell
  "Utility functions for Cromwell."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [ptc.tools.gcs :as gcs]
            [ptc.util.misc :as misc]))

(defn status
  "Status of the workflow with ID at CROMWELL-URL."
  [cromwell-url id]
  (let [url (str/join "/" [cromwell-url "api" "workflows" "v1" id "status"])]
    (->>  {:method       :get           ; :debug true :debug-body true
           :content-type :application/json
           :url          url
           :headers      (misc/get-auth-header!)}
          client/request :body misc/parse-json-string :status)))

(defn query
  "Query for a workflow with ID at CROMWELL-URL."
  [cromwell-url id]
  (->> (str cromwell-url "/api/workflows/v1/" id "/query")
       (client/get {:headers (misc/get-auth-header!)})
       :body misc/parse-json-string :results))

(defn work-around-cromwell-fail-bug
  "Wait 2 seconds and ignore up to N times a bogus failure response
  from Cromwell for workflow ID.  Work around the 'sore spot' reported
  in https://github.com/broadinstitute/cromwell/issues/2671.  From
  https://github.com/broadinstitute/wfl/blob/master/api/src/zero/service/cromwell.clj#L266"
  [n cromwell-url id]
  (misc/sleep-seconds 2)
  (let [fail {"status" "fail" "message" (str "Unrecognized workflow ID: " id)}
        {:keys [body] :as bug} (try (status cromwell-url id)
                                    (catch Exception e (ex-data e)))]
    (misc/trace [bug n])
    (when (and (pos? n) bug
               (= 404 (:status bug))
               (= fail (json/read-str body)))
      (recur (dec n) cromwell-url id))))

(defn wait-for-workflow-complete
  "Return status of workflow named by ID when it completes."
  [cromwell-url id]
  (work-around-cromwell-fail-bug 9 cromwell-url id)
  (loop [cromwell-url cromwell-url id id]
    (let [seconds 15
          now (status cromwell-url id)]
      (if (#{"Submitted" "Running"} now)
        (do (log/infof "%s: Sleeping %s seconds on status: %s"
                       id seconds now)
            (misc/sleep-seconds seconds)
            (recur cromwell-url id))
        (status cromwell-url id)))))
