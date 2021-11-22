(ns ptc.tools.utils
  "Utility functions shared by tests."
  (:require [ptc.util.jms :as jms]))

(defn pushed-files
  "Extract GCS objects pushed by PTC from the workflow request
  NOTIFICATION and add the PARAMS params.txt file."
  [notification params]
  (let [push (-> jms/wfl-keys->jms-keys
                 ((juxt ::jms/chip ::jms/push))
                 (->> (apply merge)
                      keys
                      (apply juxt)))]
    (conj (remove nil? (push notification)) params)))
