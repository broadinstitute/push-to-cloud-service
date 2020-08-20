(ns ptc.tools.utils
  "Utility functions shared by tests."
  (:require [ptc.util.jms     :as jms]))

(defn pushed-files
  "Get files pushed by PTC."
  [notification params]
  (let [push (-> jms/notification-keys->jms-keys
                 ((juxt ::jms/chip ::jms/push))
                 (->> (apply merge)
                      keys
                      (apply juxt)))]
    (conj (push notification) params)))
