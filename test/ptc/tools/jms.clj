(ns ptc.tools.jms
  "Tools to aid JMS tests."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [ptc.start :as start]
            [ptc.util.environment :as env]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]))

;; https://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection
;;
(defn call-with-test-connection
  "Call PRODUCE-CONSUME to use QUEUE from a local JMS connection."
  [queue produce-consume]
  (with-open [connection (start/create-queue-connection
                          "vm://localhost?broker.persistent=false")]
    (produce-consume connection queue)))

(defn with-queue-connection
  "CALL with the JMS URL, QUEUE and VAULT-PATH for testing."
  [url queue vault-path closure]
  (let [{:keys [username password]} (misc/vault-secrets vault-path)]
    (with-open [connection (start/create-queue-connection url username password)]
      (closure connection queue))))

(def good-jms-message
  "Example JMS message for testing."
  (delay (edn/read-string (slurp "test/data/good-jms.edn"))))

(defn fix-paths
  "Fix the local file paths of the JMS message in FILE."
  [file]
  (letfn [(canonicalize [file] (-> file io/file .getCanonicalPath io/file))]
    (let [{:keys [::jms/chip ::jms/push]} jms/wfl-keys->jms-keys
          push-keys (vals (merge chip push))
          infile    (canonicalize file)
          dir       (io/file (.getParent infile))
          content   (edn/read-string (slurp infile))]
      (letfn [(one [leaf] (.getCanonicalPath (io/file dir leaf)))
              (all [workflow]
                (let [old (select-keys workflow push-keys)]
                  (merge workflow (zipmap (keys old) (map one (vals old))))))]
        (update-in content [::jms/Properties :payload :workflow] all)))))

(defn queue-messages
  "Queue N copies of MESSAGE given a JMS URL, QUEUE and VAULT-PATH."
  [message n url queue vault-path]
  (let [blame    (or (System/getenv "USER") "aou-ptc-jms-test/queue-message")
        payload  (-> message jms/encode ::jms/Properties)
        enqueue! (fn [[con queue]] (start/produce con queue blame payload))]
    (with-queue-connection url queue vault-path
      (fn [con queue]
        (run! enqueue! (repeat n [con queue]))))))

(defn -main
  [& args]
  (let [[n
         zamboni-activemq-server-url
         zamboni-activemq-queue-name
         zamboni-activemq-secret-path] args
        version [::jms/Properties :payload :workflow :analysisCloudVersion]
        count   (edn/read-string n)
        message (-> "./test/data/plumbing-test-jms-dev.edn"
                    slurp edn/read-string
                    (assoc-in version (rand-int Integer/MAX_VALUE)))]
    (when-not (pos-int? count)
      (throw (IllegalArgumentException.
              (format "%s is not a positive integer" n))))
    (queue-messages message count
                    zamboni-activemq-server-url
                    zamboni-activemq-queue-name
                    zamboni-activemq-secret-path)))
