(ns ptc.tools.jms
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [ptc.start :as start]
            [ptc.util.misc :as misc]
            [ptc.util.jms :as jms]))

;; Local testing for ActiveMQ
;; https://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection
(defn with-test-queue-connection
  "CALL with a local JMS connection for testing."
  [closure]
  (let [url             "vm://localhost?broker.persistent=false"
        test-queue-name "test.queue"]
    (with-open [connection (start/create-queue-connection url)]
      (closure connection test-queue-name))))

(defn with-queue-connection
  "CALL with the ENV JMS connection for testing."
  [env closure]
  (let [config {:prod {:url        "failover:ssl://vpicard-jms-prod.broadinstitute.org:61616"
                       :queue      "wfl.broad.pushtocloud.enqueue.prod"
                       :vault-path "secret/dsde/gotc/prod/activemq/logins/zamboni"}
                :dev  {:url        "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616"
                       :queue      "wfl.broad.pushtocloud.enqueue.dev"
                       :vault-path "secret/dsde/gotc/dev/activemq/logins/zamboni"}}]
    (when-not (contains? config env)
      (throw (IllegalArgumentException. (str "No such environment " (name env)))))
    (let [{:keys [url queue vault-path]} (config env)
          {:keys [username password]} (misc/vault-secrets vault-path)]
      (with-open [connection (start/create-queue-connection url username password)]
        (closure connection queue)))))

(def message
  "Example JMS message for testing."
  (delay (edn/read-string (slurp "test/data/good-jms.edn"))))

(defn fix-paths
  "Fix the local file paths of the JMS message in FILE."
  [file]
  (letfn [(canonicalize [file] (-> file io/file .getCanonicalPath io/file))]
    (let [{:keys [::jms/chip ::jms/push]} jms/notification-keys->jms-keys
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
  "Queue N copies of MESSAGE to the ENV queue."
  [n env message]
  (let [blame (or (System/getenv "USER") "aou-ptc-jms-test/queue-message")]
    (let [payload  (-> message jms/encode ::jms/Properties)
          enqueue! (fn [[con queue]] (start/produce con queue blame payload))]
      (with-queue-connection env
        (fn [con queue]
          (run! enqueue! (repeat n [con queue])))))))

(defn -main
  [& args]
  (let [n                (edn/read-string (first args))
        env              (edn/read-string (second args))
        analysis-version (rand-int Integer/MAX_VALUE)
        where            [::jms/Properties :payload :workflow :analysisCloudVersion]
        jms-message      (fix-paths "./test/data/good-jms.edn")
        message          (assoc-in jms-message where analysis-version)]
    (when-not (pos-int? n)
      (throw (IllegalArgumentException. "Must specify a positive integer")))
    (queue-messages n (keyword env) message)))
