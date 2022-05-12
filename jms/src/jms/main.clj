(ns jms.main
  "Count messages on push-to-cloud JMS queues."
  (:require [clojure.pprint     :refer [pprint]]
            [clojure.string     :as str]
            [vault.client.http]         ; vault needs this
            [vault.core         :as vault])
  (:import [javax.jms Session]
           [org.apache.activemq ActiveMQSslConnectionFactory]))

(def environments
  "Map deployment environments to JMS connections."
  {:dev     {:queues ["wfl.broad.pushtocloud.enqueue.dev"
                      "wfl.broad.pushtocloud.enqueue.dev-dlq"]
             :url    "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616"
             :vault  "secret/dsde/gotc/dev/activemq/logins/zamboni"}
   :prod    {:queues ["wfl.broad.pushtocloud.enqueue.prod"
                      "wfl.broad.pushtocloud.enqueue.prod-dlq"]
             :url    "failover:ssl://vpicard-jms-prod.broadinstitute.org:61616"
             :vault  "secret/dsde/gotc/prod/activemq/logins/zamboni"}
   :staging {:queues ["wfl.broad.pushtocloud.enqueue.staging"
                      "wfl.broad.pushtocloud.enqueue.staging-dlq"]
             :url    "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616"
             :vault  "secret/dsde/gotc/dev/activemq/logins/zamboni"}})

(defn usage
  "Return a Usage message for THE-ARGS."
  [the-args]
  (-> ["jms:   Count messages on push-to-cloud JMS queues."
       ""
       "Usage: jms <env>"
       ""
       "Where: <env> is one of: %s"
       ""
       "You ran: clj -M:jms %s"]
      (->> (str/join \newline))
      (format (str/join \space (map name (keys environments)))
              (str/join \space the-args))))

(defn connect
  "The JMS connection map for the ENV environment."
  [env]
  (or (environments (keyword env))
      (throw (IllegalArgumentException.
              (format "jms: %s must be one of: %s"
                      env (map name (keys environments)))))))

(defn vault-secrets
  "Return the secrets at PATH in vault."
  [path]
  (let [token (->> [(System/getProperty "user.home") ".vault-token"]
                   (str/join "/")
                   slurp)]
    (vault/read-secret
     (doto (vault/new-client "https://clotho.broadinstitute.org:8200")
       (vault/authenticate! :token token))
     path {})))

(defn count-messages
  "Use SESSION to return a count of the messages on QUEUE."
  [^Session session queue]
  (let [session-queue (.createQueue session queue)]
    (let [browser (.createBrowser session session-queue)]
      (count (enumeration-seq (.getEnumeration browser))))))

(defn report-counts
  "Count messages on the JMS queues in ENVIRONMENT."
  [environment]
  (let [{:keys [dlq queues url vault]} (connect environment)
        {:keys [username password]}    (vault-secrets vault)]
    (with-open [connection (-> url
                               ActiveMQSslConnectionFactory.
                               (.createQueueConnection username password))
                session    (.createSession (doto connection .start)
                                           true ; transacted?
                                           Session/SESSION_TRANSACTED)]
      (into {} (map (fn [q] [q (count-messages session q)]) queues)))))

(defn -main
  "Parse THE-ARGS into a JMS command line, and run it."
  [& the-args]
  (when-not (== 1 (count the-args))
    (binding [*out* *err*]
      (println (usage the-args))
      (System/exit 1)))
  (let [help?       #{"h" "-h" "--h" "-help" "--help" "help"}
        environment (first the-args)]
    (when (help? (str/lower-case environment))
      (println (usage the-args))
      (System/exit 0))
    (pprint (report-counts environment))))
