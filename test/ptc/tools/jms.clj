(ns ptc.tools.jms
  (:require [clojure.edn :as edn]
            [ptc.start :as start]
            [ptc.util.misc :as misc]))

;; Local testing for ActiveMQ
;; https://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection
(defn with-test-queue-connection
  "CALL with a local JMS connection for testing."
  [closure]
  (let [url "vm://localhost?broker.persistent=false"
        test-queue-name "test.queue"]
    (with-open [connection (start/create-queue-connection url)]
      (closure connection test-queue-name))))

(defn with-dev-queue-connection
  "CALL with the dev JMS connection for testing."
  [closure]
  (let [url            "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616"
        dev-queue-name "wfl.broad.pushtocloud.enqueue.dev"
        vault-path     "secret/dsde/gotc/dev/activemq/logins/zamboni"
        {:keys [username password]} (misc/vault-secrets vault-path)]
    (with-open [connection (start/create-queue-connection url username password)]
      (closure connection dev-queue-name))))

(def message
  "Example JMS message for testing."
  (delay (edn/read-string (slurp "test/data/good-jms.edn"))))
