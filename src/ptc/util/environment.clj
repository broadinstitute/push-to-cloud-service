(ns ptc.util.environment
  "Define a default dev environment."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ptc.util.misc :as misc])
  (:import [java.io FileNotFoundException]))

(def ^:private defaults
  "Default environment variables to development values for testing."
  {"CROMWELL_URL"
   "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org"
   "PTC_BUCKET_URL"
   "gs://dev-aou-arrays-input"
   "WFL_URL"
   "https://dev-wfl.gotc-dev.broadinstitute.org"
   "ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME"
   "wfl.broad.pushtocloud.enqueue.dev-dlq"
   "ZAMBONI_ACTIVEMQ_QUEUE_NAME"
   "wfl.broad.pushtocloud.enqueue.dev"
   "ZAMBONI_ACTIVEMQ_SECRET_PATH"
   "secret/dsde/gotc/dev/activemq/logins/zamboni"
   "ZAMBONI_ACTIVEMQ_SERVER_URL"
   "failover:ssl://vpicard-jms-dev.broadinstitute.org:61616"})

(defn getenv
  "Get the environment variable value or its default."
  [name]
  (log/debug (format "Reading environment variable %s" name))
  (let [result (System/getenv name)]
    (if (nil? result) (defaults name) result)))

(defn getenv-or-throw
  "Get value of environment variable NAME or throw if nil."
  [name]
  (let [value (getenv name)]
    (when (nil? value)
      (throw (IllegalStateException. (str name " must not be nil"))))
    value))
