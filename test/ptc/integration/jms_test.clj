(ns ptc.integration.integration-test
  (:require [clojure.data.json :as json]
            [clojure.test      :refer [deftest is testing]]
            [clojure.edn       :as edn]
            [ptc.start         :as start]
            [ptc.util.gcs      :as gcs]
            [ptc.util.misc     :as misc]
            [ptc.util.jms      :as jms]
            [taoensso.timbre   :as timbre])
  (:import [org.apache.activemq ActiveMQSslConnectionFactory]
           (java.util UUID)))

(def gcs-test-bucket
  "Throw test files in this bucket."
  "broad-gotc-dev-zero-test")

(def delete-test-object
  (comp (partial gcs/delete-object gcs-test-bucket) :name))

(defmacro with-temporary-gcs-folder
  "
  Create a temporary folder in GCS-TEST-BUCKET for use in BODY.
  The folder will be deleted after execution transfers from BODY.

  Example
  -------
    (with-temporary-gcs-folder uri
      ;; use temporary folder at `uri`)
      ;; <- temporary folder deleted
  "
  [uri & body]
  `(let [name# (str "ptc-test-" (UUID/randomUUID) "/")
         ~uri (gcs/gs-url gcs-test-bucket name#)]
     (try ~@body
          (finally
            (->>
              (gcs/list-objects gcs-test-bucket name#)
              (run! delete-test-object))))))

;; Local testing for ActiveMQ
;; https://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection
;;
(defn with-test-jms-connection
  "CALL with a local JMS connection for testing."
  [call]
  (let [url     "vm://localhost?broker.persistent=false"
        factory (new ActiveMQSslConnectionFactory url)
        queue   "test.queue"]
    (with-open [connection (.createQueueConnection factory)]
      (call connection queue))))

(deftest integration
  (let [bad     (misc/slurp-json "./test/data/bad-jms.json")
        good    (misc/slurp-json "./test/data/good-jms.json")
        missing (re-pattern jms/missing-keys-message)]
    (with-temporary-gcs-folder prefix
      (with-test-jms-connection
        (fn [connection queue]
          (start/produce connection queue bad bad))
        (is (thrown-with-msg? IllegalArgumentException missing
              (jms/handle-message prefix bad)))))))

(comment
  (start/with-push-to-cloud-jms-connection "dev"
    (fn [connection queue]
      (timbre/spy :warn [connection queue])
      #_(start/produce connection queue
          "TBL" (misc/slurp-json "./test/data/good-jms.json"))
      (-> [connection queue] identity
        (->> (apply start/peek-message) .getProperties (into {}))
        (get "payload")
        (json/read-str :key-fn keyword))))
  )
