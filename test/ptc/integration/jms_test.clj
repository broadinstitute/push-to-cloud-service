(ns ptc.integration.integration-test
  (:require [clojure.data.json :as json]
            [clojure.java.io   :as io]
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
  `(let [name# (str "ptc-test-" (UUID/randomUUID))
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

(defn list-gcs-folder
  "Return the contents of folder in GCS."
  [folder]
  (apply gcs/list-objects (gcs/parse-gs-url folder)))

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

(defn gcs-cat
  "Return the content of the GCS object at URL."
  [url]
  (misc/shell! "gsutil" "cat" url))

(defn gcs-edn
  "Return the JSON in GCS URL as EDN."
  [url]
  (-> url gcs-cat (json/read-str :key-fn keyword)))

(deftest integration
  (let [pushed  (-> jms/notification-keys->jms-keys
                  ((juxt ::jms/chip ::jms/push))
                  (->> (apply merge))
                  keys
                  (->> (apply juxt)))
        bad     (fix-paths "./test/data/bad-jms.edn")
        good    (fix-paths "./test/data/good-jms.edn")
        missing (re-pattern jms/missing-keys-message)]
    (with-temporary-gcs-folder folder
      (with-test-jms-connection
        (fn [connection queue]
          #_(start/produce connection queue
              "BAD" (::jms/Properties (jms/encode bad)))
          #_(let [msg (start/consume connection queue)]
              (is (thrown-with-msg? IllegalArgumentException missing
                    (jms/handle-message folder msg)))
              (is (empty? (list-gcs-folder folder))))
          (start/produce connection queue
            "GOOD" (::jms/Properties (jms/encode good)))
          (let [msg (start/consume connection queue)
                url (jms/handle-message folder msg)
                {:keys [notifications] :as request} (gcs-edn url)]
            (misc/trace request)
            (misc/trace (pushed (first notifications)))
            (misc/trace (list-gcs-folder folder))))))))

(comment
  (with-temporary-gcs-folder folder
    (misc/trace folder)
    (apply gcs/list-objects (gcs/parse-gs-url folder)))
  (integration)
  (start/with-push-to-cloud-jms-connection "dev"
    (fn [connection queue]
      (timbre/spy :warn [connection queue])
      #_(start/produce connection queue
          "TBL" (misc/slurp-json "./test/data/good-jms.json"))
      (with-open [session (start/create-session connection true)])
      (jms/ednify (start/peek-message connection queue))))
  )
