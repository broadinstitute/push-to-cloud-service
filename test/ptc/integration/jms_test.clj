(ns ptc.integration.jms-test
  (:require [clojure.data :as data]
            [clojure.edn       :as edn]
            [clojure.java.io   :as io]
            [clojure.set       :as set]
            [clojure.test      :refer [deftest is testing]]
            [ptc.start         :as start]
            [ptc.tools.gcs      :as gcs]
            [ptc.util.jms      :as jms]
            [ptc.util.misc      :as misc])
  (:import [java.util UUID]
           [org.apache.activemq ActiveMQSslConnectionFactory]))

(def gcs-test-bucket
  "Throw test files in this bucket."
  "broad-gotc-dev-zero-test")

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
         ~uri (misc/gs-url gcs-test-bucket name#)]
     (try
       ~@body
       (finally
         (->>
          (gcs/list-objects gcs-test-bucket name#)
          (run! (comp (partial gcs/delete-object gcs-test-bucket) :name)))))))

;; Local testing for ActiveMQ
;; https://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection
;;
(defn with-test-jms-connection
  "CALL with a local JMS connection for testing."
  [call]
  (let [url "vm://localhost?broker.persistent=false"
        factory (new ActiveMQSslConnectionFactory url)
        queue "test.queue"]
    (with-open [connection (.createQueueConnection factory)]
      (call connection queue))))

(defn fix-paths
  "Fix the local file paths of the JMS message in FILE."
  [file]
  (letfn [(canonicalize [file] (-> file io/file .getCanonicalPath io/file))]
    (let [{:keys [::jms/chip ::jms/push]} jms/notification-keys->jms-keys
          push-keys (vals (merge chip push))
          infile (canonicalize file)
          dir (io/file (.getParent infile))
          content (edn/read-string (slurp infile))]
      (letfn [(one [leaf] (.getCanonicalPath (io/file dir leaf)))
              (all [workflow]
                (let [old (select-keys workflow push-keys)]
                  (merge workflow (zipmap (keys old) (map one (vals old))))))]
        (update-in content [::jms/Properties :payload :workflow] all)))))

(deftest push-notification-for-jms
  (let [path [::jms/Properties :payload :workflow]
        push (-> jms/notification-keys->jms-keys
                 ((juxt ::jms/chip ::jms/push))
                 (->> (apply merge)
                      keys
                      (apply juxt)))
        bad (fix-paths "./test/data/bad-jms.edn")
        good (fix-paths "./test/data/good-jms.edn")
        missing (-> good (data/diff bad) first (get-in path) keys first
                    (->> (str jms/missing-keys-message ".*"))
                    re-pattern)
        workflow (get-in good path)]
    (with-temporary-gcs-folder folder
      (with-test-jms-connection
        (fn [connection queue]
          (testing "a BAD message"
            (start/produce connection queue
                           "BAD" (::jms/Properties (jms/encode bad)))
            (let [msg (start/consume connection queue)]
              (is (thrown-with-msg? IllegalArgumentException missing
                                    (jms/handle-message folder (jms/ednify msg))))
              (is (empty? (->> folder
                               gcs/parse-gs-url
                               (apply gcs/list-objects))))))
          (testing "a GOOD message"
            (start/produce connection queue
                           "GOOD" (::jms/Properties (jms/encode good)))
            (let [msg (start/consume connection queue)
                  [params ptc] (jms/handle-message folder (jms/ednify msg))
                  {:keys [notifications] :as request} (gcs/gcs-edn ptc)
                  pushed (push (first notifications))
                  gcs (gcs/list-gcs-folder folder)
                  union (set/union (set gcs) (set pushed))
                  diff (set/difference (set gcs) (set pushed))]
              (is (== (count pushed) (count (set pushed))))
              (is (== (count gcs) (count (set gcs))))
              (is (== (count union) (count (set gcs))))
              (is (== 2 (count diff)))
              (is (= diff (set [params ptc])))
              (is (= (jms/jms->params workflow) (gcs/gcs-cat params))))))))))

(defn queue-messages
  "Queue N messages to the 'dev' queue."
  [n env message]
  (let [blame (or (System/getenv "USER") "aou-ptc-jms-test/queue-message")]
    (letfn [(make [n] (-> message
                          jms/encode
                          ::jms/Properties))]
      (start/with-push-to-cloud-jms-connection env
        (fn [connection queue]
          (run! (partial start/produce connection queue blame)
                (map make (range 1 (inc n)))))))))

(defn -main
  [& args]
  (let [n (edn/read-string (first args))
        env "dev"
        analysis-version (rand-int Integer/MAX_VALUE)
        where [::jms/Properties :payload :workflow :analysisCloudVersion]
        jms-message (fix-paths "./test/data/good-jms.edn")
        message (assoc-in where analysis-version jms-message)]
    (queue-messages n env message)))
