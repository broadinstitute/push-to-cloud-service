(ns ptc.start
  (:gen-class)
  (:require [clojure.data :as data]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ptc.ptc :as ptc]
            [ptc.util.environment :as env]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc])
  (:import [javax.jms TextMessage DeliveryMode Session]
           [org.apache.activemq ActiveMQSslConnectionFactory]))

(defn create-queue-connection
  "Create a javax.jms.QueueConnection to an ActiveMQ server"
  ([url username password]
   (->
    (new ActiveMQSslConnectionFactory url)
    (.createQueueConnection username password)))
  ([url]
   (->
    (new ActiveMQSslConnectionFactory url)
    (.createQueueConnection))))

(defn create-session
  "Create a transacted JMS session on CONNECTION."
  [connection]
  (.createSession connection false Session/AUTO_ACKNOWLEDGE))

(defn create-session-transacted
  "Create a transacted JMS session on CONNECTION."
  [connection]
  (.createSession connection true Session/SESSION_TRANSACTED))

;; We are using sync receipt for now
;; https://activemq.apache.org/maven/apidocs/org/apache/activemq/ActiveMQMessageConsumer.html
;;
(defn consume
  "The text from a message from JMS QUEUE through CONNECTION."
  [connection queue]
  (with-open [session  (create-session connection)
              consumer (.createConsumer session (.createQueue session queue))]
    (.start connection)
    (log/infof "Consumer %s: attempting to consume message."
               (.getConsumerId consumer))
    (.receive consumer)))

(defn peek-message
  "Peek 1 message from JMS QUEUE through CONNECTION."
  [connection queue]
  (with-open [session (create-session connection)
              browser (.createBrowser session (.createQueue session queue))]
    (.start connection)
    (log/debugf "Browser: attempting to peek message.")
    (let [msg-enum (.getEnumeration browser)]
      (when (not (.hasMoreElements msg-enum))
        (Thread/sleep 10000))
      (.nextElement msg-enum))))

(defn produce
  "Enqueue the TEXT with PROPERTIES map to JMS QUEUE through CONNECTION."
  [connection queue text properties]
  (letfn [(add-property [^TextMessage message k v]
            (.setStringProperty message (name k) v))]
    (with-open [session (create-session-transacted connection)]
      (let [queue   (.createQueue session queue)
            message (.createTextMessage session text)]
        (doseq [[k v] properties] (add-property message k v))
        (with-open [producer (.createProducer session queue)]
          (.setDeliveryMode producer DeliveryMode/PERSISTENT)
          (.start connection)
          (.send producer message)
          (.commit session))))))

(defn listen-and-consume-from-queue
  "Call TASK! on messages from QUEUE on CONNECTION until it returns false."
  [task! connection queue]
  (loop [counter 0]
    (if-let [peeked (peek-message connection queue)]
      (let [peeked (jms/ednify peeked)]
        (log/infof "Peeked message %s: %s" counter peeked)
        (if (task! peeked connection)
          (let [consumed (jms/ednify (consume connection queue))]
            (log/infof "Task complete, consumed message %s" counter)
            (if (not (jms/message-ids-equal? peeked consumed))
              (log/warnf
               (str/join \space ["Messages differ:"
                                 (with-out-str
                                   (pprint (data/diff peeked consumed)))])))
            (recur (inc counter)))
          (do (log/errorf "Cannot consume %s" peeked)
              peeked)))
      (recur counter))))

(defn ^:private handle-or-dlq
  "Handle the JMS on CONNECTION or forward to the dead letter queue."
  [jms connection]
  (let [dlq    (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME")
        bucket (env/getenv-or-throw "PTC_BUCKET_URL")]
    (misc/trace jms)
    (try (jms/handle-message bucket jms)
         true
         (catch Throwable x
           (log/errorf "Catch %s and move %s to %s" x jms dlq)
           (-> jms jms/encode ::jms/Properties
               misc/trace
               (->> (produce connection dlq (str x))))
           true))))

(defn ^:private message-loop
  "Loop and consume messages using the Zamboni ActiveMQ server."
  []
  (let [queue   (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_QUEUE_NAME")
        url     (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SERVER_URL")
        secrets (env/getenv-or-throw "ZAMBONI_ACTIVEMQ_SECRET_PATH")
        {:keys [username password]} (misc/vault-secrets secrets)]
    (try
      (with-open [connection (create-queue-connection url username password)]
        (listen-and-consume-from-queue handle-or-dlq connection queue))
      (catch Throwable x
        (log/fatal x "Fatal error in message loop")
        (System/exit 1)))))

(defn -main
  []
  (log/infof "%s starting up" ptc/the-name)
  (message-loop))
