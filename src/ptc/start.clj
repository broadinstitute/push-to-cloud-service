(ns ptc.start
  (:gen-class)
  (:require [clojure.data :as data]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ptc.ptc :as ptc]
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
  "Listen to QUEUE on CONNECTION for messages,
  and call (TASK! message) until it is false."
  [task! connection queue]
  (loop [counter 0]
    (if-let [peeked (peek-message connection queue)]
      ; to avoid NPE on ednify
      (let [peeked (jms/ednify peeked)]
        (do
          (log/infof "Peeked message %s: %s" counter peeked)
          (if (task! peeked)
            (let [consumed (jms/ednify (consume connection queue))]
              (log/infof "Task complete, consumed message %s" counter)
              (if (not (misc/message-ids-equal? peeked consumed))
                (log/warnf
                 (str/join \space ["Messages differ:"
                                   (with-out-str (pprint (data/diff peeked consumed)))])))
              (recur (inc counter)))
            (do
              (log/errorf
               (str/join
                \space ["Task returned nil/false,"
                        "not consuming message %s and instead exiting"])
               counter)
              peeked))))
      (recur counter))))

(defn- message-loop
  "Loop and consume messages using the Zamboni ActiveMQ server."
  []
  (let [queue          (misc/getenv-or-throw "ZAMBONI_ACTIVEMQ_QUEUE_NAME")
        url            (misc/getenv-or-throw "ZAMBONI_ACTIVEMQ_URL")
        vault-path     (misc/getenv-or-throw "ZAMBONI_ACTIVEMQ_SECRET_PATH")
        bucket-url     (misc/getenv-or-throw "PTC_BUCKET_URL")
        upload-sample! (partial jms/handle-message bucket-url)
        {:keys [username password]} (misc/vault-secrets vault-path)]
    (while true
      (try
        (with-open [connection (create-queue-connection url username password)]
          (listen-and-consume-from-queue upload-sample! connection queue))
        (catch Throwable x
          (log/error x "caught in message-loop"))))))

(defn -main
  []
  (log/infof "%s starting up" ptc/the-name)
  (message-loop))
