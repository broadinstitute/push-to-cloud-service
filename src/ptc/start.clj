(ns ptc.start
  (:gen-class)
  (:require [clojure.data          :as data]
            [clojure.pprint        :refer [pprint]]
            [clojure.string        :as str]
            [clojure.tools.logging :as log]
            [ptc.ptc               :as ptc]
            [ptc.util.jms          :as jms]
            [ptc.util.misc         :as misc])
  (:import [javax.jms TextMessage DeliveryMode Session JMSException]
           [org.apache.activemq ActiveMQSslConnectionFactory]))

(defn with-push-to-cloud-jms-connection
  "Call (use connection queue) for the JMS queue in ENVIRONMENT with PUSH-TO as the parameter."
  [environment push-to use]
  (let [path (format "secret/dsde/gotc/%s/activemq/logins/zamboni" environment)
        {:keys [url username password queue]} (misc/vault-secrets path)
        factory (new ActiveMQSslConnectionFactory url)]
    (with-open [connection (.createQueueConnection factory username password)]
      (use connection queue push-to))))

(defn create-session
  "Open a JMS session on CONNECTION, conditionally TRANSACTED?"
  [connection transacted?]
  (.createSession connection transacted?
                  (if transacted?
                    Session/SESSION_TRANSACTED
                    Session/AUTO_ACKNOWLEDGE)))

;; We are using sync receipt for now
;; https://activemq.apache.org/maven/apidocs/org/apache/activemq/ActiveMQMessageConsumer.html
;;
(defn consume
  "The text from a message from JMS QUEUE through CONNECTION."
  [connection queue]
  (with-open [session (create-session connection false)]
    (let [queue (.createQueue session queue)]
      (with-open [consumer (.createConsumer session queue)]
        (.start connection)
        (log/infof
         "Consumer %s: attempting to consume message."
         (.getConsumerId consumer))
        (.receive consumer)))))

(defn peek-message
  "Peek 1 message from JMS QUEUE through CONNECTION."
  [connection queue]
  (with-open [session (create-session connection false)]
    (let [queue (.createQueue session queue)]
      (with-open [browser (.createBrowser session queue)]
        (.start connection)
        (log/debugf "Browser: attempting to peek message.")
        (let [msg-enum (.getEnumeration browser)]
          (when (not (.hasMoreElements msg-enum))
            (Thread/sleep 10000))
          (.nextElement msg-enum))))))

(defn produce
  "Enqueue the TEXT with PROPERTIES map to JMS QUEUE through CONNECTION."
  [connection queue text properties]
  (letfn [(add-property [^TextMessage message k v]
            (.setStringProperty message (name k) v))
          (send [connection queue]
            (with-open [session (create-session connection true)]
              (let [queue (.createQueue session queue)
                    message (.createTextMessage session text)]
                (doseq [[k v] properties]
                  (add-property message k v))
                (with-open [producer (.createProducer session queue)]
                  (.setDeliveryMode producer DeliveryMode/PERSISTENT)
                  (.start connection)
                  (.send producer message)
                  (.commit session)))))]
    (send connection queue)))

(defn listen-and-consume-from-queue
  "Listen to QUEUE on CONNECTION for messages,
  and call (TASK! message) with PUSH-TO param
  until it is false."
  ([task! connection queue push-to]
   (loop [counter 0]
     (if-let [peeked (peek-message connection queue)]
       ; to avoid NPE on ednify
       (let [peeked (jms/ednify peeked)]
         (do (log/infof "Peeked message %s: %s" counter peeked)
             (if (task! push-to peeked)
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
  ([connection queue push-to]
   (listen-and-consume-from-queue jms/handle-message connection queue push-to)))

(defn message-loop
  "Loop with a JMS connection in ENVIRONMENT that pushes data to PUSH-TO."
  [environment push-to]
  (while true
    (try
      (with-push-to-cloud-jms-connection
        environment
        push-to
        listen-and-consume-from-queue)
      (catch Throwable x
        (log/error x "caught in message-loop")))))

(defn -main
  []
  (let [ptc-bucket-name (or (System/getenv "ptc_bucket_name") "broad-gotc-dev-zero-test")
        push-to (misc/gs-url ptc-bucket-name)
        environment (or (System/getenv "environment") "dev")]
    (log/infof "%s starting up on %s" ptc/the-name environment)
    (message-loop environment push-to)))
