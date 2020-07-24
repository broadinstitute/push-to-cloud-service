(ns ptc.start
  (:gen-class)
  (:require [ptc.ptc         :as ptc]
            [ptc.util.misc   :as misc]
            [clojure.tools.logging :as log])
  (:import  [org.apache.activemq ActiveMQSslConnectionFactory]
            [javax.jms TextMessage DeliveryMode Session JMSException]))

(defn with-push-to-cloud-jms-connection
  "Call (use connection queue) for the JMS queue in ENVIRONMENT."
  [environment use]
  (let [path (format "secret/dsde/gotc/%s/activemq/logins/zamboni" environment)
        {:keys [url username password queue]} (misc/vault-secrets path)
        factory (new ActiveMQSslConnectionFactory url)]
    (with-open [connection (.createQueueConnection factory username password)]
      (use connection queue))))

(defn create-session
  "Open a JMS session on CONNECTION, conditionally TRANSACTED?"
  [connection transacted?]
  (.createSession connection transacted?
    (if transacted? Session/SESSION_TRANSACTED Session/AUTO_ACKNOWLEDGE)))

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
        (log/infof "Consumer %s: attempting to consume message." (.getConsumerId consumer))
        #_(.receive consumer)))))

(defn peek-message
  "Peek 1 message from JMS QUEUE through CONNECTION."
  [connection queue]
  (with-open [session (create-session connection false)]
    (let [queue (.createQueue session queue)]
      (with-open [browser (.createBrowser session queue)]
        (.start connection)
        (log/infof "Browser: attempting to peek message.")
        (let [msg-enum (.getEnumeration browser)]
          (when (.hasMoreElements msg-enum)
            (timbre/spy :info (.nextElement msg-enum))))))))

(defn produce
  "Enqueue the TEXT with PROPERTIES map to JMS QUEUE through CONNECTION."
  [connection queue text properties]
  (letfn [(add-property [^TextMessage message k v]
            (.setStringProperty message k v))
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

(defn parse-message
  "Parse the JMS MESSAGE."
  [^TextMessage message]
  (let [parsed {:headers (.getText message)}]
    (assoc parsed :properties
      (into {} (for [[k v] (.getProperties message)] [k v])))))

(defn listen-and-consume-from-queue
  "Listen to QUEUE on CONNECTION for messages,
  and call (TASK! message) until it is false."
  ([connection queue task!]
   (loop [counter 0]
<<<<<<< HEAD
     (if-let [peeked-message (parse-message (peek-message connection queue))]
       (do (log/infof "Peeked message %s: %s" counter peeked-message)
           (if (task! peeked-message)
             (let [consumed-message (parse-message (consume connection queue))]
               (log/infof "Task complete, consumed message %s" counter)
               (if (not (misc/message-ids-equal? peek-message consumed-message))
                 (log/warnf "Is PTC in parallel? Peeked message ID %s not equal to consumed ID %s"
                            (-> peek-message :headers :message-id)
                            (-> consumed-message :headers :message-id)))
               (recur (inc counter)))
             (do
               (log/errorf "Task returned nil/false, not consuming message %s and instead exiting" counter)
               peeked-message)))
=======
     (if-let [peeked (parse-message (peek-message connection queue))]
       (do (timbre/spy :info [counter peeked])
           (if (task! peeked)
             (let [consumed (parse-message (consume connection queue))]
               (timbre/spy :info [counter consumed])
               (when-not (= peeked consumed)
                 (timbre/spy :warn (-> peeked   :headers :message-id))
                 (timbre/spy :warn (-> consumed :headers :message-id)))
               (recur (inc counter)))
             (timbre/spy :warn peeked)))
>>>>>>> fd4aed6... Simplify and spy.
       (recur counter))))
  ([connection queue]
   (listen-and-consume-from-queue connection queue identity)))

(defn message-loop
  "Loop with a JMS connection in ENVIRONMENT."
  [environment]
  (while true
    (try
<<<<<<< HEAD
      (with-push-to-cloud-jms-connection environment listen-and-consume-from-queue)
      (catch JMSException e
        (log/error e "JMS-specific exception stopped message-loop"))
      (catch Throwable e
        (log/error e "General throwable stopped message-loop")))))
=======
      (with-push-to-cloud-jms-connection
        environment listen-and-consume-from-queue)
      (catch JMSException e (timbre/error (str (.getMessage e)))))))
>>>>>>> fd4aed6... Simplify and spy.

(defn -main
  []
  (let [environment (or (System/getenv "environment") "dev")]
    (log/infof "%s starting up on %s" ptc/the-name environment)
    (message-loop environment)))
