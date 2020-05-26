(ns start
  (:gen-class)
  (:require [ptc]
            [util.misc        :as misc]
            [clojure.edn      :as edn]
            [clojure.walk     :as walk]
            [taoensso.timbre  :as timbre])
  (:import  [org.apache.activemq ActiveMQSslConnectionFactory]
            [javax.jms TextMessage DeliveryMode Session JMSException]))

(defn with-push-to-cloud-jms-connection
  "CALL (use connection queue) for the push-to-cloud JMS queue with ENVIRONMENT."
  [environment call]
  (let [vault-path (format "secret/dsde/gotc/%s/activemq/logins/zamboni" environment)
        {:keys [url username password queue]} (misc/vault-secrets vault-path)
        factory (new ActiveMQSslConnectionFactory url)]
    (with-open [connection (.createQueueConnection factory username password)]
      (call connection queue))))

;; We are using sync receipt for now
;; check https://activemq.apache.org/maven/apidocs/org/apache/activemq/ActiveMQMessageConsumer.html
;;
(defn consume
  "Nil on timeout or the text from a message from JMS QUEUE through CONNECTION."
  [connection queue]
  (let [transacted? false
        timeout 10000]
    (with-open [session (.createSession connection transacted? Session/AUTO_ACKNOWLEDGE)]
      (let [queue (.createQueue session queue)]
        (with-open [consumer (.createConsumer session queue)]
          (.start connection)
          (timbre/info (format "Consumer %s: attempting to consume message." (.getConsumerId consumer)))
          (.receive consumer timeout))))))

(defn peek-message
  "Peek 1 message from JMS QUEUE through CONNECTION."
  [connection queue]
  (let [transacted? false]
    (with-open [session (.createSession connection transacted? Session/AUTO_ACKNOWLEDGE)]
      (let [queue (.createQueue session queue)]
        (with-open [browser (.createBrowser session queue)]
          (.start connection)
          (timbre/info (format "Browser: attempting to peek message."))
          (let [msg-enum (.getEnumeration browser)]
            (when (.hasMoreElements msg-enum)
              (.nextElement msg-enum))))))))

(defn produce
  "Enqueue the TEXT with PROPERTIES map to JMS QUEUE through CONNECTION."
  [connection queue text properties]
  (letfn [(add-property [^TextMessage message k v] (.setStringProperty message k v))
          (send [connection queue]
            (let [transacted? true]
              (with-open [session (.createSession connection transacted? Session/SESSION_TRANSACTED)]
                (let [queue (.createQueue session queue)
                      message (.createTextMessage session text)]
                  (doseq [[k v] properties]
                    (add-property message k v))
                  (with-open [producer (.createProducer session queue)]
                    (.setDeliveryMode producer DeliveryMode/PERSISTENT)
                    (.start connection)
                    (.send producer message)
                    (.commit session))))))]
    (send connection queue)))

(defn parse-message
  "Parse the MESSAGE fetched from JMS queue."
  [^TextMessage message]
  (let [parsed {:headers (.getText message)}]
    (assoc parsed :properties
           (into {} (for [[k v] (.getProperties message)]
                      [k v])))))

(defn listen-and-consume-from-queue
  "Listen on a QUEUE and synchronously consume messages with CONNECTION."
  [connection queue]
  (let [msg (edn/read-string (slurp "./test/data/test_msg.edn"))
        title (get-in msg [:headers :message-id])
        text  (walk/stringify-keys (:properties msg))]
    (loop [counter 0]
      (produce connection queue title text)
      (try (when-let [message (consume connection queue)]
             (let [message (parse-message message)]
               (timbre/info (format "Consumed message %s: %s: %s" counter (:headers message) (:properties message)))))
           (catch JMSException e (timbre/error (str (.getMessage e)))))
      (recur (inc counter)))))

(defn message-loop
  "A blocking message loop that periodically does something."
  [environment]
  (with-push-to-cloud-jms-connection environment listen-and-consume-from-queue))

(defn -main []
  (let [environment (or (System/getenv "environment") "dev")]
    (timbre/info (format "%s starting up on %s" ptc/the-name environment))
    (message-loop environment)))
