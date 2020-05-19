(ns start
  (:gen-class)
  (:require [util]
            [taoensso.timbre  :as timbre])
  (:import  [org.apache.activemq ActiveMQSslConnectionFactory]
            [javax.jms TextMessage DeliveryMode Session]))

(defn with-push-to-cloud-jms-connection
  "CALL (use connection destination) for the push-to-cloud JMS queue with ENVIRONMENT."
  [environment call]
  (let [vault-path (format "secret/dsde/gotc/%s/activemq/logins/zamboni" environment)
        {:keys [url username password queue]} (util/vault-secrets vault-path)
        factory (new ActiveMQSslConnectionFactory url)]
    (with-open [connection (.createQueueConnection factory username password)]
      (call connection queue))))

(defn consume
  "Nil on timeout or the text from a message from JMS queue DESTINATION through CONNECTION."
  [connection destination]
  (let [transacted? false
        timeout 10000]
    (with-open [session (.createSession connection transacted? Session/AUTO_ACKNOWLEDGE)]
      (let [queue (.createQueue session destination)]
        (with-open [consumer (.createConsumer session queue)]
          (.start connection)
          (.receive consumer timeout))))))

(defn produce
  "Enqueue the TEXT with PROPERTIES map to JMS queue DESTINATION through CONNECTION."
  [connection destination text properties]
  (letfn [(add-property [^TextMessage message k v] (.setStringProperty message k v))
          (send [connection destination]
            (let [transacted? true]
              (with-open [session (.createSession connection transacted? Session/SESSION_TRANSACTED)]
                (let [queue (.createQueue session destination)
                      message (.createTextMessage session text)]
                  (doseq [[k v] properties]
                    (add-property message k v))
                  (with-open [producer (.createProducer session queue)]
                    (.setDeliveryMode producer DeliveryMode/PERSISTENT)
                    (.start connection)
                    (.send producer message)
                    (.commit session))))))]
    (send connection destination)))

(defn listen-and-consume-from-queue
  "Listen on a QUEUE and keep consuming messages with CONNECTION and CADENCE."
  [connection queue]
   (loop [counter 0]
     (produce connection queue "hornet" {"hello" (format "world! %s" counter)})
     (when-let [messageText (consume connection queue)]
       (timbre/info (.getText messageText))
       (doseq [[k v] (.getProperties messageText)]
         (timbre/info (str k v))))
     (recur (inc counter))))

(defn message-loop
  "A blocking message loop that periodically does something."
  [environment]
  (with-push-to-cloud-jms-connection environment listen-and-consume-from-queue))

(defn -main []
  (let [environment (System/getenv "environment")]
    (timbre/info (format "%s starting up on %s" ptc/the-name environment))
    (message-loop environment)))
