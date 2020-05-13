(ns start
  (:gen-class)
  (:require [util])
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

(defn send-text
  "Enqueue the TEXT with PROPERTIES map to push-to-cloud JMS queue with ENVIRONMENT."
  [environment text properties]
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
    (with-push-to-cloud-jms-connection environment send)))

(defn receive-text
  "Nil on timeout or the text from a message from the push-to-cloud JMS queue with ENVIRONMENT."
  [environment]
  (letfn [(receive [connection destination]
            (let [transacted? false
                  timeout 10000]
              (with-open [session (.createSession connection transacted? Session/AUTO_ACKNOWLEDGE)]
                (let [queue (.createQueue session destination)]
                  (with-open [consumer (.createConsumer session queue)]
                    (.start connection)
                    (.receive consumer timeout))))))]
    (with-push-to-cloud-jms-connection environment receive)))

(defn plus
  "Return the sum of A and B. Just for testing."
  [^Integer a ^Integer b]
  (+ a b))

(defn message-loop
  "A blocking message loop that periodically do something."
  [environment]
  (loop []
    (let [_ (send-text environment "hornet" {"hello" "world!"})]
      (when-let [messageText (receive-text environment)]
        (println (.getText messageText))
        (doseq [[k v] (.getProperties messageText)]
          (prn k v))
        (recur)))))

(comment
  (message-loop "dev")

  (let [environment "dev"]
    ;;(send-text environment "hornet" {"hello" "world!"})
    (when-let [msg (receive-text environment)]
      (.getText msg)))

  (util/reach-out-and-touch-everyone {:hello "world!"} []))

(defn -main []
  (println "Hello world!")
  (let [environment (System/getenv "environment")]
    (message-loop environment)))
