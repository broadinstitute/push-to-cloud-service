(ns start
  (:gen-class)
  (:require [clj-time.core   :as t])
  (:import  [org.apache.activemq ActiveMQSslConnectionFactory]
            [javax.jms TextMessage DeliveryMode Session]))

(defn with-push-to-cloud-jms-connection
  "CALL (use connection destination) for the push-to-cloud JMS queue with URL, QUEUE, USERNAME and PASSWORD."
  [url queue username password call]
  (let [factory (new ActiveMQSslConnectionFactory url)]
    (with-open [connection (.createQueueConnection factory username password)]
      (call connection queue))))

(defn send-text
  "Enqueue the TEXT with PROPERTIES map to push-to-cloud JMS queue with URL, QUEUE, USERNAME and PASSWORD."
  [url queue username password text properties]
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
    (with-push-to-cloud-jms-connection url queue username password send)))

(defn receive-text
  "Nil on timeout or the text from a message from the push-to-cloud JMS queue with URL, QUEUE, USERNAME and PASSWORD"
  [url queue username password]
  (letfn [(receive [connection destination]
            (let [transacted? false
                  timeout 10000]
              (with-open [session (.createSession connection transacted? Session/AUTO_ACKNOWLEDGE)]
                (let [queue (.createQueue session destination)]
                  (with-open [consumer (.createConsumer session queue)]
                    (.start connection)
                    (.receive consumer timeout))))))]
    (with-push-to-cloud-jms-connection url queue username password receive)))

(defn plus
  "Return the sum of A and B. Just for testing."
  [^Integer a ^Integer b]
  (+ a b))

(comment
  (let [queue     "wfl.broad.pushtocloud.enqueue.dev"
        url       "get this from the WFL environments.clj!!"
        vault     "secret/dsde/gotc/dev/activemq/logins/zamboni"
        username  "get this from above vault!!"
        password  "get this from above vault!!"]
    (send-text url queue username password "hornet" {"hello" "world!"})
    (.getText (receive-text url queue username password))))

(defn -main []
  (println "Hello world!"))
