(ns ptc.util.jms
  "Talk to JMS queues."
  (:require [zero.environments :as env]
            [zero.util :as util])
  (:import [org.apache.activemq ActiveMQSslConnectionFactory]
           [javax.jms Connection DeliveryMode Session]))

(defn with-pushtocloud-jms-connection
  "Call (USE connection destination) for the pushtocloud JMS queue
  in ENVIRONMENT."
  [environment call]
  (let [{:keys [queue url vault]} (get-in env/stuff [environment :jms])
        factory (new ActiveMQSslConnectionFactory url)
        {:keys [username password]} (util/vault-secrets vault)]
    (with-open [connection (.createQueueConnection factory username password)]
      (call connection queue))))

(defn send-text
  "Put TEXT with PROPERTIES map on pushtocloud JMS queue in ENVIRONMENT."
  [environment text properties]
  (letfn [(add-property [message k v] (.setStringProperty message k v))
          (send [connection destination]
            (let [transacted? true]
              (with-open [session (.createSession connection
                                    transacted?
                                    Session/SESSION_TRANSACTED)]
                (let [queue (.createQueue session destination)
                      message (.createTextMessage session text)]
                  (doseq [[k v] properties]
                    (add-property message k v))
                  (with-open [producer (.createProducer session queue)]
                    (.setDeliveryMode producer DeliveryMode/PERSISTENT)
                    (.start connection)
                    (.send producer message)
                    (.commit session))))))]
    (with-pushtocloud-jms-connection environment send)))

(defn receive-text
  "Nil on timeout or the text from a message from the pushtocloud JMS
  queue in ENVIRONMENT."
  [environment]
  (letfn [(receive [connection destination]
            (let [transacted? false
                  timeout 10000]        ; 10 seconds in milliseconds
              (with-open [session (.createSession connection
                                    transacted?
                                    Session/AUTO_ACKNOWLEDGE)]
                (let [queue (.createQueue session destination)]
                  (with-open [consumer (.createConsumer session queue)]
                    (.start connection)
                    (.receive consumer timeout))))))]
    (with-pushtocloud-jms-connection environment receive)))
