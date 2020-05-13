(ns util
  "Utility functions shared across this program."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [vault.client.http]
            [vault.core         :as vault]
            [ptc])
  (:import [org.apache.commons.mail SimpleEmail]))

(defn vault-secrets
  "Return the vault-secrets at PATH."
  [path]
  (let [token-path (str (System/getProperty "user.home") "/.vault-token")]
    (try (vault/read-secret
           (doto (vault/new-client "https://clotho.broadinstitute.org:8200/")
             (vault/authenticate! :token (slurp token-path)))
           path)
         (catch Throwable e
           (let [error (get-in (Throwable->map e) [:via 0 :message])
                 lines ["%1$s: %2$s" "%1$s: Run 'vault login' and try again."]
                 msg   (format (str/join \newline lines) ptc/the-name error)]
             (println msg))))))

(defn email
  "Email MESSAGE to TO-LIST from with SUBJECT."
  [to-list subject message]
  (letfn [(add-to [mail to] (.addTo mail to))
          (add-to-list [mail to-list] (run! (partial add-to mail) to-list))]
    (let [from (str ptc/the-name "@broadinstitute.org")]
      (doto (-> (new SimpleEmail)
                (.setFrom from)
                (.setSubject subject)
                (.setMsg message))
        (add-to-list to-list)
        (.setAuthentication from "fake-password")
        (.setHostName "smtp.gmail.com")
        (.send)))))

(defn reach-out-and-touch-everyone
  "Report MSG in email to TO-LIST. "
  [msg to-list]
  (let [message msg]
    (email (or (seq to-list) ["tbl@broadinstitute.org"
                              "chengche@broadinstitute.org"])
           "This thing is from PTC service"
           (with-out-str (pprint message)))))

(comment
  (reach-out-and-touch-everyone {:hello "world!"} [])
  (println (vault-secrets "secret/dsde/gotc/dev/wfl/users")))
