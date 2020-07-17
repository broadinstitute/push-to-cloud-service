(ns ptc.util.misc
  "Miscellaneous utility functions shared across this program."
  (:require [clojure.pprint     :refer [pprint]]
            [clojure.string     :as str]
            [vault.core         :as vault]
            [clojure.java.shell :as shell]
            [ptc.ptc            :as ptc])
  (:import [org.apache.commons.mail SimpleEmail]))

(defmacro do-or-nil
  "Value of BODY or nil if it throws."
  [& body]
  `(try (do ~@body)
        (catch Exception x#
          (println x#))))

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
  [message to-list]
  (letfn [(add-to [mail to] (.addTo mail to))
          (add-to-list [mail to-list] (run! (partial add-to mail) to-list))]
    (let [from (str ptc/the-name "@broadinstitute.org")
          subject "This thing is from PTC service"]
      (doto (-> (new SimpleEmail)
                (.setFrom from)
                (.setSubject subject)
                (.setMsg message))
        (add-to-list to-list)
        (.setAuthentication from "fake-password")
        (.setHostName "smtp.gmail.com")
        (.send)))))

(defn notify-everyone-on-the-list-with-message
  "Notify everyone on the TO-LIST with MSG using METHOD."
  [method msg to-list]
  (method (with-out-str (pprint msg))
          (or (seq to-list) ["tbl@broadinstitute.org"
                             "chengche@broadinstitute.org"])))

(defn shell!
  "Run ARGS in a shell and return stdout or throw."
  [& args]
  (let [{:keys [exit err out]} (apply shell/sh args)]
    (when-not (zero? exit)
      (throw (Exception. (format "%s: %s exit status from: %s : %s"
                                 ptc/the-name exit args err))))
    (str/trim out)))

(defn get-auth-header!
  "Return an Authorization header with a Bearer token."
  []
  {"Authorization"
   (str "Bearer" \space (shell! "gcloud" "auth" "print-access-token"))})
