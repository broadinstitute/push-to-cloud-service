(ns ptc.util.misc
  "Miscellaneous utility functions shared across this program."
  (:require [clojure.data.json     :as json]
            [clojure.java.io       :as io]
            [clojure.java.shell    :as shell]
            [clojure.pprint        :refer [pprint]]
            [clojure.string        :as str]
            [clojure.tools.logging :as log]
            [ptc.ptc               :as ptc]
            [vault.client.http]         ; vault.core needs this
            [vault.core            :as vault])
  (:import [java.util UUID]
           [org.apache.commons.mail SimpleEmail]))

(defmacro do-or-nil
  "Value of BODY or nil if it throws."
  [& body]
  `(try (do ~@body)
        (catch Exception x#
          (println x#))))

(defmacro dump
  "Dump [EXPRESSION VALUE] where VALUE is EXPRESSION's value."
  [expression]
  `(let [x# ~expression]
     (do
       (pprint ['~expression x#])
       x#)))

(defmacro trace
  "Like DUMP but include location metadata."
  [expression]
  (let [{:keys [line column]} (meta &form)]
    `(let [x# ~expression]
       (do
         (pprint {:file ~*file* :line ~line '~expression x#})
         x#))))

(defn vault-secrets
  "Return the vault-secrets at PATH."
  [path]
  (let [token-path (str (System/getProperty "user.home") "/.vault-token")]
    (try (vault/read-secret
          (doto (vault/new-client "https://clotho.broadinstitute.org:8200/")
            (vault/authenticate! :token (slurp token-path)))
          path)
         (catch Throwable e
           (log/warn e "Issue with Vault")
           (log/debug "Perhaps run 'vault login' and try again")))))

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

(defn slurp-json
  "Nil or the JSON in FILE."
  [file]
  (do-or-nil
   (with-open [^java.io.Reader in (io/reader file)]
     (json/read in :key-fn keyword))))

(def uuid-nil
  "The nil UUID."
  (UUID/fromString "00000000-0000-0000-0000-000000000000"))


(defn gs-url
  "Format BUCKET and OBJECT into a gs://bucket/object URL."
  ([bucket object]
   (when-not (and (string?        bucket)
                  (seq            bucket)
                  (not-any? #{\/} bucket))
     (let [fmt "The bucket (%s) must be a non-empty string."
           msg (format fmt bucket)]
       (throw (IllegalArgumentException. msg))))
   (if (nil? object)
     (str "gs://" bucket)
     (str "gs://" bucket "/" object)))
  ([bucket]
   (gs-url bucket nil)))

(defn sleep-seconds
  "Sleep for N seconds."
  [n]
  (Thread/sleep (* n 1000)))


(defn parse-json-string
  "Parse the json string STR into a keyword-string map"
  [str]
  (json/read-str str :key-fn keyword))

(defn message-ids-equal?
  "True when the IDs of MESSAGES are the same. Otherwise false."
  [& messages]
  (or (empty? messages)
      (apply = (map :properties messages))))
