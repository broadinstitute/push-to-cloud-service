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
           [java.util.concurrent TimeUnit]
           [org.apache.commons.mail SimpleEmail]
           [java.time OffsetDateTime ZoneId]
           (java.io IOException)))

(defmacro do-or-nil
  "Value of BODY or nil if it throws."
  [& body]
  `(try (do ~@body)
        (catch Exception x#
          (println x#))))

(defmacro do-or-nil-silently
  "Value of BODY or nil if it throws, without printing any exceptions.
  See also [[do-or-nil]]."
  [& body]
  `(try (do ~@body)
        (catch Exception x#)))

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
  (let [env-token  (System/getenv "VAULT_TOKEN")
        token-path (str (System/getProperty "user.home") "/.vault-token")]
    (try (vault/read-secret
          (doto (vault/new-client "https://clotho.broadinstitute.org:8200/")
            (vault/authenticate! :token (or env-token (slurp token-path))))
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
    (when-not (zero? exit) (throw (IOException. err)))
    (str/trim out)))

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

(defn parse-json-string
  "Parse the json string STR into a keyword-string map"
  [str]
  (json/read-str str :key-fn keyword))

(defn message-ids-equal?
  "True when the IDs of MESSAGES are the same. Otherwise false."
  [& messages]
  (or (empty? messages)
      (apply = (map :properties messages))))

(defn getenv-or-throw
  "Get value of environment variable NAME or throw if nil."
  [name]
  (let [value (System/getenv name)]
    (when (nil? value)
      (throw (IllegalStateException. (str name " must not be nil"))))
    value))

(defn parse-gs-url
  "Return BUCKET and OBJECT from a gs://bucket/object URL."
  [url]
  (let [[gs-colon nada bucket object] (str/split url #"/" 4)]
    (when-not
     (and (every? seq [gs-colon bucket])
          (= "gs:" gs-colon)
          (= "" nada))
      (throw (IllegalArgumentException. (format "Bad GCS URL: '%s'" url))))
    [bucket (or object "")]))

;; visible-for-testing
(defn retry-on-server-error [seconds thunk]
  (let [max 3]
    (loop [attempt 1]
      (or (try
            (thunk)
            (catch IOException ex
              (when-not (and
                         (str/includes? (.getMessage ex) "503 Server Error")
                         (< attempt max))
                (throw ex))
              (log/warnf "received 503 (attempt %s of %s)" attempt max)
              (log/info "sleeping before another attempt")
              (.sleep TimeUnit/SECONDS seconds)))
          (recur (inc attempt))))))

(defn gsutil [& args]
  "Shell out to gsutil with ARGS. Retry when gsutil responds with 503."
  (retry-on-server-error 30 #(apply shell! "gsutil" args)))

(defn gcs-object-exists?
  "Return PATH when there is a GCS object at PATH.  Otherwise nil."
  [path]
  (when (string? path)
    (do-or-nil-silently
     (gsutil "stat" path))))

(defn get-md5-hash
  "Return the md5 hash of a file."
  [path]
  (-> (gsutil "hash" "-m" path)
      (str/split #":")
      (last)
      (str/trim)))

