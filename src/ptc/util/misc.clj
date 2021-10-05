(ns ptc.util.misc
  "Miscellaneous utility functions shared across this program."
  (:require [clojure.data.json     :as json]
            [clojure.java.io       :as io]
            [clojure.java.shell    :as shell]
            [clojure.pprint        :refer [pprint]]
            [clojure.string        :as str]
            [clojure.tools.logging :as log]
            [vault.client.http]         ; vault.core needs this
            [vault.core            :as vault])
  (:import [java.io IOException]
           [java.util UUID]))

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

(defn sleep-seconds
  "Sleep for N seconds."
  [n]
  (Thread/sleep (* n 1000)))

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

(defn parse-json-string
  "Parse the json string STR into a keyword-string map"
  [str]
  (json/read-str str :key-fn keyword))

(defn message-ids-equal?
  "True when the IDs of MESSAGES are the same. Otherwise false."
  [& messages]
  (or (empty? messages)
      (apply = (map :properties messages))))

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
              (sleep-seconds seconds)))
          (recur (inc attempt))))))

(defn get-auth-header!
  "Return an Authorization header with a Bearer token."
  []
  {"Authorization"
   (str "Bearer" \space (shell! "gcloud" "auth" "print-access-token"))})
