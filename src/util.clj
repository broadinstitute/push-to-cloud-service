(ns util
  "Utility functions shared across this program."
  (:require [clojure.string :as str]
            [vault.client.http]
            [vault.core         :as vault]
            [ptc]))

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

(comment
  (println (vault-secrets "secret/dsde/gotc/dev/wfl/users")))
