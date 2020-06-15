(ns ptc.util.once
  "Low-level vars to define exactly once and auth related junk."
  (:require [clojure.data.json :as json]
            [ptc.util.misc     :as misc])
  (:import [com.google.auth.oauth2 UserCredentials]
           [java.net URI]))

;; https://developers.google.com/identity/protocols/OAuth2InstalledApp#refresh
;;
(defn new-user-credentials
  "NIL or new UserCredentials for call. "
  []
  (when-let [out (->> ["gcloud" "auth" "print-access-token" "--format=json"]
                      (apply misc/shell!)
                      misc/do-or-nil)]
    (let [{:strs [client_id client_secret refresh_token token_uri]}
          (json/read-str out)]
      (when (and client_id client_secret refresh_token token_uri)
        (.build (doto (UserCredentials/newBuilder)
                  (.setClientId client_id)
                  (.setClientSecret client_secret)
                  (.setRefreshToken refresh_token)
                  (.setTokenServerUri (new URI token_uri))))))))

;; The re-usable credentials object for token generation
;;
(defonce the-cached-credentials (delay (new-user-credentials)))

(defn get-auth-header!
  "Return a valid auth header, refresh the access token
  under the hood if necessary."
  []
  (misc/bearer-token-header-for @the-cached-credentials))

(comment
  (get-auth-header!))
