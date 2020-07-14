(ns ptc.util.once
  "Low-level vars to define exactly once and auth related junk."
  (:require [clojure.data.json :as json]
            [ptc.util.misc     :as misc]))

(defn get-auth-header!
  "Return a valid auth header, refresh the access token
  under the hood if necessary."
  []
  {"Authorization"
   (str "Bearer " (misc/shell! "gcloud" "auth" "print-access-token"))})

(comment
  (get-auth-header!)
  )
