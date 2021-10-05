(ns ptc.util.gcs
  "Talk to Google Cloud Storage."
  (:require  [clojure.data.json :as json]
             [clojure.string :as str]
             [clj-http.client :as http]
             [ptc.util.misc  :as misc]))

(def api-url
  "The Google Cloud API URL."
  "https://www.googleapis.com/")

(def storage-url
  "The Google Cloud URL for storage operations."
  (str api-url "storage/v1/"))

(def bucket-url
  "The Google Cloud Storage URL for bucket operations."
  (str storage-url "b/"))

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

(defn gsutil
  "Shell out to gsutil with ARGS. Retry when gsutil responds with 503."
  [& args]
  (misc/retry-on-server-error 30 #(apply misc/shell! "gsutil" args)))

(defn gcs-object-exists?
  "Return PATH when there is a GCS object at PATH.  Otherwise nil."
  [path]
  (when (string? path)
    (misc/do-or-nil-silently
     (gsutil "stat" path))))

(defn get-md5-hash
  "Return the MD5 hash of a file."
  [path]
  (-> (gsutil "hash" "-m" path)
      (str/split #":")
      (last)
      (str/trim)))

(defn get-auth-header!
  "Return an Authorization header with a Bearer token."
  []
  {"Authorization"
   (str "Bearer" \space (misc/shell! "gcloud" "auth" "print-access-token"))})

(defn list-objects
  "The objects in BUCKET with PREFIX in a lazy sequence."
  ([bucket prefix]
   (letfn [(each [pageToken]
             (let [{:keys [items nextPageToken]}
                   (-> {:method       :get ; :debug true :debug-body true
                        :url          (str bucket-url bucket "/o")
                        :content-type :application/json
                        :headers      (get-auth-header!)
                        :query-params {:prefix prefix
                                       :maxResults 999
                                       :pageToken pageToken}}
                       http/request
                       :body
                       (json/read-str :key-fn keyword))]
               (lazy-cat items (when nextPageToken (each nextPageToken)))))]
     (each "")))
  ([bucket]
   (list-objects bucket "")))
