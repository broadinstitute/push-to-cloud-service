(ns ptc.tools.gcs
  "Utility functions for Google Cloud Storage shared across this program."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clj-http.client :as http]
            [clj-http.util :as http-util]
            [ptc.util.gcs :as gcs]
            [ptc.util.misc :as misc])
  (:import [java.util UUID]))

(def api-url
  "The Google Cloud API URL."
  "https://www.googleapis.com/")

(def storage-url
  "The Google Cloud URL for storage operations."
  (str api-url "storage/v1/"))

(def bucket-url
  "The Google Cloud Storage URL for bucket operations."
  (str storage-url "b/"))

(defn bucket-object-url
  "The API URL referring to OBJECT in BUCKET."
  [bucket object]
  (str bucket-url bucket "/o/" (http-util/url-encode object)))

(def upload-url
  "The Google Cloud Storage URL for upload operations."
  (str api-url "upload/storage/v1/b/"))

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
                   (-> {:method       :get   ;; :debug true :debug-body true
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

(defn list-gcs-folder
  "Nil or URLs for the GCS objects of folder."
  [folder]
  (-> folder
      (vector "**")
      (->> (str/join "/")
           (gcs/gsutil "ls"))
      (str/split #"\n")
      misc/do-or-nil))

(defn delete-object
  "Delete URL or OBJECT from BUCKET"
  ([url]
   (gcs/gsutil "rm" url))
  ([bucket object]
   (delete-object (str "gs://" bucket "/" object))))

(defn upload-file
  "Upload FILE to BUCKET with name OBJECT."
  ([file url]
   (gcs/gsutil "cp" file url))
  ([file bucket object]
   (upload-file file (str "gs://" bucket "/" object))))

(defn gcs-cat
  "Return the content of the GCS object at URL."
  [url]
  (misc/shell! "gsutil" "cat" url))

(defn gcs-edn
  "Return the JSON in GCS URL as EDN."
  [url]
  (-> url gcs-cat misc/parse-json-string))

(defn wait-for-files-in-bucket
  "Block until gsutil successfully `stat`s each `gs://` path in `files`.
  Return `true`, but may block forever."
  [files]
  (let [seconds 15]
    (if-let [file (first files)]
      (if (gcs/gcs-object-exists? file)
        (do
          (log/infof "Found %s in bucket" file)
          (recur (rest files)))
        (do
          (log/infof "Couldn't find %s, sleeping %s seconds" file seconds)
          (misc/sleep-seconds seconds)
          (recur files)))
      true)))

(def gcs-test-bucket
  "Throw test files in this bucket."
  "broad-gotc-dev-wfl-ptc-test-outputs")

(defmacro with-temporary-gcs-folder
  "
  Create a temporary folder in GCS-TEST-BUCKET for use in BODY.
  The folder will be deleted after execution transfers from BODY.

  Example
  -------
    (with-temporary-gcs-folder uri
      ;; use temporary folder at `uri`)
      ;; <- temporary folder deleted
  "
  [uri & body]
  `(let [name# (str "ptc-test-" (UUID/randomUUID))
         ~uri (gcs/gs-url gcs-test-bucket name#)]
     (try
       ~@body
       (finally
         (->>
          (list-objects gcs-test-bucket name#)
          (run! (comp (partial delete-object gcs-test-bucket) :name)))))))
