(ns ptc.tools.gcs
  "Utility functions for Google Cloud Storage shared across this program."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ptc.util.gcs :as gcs]
            [ptc.util.misc :as misc])
  (:import [java.util UUID]))

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

(defn wait-for-files
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
          (gcs/list-objects gcs-test-bucket name#)
          (run! (comp (partial delete-object gcs-test-bucket) :name)))))))
