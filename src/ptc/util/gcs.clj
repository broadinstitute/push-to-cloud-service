(ns ptc.util.gcs
  "Talk to Google Cloud Storage."
  (:require [clojure.string :as str]
            [ptc.util.misc  :as misc]))

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
