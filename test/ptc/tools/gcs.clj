(ns ptc.tools.gcs
  "Utility functions for Google Cloud Storage shared across this program."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clj-http.client :as http]
            [ptc.util.misc :as misc]
            [clj-http.util :as http-util])
  (:import [org.apache.tika Tika]
           [java.util.concurrent TimeUnit]))

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
           (misc/shell! "gsutil" "ls"))
      (str/split #"\n")
      misc/do-or-nil))

(defn delete-object
  "Delete URL or OBJECT from BUCKET"
  ([bucket object headers]
   (http/request {:method  :delete ;; :debug true :debug-body true
                  :url     (bucket-object-url bucket object)
                  :headers headers}))
  ([bucket object]
   (delete-object bucket object (get-auth-header!)))
  ([url]
   (apply delete-object (parse-gs-url url))))

(defn upload-file
  "Upload FILE to BUCKET with name OBJECT."
  ([file bucket object headers]
   (let [body (io/file file)]
     (-> {:method       :post  ;; :debug true :debug-body true
          :url          (str upload-url bucket "/o")
          :query-params {:uploadType "media"
                         :name       object}
          :content-type (.detect (new Tika) body)
          :headers      headers
          :body         body}
         http/request
         :body
         (json/read-str :key-fn keyword))))
  ([file bucket object]
   (upload-file file bucket object (get-auth-header!)))
  ([file url]
   (apply upload-file file (parse-gs-url url))))

(defn gcs-cat
  "Return the content of the GCS object at URL."
  [url]
  (misc/shell! "gsutil" "cat" url))

(defn gcs-edn
  "Return the JSON in GCS URL as EDN."
  [url]
  (-> url gcs-cat misc/parse-json-string))

(defn wait-for-files-in-bucket
  "Wait for gsutil to successfully `stat` each given `gs://` file path."
  [files]
  (let [seconds 15
        file (first files)]
    (when file
      (loop []
        (if (not (try
                   (misc/shell! "gsutil" "stat" file)
                   (catch Exception _
                     nil)))
          (do
            (log/infof "Sleeping %s seconds" seconds)
            (.sleep TimeUnit/SECONDS seconds)
            (recur))))
      (wait-for-files-in-bucket (rest files)))))

