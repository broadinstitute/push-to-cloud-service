(ns util.gcs
  "Utility functions for Google Cloud Storage shared across this program."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.data.json :as json]
            [clojure.java.io    :as io]
            [clj-http.client    :as http]
            [vault.client.http]
            [ptc]
            [util.once]         :as once)
  (:import [org.apache.tika Tika]))

(def api-url
  "The Google Cloud API URL."
  "https://www.googleapis.com/")

(def storage-url
  "The Google Cloud URL for storage operations."
  (str api-url "storage/v1/"))

(def bucket-url
  "The Google Cloud Storage URL for bucket operations."
  (str storage-url "b/"))

(def upload-url
  "The Google Cloud Storage URL for upload operations."
  (str api-url "upload/storage/v1/b/"))

(defn list-objects
  "The objects in BUCKET with PREFIX in a lazy sequence."
  ([bucket prefix]
   (letfn [(each [pageToken]
             (let [{:keys [items nextPageToken]}
                   (-> {:method       :get  ;; :debug true :debug-body true
                        :url          (str bucket-url bucket "/o")
                        :content-type :application/json
                        :headers      (once/get-auth-header!)
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
   (upload-file file bucket object (once/get-auth-header!)))
  ([file url]
   (apply upload-file file (parse-gs-url url))))
