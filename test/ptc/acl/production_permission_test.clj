(ns ptc.acl.production-permission-test
  "Test that the right permissions are granted for AoU project."
  (:require [ptc.tools.gcs :as gcs]
            [ptc.tools.cromwell :as cromwell]
            [ptc.util.misc :as misc]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]])
  (:import [com.google.auth.oauth2 GoogleCredentials]))

(def aou-in-bucket
  "Storage bucket for running ptc.acl test with. Note this
  is the actual bucket PTC pushes data to."
  "broad-aou-arrays-input")

(def aou-out-bucket
  "Storage bucket for running ptc.acl test with. Note this
  is the actual bucket storing arrays pipeline outputs."
  "broad-aou-arrays-output")

(def aou-cromwell
  "URL to the AoU Cromwell."
  "https://cromwell-aou.gotc-prod.broadinstitute.org")

(def test-user
  "Vault path to the service account key of ACL test user."
  "secret/dsde/gotc/prod/aou/acl-test-user.json")

(defn get-test-user-header
  "Generate auth header from the ACL test user service account."
  []
  (let [scope ["https://www.googleapis.com/auth/cloud-platform"
               "https://www.googleapis.com/auth/userinfo.email"
               "https://www.googleapis.com/auth/userinfo.profile"]
        token (some-> test-user misc/vault-secrets (:value) .getBytes
                      io/input-stream GoogleCredentials/fromStream
                      (.createScoped scope)
                      .refreshAccessToken .getTokenValue)]
    (is token "No credentials for test-user.")
    {"Authorization" (str/join \space ["Bearer" token])}))

(deftest ^:excluded bucket-permission-test
  (is false "Do you really want to run this in production?")
  (testing "Unauthorized user cannot list the PTC buckets."
    (with-redefs [gcs/get-auth-header! get-test-user-header]
      (try
        (hash (gcs/list-objects aou-in-bucket))
        (catch Exception e
          (is (= 403 (:status (ex-data e)))
              "The user is able to list the input bucket!!")))
      (try
        (hash (gcs/list-objects aou-out-bucket))
        (catch Exception e
          (is (= 403 (:status (ex-data e)))
              "The user is able to list the output bucket!!")))))
  (testing "Unauthorized user cannot upload object to the PTC buckets."
    (with-redefs [gcs/get-auth-header! get-test-user-header]
      (try
        (hash (gcs/upload-file "deps.edn" aou-in-bucket "deps.edn"))
        (catch Exception e
          (is (= 403 (:status (ex-data e)))
              "The user is able to upload object to the input bucket!!")))
      (try
        (hash (gcs/upload-file "deps.edn" aou-out-bucket "deps.edn"))
        (catch Exception e
          (is (= 403 (:status (ex-data e)))
              "The user is able to upload object to the output bucket!!")))))
  (testing "Unauthorized user cannot delete object from the PTC buckets."
    (with-redefs [gcs/get-auth-header! get-test-user-header]
      (try
        (hash (gcs/delete-object aou-in-bucket "deps.edn"))
        (catch Exception e
          (is (contains? #{403 404} (:status (ex-data e)))
              "The user is able to delete object from the input bucket!!")))
      (try
        (hash (gcs/delete-object aou-out-bucket "deps.edn"))
        (catch Exception e
          (is (contains? #{403 404} (:status (ex-data e)))
              "The user is able to delete object from the output bucket!!"))))))

(deftest ^:excluded workflow-permission-test
  (is false "Do you really want to run this in production?")
  (testing "Unauthorized users cannot query for workflows in the AoU Cromwell."
    (try
      (with-redefs [gcs/get-auth-header! get-test-user-header]
        (hash (cromwell/query aou-cromwell misc/uuid-nil)))
      (catch Exception e
        (is (= 401 (:status (ex-data e)))
            "The user is able to query for a workflow!!"))))
  (testing "Unauthorized users cannot get statuses of workflows in the AoU Cromwell."
    (try
      (with-redefs [gcs/get-auth-header! get-test-user-header]
        (hash (cromwell/status aou-cromwell misc/uuid-nil)))
      (catch Exception e
        (is (= 401 (:status (ex-data e)))
            "The user is able to get status of a workflow!!")))))
