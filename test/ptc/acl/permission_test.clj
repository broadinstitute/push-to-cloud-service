(ns ptc.acl.permission-test
  "Test that the right permissions are granted for AoU project."
  (:require [ptc.tools.gcs  :as gcs]
            [ptc.tools.cromwell  :as cromwell]
            [ptc.util.misc     :as misc]
            [clojure.string      :as str]
            [clojure.java.io   :as io]
            [clojure.test  :refer [deftest is testing]])
  (:import [com.google.auth.oauth2 GoogleCredentials]))

(def aou-bucket
  "Storage bucket for running ptc.acl test with. Note this
  is the actual bucket PTC pushes data to."
  "gcf-sources-163624468465-us-central1")

(def aou-cromwell
  "URL to the AoU Cromwell."
  "https://cromwell-aou.gotc-prod.broadinstitute.org")

(def test-user
  "Vault path to the service account key of ACL test user."
  "secret/dsde/gotc/prod/aou/acl-test-user.json")

(defn get-test-user-header
  "Generate auth header from the ACL test user service account."
  []
  (let [token (some-> test-user misc/vault-secrets (:value) .getBytes
                io/input-stream GoogleCredentials/fromStream
                (.createScoped ["https://www.googleapis.com/auth/cloud-platform"
                                "https://www.googleapis.com/auth/userinfo.email"
                                "https://www.googleapis.com/auth/userinfo.profile"])
                .refreshAccessToken .getTokenValue)]
    {"Authorization" (str/join \space ["Bearer" token])}))

(deftest bucket-permission-test
  (testing "Unauthorized user cannot list the PTC bucket."
    (try
      (with-redefs [misc/get-auth-header! get-test-user-header]
        (hash (gcs/list-objects aou-bucket)))
      (catch Exception e
        (is (= 403 (:status (ex-data e)))
          "The user is able to list the bucket!!"))))
  (testing "Unauthorized user cannot upload object to the PTC bucket."
    (try
      (with-redefs [misc/get-auth-header! get-test-user-header]
        (hash (gcs/upload-file "deps.edn" aou-bucket "deps.edn")))
      (catch Exception e
        (is (= 403 (:status (ex-data e)))
          "The user is able to upload object to the bucket!!"))))
  (testing "Unauthorized user cannot delete object from the PTC bucket."
    (try
      (with-redefs [misc/get-auth-header! get-test-user-header]
        (hash (gcs/delete-object aou-bucket "deps.edn")))
      (catch Exception e
        (is (contains? #{403 404} (:status (ex-data e)) )
          "The user is able to delete object from the bucket!!")))))

(deftest workflow-permission-test
  (testing "Unauthorized users cannot query for workflows in the AoU Cromwell."
    (try
      (with-redefs [misc/get-auth-header! get-test-user-header]
        (hash (cromwell/query aou-cromwell misc/uuid-nil)))
      (catch Exception e
        (is (= 401 (:status (ex-data e)))
          "The user is able to query for a workflow!!"))))
  (testing "Unauthorized users cannot get statuses of workflows in the AoU Cromwell."
    (try
      (with-redefs [misc/get-auth-header! get-test-user-header]
        (hash (cromwell/status aou-cromwell misc/uuid-nil)))
      (catch Exception e
        (is (= 401 (:status (ex-data e)))
          "The user is able to get status of a workflow!!")))))
