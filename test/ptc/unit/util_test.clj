(ns ptc.unit.util-test
  "Test some ptc.util functions."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [ptc.tools.gcs :as gcs]
            [ptc.util.misc :as misc])
  (:import [java.io IOException]))

(deftest gs-url-test
  (testing "URL utilities"
    (testing "parse-gs-url ok"
      (is (= ["b" "obj/ect"] (gcs/parse-gs-url "gs://b/obj/ect")))
      (is (= ["b" "obj/ect/"] (gcs/parse-gs-url "gs://b/obj/ect/")))
      (is (= ["b" ""] (gcs/parse-gs-url "gs://b/")))
      (is (= ["b" ""] (gcs/parse-gs-url "gs://b"))))
    (testing "parse-gs-url bad"
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "x")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "x/y")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "/")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "file://x/y")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "gs:")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "gs:/b/o")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "gs:///o/"))))))

(deftest test-retry-on-server-error
  (letfn [(throw-n-times [n]
            (let [counter (atom n)]
              #(let [x (swap! counter dec)]
                 (or (> 1 x) (throw (IOException. "503 Server Error"))))))]
    (testing "handles at most 3 retries"
      (is (misc/retry-on-server-error 1 (throw-n-times 3))))
    (testing "Gives up after third"
      (is (thrown? Exception (misc/retry-on-server-error 1 (throw-n-times 4)))))))
