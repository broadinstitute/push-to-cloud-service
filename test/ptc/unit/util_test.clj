(ns ptc.unit.util-test
  (:require [clojure.test  :refer [deftest is testing]]
            [ptc.util.misc :as misc]
            [ptc.util.gcs  :as gcs]))

(deftest test-notify-everyone-on-the-list-with-message
  (letfn [(notify [msg to-list]
            (map (partial str msg) to-list))]
    (let [msg      "test"
          ppl      ["A" "B"]
          expected ["\"test\"\nA" "\"test\"\nB"]]
      (testing "notify-everyone-on-the-list-with-message works for notify func"
        (is (= (misc/notify-everyone-on-the-list-with-message notify msg ppl)
               expected))))))

(deftest gs-url-test
  (testing "URL utilities"
    (testing "parse-gs-url ok"
      (is (= ["b" "obj/ect"]  (gcs/parse-gs-url "gs://b/obj/ect")))
      (is (= ["b" "obj/ect/"] (gcs/parse-gs-url "gs://b/obj/ect/")))
      (is (= ["b" ""]         (gcs/parse-gs-url "gs://b/")))
      (is (= ["b" ""]         (gcs/parse-gs-url "gs://b"))))
    (testing "parse-gs-url bad"
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "x")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "x/y")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "/")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "file://x/y")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "gs:")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "gs:/b/o")))
      (is (thrown? IllegalArgumentException (gcs/parse-gs-url "gs:///o/"))))))