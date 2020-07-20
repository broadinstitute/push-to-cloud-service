(ns ptc.unit.util-test
  (:require [clojure.test  :refer [deftest is testing]]
            [ptc.util.misc :as misc]
            [ptc.util.gcs  :as gcs]
            [clojure.edn   :as edn]))

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

(deftest test-message-id-equality
  (let [test-msg (edn/read-string (slurp "test/data/test_msg.edn"))
        test-msg-different (edn/read-string (slurp "test/data/test_msg_diff_id.edn"))
        test-msg-same-id (edn/read-string (slurp "test/data/test_msg_same_id.edn"))]
    (testing "message ID equality"
      (testing "true with no arguments"
        (is (misc/message-ids-equal?)))
      (testing "true with one argument"
        (is (misc/message-ids-equal? test-msg)))
      (testing "test_msg equal to itself"
        (is (misc/message-ids-equal? test-msg test-msg)))
      (testing "test_msg equal to a different message with same id"
        (is (misc/message-ids-equal? test-msg test-msg-same-id)))
      (testing "test_msg not equal to different message with different id"
        (is (not (misc/message-ids-equal? test-msg test-msg-different))))
      (testing "not equal even if only one argument isn't"
        (is (not (misc/message-ids-equal? test-msg test-msg test-msg-different)))))))
