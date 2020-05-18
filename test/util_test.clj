(ns util-test
  (:require [clojure.test :refer :all]
            [util]))

(deftest test-notify-everyone-on-the-list-with-message
  "Test the notify function works."
  (letfn [(notify [msg to-list]
            (map (partial str msg) to-list))]
    (let [msg      "test"
          ppl      ["A" "B"]
          expected ["\"test\"\nA" "\"test\"\nB"]]
      (is (= (util/notify-everyone-on-the-list-with-message notify msg ppl)
             expected)))))
