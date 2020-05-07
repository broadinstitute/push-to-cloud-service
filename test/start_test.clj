(ns start-test
  (:require [clojure.test :refer :all]
            [start]))

(deftest adding-numbers
  (is (= 4 (start/plus 2 2))))