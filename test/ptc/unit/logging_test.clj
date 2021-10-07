(ns ptc.unit.logging-test
  "Test that logging works through several layers of delegation."
  (:require [clojure.test :refer [is deftest testing]]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.test :refer [logged? with-log]]))

;; This file exists as a smoke test for having not totally messed up
;; the logging dependencies (since clojure.tools.logging finds a
;; backend at runtime)

(deftest level-test
  (testing "basic logging levels"
    (with-log
      (log/info "Hello World!")
      (is (logged? 'ptc.unit.logging-test :info "Hello World!"))
      (log/warn "This is a warning")
      (is (logged? 'ptc.unit.logging-test :warn "This is a warning"))
      (log/error "and this is an error")
      (is (logged? 'ptc.unit.logging-test :error "and this is an error"))
      (log/debug "This is a debugging message")
      (is (logged? 'ptc.unit.logging-test :debug "This is a debugging message"))
      (log/trace "This is a trace")
      (is (logged? 'ptc.unit.logging-test :trace "This is a trace")))))

(deftest format-test
  (testing "formatting (more for demonstration than assurance)"
    (with-log
      (log/info "abc" 6 "abcd")
      (is (logged? 'ptc.unit.logging-test :info "abc 6 abcd"))
      (log/infof "%s %s" "abc" 123)
      (is (logged? 'ptc.unit.logging-test :info "abc 123")))))

(deftest exception-test
  (testing "exception output"
    (with-log
      (try
        (int "not an int")
        (catch Exception e
          (log/error e "Oops!"))
        (finally
          (is (logged? 'ptc.unit.logging-test
                       :error [Throwable #"cannot be cast"] #"Oops!")))))))
