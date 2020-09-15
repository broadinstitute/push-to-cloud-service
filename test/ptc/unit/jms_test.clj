(ns ptc.unit.jms-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]))

(def read-jms-message (comp edn/read-string slurp))

(deftest use-on-prem-idat-paths
  (testing "WFL uses the on-prem idat files if they do not exist in the cloud."
    (let [jms (read-jms-message "./test/data/good-jms.edn")
          workflow (get-in jms [::jms/Properties :payload :workflow])
          {:keys [cloudGreenIdatPath cloudRedIdatPath]} workflow
          push (::jms/push (jms/wfl-keys->jms-keys-for workflow))]
      (is (not (misc/gcs-object-exists? cloudGreenIdatPath)))
      (is (not (misc/gcs-object-exists? cloudRedIdatPath)))
      (is (= (:green_idat_cloud_path push) :greenIDatPath))
      (is (= (:red_idat_cloud_path   push) :redIDatPath)))))

(deftest use-cloud-idat-paths
  (testing "WFL uses the cloud idat paths if they exist."
    (let [jms (read-jms-message "./test/data/reprocessing-jms.edn")
          workflow (get-in jms [::jms/Properties :payload :workflow])
          {:keys [cloudGreenIdatPath cloudRedIdatPath]} workflow
          push (::jms/push (jms/wfl-keys->jms-keys-for workflow))]
      (is (misc/gcs-object-exists? cloudGreenIdatPath))
      (is (misc/gcs-object-exists? cloudRedIdatPath))
      (is (= (:green_idat_cloud_path push) :cloudGreenIdatPath))
      (is (= (:red_idat_cloud_path   push) :cloudRedIdatPath)))))
