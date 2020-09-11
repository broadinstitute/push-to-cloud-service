(ns ptc.unit.jms-test
  (:require [clojure.test :refer [deftest is testing]]
            [ptc.util.jms :as jms]
            [ptc.util.misc :as misc]))

(deftest use-on-prem-idat-paths
  (testing "WFL uses the on-prem idat files if they do not exist in the cloud."
    (let [jms (clojure.edn/read-string (slurp "./test/data/good-jms.edn"))
          workflow (get-in jms [::jms/Properties :payload :workflow])
          cloud-green-idat (get workflow :cloudGreenIdatPath)
          cloud-red-idat (get workflow :cloudRedIdatPath)
          push (get jms/wfl-keys->jms-keys ::jms/push)
          updated-keys (jms/handle-existing-cloud-paths jms/push-or-copy-keys push workflow)]
      (is (= (misc/file-exists-or-nil cloud-green-idat) nil))
      (is (= (misc/file-exists-or-nil cloud-red-idat) nil))
      (is (= (get updated-keys :green_idat_cloud_path) :greenIDatPath))
      (is (= (get updated-keys :red_idat_cloud_path) :redIDatPath)))))

(deftest use-cloud-idat-paths
  (testing "WFL uses the cloud idat paths if they exist."
    (let [jms (clojure.edn/read-string (slurp "./test/data/reprocessing-jms.edn"))
          workflow (get-in jms [::jms/Properties :payload :workflow])
          cloud-green-idat (get workflow :cloudGreenIdatPath)
          cloud-red-idat (get workflow :cloudRedIdatPath)
          push (get jms/wfl-keys->jms-keys ::jms/push)
          updated-keys (jms/handle-existing-cloud-paths jms/push-or-copy-keys push workflow)]
      (is (not= (misc/file-exists-or-nil cloud-green-idat) nil))
      (is (not= (misc/file-exists-or-nil cloud-red-idat) nil))
      (is (= (get updated-keys :green_idat_cloud_path) :cloudGreenIdatPath))
      (is (= (get updated-keys :red_idat_cloud_path) :cloudRedIdatPath)))))
