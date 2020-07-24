(ns ptc.util.jms
  "Frob JMS messages into upload actions and workflow parameters."
  (:require [clojure.data.json :as json]
            [clojure.string    :as str]
            [ptc.util.gcs      :as gcs]
            [ptc.util.misc     :as misc]))

(def cromwell
  "Use this Cromwell.  Should depend on deployment environment."
  "https://cromwell-gotc-auth.gotc-dev.broadinstitute.org/")

(def environment
  "Run in this deployment environment.
  WFL should get this from ZERO_DEPLOY_ENVIRONMENT"
  "aou-dev")

(def uuid
  "Pass this to WFL for some reason.  WFL should generate this UUID."
  misc/uuid-nil)

(def append-to-aou-request
  "An empty append_to_aou request without notifications."
  {:cromwell cromwell
   :environment environment
   :uuid (str uuid)
   :notifications []})

(def params-keys->jms-keys
  "Map params.txt keys to their associated keys in a JMS message."
  {:CHIP_TYPE_NAME         :chipName
   :CHIP_WELL_BARCODE      :chipWellBarcode
   :INDIVIDUAL_ALIAS       :collaboratorParticipantId
   :LAB_BATCH              :labBatch
   :PARTICIPANT_ID         :participantId
   :PRODUCT_FAMILY         :productFamily
   :PRODUCT_NAME           :productName
   :PRODUCT_ORDER_ID       :productOrderId
   :PRODUCT_PART_NUMBER    :productPartNumber
   :REGULATORY_DESIGNATION :regulatoryDesignation
   :RESEARCH_PROJECT_ID    :researchProjectId
   :SAMPLE_ALIAS           :collaboratorSampleId
   :SAMPLE_GENDER          :gender
   :SAMPLE_ID              :sampleId
   :SAMPLE_LSID            :sampleLsid})

(def notification-keys->jms-keys
  (->> ["action" "request notification key"   "JMS key"
        ::copy   :analysis_version_number     :analysisCloudVersion
        ::copy   :chip_well_barcode           :chipWellBarcode
        ::copy   :reported_gender             :gender
        ::copy   :sample_alias                :sampleAlias
        ::copy   :sample_lsid                 :sampleLsid
        ::chip   :bead_pool_manifest_file     :beadPoolManifestPath
        ::chip   :cluster_file                :clusterFilePath
        ::chip   :extended_chip_manifest_file :chipManifestPath
        ::chip   :gender_cluster_file         :genderClusterFilePath
        ::chip   :zcall_thresholds_file       :zCallThresholdsPath
        ::push   :green_idat_cloud_path       :greenIDatPath
        ::push   :red_idat_cloud_path         :redIDatPath]
    (partition-all 3) rest (group-by first)
    (map (fn [[k v]] [k (into {} (map (comp vec rest) v))]))
    (into {})))

(defn cloud-prefix
  "Return the cloud GCS URL with PREFIX for JMS payload."
  [prefix {:keys [workflow] :as jms}]
  (let [{:keys [analysisCloudVersion chipName chipWellBarcode]} workflow]
    (str/join "/" [prefix chipName chipWellBarcode analysisCloudVersion])))

(defn jms->params
  "Replace keys in JMS with their params.txt names."
  [{:keys [workflow] :as jms}]
  (letfn [(rekey [m [k v]] (assoc m k (v workflow)))]
    (reduce rekey {} params-keys->jms-keys)))

(defn spit-params
  "Spit a params.txt for JMS payload into the cloud at PREFIX,
  then return its path in the cloud."
  [prefix {:keys [workflow] :as jms}]
  (letfn [(stringify [[k v]] (str/join "=" [(name k) v]))]
    (let [result (str/join "/" [(cloud-prefix prefix jms) "params.txt"])
          params (str/join \newline (map stringify (jms->params jms)))]
      (misc/shell! "gsutil" "cp" "-" result :in (str params \newline))
      result)))

;; Push the chip files too until we figure something else out.
;;
(defn jms->notification
  "Push files and return notification for JMS payload at PREFIX."
  [prefix {:keys [workflow] :as jms}]
  (let [cloud (cloud-prefix prefix jms)
        {:keys [::chip ::copy ::push]} notification-keys->jms-keys
        chip-and-push (merge chip push)
        sources (map workflow (vals chip-and-push))]
    (letfn [(rekey    [m [k v]] (assoc m k (v workflow)))
            (cloudify [m [k v]]
              (assoc m k
                (str/join "/"
                  [cloud (last (str/split (v workflow) #"/"))])))]
      (apply misc/shell! "gsutil" "cp" (concat sources [cloud]))
      (reduce cloudify (reduce rekey {} copy) chip-and-push))))

(defn spit-append-to-aou-request
  "Push an append_to_aou request for JMS to the cloud at PREFIX."
  [prefix jms]
  (let [result (str/join "/" [(cloud-prefix prefix jms) "ptc.json"])
        request (update append-to-aou-request
                  :notifications conj (jms->notification prefix jms))]
    (misc/shell! "gsutil" "cp" "-" result :in (json/write-str request))
    result))

(defn handle-message
  "Push to cloud at PREFIX all the files for JMS message."
  [jms]
  (let [prefix "gs://broad-gotc-dev-storage/tbl/ptc"]
    (spit-params prefix jms)
    (spit-append-to-aou-request prefix jms)))
