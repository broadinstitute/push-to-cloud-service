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

(def notification-keys->jms-keys-table
  "How to satisfy notification keys in WFL request."
  ["action" "request notification key"   "JMS key"
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
   ::push   :red_idat_cloud_path         :redIDatPath])

;; There are others, but these are not null in the sample messages.
;;
(def header-map
  "Map keywords naming JMS headers to their getters."
  {:arrival              #(.getArrival              %)
   :brokerInTime         #(.getBrokerInTime         %)
   :brokerOutTime        #(.getBrokerOutTime        %)
   :commandId            #(.getCommandId            %)
   :compressed           #(.isCompressed            %)
   :destination          #(.getDestination          %)
   :droppable            #(.isDroppable             %)
   :expiration           #(.getExpiration           %)
   :groupSequence        #(.getGroupSequence        %)
   :marshalledProperties #(.getMarshalledProperties %)
   :messageId            #(.getMessageId            %)
   :persistent           #(.isPersistent            %)
   :priority             #(.getPriority             %)
   :producerId           #(.getProducerId           %)
   :readOnlyBody         #(.isReadOnlyBody          %)
   :readOnlyProperties   #(.isReadOnlyProperties    %)
   :redeliveryCounter    #(.getRedeliveryCounter    %)
   :responseRequired     #(.isResponseRequired      %)
   :size                 #(.getSize                 %)
   :timestamp            #(.getTimestamp            %)})

(def notification-keys->jms-keys
  "Map action to map of WFL request notification keys to JMS keys."
  (->> notification-keys->jms-keys-table
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
  (letfn [(rekey [m [k v]] (assoc m k (v workflow)))
          (nilval [k m] (when (nil? (k m)) k))]
    (reduce rekey {} params-keys->jms-keys)))

(defn push-params
  "Push a params.txt for JMS payload into the cloud at PREFIX,
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
                  [cloud (last (str/split (v workflow) #"/"))])))
            (nilval [k m] (when (nil? (k m)) k))]
      (apply misc/shell! "gsutil" "cp" (concat sources [cloud]))
      (reduce cloudify (reduce rekey {} copy) chip-and-push))))

(defn push-append-to-aou-request
  "Push an append_to_aou request for JMS to the cloud at PREFIX."
  [prefix jms]
  (let [result (str/join "/" [(cloud-prefix prefix jms) "ptc.json"])
        request (update append-to-aou-request
                  :notifications conj (jms->notification prefix jms))]
    (misc/shell! "gsutil" "cp" "-" result :in (json/write-str request))
    result))

(def required-jms-keys
  "Sort all the keys required to handle a JMS message."
  (sort (into (->> notification-keys->jms-keys-table
                (partition-all 3)
                rest
                (map (fn [[_ _ key]] key))
                set)
          (vals params-keys->jms-keys))))

(defn ednify
  "Return a EDN representation of the JMS MESSAGE with keyword keys."
  [message]
  (letfn [(headerify [m [k v]] (assoc m k (v message)))
          (unjsonify [s] (json/read-str s :key-fn keyword))]
    (let [raw (into {} (.getProperties message))
          keyed (zipmap (map keyword (keys raw)) (vals raw))]
      {::Headers    (reduce headerify {} header-map)
       ::Properties (update keyed :payload unjsonify)})))

(defn jmsify
  "Return the JMS message represented by the MESSAGE in EDN."
  [{keys [::Headers ::Properties] :as message}]
  (letfn [(headerify [m [k v]] (assoc m k (v message)))
          (unjsonify [s] (json/read-str s :key-fn keyword))]
    (let [raw (into {} (.getProperties message))
          keyed (zipmap (map keyword (keys raw)) (vals raw))]
      {::Headers    (reduce headerify {} header-map)
       ::Properties (update keyed :payload unjsonify)})))

(def missing-keys-message
  "Missing JMS keys:")

(defn handle-message
  "Throw or push to cloud at PREFIX all the files for JMS message."
  [prefix {:keys [workflow] :as jms}]
  (let [missing (keep (fn [k] (when (nil? (k workflow)) k)) required-jms-keys)]
    (when (seq missing)
      (throw (IllegalArgumentException.
               (str/join \space missing-keys-message missing)))))
  (push-params prefix jms)
  (push-append-to-aou-request prefix jms))
