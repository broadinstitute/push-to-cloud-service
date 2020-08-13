(ns ptc.util.jms
  "Frob JMS messages into upload actions and workflow parameters."
  (:require [clojure.data.json :as json]
            [clojure.string    :as str]
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
  "Return the cloud GCS URL with PREFIX for WORKFLOW."
  [prefix workflow]
  (let [{:keys [analysisCloudVersion chipName chipWellBarcode]} workflow]
    (str/join "/" [prefix chipName chipWellBarcode analysisCloudVersion])))

(defn jms->params
  "Replace JMS keys in WORKFLOW with their params.txt names."
  [workflow]
  (letfn [(stringify [[k v]] (str/join "=" [(name k) v]))
          (rekey [m [k v]] (assoc m k (v workflow)))]
    (->> params-keys->jms-keys
         (reduce rekey {})
         (map stringify)
         (str/join \newline))))

(defn push-params
  "Push a params.txt for the WORKFLOW into the cloud at PREFIX,
  then return its path in the cloud."
  [prefix workflow]
  (let [result (str/join "/" [(cloud-prefix prefix workflow) "params.txt"])]
    (misc/shell! "gsutil" "cp" "-" result :in (jms->params workflow))
    result))

;; Push the chip files too until we figure something else out.
;;
(defn jms->notification
  "Push files to PREFIX and return notification for WORKFLOW."
  [prefix workflow]
  (let [cloud (cloud-prefix prefix workflow)
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
  "Push an append_to_aou request for WORKFLOW to the cloud at PREFIX."
  [prefix workflow]
  (let [result (str/join "/" [(cloud-prefix prefix workflow) "ptc.json"])
        request (update append-to-aou-request
                        :notifications conj (jms->notification prefix workflow))]
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
    (let [headers {::Headers (reduce headerify {} header-map)}
          raw     (into {} (.getProperties message))
          keyed   (zipmap (map keyword (keys raw)) (vals raw))]
      (if (:payload keyed)
        (assoc headers ::Properties (update keyed :payload unjsonify))
        headers))))

(comment
  (ednify nil))

(defn encode
  "Encode EDN MESSAGE ::Properties :payload for a PTC JMS message."
  [{:keys [::Properties] :as message}]
  (letfn [(jsonify [payload] (json/write-str payload
                                             :escape-js-separators false
                                             :escape-slash false))]
    (assoc message ::Properties (update Properties :payload jsonify))))

(def missing-keys-message "Missing JMS keys:") ; for tests

(defn handle-message
  "Throw or push to cloud at PREFIX all the files for ednified JMS message."
  [prefix jms]
  (let [workflow (get-in jms [::Properties :payload :workflow])
        missing? (fn [k] (when (nil? (k workflow)) k))
        missing (keep missing? required-jms-keys)]
    (when (seq missing)
      (throw (IllegalArgumentException.
              (str/join \space [missing-keys-message (vec missing)]))))
    [(push-params prefix workflow)
     (push-append-to-aou-request prefix workflow)]))
