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
  "Pass this to WFL for some reason. WFL should generate the UUID."
  misc/uuid-nil)

(def append-to-aou-request
  "An empty append_to_aou request without notifications."
  {:cromwell cromwell
   :environment environment
   :uuid (str uuid)
   :notifications []})

(def wfl-keys->jms-keys-table
  "How to satisfy notification keys in WFL request."
  ["action" "req'd?" "request notification key"   "JMS key"
   ::copy   true     :analysis_version_number     :analysisCloudVersion
   ::copy   true     :chip_well_barcode           :chipWellBarcode
   ::copy   true     :reported_gender             :gender
   ::copy   true     :sample_alias                :sampleAlias
   ::copy   true     :sample_lsid                 :sampleLsid
   ::copy   true     :call_rate_threshold         :callRateThreshold
   ::chip   true     :bead_pool_manifest_file     :beadPoolManifestPath
   ::chip   true     :cluster_file                :clusterFilePath
   ::chip   false    :gender_cluster_file         :genderClusterFilePath
   ::chip   false    :zcall_thresholds_file       :zCallThresholdsPath
   ::push   true     :green_idat_cloud_path       :greenIDatPath
   ::push   true     :red_idat_cloud_path         :redIDatPath
   ::param  true     :CHIP_TYPE_NAME              :chipName
   ::param  true     :CHIP_WELL_BARCODE           :chipWellBarcode
   ::param  true     :INDIVIDUAL_ALIAS            :collaboratorParticipantId
   ::param  true     :LAB_BATCH                   :labBatch
   ::param  true     :PARTICIPANT_ID              :participantId
   ::param  true     :PRODUCT_FAMILY              :productFamily
   ::param  true     :PRODUCT_NAME                :productName
   ::param  true     :PRODUCT_ORDER_ID            :productOrderId
   ::param  true     :PRODUCT_PART_NUMBER         :productPartNumber
   ::param  true     :REGULATORY_DESIGNATION      :regulatoryDesignation
   ::param  true     :RESEARCH_PROJECT_ID         :researchProjectId
   ::param  true     :SAMPLE_ALIAS                :sampleAlias
   ::param  true     :SAMPLE_GENDER               :gender
   ::param  true     :SAMPLE_ID                   :sampleId
   ::param  true     :SAMPLE_LSID                 :sampleLsid])

(def required-jms-keys
  "Sort all the keys required to handle a JMS message."
  (letfn [(required? [[_ reqd? _ jms]] (when reqd? jms))]
    (->> wfl-keys->jms-keys-table
         (partition-all 4) rest
         (keep required?) set sort)))

(def wfl-keys->jms-keys
  "Map action to map of WFL request notification keys to JMS keys."
  (letfn [(ignore-required-column-for-now [row] (replace (vec row) [0 2 3]))
          (key->key [[k v]] [k (into {} (map (comp vec rest) v))])]
    (->> wfl-keys->jms-keys-table
         (partition-all 4) rest
         (map ignore-required-column-for-now)
         (group-by first)
         (map key->key)
         (into {}))))

(defn jms->params
  "Replace JMS keys in WORKFLOW with their params.txt names."
  [workflow]
  (letfn [(stringify [[k v]] (str/join "=" [(name k) v]))
          (rekey [m [k v]] (assoc m k (v workflow)))]
    (->> wfl-keys->jms-keys ::param
         (reduce rekey {})
         (map stringify)
         (str/join \newline))))

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

(defn cloud-prefix
  "Return the cloud GCS URL with PREFIX for WORKFLOW."
  [prefix workflow]
  (let [{:keys [analysisCloudVersion chipName chipWellBarcode]} workflow]
    (str/join "/" [prefix chipName chipWellBarcode analysisCloudVersion])))

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
        {:keys [::chip ::copy ::push]} wfl-keys->jms-keys
        chip-and-push (merge chip push)
        sources (keep workflow (vals chip-and-push))]
    (letfn [(rekey    [m [k v]] (assoc m k (v workflow)))
            (cloudify [m [k v]]
              (if-let [path (v workflow)]
                (assoc m k (str/join "/" [cloud (last (str/split path #"/"))]))
                m))]
      (apply misc/shell! "gsutil" "cp" (concat sources [cloud]))
      (reduce cloudify (reduce rekey {} copy) chip-and-push))))

(defn get-extended-chip-manifest
  "Get the extended_chip_manifest_file from the JMS message"
  [workflow]
  (let [aou-reference-bucket (or (misc/getenv-or-throw "AOU_REFERENCE_BUCKET") "broad-arrays-dev-storage")
        cloud-chip-metadata-dir (get workflow :cloudChipMetaDataDirectory)
        bucket (first (misc/parse-gs-url cloud-chip-metadata-dir))
        aou-chip-metadata-dir (str/replace cloud-chip-metadata-dir bucket aou-reference-bucket)
        extended-chip-manifest-file-name (get workflow :extendedIlluminaManifestFileName)]
    (str aou-chip-metadata-dir extended-chip-manifest-file-name)))

(defn push-append-to-aou-request
  "Push an append_to_aou request for WORKFLOW to the cloud at PREFIX."
  [prefix workflow params]
  (let [result (str/join "/" [(cloud-prefix prefix workflow) "ptc.json"])
        request (update append-to-aou-request
                        :notifications conj (jms->notification prefix workflow))
        extended-chip-manifest (get-extended-chip-manifest workflow)
        contents (-> request
                     (assoc-in [:notifications 0 :params_file] params)
                     (assoc-in [:notifications 0 :extended_chip_manifest_file] extended-chip-manifest))]
    (misc/shell! "gsutil" "cp" "-" result :in (json/write-str contents))
    result))

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
    (let [params (push-params prefix workflow)]
      [params (push-append-to-aou-request prefix workflow params)])))
