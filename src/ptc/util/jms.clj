(ns ptc.util.jms
  "Adapt JMS messages into upload actions and workflow parameters."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [ptc.util.misc :as misc]))

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
  "An empty append_to_aou request with placeholder symbols."
  {:cromwell cromwell
   :environment environment
   :extended_chip_manifest_file 'extended_chip_manifest_file
   :notifications 'notifications
   :params_file 'params_file
   :uuid (str uuid)})

(def wfl-keys->jms-keys-table
  "How to satisfy notification keys in WFL request."
  ["action" "req'd?" "request notification key"           "JMS key"
   ::copy   true     :analysis_version_number             :analysisCloudVersion
   ::copy   true     :chip_well_barcode                   :chipWellBarcode
   ::copy   true     :cloud_chip_metadata_directory       :cloudChipMetaDataDirectory
   ::copy   false    :control_sample_name                 :controlSampleName
   ::push   false    :control_sample_vcf_file             :controlSampleCloudVcfPath
   ::push   false    :control_sample_vcf_index_file       :controlSampleCloudVcfIndexPath
   ::push   false    :control_sample_intervals_file       :controlSampleCloudIntervalsFilePath
   ::copy   true     :environment                         :environment
   ::copy   true     :extended_illumina_manifest_filename :extendedIlluminaManifestFileName
   ::copy   false    :minor_allele_frequency_file         :minorAlleleFrequencyFileCloudPath
   ::copy   true     :reported_gender                     :gender
   ::copy   true     :sample_alias                        :sampleAlias
   ::copy   true     :sample_lsid                         :sampleLsid
   ::copy   true     :call_rate_threshold                 :callRateThreshold
   ::copy   true     :vault_token_path                    :vaultTokenPath
   ::chip   true     :bead_pool_manifest_file             :beadPoolManifestPath
   ::chip   true     :cluster_file                        :clusterFilePath
   ::chip   false    :gender_cluster_file                 :genderClusterFilePath
   ::chip   false    :zcall_thresholds_file               :zCallThresholdsPath
   ::push   true     :green_idat_cloud_path               [:cloudGreenIdatPath :greenIDatPath]
   ::push   true     :red_idat_cloud_path                 [:cloudRedIdatPath   :redIDatPath]
   ::param  true     :CHIP_TYPE_NAME                      :chipName
   ::param  true     :CHIP_WELL_BARCODE                   :chipWellBarcode
   ::param  true     :INDIVIDUAL_ALIAS                    :collaboratorParticipantId
   ::param  true     :LAB_BATCH                           :labBatch
   ::param  true     :PARTICIPANT_ID                      :participantId
   ::param  true     :PRODUCT_FAMILY                      :productFamily
   ::param  true     :PRODUCT_NAME                        :productName
   ::param  true     :PRODUCT_ORDER_ID                    :productOrderId
   ::param  true     :PRODUCT_PART_NUMBER                 :productPartNumber
   ::param  true     :PRODUCT_TYPE                        :productType
   ::param  true     :REGULATORY_DESIGNATION              :regulatoryDesignation
   ::param  true     :RESEARCH_PROJECT_ID                 :researchProjectId
   ::param  true     :SAMPLE_ALIAS                        :sampleAlias
   ::param  true     :SAMPLE_GENDER                       :gender
   ::param  true     :SAMPLE_ID                           :sampleId
   ::param  true     :SAMPLE_LSID                         :sampleLsid])

(def required-jms-keys
  "All the keys required to handle a JMS message."
  (letfn [(required? [[_ reqd? _ jms]] (when reqd? jms))]
    (->> wfl-keys->jms-keys-table
         (partition-all 4) rest
         (keep required?) set)))

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

(defn wfl-keys->jms-keys-for
  "Return wfl-keys->jms-keys modified for WORKFLOW."
  [workflow]
  (letfn [(adapt [keymap [k v]] (assoc-in keymap [::push k] v))]
    (reduce adapt (dissoc wfl-keys->jms-keys ::push)
            (for [[k [cloud local]] (::push wfl-keys->jms-keys)]
              (if (misc/gcs-object-exists? (cloud workflow))
                [k cloud]
                [k local])))))

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
  (let [{:keys [analysisCloudVersion chipName chipWellBarcode environment]} workflow]
    (str/join "/" [prefix (str/lower-case environment) chipName chipWellBarcode analysisCloudVersion])))

(defn push-params
  "Push a params.txt for the WORKFLOW into the cloud at PREFIX,
  then return its path in the cloud."
  [prefix workflow]
  (let [result (str/join "/" [(cloud-prefix prefix workflow) "params.txt"])]
    (misc/gsutil "cp" "-" result :in (jms->params workflow))
    result))

(defn jms->notification
  "Push files to PREFIX and return notification for WORKFLOW."
  [prefix workflow]
  (let [cloud (cloud-prefix prefix workflow)
        {:keys [::chip ::copy ::push]} (wfl-keys->jms-keys-for workflow)
        chip-and-push (merge chip push)
        sources (keep workflow (vals chip-and-push))]
    (letfn [(rekey    [m [k v]] (assoc m k (v workflow)))
            (cloudify [m [k v]]
              (if-let [path (v workflow)]
                (assoc m k (str/join "/" [cloud (last (str/split path #"/"))]))
                m))]
      (doseq [f sources]
        (let [hash (misc/get-md5-hash f)]
          (misc/gsutil "-h" (str "Content-MD5:" hash) "cp" f cloud)))
      (reduce cloudify (reduce rekey {} copy) chip-and-push))))

(def aou-reference-bucket
  "The AllOfUs reference bucket or broad-arrays-dev-storage."
  (or (System/getenv "AOU_REFERENCE_BUCKET")
      "broad-arrays-dev-storage"))

(defn get-extended-chip-manifest
  "Get the extended_chip_manifest_file from _WORKFLOW."
  [{:keys [cloudChipMetaDataDirectory extendedIlluminaManifestFileName]
    :as _workflow}]
  (let [[bucket _] (misc/parse-gs-url cloudChipMetaDataDirectory)]
    (str (str/replace-first cloudChipMetaDataDirectory
                            bucket aou-reference-bucket)
         extendedIlluminaManifestFileName)))

(defn push-append-to-aou-request
  "Push an append_to_aou request for WORKFLOW to the cloud at PREFIX
  with PARAMS."
  [prefix workflow params]
  (let [ptc (str/join "/" [(cloud-prefix prefix workflow) "ptc.json"])]
    (-> prefix
        (jms->notification workflow)
        (assoc :params_file params)
        (assoc :extended_chip_manifest_file (get-extended-chip-manifest workflow))
        vector
        (->> (assoc append-to-aou-request :notifications))
        json/write-str
        (->> (misc/gsutil "cp" "-" ptc :in)))
    [params ptc]))

(defn ednify
  "Return an EDN representation of the JMS MESSAGE with keyword keys."
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
        optional (group-by vector? required-jms-keys)
        required (sort (optional false))
        one-ofs  (map set (optional true))]
    (letfn [(missing? [k] (when (nil? (k workflow)) k))
            (none? [one-of]
              (when (not-any? one-of (keys workflow))
                one-of))]
      (let [missing (concat (keep missing? required) (keep none? one-ofs))]
        (when (seq missing)
          (throw (IllegalArgumentException.
                  (str/join \space [missing-keys-message (vec missing)])))))
      (let [params (push-params prefix workflow)]
        (push-append-to-aou-request prefix workflow params)))))
