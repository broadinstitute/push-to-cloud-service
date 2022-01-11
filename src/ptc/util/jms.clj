(ns ptc.util.jms
  "Adapt JMS messages into upload actions and workflow parameters."
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ptc.util.gcs :as gcs]
            [ptc.util.misc :as misc])
  (:import [java.io FileNotFoundException]))

(def chips
  "The processing chips from most recent to oldest."
  ["GDA-8v1-0_D1"
   "GDA-8v1-0_A5"])

;; Note :environment is first and :analysisCloudVersion is last.
;;
(def cloud-keys
  "Workflow keys ordered for the destination path in the cloud."
  [:environment :chipName :chipWellBarcode :analysisCloudVersion])

;; Pass uuid-nil because WFL should generate the UUID.
;;
(def append-to-aou-request
  "An empty append_to_aou request with placeholder symbols."
  {:cromwell                    'cromwell
   :environment                 'environment
   :extended_chip_manifest_file 'extended_chip_manifest_file
   :notifications               'notifications
   :params_file                 'params_file
   :uuid                        (str misc/uuid-nil)})

(def wfl-keys->jms-keys-table
  "How to satisfy notification keys in WFL request."
  ["action" "req'd?" "request notification key"           "JMS key"
   ::copy   true     :analysis_version_number             :analysisCloudVersion
   ::copy   true     :chip_well_barcode                   :chipWellBarcode
   ::copy   true     :cloud_chip_metadata_directory       :cloudChipMetaDataDirectory
   ::copy   false    :control_data_directory              :controlDataDirectory
   ::copy   false    :control_sample_name                 :controlSampleName
   ::copy   false    :control_sample_vcf_file             :controlSampleCloudVcfPath
   ::copy   false    :control_sample_vcf_index_file       :controlSampleCloudVcfIndexPath
   ::copy   false    :control_sample_intervals_file       :controlSampleCloudIntervalsFilePath
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
   ::push   true     :green_idat_cloud_path               :greenIDatPath
   ::push   true     :red_idat_cloud_path                 :redIDatPath
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

(defn in-cloud-folder
  "Return the path to LEAF under PREFIX for WORKFLOW."
  [prefix workflow leaf]
  (let [[env & tail] (conj ((apply juxt cloud-keys) workflow) leaf)]
    (str/join "/" (conj tail (str/lower-case env) prefix))))

(defn ^:private latest-cloud-version
  "Nil or the path PREFIX/N/SUFFIX where N is the greatest integer and
  an object exists in the cloud at that path."
  [prefix suffix]
  (let [prefixed (str prefix "/")
        suffixed (str "/" suffix)
        front    (count prefixed)
        back     (count suffixed)]
    (letfn [(suffixed? [[_ object]] (str/ends-with? object suffixed))
            (parse     [url]        (subs url front (- (count url) back)))
            (unparse   [n]          (str prefixed n suffixed))]
      (-> prefix gcs/list-objects
          (->> (map (juxt :bucket :name))
               (filter suffixed?)
               (map (comp edn/read-string parse (partial apply gcs/gs-url)))
               (sort >))
          first unparse misc/do-or-nil))))

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

(defn push-params
  "Push a params.txt for the WORKFLOW into the cloud at PREFIX,
  then return its path in the cloud."
  [prefix workflow]
  (let [result (in-cloud-folder prefix workflow "params.txt")]
    (gcs/gsutil "cp" "-" result :in (jms->params workflow))
    result))

;; https://broadinstitute.atlassian.net/wiki/spaces/GHConfluence/pages/2853961731/2021-07-28+AoU+Processing+Issue+Discussion
;; Look first in local filesystem for (input-key workflow).
;; Then try "prefix/environment/path/leaf" in the cloud.
;; If still not found, try "prefix/path/leaf" in the cloud.
;; Finally look for cloud files that differ in :analysisCloudVersion parts.
;; Otherwise throw.
;;
(defn ^:private find-input-or-throw
  "Throw or find the input file in WORKFLOW using INPUT-KEY and PREFIX."
  [prefix workflow input-key]
  (let [local-file   (input-key workflow)
        join         (partial str/join "/")
        leaf         (last (str/split local-file #"/"))
        [env & tail] ((apply juxt (butlast cloud-keys)) workflow)
        unversioned  (vec (cons (str/lower-case env) tail))
        parts        (conj unversioned leaf)
        new-result   (join (cons prefix parts))
        old-result   (join (cons prefix (rest parts)))]
    (or (when (.exists (io/file local-file))
          (gcs/gsutil "-h" (str "Content-MD5:" (gcs/get-md5-hash local-file))
                      "cp" local-file new-result)
          new-result)
        (when (gcs/gcs-object-exists? new-result) new-result)
        (when (gcs/gcs-object-exists? old-result) old-result)
        (let [new-prefix   (join (cons prefix unversioned))
              old-prefix   (join (cons prefix (rest unversioned)))]
          (or (latest-cloud-version new-prefix leaf)
              (latest-cloud-version old-prefix leaf)
              (let [message (format "Cannot find %s in %s" leaf
                                    [local-file new-result old-result
                                     (join [new-prefix "*" leaf])
                                     (join [old-prefix "*" leaf])])]
                (log/error message)
                (throw (FileNotFoundException. message))))))))

(def aou-reference-bucket
  "The AllOfUs reference bucket or broad-arrays-dev-storage."
  (or (System/getenv "AOU_REFERENCE_BUCKET")
      "broad-arrays-dev-storage"))

(defn get-extended-chip-manifest
  "Get the extended_chip_manifest_file from _WORKFLOW."
  [{:keys [cloudChipMetaDataDirectory extendedIlluminaManifestFileName]
    :as   _workflow}]
  (let [[bucket _] (gcs/parse-gs-url cloudChipMetaDataDirectory)]
    (str (str/replace-first cloudChipMetaDataDirectory
                            bucket aou-reference-bucket)
         extendedIlluminaManifestFileName)))

;; Ignore cloud paths for files with ::push key.
;;
(defn jms->notification
  "Push files to PREFIX and return notification for WORKFLOW."
  [prefix {:keys [cloudChipMetaDataDirectory] :as workflow}]
  (let [{:keys [::chip ::copy ::push]} wfl-keys->jms-keys
        chips  (->> chip vals
                    (map workflow)
                    (map (fn [field]
                           (when-not (nil? field)
                             (str cloudChipMetaDataDirectory
                                  (last (str/split field #"/"))))))
                    (zipmap (keys chip))
                    (filter second)
                    (into {}))
        copies (reduce (fn [m [k v]] (assoc m k (v workflow))) {} copy)
        pushes (->> push vals
                    (map (partial find-input-or-throw prefix workflow))
                    (zipmap (keys push)))]
    (merge chips copies pushes)))

(defn push-append-to-aou-request
  "Push an append_to_aou request for WORKFLOW to the cloud at PREFIX
  with PARAMS."
  [prefix workflow params]
  (let [ptc-json (in-cloud-folder prefix workflow "ptc.json")]
    (-> prefix
        (jms->notification workflow)
        (assoc :params_file params)
        (assoc :extended_chip_manifest_file (get-extended-chip-manifest workflow))
        vector
        (->> (assoc append-to-aou-request :notifications))
        json/write-str
        (->> (gcs/gsutil "cp" "-" ptc-json :in)))
    [params ptc-json]))

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

(def ^:private missing-keys-message "Missing JMS keys:")

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

(defn message-ids-equal?
  "True when the IDs of JMS MESSAGES are the same. Otherwise false."
  [& messages]
  (let [ids (map (comp :messageId ::Headers) messages)]
    (or (empty? ids)
        (and (not-any? nil? ids)
             (apply = ids)))))
