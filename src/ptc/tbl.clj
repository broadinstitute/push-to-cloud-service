(ns ptc.tbl
  "Frob JMS messages into upload actions and workflow parameters."
  (:require [ptc.util.misc   :as misc]))

(def params-keys
  "Keys in the params.txt file."
  [:CHIP_TYPE_NAME
   :CHIP_WELL_BARCODE
   :INDIVIDUAL_ALIAS
   :LAB_BATCH
   :PARTICIPANT_ID
   :PRODUCT_FAMILY
   :PRODUCT_NAME
   :PRODUCT_ORDER_ID
   :PRODUCT_PART_NUMBER
   :REGULATORY_DESIGNATION
   :RESEARCH_PROJECT_ID
   :SAMPLE_ALIAS
   :SAMPLE_GENDER
   :SAMPLE_ID
   :SAMPLE_LSID])

(def workflow-notification-keys
  "Per-workflow keys for the :notifications in a WFL request."
  [:analysis_version_number
   :chip_well_barcode
   :reported_gender
   :sample_alias
   :sample_lsid
   :bead_pool_manifest_file
   :cluster_file
   :extended_chip_manifest_file
   :gender_cluster_file
   :zcall_thresholds_file
   :green_idat_cloud_path
   :red_idat_cloud_path
   :params_file])

(def workload-keys
  "Workload keys in a WFL request."
  [:cromwell
   :environment
   :uuid
   :notifications])

(def http-url-keys
  "JMS message keys that name HTTP URLs."
  [:cromwellBaseUrl
   :mercuryFingerprintStoreURI])

                  (def gcs-keys
                    "JMS message keys that name GCS cloud paths."
                    [:analysisCloudPath
                     :cloudChipMetaDataDirectory
                     :cloudGreenIdatPath
                     :cloudRedIdatPath
                     :controlDataDirectory
                     :cromwellBaseExecutionCloudPath
                     :monitoringScriptPath
                     :vaultTokenPath])

                  (def nfs-keys
                    "JMS message keys that name NFS filesystem paths."
                    [:analysisDirectory
                     :beadPoolManifestPath
                     :bsMap
                     :bwa
                     :bwa64
                     :bwaMem
                     :bwaMem_0_7_15
                     :chipManifestPath
                     :clioClientJarPath
                     :cloudSoftwarePath
                     :clusterFilePath
                     :cromwellJarPath
                     :cromwellWorkflowDependenciesZip
                     :cromwellWorkflowOptionsFile
                     :cromwellWorkflowWdlFile
                     :dbSnpFilePath
                     :extraRLibsDir
                     :flowcellAnalysisDirectory
                     :gatk2Jar
                     :haplotypeMap
                     :identifyBamId
                     :jarPath
                     :jmsVaultPath
                     :latexClassesDir
                     :maq
                     :mercuryFingerprintStoreCredentialsVaultPath
                     :podRoot
                     :referenceFasta
                     :referenceResourceFile
                     :samblaster
                     :samtools
                     :samtoolsRapidQc
                     :serviceAccountJsonPath
                     :serviceAccountJsonVaultPath
                     :snap
                     :starAligner
                     :topHat])

                  (def other-keys
                    "JMS message keys not in above collections."
                    [:aggregationPendingQueue
                     :analysisCloudVersion
                     :blacklistSchema
                     :callRateThreshold
                     :chipName
                     :chipWellBarcode
                     :clioPort
                     :clioServer
                     :clioUseHttps
                     :collaboratorParticipantId
                     :cromwellWorkflowGoogleProject
                     :cromwellWorkflowName
                     :environment
                     :errors
                     :extendedIlluminaManifestFileName
                     :extendedIlluminaManifestVersion
                     :farpointQueue
                     :fileNameSafeSampleAlias
                     :gender
                     :isPodWorkflow
                     :jmsPort
                     :jmsServer
                     :labBatch
                     :metricsSchema
                     :negativeControl
                     :notificationEmailAddresses
                     :participantId
                     :picardSchema
                     :pipelineGitHash
                     :pipelineVersion
                     :podName
                     :positiveControl
                     :productFamily
                     :productName
                     :productOrderId
                     :productPartNumber
                     :rapidQcPendingAggregationQueue
                     :regulatoryDesignation
                     :requeryLimsForIdats
                     :researchProjectId
                     :sampleAlias
                     :sampleId
                     :sampleLsid
                     :snapThreadCount
                     :variantCallingRequestQueue
                     :warnings])

                  (def all-keys
                    "All the keys in the :workflow part of the JMS message."
                    [:aggregationPendingQueue
                     :analysisCloudPath
                     :analysisCloudVersion
                     :analysisDirectory
                     :beadPoolManifestPath
                     :blacklistSchema
                     :bsMap
                     :bwa
                     :bwa64
                     :bwaMem
                     :bwaMem_0_7_15
                     :callRateThreshold
                     :chipManifestPath
                     :chipName
                     :chipWellBarcode
                     :clioClientJarPath
                     :clioPort
                     :clioServer
                     :clioUseHttps
                     :cloudChipMetaDataDirectory
                     :cloudGreenIdatPath
                     :cloudRedIdatPath
                     :cloudSoftwarePath
                     :clusterFilePath
                     :collaboratorParticipantId
                     :controlDataDirectory
                     :cromwellBaseExecutionCloudPath
                     :cromwellBaseUrl
                     :cromwellJarPath
                     :cromwellWorkflowDependenciesZip
                     :cromwellWorkflowGoogleProject
                     :cromwellWorkflowName
                     :cromwellWorkflowOptionsFile
                     :cromwellWorkflowWdlFile
                     :dbSnpFilePath
                     :environment
                     :errors
                     :extendedIlluminaManifestFileName
                     :extendedIlluminaManifestVersion
                     :extraRLibsDir
                     :farpointQueue
                     :fileNameSafeSampleAlias
                     :flowcellAnalysisDirectory
                     :gatk2Jar
                     :gender
                     :haplotypeMap
                     :identifyBamId
                     :isPodWorkflow
                     :jarPath
                     :jmsPort
                     :jmsServer
                     :jmsVaultPath
                     :labBatch
                     :latexClassesDir
                     :maq
                     :mercuryFingerprintStoreCredentialsVaultPath
                     :mercuryFingerprintStoreURI
                     :metricsSchema
                     :monitoringScriptPath
                     :negativeControl
                     :notificationEmailAddresses
                     :participantId
                     :picardSchema
                     :pipelineGitHash
                     :pipelineVersion
                     :podName
                     :podRoot
                     :positiveControl
                     :productFamily
                     :productName
                     :productOrderId
                     :productPartNumber
                     :rapidQcPendingAggregationQueue
                     :referenceFasta
                     :referenceResourceFile
                     :regulatoryDesignation
                     :requeryLimsForIdats
                     :researchProjectId
                     :samblaster
                     :sampleAlias
                     :sampleId
                     :sampleLsid
                     :samtools
                     :samtoolsRapidQc
                     :serviceAccountJsonPath
                     :serviceAccountJsonVaultPath
                     :snap
                     :snapThreadCount
                     :starAligner
                     :topHat
                     :variantCallingRequestQueue
                     :vaultTokenPath
                     :warnings])

                  (def inputs
                    "The Arrays.wdl inputs without the :Arrays prefix."
                    {:IlluminaGenotypingArray.IlluminaGenotypingArray.AutoCall.is_gender_autocall "Boolean? (optional)"
                     :IlluminaGenotypingArray.IlluminaGenotypingArray.GtcToVcf.memory "Float? (optional)"
                     :IlluminaGenotypingArray.IlluminaGenotypingArray.SelectFingerprintVariants.excludeFiltered "Boolean (optional, default = false)"
                     :IlluminaGenotypingArray.IlluminaGenotypingArray.SelectFingerprintVariants.excludeNonVariants "Boolean (optional, default = false)"
                     :IlluminaGenotypingArray.IlluminaGenotypingArray.SelectVariantsForGenotypeConcordance.excludeNonVariants "Boolean (optional, default = false)"
                     :IlluminaGenotypingArray.IlluminaGenotypingArray.SelectVariantsForGenotypeConcordance.variant_rsids_file "File? (optional)"
                     :IlluminaGenotypingArray.chip_type "String? (optional)"
                     :UploadEmptyArraysMetrics.arrays_control_code_summary_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.arrays_variant_calling_summary_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.fingerprinting_detail_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.fingerprinting_summary_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.genotype_concordance_contingency_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.genotype_concordance_detail_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.genotype_concordance_summary_metrics "File? (optional)"
                     :UploadEmptyArraysMetrics.verify_id_metrics "File? (optional)"
                     :analysis_version_number "Int"
                     :autocall_version "String (optional, default = \"3.0.0\")"
                     :bead_pool_manifest_file "File"
                     :call_rate_threshold "Float"
                     :chip_type "String? (optional)"
                     :chip_well_barcode "String"
                     :cluster_file "File"
                     :cluster_filename "String? (optional)"
                     :contamination_controls_vcf "File? (optional)"
                     :control_sample_intervals_file "File? (optional)"
                     :control_sample_name "String? (optional)"
                     :control_sample_vcf_file "File? (optional)"
                     :control_sample_vcf_index_file "File? (optional)"
                     :dbSNP_vcf "File"
                     :dbSNP_vcf_index "File"
                     :disk_size "Int"
                     :environment "String"
                     :extended_chip_manifest_file "File"
                     :fingerprint_genotypes_vcf_file "File? (optional)"
                     :fingerprint_genotypes_vcf_index_file "File? (optional)"
                     :gender_cluster_file "File? (optional)"
                     :genotype_concordance_threshold "Float (optional, default = 0.98)"
                     :green_idat_cloud_path "File"
                     :haplotype_database_file "File"
                     :params_file "File"
                     :preemptible_tries "Int"
                     :red_idat_cloud_path "File"
                     :ref_dict "File"
                     :ref_fasta "File"
                     :ref_fasta_index "File"
                     :reported_gender "String"
                     :sample_alias "String"
                     :sample_lsid "String"
                     :subsampled_metrics_interval_list "File? (optional)"
                     :variant_rsids_file "File"
                     :vault_token_path "File"
                     :zcall_thresholds_file "File? (optional)"})
