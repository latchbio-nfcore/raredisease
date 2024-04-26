/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { paramsSummaryLog; paramsSummaryMap; fromSamplesheet } from 'plugin/nf-validation'
include { paramsSummaryMultiqc   } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_raredisease_pipeline'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CHECK MANDATORY PARAMETERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

def mandatoryParams = [
    "aligner",
    "analysis_type",
    "fasta",
    "input",
    "intervals_wgs",
    "intervals_y",
    "platform",
    "variant_catalog",
    "variant_caller"
]
def missingParamsCount = 0

if (params.run_rtgvcfeval) {
    mandatoryParams += ["rtg_truthvcfs"]
}

if (!params.skip_snv_annotation) {
    mandatoryParams += ["genome", "vcfanno_resources", "vcfanno_toml", "vep_cache", "vep_cache_version",
    "gnomad_af", "score_config_snv", "variant_consequences_snv"]
}

if (!params.skip_sv_annotation) {
    mandatoryParams += ["genome", "vep_cache", "vep_cache_version", "score_config_sv", "variant_consequences_sv"]
    if (!params.svdb_query_bedpedbs && !params.svdb_query_dbs) {
        println("params.svdb_query_bedpedbs or params.svdb_query_dbs should be set.")
        missingParamsCount += 1
    }
}

if (!params.skip_mt_annotation) {
    mandatoryParams += ["genome", "mito_name", "vcfanno_resources", "vcfanno_toml", "vep_cache_version", "vep_cache", "variant_consequences_snv"]
}

if (params.analysis_type.equals("wes")) {
    mandatoryParams += ["target_bed"]
}

if (params.variant_caller.equals("sentieon")) {
    mandatoryParams += ["ml_model"]
}

if (!params.skip_germlinecnvcaller) {
    mandatoryParams += ["ploidy_model", "gcnvcaller_model"]
}

if (!params.skip_vep_filter) {
    if (!params.vep_filters && !params.vep_filters_scout_fmt) {
        println("params.vep_filters or params.vep_filters_scout_fmt should be set.")
        missingParamsCount += 1
    } else if (params.vep_filters && params.vep_filters_scout_fmt) {
        println("Either params.vep_filters or params.vep_filters_scout_fmt should be set.")
        missingParamsCount += 1
    }
}

if (!params.skip_me_annotation) {
    mandatoryParams += ["mobile_element_svdb_annotations", "variant_consequences_snv"]
}

if (!params.skip_gens) {
    mandatoryParams += ["gens_gnomad_pos", "gens_interval_list", "gens_pon_female", "gens_pon_male"]
}

for (param in mandatoryParams.unique()) {
    if (params[param] == null) {
        println("params." + param + " not set.")
        missingParamsCount += 1
    }
}

if (missingParamsCount>0) {
    error("\nSet missing parameters and restart the run. For more information please check usage documentation on github.")
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES AND SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


//
// MODULE: Installed directly from nf-core/modules
//

include { FASTQC              } from '../modules/nf-core/fastqc/main'
include { MULTIQC             } from '../modules/nf-core/multiqc/main'
include { PEDDY               } from '../modules/nf-core/peddy/main'
include { SMNCOPYNUMBERCALLER } from '../modules/nf-core/smncopynumbercaller/main'

//
// MODULE: Local modules
//

include { RENAME_ALIGN_FILES as RENAME_BAM_FOR_SMNCALLER } from '../modules/local/rename_align_files'
include { RENAME_ALIGN_FILES as RENAME_BAI_FOR_SMNCALLER } from '../modules/local/rename_align_files'

//
// SUBWORKFLOWS
//

include { ALIGN                                              } from '../subworkflows/local/align'
include { ANNOTATE_CSQ_PLI as ANN_CSQ_PLI_MT                 } from '../subworkflows/local/annotate_consequence_pli'
include { ANNOTATE_CSQ_PLI as ANN_CSQ_PLI_SNV                } from '../subworkflows/local/annotate_consequence_pli'
include { ANNOTATE_CSQ_PLI as ANN_CSQ_PLI_SV                 } from '../subworkflows/local/annotate_consequence_pli'
include { ANNOTATE_GENOME_SNVS                               } from '../subworkflows/local/annotate_genome_snvs'
include { ANNOTATE_MOBILE_ELEMENTS                           } from '../subworkflows/local/annotate_mobile_elements'
include { ANNOTATE_MT_SNVS                                   } from '../subworkflows/local/annotate_mt_snvs'
include { ANNOTATE_STRUCTURAL_VARIANTS                       } from '../subworkflows/local/annotate_structural_variants'
include { CALL_MOBILE_ELEMENTS                               } from '../subworkflows/local/call_mobile_elements'
include { CALL_REPEAT_EXPANSIONS                             } from '../subworkflows/local/call_repeat_expansions'
include { CALL_SNV                                           } from '../subworkflows/local/call_snv'
include { CALL_STRUCTURAL_VARIANTS                           } from '../subworkflows/local/call_structural_variants'
include { GENERATE_CLINICAL_SET as GENERATE_CLINICAL_SET_MT  } from '../subworkflows/local/generate_clinical_set'
include { GENERATE_CLINICAL_SET as GENERATE_CLINICAL_SET_SNV } from '../subworkflows/local/generate_clinical_set'
include { GENERATE_CLINICAL_SET as GENERATE_CLINICAL_SET_SV  } from '../subworkflows/local/generate_clinical_set'
include { GENERATE_CYTOSURE_FILES                            } from '../subworkflows/local/generate_cytosure_files'
include { GENS                                               } from '../subworkflows/local/gens'
include { PREPARE_REFERENCES                                 } from '../subworkflows/local/prepare_references'
include { QC_BAM                                             } from '../subworkflows/local/qc_bam'
include { RANK_VARIANTS as RANK_VARIANTS_MT                  } from '../subworkflows/local/rank_variants'
include { RANK_VARIANTS as RANK_VARIANTS_SNV                 } from '../subworkflows/local/rank_variants'
include { RANK_VARIANTS as RANK_VARIANTS_SV                  } from '../subworkflows/local/rank_variants'
include { SCATTER_GENOME                                     } from '../subworkflows/local/scatter_genome'
include { SUBSAMPLE_MT                                       } from '../subworkflows/local/subsample_mt'
include { VARIANT_EVALUATION                                 } from '../subworkflows/local/variant_evaluation'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow RAREDISEASE {

    take:
    ch_samplesheet // channel: samplesheet read in from --input

    main:

    ch_versions = Channel.empty()
    ch_multiqc_files = Channel.empty()

    ch_samples   = ch_samplesheet.map { meta, fastqs -> meta}
    ch_pedfile   = ch_samples.toList().map { file(CustomFunctions.makePed(it, params.outdir)) }
    ch_case_info = ch_samples.toList().map { CustomFunctions.createCaseChannel(it) }

    // Initialize file channels for PREPARE_REFERENCES subworkflow
    ch_genome_fasta             = Channel.fromPath(params.fasta).map { it -> [[id:it[0].simpleName], it] }.collect()
    ch_genome_fai               = params.fai            ? Channel.fromPath(params.fai).map {it -> [[id:it[0].simpleName], it]}.collect()
                                                        : Channel.empty()
    ch_gnomad_af_tab            = params.gnomad_af      ? Channel.fromPath(params.gnomad_af).map{ it -> [[id:it[0].simpleName], it] }.collect()
                                                        : Channel.value([[],[]])
    ch_dbsnp                    = params.known_dbsnp    ? Channel.fromPath(params.known_dbsnp).map{ it -> [[id:it[0].simpleName], it] }.collect()
                                                        : Channel.value([[],[]])
    ch_mt_fasta                 = params.mt_fasta       ? Channel.fromPath(params.mt_fasta).map { it -> [[id:it[0].simpleName], it] }.collect()
                                                        : Channel.empty()
    ch_target_bed_unprocessed   = params.target_bed     ? Channel.fromPath(params.target_bed).map{ it -> [[id:it[0].simpleName], it] }.collect()
                                                        : Channel.value([[],[]])
    ch_vep_cache_unprocessed    = params.vep_cache      ? Channel.fromPath(params.vep_cache).map { it -> [[id:'vep_cache'], it] }.collect()
                                                        : Channel.value([[],[]])

    // Prepare references and indices.
    PREPARE_REFERENCES (
        ch_genome_fasta,
        ch_genome_fai,
    )
    .set { ch_references }

    // Gather built indices or get them from the params
    ch_cadd_header              = Channel.fromPath("$projectDir/assets/cadd_to_vcf_header_-1.0-.txt", checkIfExists: true).collect()
    ch_cadd_resources           = params.cadd_resources                     ? Channel.fromPath(params.cadd_resources).collect()
                                                                            : Channel.value([])
    ch_call_interval            = params.call_interval                      ? Channel.fromPath(params.call_interval).map {it -> [[id:it[0].simpleName], it]}.collect()
                                                                            : Channel.value([[:],[]])
    ch_foundin_header           = Channel.fromPath("$projectDir/assets/foundin.hdr", checkIfExists: true).collect()
    ch_gcnvcaller_model         = params.gcnvcaller_model                   ? Channel.fromPath(params.gcnvcaller_model).splitCsv ( header:true )
                                                                            .map { row ->
                                                                                return [[id:file(row.models).simpleName], row.models]
                                                                            }
                                                                            : Channel.empty()
    ch_genome_bwaindex          = params.bwa                                ? Channel.fromPath(params.bwa).map {it -> [[id:it[0].simpleName], it]}.collect()
                                                                            : ch_references.genome_bwa_index
    ch_genome_bwamem2index      = params.bwamem2                            ? Channel.fromPath(params.bwamem2).map {it -> [[id:it[0].simpleName], it]}.collect()
                                                                            : ch_references.genome_bwamem2_index
    ch_genome_fai               = ch_references.genome_fai
    ch_genome_dictionary        = params.sequence_dictionary                ? Channel.fromPath(params.sequence_dictionary).map {it -> [[id:it[0].simpleName], it]}.collect()
                                                                            : ch_references.genome_dict

    ch_versions                 = ch_versions.mix(ch_references.versions)

    //
    // ALIGNING READS, FETCH STATS, AND MERGE.
    //
    ALIGN (
        ch_samplesheet,
        ch_genome_fasta,
        ch_genome_fai,
        ch_genome_bwaindex,
        ch_genome_bwamem2index,
        ch_genome_dictionary,
        params.platform
    )
    .set { ch_mapped }
    ch_versions   = ch_versions.mix(ALIGN.out.versions)

    //
    // Collate and save software versions
    //
    softwareVersionsToYAML(ch_versions)
        .collectFile(storeDir: "${params.outdir}/pipeline_info", name: 'nf_core_pipeline_software_mqc_versions.yml', sort: true, newLine: true)
        .set { ch_collated_versions }

    emit:
    multiqc_report = Channel.empty() // channel: /path/to/multiqc_report.html
    versions       = ch_versions                 // channel: [ path(versions.yml) ]
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
