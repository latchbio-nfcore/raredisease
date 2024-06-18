from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: typing.Optional[LatchFile], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], bwa: typing.Optional[LatchDir], bwamem2: typing.Optional[LatchDir], bwameme: typing.Optional[LatchDir], cadd_resources: typing.Optional[LatchDir], fai: typing.Optional[LatchFile], fasta: LatchFile, gcnvcaller_model: typing.Optional[LatchFile], gnomad_af: typing.Optional[str], gnomad_af_idx: typing.Optional[str], igenomes_ignore: typing.Optional[bool], intervals_wgs: str, intervals_y: str, known_dbsnp: typing.Optional[str], known_dbsnp_tbi: typing.Optional[str], local_genomes: typing.Optional[LatchDir], mobile_element_references: typing.Optional[LatchFile], mobile_element_svdb_annotations: typing.Optional[str], ml_model: typing.Optional[str], mt_fasta: typing.Optional[LatchFile], ploidy_model: typing.Optional[LatchDir], readcount_intervals: typing.Optional[LatchFile], reduced_penetrance: typing.Optional[str], rtg_truthvcfs: typing.Optional[LatchFile], save_reference: typing.Optional[bool], score_config_mt: typing.Optional[str], score_config_snv: typing.Optional[str], score_config_sv: typing.Optional[str], sdf: typing.Optional[LatchDir], sequence_dictionary: typing.Optional[str], svdb_query_bedpedbs: typing.Optional[LatchFile], svdb_query_dbs: typing.Optional[LatchFile], target_bed: typing.Optional[str], variant_catalog: typing.Optional[LatchFile], sample_id_map: typing.Optional[LatchFile], vcf2cytosure_blacklist: typing.Optional[LatchFile], vcfanno_resources: typing.Optional[str], vcfanno_toml: typing.Optional[str], vcfanno_lua: typing.Optional[str], vep_cache: typing.Optional[str], vep_plugin_files: typing.Optional[LatchFile], vep_filters: typing.Optional[str], vep_filters_scout_fmt: typing.Optional[str], bwa_as_fallback: typing.Optional[bool], run_mt_for_wes: typing.Optional[bool], run_rtgvcfeval: typing.Optional[bool], save_mapped_as_cram: typing.Optional[bool], skip_fastqc: typing.Optional[bool], skip_fastp: typing.Optional[bool], skip_haplocheck: typing.Optional[bool], skip_gens: typing.Optional[bool], skip_germlinecnvcaller: typing.Optional[bool], skip_eklipse: typing.Optional[bool], skip_peddy: typing.Optional[bool], skip_qualimap: typing.Optional[bool], skip_me_calling: typing.Optional[bool], skip_me_annotation: typing.Optional[bool], skip_mt_annotation: typing.Optional[bool], skip_mt_subsample: typing.Optional[bool], skip_snv_annotation: typing.Optional[bool], skip_sv_annotation: typing.Optional[bool], skip_vep_filter: typing.Optional[bool], rmdup: typing.Optional[bool], call_interval: typing.Optional[str], variant_consequences_snv: typing.Optional[str], variant_consequences_sv: typing.Optional[str], bait_padding: typing.Optional[int], genome: typing.Optional[str], mito_name: typing.Optional[str], analysis_type: typing.Optional[str], platform: typing.Optional[str], ngsbits_samplegender_method: typing.Optional[str], skip_vcf2cytosure: typing.Optional[bool], aligner: typing.Optional[str], min_trimmed_length: typing.Optional[int], mt_subsample_rd: typing.Optional[int], mt_subsample_seed: typing.Optional[int], cnvnator_binsize: typing.Optional[int], sentieon_dnascope_pcr_indel_model: typing.Optional[str], variant_caller: typing.Optional[str], variant_type: typing.Optional[str], vep_cache_version: typing.Optional[int]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
                *get_flag('input', input),
                *get_flag('outdir', outdir),
                *get_flag('bait_padding', bait_padding),
                *get_flag('bwa', bwa),
                *get_flag('bwamem2', bwamem2),
                *get_flag('bwameme', bwameme),
                *get_flag('cadd_resources', cadd_resources),
                *get_flag('fai', fai),
                *get_flag('fasta', fasta),
                *get_flag('gcnvcaller_model', gcnvcaller_model),
                *get_flag('genome', genome),
                *get_flag('gnomad_af', gnomad_af),
                *get_flag('gnomad_af_idx', gnomad_af_idx),
                *get_flag('igenomes_ignore', igenomes_ignore),
                *get_flag('intervals_wgs', intervals_wgs),
                *get_flag('intervals_y', intervals_y),
                *get_flag('known_dbsnp', known_dbsnp),
                *get_flag('known_dbsnp_tbi', known_dbsnp_tbi),
                *get_flag('local_genomes', local_genomes),
                *get_flag('mito_name', mito_name),
                *get_flag('mobile_element_references', mobile_element_references),
                *get_flag('mobile_element_svdb_annotations', mobile_element_svdb_annotations),
                *get_flag('ml_model', ml_model),
                *get_flag('mt_fasta', mt_fasta),
                *get_flag('ploidy_model', ploidy_model),
                *get_flag('readcount_intervals', readcount_intervals),
                *get_flag('reduced_penetrance', reduced_penetrance),
                *get_flag('rtg_truthvcfs', rtg_truthvcfs),
                *get_flag('save_reference', save_reference),
                *get_flag('score_config_mt', score_config_mt),
                *get_flag('score_config_snv', score_config_snv),
                *get_flag('score_config_sv', score_config_sv),
                *get_flag('sdf', sdf),
                *get_flag('sequence_dictionary', sequence_dictionary),
                *get_flag('svdb_query_bedpedbs', svdb_query_bedpedbs),
                *get_flag('svdb_query_dbs', svdb_query_dbs),
                *get_flag('target_bed', target_bed),
                *get_flag('variant_catalog', variant_catalog),
                *get_flag('sample_id_map', sample_id_map),
                *get_flag('vcf2cytosure_blacklist', vcf2cytosure_blacklist),
                *get_flag('vcfanno_resources', vcfanno_resources),
                *get_flag('vcfanno_toml', vcfanno_toml),
                *get_flag('vcfanno_lua', vcfanno_lua),
                *get_flag('vep_cache', vep_cache),
                *get_flag('vep_plugin_files', vep_plugin_files),
                *get_flag('vep_filters', vep_filters),
                *get_flag('vep_filters_scout_fmt', vep_filters_scout_fmt),
                *get_flag('analysis_type', analysis_type),
                *get_flag('bwa_as_fallback', bwa_as_fallback),
                *get_flag('platform', platform),
                *get_flag('ngsbits_samplegender_method', ngsbits_samplegender_method),
                *get_flag('run_mt_for_wes', run_mt_for_wes),
                *get_flag('run_rtgvcfeval', run_rtgvcfeval),
                *get_flag('save_mapped_as_cram', save_mapped_as_cram),
                *get_flag('skip_fastqc', skip_fastqc),
                *get_flag('skip_fastp', skip_fastp),
                *get_flag('skip_haplocheck', skip_haplocheck),
                *get_flag('skip_gens', skip_gens),
                *get_flag('skip_germlinecnvcaller', skip_germlinecnvcaller),
                *get_flag('skip_eklipse', skip_eklipse),
                *get_flag('skip_peddy', skip_peddy),
                *get_flag('skip_qualimap', skip_qualimap),
                *get_flag('skip_me_calling', skip_me_calling),
                *get_flag('skip_me_annotation', skip_me_annotation),
                *get_flag('skip_mt_annotation', skip_mt_annotation),
                *get_flag('skip_mt_subsample', skip_mt_subsample),
                *get_flag('skip_snv_annotation', skip_snv_annotation),
                *get_flag('skip_sv_annotation', skip_sv_annotation),
                *get_flag('skip_vcf2cytosure', skip_vcf2cytosure),
                *get_flag('skip_vep_filter', skip_vep_filter),
                *get_flag('aligner', aligner),
                *get_flag('min_trimmed_length', min_trimmed_length),
                *get_flag('mt_subsample_rd', mt_subsample_rd),
                *get_flag('mt_subsample_seed', mt_subsample_seed),
                *get_flag('rmdup', rmdup),
                *get_flag('call_interval', call_interval),
                *get_flag('cnvnator_binsize', cnvnator_binsize),
                *get_flag('sentieon_dnascope_pcr_indel_model', sentieon_dnascope_pcr_indel_model),
                *get_flag('variant_caller', variant_caller),
                *get_flag('variant_type', variant_type),
                *get_flag('variant_consequences_snv', variant_consequences_snv),
                *get_flag('variant_consequences_sv', variant_consequences_sv),
                *get_flag('vep_cache_version', vep_cache_version)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_raredisease", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)



@workflow(metadata._nextflow_metadata)
def nf_nf_core_raredisease(input: typing.Optional[LatchFile], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], bwa: typing.Optional[LatchDir], bwamem2: typing.Optional[LatchDir], bwameme: typing.Optional[LatchDir], cadd_resources: typing.Optional[LatchDir], fai: typing.Optional[LatchFile], fasta: LatchFile, gcnvcaller_model: typing.Optional[LatchFile], gnomad_af: typing.Optional[str], gnomad_af_idx: typing.Optional[str], igenomes_ignore: typing.Optional[bool], intervals_wgs: str, intervals_y: str, known_dbsnp: typing.Optional[str], known_dbsnp_tbi: typing.Optional[str], local_genomes: typing.Optional[LatchDir], mobile_element_references: typing.Optional[LatchFile], mobile_element_svdb_annotations: typing.Optional[str], ml_model: typing.Optional[str], mt_fasta: typing.Optional[LatchFile], ploidy_model: typing.Optional[LatchDir], readcount_intervals: typing.Optional[LatchFile], reduced_penetrance: typing.Optional[str], rtg_truthvcfs: typing.Optional[LatchFile], save_reference: typing.Optional[bool], score_config_mt: typing.Optional[str], score_config_snv: typing.Optional[str], score_config_sv: typing.Optional[str], sdf: typing.Optional[LatchDir], sequence_dictionary: typing.Optional[str], svdb_query_bedpedbs: typing.Optional[LatchFile], svdb_query_dbs: typing.Optional[LatchFile], target_bed: typing.Optional[str], variant_catalog: typing.Optional[LatchFile], sample_id_map: typing.Optional[LatchFile], vcf2cytosure_blacklist: typing.Optional[LatchFile], vcfanno_resources: typing.Optional[str], vcfanno_toml: typing.Optional[str], vcfanno_lua: typing.Optional[str], vep_cache: typing.Optional[str], vep_plugin_files: typing.Optional[LatchFile], vep_filters: typing.Optional[str], vep_filters_scout_fmt: typing.Optional[str], bwa_as_fallback: typing.Optional[bool], run_mt_for_wes: typing.Optional[bool], run_rtgvcfeval: typing.Optional[bool], save_mapped_as_cram: typing.Optional[bool], skip_fastqc: typing.Optional[bool], skip_fastp: typing.Optional[bool], skip_haplocheck: typing.Optional[bool], skip_gens: typing.Optional[bool], skip_germlinecnvcaller: typing.Optional[bool], skip_eklipse: typing.Optional[bool], skip_peddy: typing.Optional[bool], skip_qualimap: typing.Optional[bool], skip_me_calling: typing.Optional[bool], skip_me_annotation: typing.Optional[bool], skip_mt_annotation: typing.Optional[bool], skip_mt_subsample: typing.Optional[bool], skip_snv_annotation: typing.Optional[bool], skip_sv_annotation: typing.Optional[bool], skip_vep_filter: typing.Optional[bool], rmdup: typing.Optional[bool], call_interval: typing.Optional[str], variant_consequences_snv: typing.Optional[str], variant_consequences_sv: typing.Optional[str], bait_padding: typing.Optional[int] = 100, genome: typing.Optional[str] = 'GRCh38', mito_name: typing.Optional[str] = 'chrM', analysis_type: typing.Optional[str] = 'wgs', platform: typing.Optional[str] = 'illumina', ngsbits_samplegender_method: typing.Optional[str] = 'xy', skip_vcf2cytosure: typing.Optional[bool] = True, aligner: typing.Optional[str] = 'bwamem2', min_trimmed_length: typing.Optional[int] = 40, mt_subsample_rd: typing.Optional[int] = 150, mt_subsample_seed: typing.Optional[int] = 30, cnvnator_binsize: typing.Optional[int] = 1000, sentieon_dnascope_pcr_indel_model: typing.Optional[str] = 'CONSERVATIVE', variant_caller: typing.Optional[str] = 'deepvariant', variant_type: typing.Optional[str] = 'snp,indel', vep_cache_version: typing.Optional[int] = 110) -> None:
    """
    nf-core/raredisease

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, input=input, outdir=outdir, bait_padding=bait_padding, bwa=bwa, bwamem2=bwamem2, bwameme=bwameme, cadd_resources=cadd_resources, fai=fai, fasta=fasta, gcnvcaller_model=gcnvcaller_model, genome=genome, gnomad_af=gnomad_af, gnomad_af_idx=gnomad_af_idx, igenomes_ignore=igenomes_ignore, intervals_wgs=intervals_wgs, intervals_y=intervals_y, known_dbsnp=known_dbsnp, known_dbsnp_tbi=known_dbsnp_tbi, local_genomes=local_genomes, mito_name=mito_name, mobile_element_references=mobile_element_references, mobile_element_svdb_annotations=mobile_element_svdb_annotations, ml_model=ml_model, mt_fasta=mt_fasta, ploidy_model=ploidy_model, readcount_intervals=readcount_intervals, reduced_penetrance=reduced_penetrance, rtg_truthvcfs=rtg_truthvcfs, save_reference=save_reference, score_config_mt=score_config_mt, score_config_snv=score_config_snv, score_config_sv=score_config_sv, sdf=sdf, sequence_dictionary=sequence_dictionary, svdb_query_bedpedbs=svdb_query_bedpedbs, svdb_query_dbs=svdb_query_dbs, target_bed=target_bed, variant_catalog=variant_catalog, sample_id_map=sample_id_map, vcf2cytosure_blacklist=vcf2cytosure_blacklist, vcfanno_resources=vcfanno_resources, vcfanno_toml=vcfanno_toml, vcfanno_lua=vcfanno_lua, vep_cache=vep_cache, vep_plugin_files=vep_plugin_files, vep_filters=vep_filters, vep_filters_scout_fmt=vep_filters_scout_fmt, analysis_type=analysis_type, bwa_as_fallback=bwa_as_fallback, platform=platform, ngsbits_samplegender_method=ngsbits_samplegender_method, run_mt_for_wes=run_mt_for_wes, run_rtgvcfeval=run_rtgvcfeval, save_mapped_as_cram=save_mapped_as_cram, skip_fastqc=skip_fastqc, skip_fastp=skip_fastp, skip_haplocheck=skip_haplocheck, skip_gens=skip_gens, skip_germlinecnvcaller=skip_germlinecnvcaller, skip_eklipse=skip_eklipse, skip_peddy=skip_peddy, skip_qualimap=skip_qualimap, skip_me_calling=skip_me_calling, skip_me_annotation=skip_me_annotation, skip_mt_annotation=skip_mt_annotation, skip_mt_subsample=skip_mt_subsample, skip_snv_annotation=skip_snv_annotation, skip_sv_annotation=skip_sv_annotation, skip_vcf2cytosure=skip_vcf2cytosure, skip_vep_filter=skip_vep_filter, aligner=aligner, min_trimmed_length=min_trimmed_length, mt_subsample_rd=mt_subsample_rd, mt_subsample_seed=mt_subsample_seed, rmdup=rmdup, call_interval=call_interval, cnvnator_binsize=cnvnator_binsize, sentieon_dnascope_pcr_indel_model=sentieon_dnascope_pcr_indel_model, variant_caller=variant_caller, variant_type=variant_type, variant_consequences_snv=variant_consequences_snv, variant_consequences_sv=variant_consequences_sv, vep_cache_version=vep_cache_version)

