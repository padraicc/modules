process SAWFISH_JOINTCALL {
    tag "$meta.id"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/f8/f8f28fcf0823d39a8038b02cc5ff86d7cfbcf92e6f5c8540c25e110c6eb1f9be/data' :
        'community.wave.seqera.io/library/sawfish:2.1.1--1fd57d06a7186245' }"

    input:
    tuple val(meta), path(sample_dirs)
    tuple val(meta2), path(fasta)
    tuple val(meta3), path(bams), path(bais)
    tuple val(meta4), path(sample_csv)

    output:
    tuple val(meta), path("joint_call_dir/*_genotyped.sv.vcf.gz")     , emit: vcf
    tuple val(meta), path("joint_call_dir/*_genotyped.sv.vcf.gz.tbi") , emit: tbi
    tuple val(meta), path("joint_call_dir/contig.alignment.bam")      , emit: bam
    tuple val(meta), path("joint_call_dir/contig.alignment.bam.csi")  , emit: bam_index
    tuple val(meta), path("joint_call_dir/run.stats.json")            , emit: stats
    tuple val(meta), path("joint_call_dir/samples/")                  , emit: samples_dir
    tuple val(meta), path("joint_call_dir/sawfish.log")               , emit: log
    path "versions.yml"                                               , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def sample_args = sample_csv ? '' : sample_dirs.collect { "--sample ${it}" }.join(' ')
    def sample_csv_arg = sample_csv ? "--sample-csv ${sample_csv}" : ""

    """
    sawfish \\
        joint-call \\
        --threads $task.cpus \\
        --ref $fasta \\
        $sample_args \\
        $args \\
        $sample_csv_arg \\
        --output-dir joint_call_dir

    # Rename the output files to include prefix
    if [ -f joint_call_dir/genotyped.sv.vcf.gz ]; then
        mv joint_call_dir/genotyped.sv.vcf.gz joint_call_dir/${prefix}_genotyped.sv.vcf.gz
    fi
    if [ -f joint_call_dir/genotyped.sv.vcf.gz.tbi ]; then
        mv joint_call_dir/genotyped.sv.vcf.gz.tbi joint_call_dir/${prefix}_genotyped.sv.vcf.gz.tbi
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        sawfish: \$(sawfish --version | sed 's/sawfish //g')
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    mkdir -p joint_call_dir
    mkdir -p joint_call_dir/samples/sample0001_test/
    mkdir -p joint_call_dir/samples/sample0002_test/
    echo "##fileformat=VCFv4.4" | gzip > joint_call_dir/${prefix}_genotyped.sv.vcf.gz
    echo "VCF index content" > joint_call_dir/${prefix}_genotyped.sv.vcf.gz.tbi
    echo "BAM content" > joint_call_dir/contig.alignment.bam
    echo "BAM index content" > joint_call_dir/contig.alignment.bam.csi
    echo '{"run_stats": "mock"}' > joint_call_dir/run.stats.json
    echo "Log content" > joint_call_dir/sawfish.log
    echo "Copy number bedgraph content" > joint_call_dir/samples/sample0001_test/copynum.bedgraph
    echo "Depth content" > joint_call_dir/samples/sample0001_test/depth.bw
    echo "GC bias corrected depth content" > joint_call_dir/samples/sample0001_test/gc_bias_corrected_depth.bw
    echo "CN summary content" > joint_call_dir/samples/sample0001_test/copynum.summary.json
    echo "Copy number bedgraph content" > joint_call_dir/samples/sample0002_test/copynum.bedgraph
    echo "Depth content" > joint_call_dir/samples/sample0002_test/depth.bw
    echo "GC bias corrected depth content" > joint_call_dir/samples/sample0002_test/gc_bias_corrected_depth.bw
    echo "CN summary content" > joint_call_dir/samples/sample0002_test/copynum.summary.json

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        sawfish: \$(sawfish --version | sed 's/sawfish //g')
    END_VERSIONS
    """
}
