rule calculate_checksums:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)]
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/checksums/{{sample}}.sha1",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        )
    log:
        "logs/{project}_{sample}_checksums.log",
    threads: 1
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{project} &&
            find {sample}/* -type f | xargs shasum -a 1 > {output}
        """


rule tar_reports:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)]
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/reports/{{sample}}_reports.tar.gz",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        )
    log:
        "logs/{project}_{sample}_reports.log",
    threads: config["threads"]
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{project} && tar -cvf - {sample}/*/*.* {sample}/*/other_reports |
            pigz -p {threads} > {output}
        """


rule tar_fastqs:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)]
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/fastq/{{sample}}_fastq_{{state}}.tar",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
            state=STATES,
        )
    log:
        "logs/{project}_{sample}_{state}_fastq.log",
    threads:1
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{project} &&
            find {sample}/*/fastq_{wildcards.state} -iname "*fastq.gz" |
            xargs tar -cvf - > {output}
        """

rule tar_fast5s:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)]
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/fast5/{{sample}}_fast5_{{state}}.tar.gz",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
            state=STATES,
        )
    log:
        "logs/{project}_{sample}_{state}_fast5.log",
    threads: config["threads"]
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{project} &&
            find {sample}/*/fast5_{wildcards.state} -iname "*fast5" |
            xargs tar -cvf - |
            pigz -p {threads} > {output}
        """