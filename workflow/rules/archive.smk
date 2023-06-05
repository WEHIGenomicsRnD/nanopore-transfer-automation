rule calculate_checksums:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)],
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/checksums/{{project}}_{{sample}}.sha1",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_checksums.log",
    conda:
        "../envs/archive.yaml"
    threads: 1
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{wildcards.project} &&
            find {wildcards.sample}/* -type f | xargs shasum -a 1 > {output}
        """


rule tar_reports:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)],
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/reports/{{project}}_{{sample}}_reports.tar.gz",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_reports.log",
    conda:
        "../envs/archive.yaml"
    threads: config["threads"]
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{wildcards.project} && tar -cvf - {wildcards.sample}/*/*.* {wildcards.sample}/*/other_reports |
            pigz -p {threads} > {output}
        """


rule tar_fastqs:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)],
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/fastq/{{project}}_{{sample}}_fastq_{{state}}.tar",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
            state=STATES,
        ),
    log:
        "logs/{project}_{sample}_{state}_fastq.log",
    conda:
        "../envs/archive.yaml"
    threads: 1
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{wildcards.project} &&
            find {wildcards.sample}/*/fastq_{wildcards.state} -iname "*fastq.gz" |
            tar -cvf {output} --files-from -
        """


rule tar_raw_data:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)],
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/{raw_format}/{{project}}_{{sample}}_{raw_format}_{{state}}.tar.gz",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
            raw_format=raw_format,
            state=STATES,
        ),
    log:
        "logs/{project}_{sample}_{state}_raw_data.log",
    conda:
        "../envs/archive.yaml"
    threads: config["threads"]
    params:
        data_dir=data_dir,
        raw_format=raw_format,
    shell:
        """
        cd {params.data_dir}/{wildcards.project} &&
            find {wildcards.sample}/*/{raw_format}_{wildcards.state} -iname "*{params.raw_format}" |
            tar -cvf - --files-from - |
            pigz -p {threads} > {output}
        """


rule calculate_archive_checksums:
    input:
        get_outputs(file_types),
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/checksums/final/{{project}}_archives.sha1",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_archive_checksums.log",
    threads: 1
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{wildcards.project} &&
            find . -type f -iname "*tar*" | xargs shasum -a 1 > {output}
        """


rule validate_tars:
    input:
        get_outputs(file_types),
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/{{file_type}}/{{project}}_{{sample}}_{{file_type}}_{{state}}_list.txt",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_{file_type}_{state}_validate_tars.log",
    conda:
        "../envs/archive.yaml"
    threads: 1
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir,
    shell:
        """
        tar={wildcards.project}_{wildcards.sample}_{wildcards.file_type}_{wildcards.state}.tar*
        cd {params.data_dir}/{wildcards.project}/{params.transfer_dir}/{wildcards.file_type} &&
            if [[ $tar == *.gz ]]; then
                tar -tvf <(pigz -dc $tar) >> {output}
            else
                tar -tvf $tar >> {output}
            fi
        """


rule validate_reports:
    input:
        get_report_outputs(),
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/reports/{{project}}_{{sample}}_reports_list.txt",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_reports_validate_reports.log",
    conda:
        "../envs/archive.yaml"
    threads: 1
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir,
    shell:
        """
        tar={wildcards.project}_{wildcards.sample}_reports.tar.gz
        cd {params.data_dir}/{wildcards.project}/{params.transfer_dir}/reports &&
            tar -tvf <(pigz -dc $tar) >> {output}
        """


rule archive_complete:
    input:
        get_validate_reports_outputs(),
        get_validate_tars_outputs(),
    output:
        tar_file_counts=f"{data_dir}/{{project}}/{transfer_dir}/tar_file_counts.txt",
        sys_file_counts=f"{data_dir}/{{project}}/{transfer_dir}/system_file_counts.txt",
    log:
        "logs/{project}_archive_complete.txt",
    threads: 1
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir,
        raw_format=raw_format,
    shell:
        """
        cd {params.data_dir}/{wildcards.project}/{params.transfer_dir} &&
            wc -l */*fast*list.txt > {output.tar_file_counts}

        samples=`ls {params.data_dir}/{wildcards.project}/ | grep -v _transfer`
        for type in {params.raw_format} fastq; do
            for sample in $samples; do
                if [[ -d {params.data_dir}/{wildcards.project}/$sample ]]; then
                    for state in fail pass; do
                        count=`find {params.data_dir}/{wildcards.project}/$sample/*/${{type}}_$state -type f | wc -l`
                        echo "$count $sample $type $state" >> {output.sys_file_counts}
                    done
                fi
            done
        done
        """
