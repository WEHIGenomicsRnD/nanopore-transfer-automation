rule calculate_checksums:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)],
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/checksums/{{project}}_{{sample}}_checksums.sha1",
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

rule calculate_archive_checksums:
    input:
        get_outputs(file_types),
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}/checksums/{{project}}_archives.sha1",
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

for project, sample in zip(projects, samples):
    for file_type in file_types:
        for state in STATES:
            ext = "tar" if file_type == "fastq" else "tar.gz"
            threads = 1 if file_type == "fastq" else config["threads"]
            rule:
                name:
                    f"tar_{project}_{sample}_{file_type}_{state}"
                input:
                    f"{data_dir}/{project}/{sample}",
                output:
                    tar=f"{data_dir}/{project}/{transfer_dir}/{file_type}/{project}_{sample}_{file_type}_{state}.{ext}",
                    txt=f"{data_dir}/{project}/{transfer_dir}/{file_type}/{project}_{sample}_{file_type}_{state}_list.txt",
                log:
                    f"logs/{project}_{sample}_{file_type}_{state}_tar.log",
                conda:
                    "../envs/archive.yaml"
                threads: threads
                params:
                    data_dir=data_dir,
                    project=project,
                    sample=sample,
                    file_type=file_type,
                    state=state,
                shell:
                    """
                    if [[ "{params.file_type}" == "fastq" ]]; then
                        cd {params.data_dir}/{params.project} &&
                            find {params.sample}/*/{params.file_type}_{params.state} -iname "*fastq.gz" |
                            tar -cvf {output.tar} --files-from - ;
                        tar -tvf {output.tar} >> {output.txt}
                    else
                        cd {params.data_dir}/{params.project} &&
                            find {params.sample}/*/{params.file_type}_{params.state} -iname "*{params.file_type}" |
                            tar -cvf - --files-from - |
                            pigz -p {threads} > {output.tar} ;
                        tar -tvf <(pigz -dc {output.tar}) >> {output.txt}
                    fi
                    """

rule tar_reports:
    input:
        [f"{data_dir}/{project}/{sample}" for project, sample in zip(projects, samples)],
    output:
        tar=expand(
            "{data_dir}/{{project}}/{transfer_dir}/reports/{{project}}_{{sample}}_reports.tar.gz",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
        txt=expand(
            "{data_dir}/{{project}}/{transfer_dir}/reports/{{project}}_{{sample}}_reports_list.txt",
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
            pigz -p {threads} > {output.tar} ;
        tar -tvf <(pigz -dc {output.tar}) >> {output.txt}
        """


rule archive_complete:
    input:
        get_outputs(file_types)
    output:
        f"{data_dir}/{{project}}/{transfer_dir}/logs/{{project}}_file_counts.txt",
    log:
        "logs/{project}_archive_complete.txt",
    threads: 1
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir
    shell:
        """
        transfer_path={params.data_dir}/{wildcards.project}/{params.transfer_dir}
        samples=`ls {params.data_dir}/{wildcards.project}/ | grep -v _transfer`
        for sample in $samples; do
            tar_count=`cat $transfer_path/*/{wildcards.project}_${{sample}}*_list.txt | grep -v "/$" | wc -l`
            sys_file_count=`find {params.data_dir}/{wildcards.project}/$sample -type f | wc -l`
            echo "$sample tar file counts: $tar_count" >> {output}
            echo "$sample sys file counts: $sys_file_count" >> {output}
        done
        """
