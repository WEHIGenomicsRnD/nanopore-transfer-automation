rule calculate_checksums:
    input:
        [f"{data_dir}/{project}/{sample}/{run}" for project, sample, run, run_uid in zip(projects, samples, runs ,runs_uid)],
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run_uid}}/checksums/{{project}}_{{sample}}_{{run_uid}}_checksums.sha1",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_{run_uid}_checksums.log",
    conda:
        "../envs/archive.yaml"
    threads: 1
    params:
        data_dir=data_dir,
    shell:
        """
        cd {params.data_dir}/{wildcards.project} &&
            find {wildcards.sample}/{wildcards.run}/* -type f | xargs shasum -a 1 > {output}
        """


rule calculate_archive_checksums:
    input:
        get_outputs(file_types),
    output:
        expand(
            "{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run_uid}}/checksums/{{project}}_{{sample}}_{{run_uid}}_archives.sha1",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_{run_uid}_archive_checksums.log",
    threads: 1
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir,
    shell:
        """
        cd {params.data_dir}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run_uid} &&
            find . -type f -iname "*tar*" | xargs shasum -a 1 > {output}
        """


if "pod5" in file_types:
    for project, sample, run,run_uid in zip(projects, samples, runs, runs_uid):

        rule:
            name:
                f"tar_{project}_{sample}_{run_uid}_pod5"
            input:
                f"{data_dir}/{project}/{sample}/{run}",
            output:
                tar=f"{data_dir}/{project}/{transfer_dir}_{sample}_{run_uid}/pod5/{project}_{sample}_{run_uid}_pod5.tar.gz",
                txt=f"{data_dir}/{project}/{transfer_dir}_{sample}_{run_uid}/pod5/{project}_{sample}_{run_uid}_pod5_list.txt",
            log:
                f"logs/{project}_{sample}_{run_uid}_pod5_tar.log",
            conda:
                "../envs/archive.yaml"
            threads: config["threads"]
            params:
                data_dir=data_dir,
                project=project,
                sample=sample,
                run=run,
            shell:
                """
                cd {params.data_dir}/{params.project} &&
                    find {params.sample}/{params.run}/pod5 -iname "*.pod5" |
                    tar -cvf - --files-from - |
                    pigz -p {threads} > {output.tar} ;
                tar -tvf <(pigz -dc {output.tar}) >> {output.txt}
                """


for project, sample, run, run_uid in zip(projects, samples, runs, runs_uid):
    for file_type in file_types:
        for state in STATES:
            ext = "tar" if file_type in ["fastq", "bam"] else "tar.gz"
            threads = 1 if file_type in ["fastq", "bam"] else config["threads"]

            rule:
                name:
                    f"tar_{project}_{sample}_{run_uid}_{file_type}_{state}"
                input:
                    f"{data_dir}/{project}/{sample}/{run}",
                output:
                    tar=f"{data_dir}/{project}/{transfer_dir}_{sample}_{run_uid}/{file_type}/{project}_{sample}_{run_uid}_{file_type}_{state}.{ext}",
                    txt=f"{data_dir}/{project}/{transfer_dir}_{sample}_{run_uid}/{file_type}/{project}_{sample}_{run_uid}_{file_type}_{state}_list.txt",
                log:
                    f"logs/{project}_{sample}_{run_uid}_{file_type}_{state}_tar.log",
                conda:
                    "../envs/archive.yaml"
                threads: threads
                params:
                    data_dir=data_dir,
                    project=project,
                    sample=sample,
                    file_type=file_type,
                    state=state,
                    run=run,
                shell:
                    """
                    if [[ "{params.file_type}" == "fastq" || "{params.file_type}" == "bam" ]]; then
                        cd {params.data_dir}/{params.project} &&
                            find {params.sample}/{params.run}/{params.file_type}_{params.state} -iname "*.{params.file_type}*" |
                            tar -cvf {output.tar} --files-from - ;
                        tar -tvf {output.tar} >> {output.txt}
                    else
                        cd {params.data_dir}/{params.project} &&
                            find {params.sample}/{params.run}/{params.file_type}_{params.state} -iname "*.{params.file_type}" |
                            tar -cvf - --files-from - |
                            pigz -p {threads} > {output.tar} ;
                        tar -tvf <(pigz -dc {output.tar}) >> {output.txt}
                    fi
                    """


rule tar_reports:
    input:
        [f"{data_dir}/{project}/{sample}/{run}" for project, sample, run, ruin_uid  in zip(projects, samples, runs, runs_uid)],
    output:
        tar=expand(
            "{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run_uid}}/reports/{{project}}_{{sample}}_{{run_uid}}_reports.tar.gz",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
        txt=expand(
            "{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run_uid}}/reports/{{project}}_{{sample}}_{{run_uid}}_reports_list.txt",
            data_dir=data_dir,
            transfer_dir=transfer_dir,
        ),
    log:
        "logs/{project}_{sample}_{run_uid}_reports.log",
    conda:
        "../envs/archive.yaml"
    threads: config["threads"]
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir,
        run=run,
        project= project,
        run_uid=run_uid,
        sample=sample,
    shell:
        """
        cd {params.data_dir}/{params.project} && tar -cvf - {params.sample}/{params.run}/*.* {params.sample}/{params.run}/other_reports |
            pigz -p {threads} > {output.tar} ;
        tar -tvf <(pigz -dc {output.tar}) >> {output.txt} ;
        reports_transfer_dir={params.data_dir}/{params.project}/{params.transfer_dir}_{params.sample}_{params.run_uid}/reports ;
        for report_file in {params.sample}/{params.run}/report_*.* ; do
            report_basename=`basename $report_file`;
            cp ${{report_file}} ${{reports_transfer_dir}}/{params.project}_{params.sample}_{params.run_uid}_${{report_basename}} ;
        done
        """


rule archive_complete:
    input:
        get_outputs(file_types),
    output:
        f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run_uid}}/logs/{{project}}_{{sample}}_{{run_uid}}_file_counts.txt",
    log:
        "logs/{project}_{sample}_{run_uid}_archive_complete.txt",
    threads: 1
    params:
        data_dir=data_dir,
        transfer_dir=transfer_dir,
        run=run,
        sample=sample,
        run_uid=run_uid,
        project=project,
    shell:
        """
        transfer_path={params.data_dir}/{params.project}/{params.transfer_dir}_{params.sample}_{params.run_uid}
        count_file_regex=`echo -e ".*/{params.project}_{params.sample}_{params.run_uid}_[pod5|bam|fast|reports].*_list.txt"`
        count_files=`find $transfer_path -type f -regex $count_file_regex`
        tar_count=`cat $count_files | grep -v "/$" | wc -l`
        sys_file_count=`find {params.data_dir}/{params.project}/{params.sample}/{params.run} -type f | wc -l`
        echo "{sample} tar file counts: $tar_count" >> {output}
        echo "{sample} sys file counts: $sys_file_count" >> {output}
        """
