if delete_on_transfer:

    rule transfer:
        input:
            counts=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_file_counts.txt",
            checksums=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/checksums/{{project}}_{{sample}}_{{run_uid}}_archives.sha1",
        output:
            transfer_file=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_transfer.txt",
            complete_file=f"{data_dir}/{{project}}/{{sample}}/{{run}}/{{run_uid}}.processing.success",

        log:
            "logs/{project}_{sample}_{run}_{run_uid}_transfer.log",
        conda:
            "../envs/globus.yaml"
        threads: 1
        params:
            data_dir=data_dir,
            transfer_dir=transfer_dir,
            src_endpoint=config["src_endpoint"],
            dest_endpoint=config["dest_endpoint"],
            dest_path=config["dest_path"],
        shell:
            """
            transfer_task_id=$(globus transfer \
                {params.src_endpoint}:{params.data_dir}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
                {params.dest_endpoint}:{params.dest_path}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
                --recursive \
                --verify-checksum \
                -F json | jq '.task_id' -r)
            globus task wait "${transfer_task_id}" --heartbeat

            delete_task_id=$(globus delete \
                 {params.src_endpoint}:{params.data_dir}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
                 --recursive -F json | jq '.task_id' -r)
            globus task wait "${delete_task_id}" --heartbeat
            
            touch {output.complete_file}
            """
else:

    # NOTE: this step will only invoke the transfer but there is no guarantee that it
    # will be successful. Check the Globus dashboard for the status of the transfer.
    rule transfer:
        input:
            counts=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_file_counts.txt",
            checksums=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/checksums/{{project}}_{{sample}}_{{run_uid}}_archives.sha1",
        output:
            transfer_file=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_transfer.txt",
            complete_file=f"{data_dir}/{{project}}/{{sample}}/{{run}}/{{run_uid}}.processing.success",
            
        log:
            "logs/{project}_{sample}_{run}_{run_uid}_transfer.log",
        conda:
            "../envs/globus.yaml"
        threads: 1
        params:
            data_dir=data_dir,
            transfer_dir=transfer_dir,
            src_endpoint=config["src_endpoint"],
            dest_endpoint=config["dest_endpoint"],
            dest_path=config["dest_path"],
        shell:
            """
            transfer_task_id=$(globus transfer \
                {params.src_endpoint}:{params.data_dir}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
                {params.dest_endpoint}:{params.dest_path}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
                --recursive \
                --verify-checksum \
                -F json | jq '.task_id' -r)
            globus task wait "${transfer_task_id}" --heartbeat

            touch {output.complete_file}
            """
