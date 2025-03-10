if delete_on_transfer:

    # NOTE: this step uses a Globus data flow, so first we need to create the json file
    rule create_globus_json_input:
        input:
            counts=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_file_counts.txt",
            checksums=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/checksums/{{project}}_{{sample}}_{{run_uid}}_archives.sha1",
        output:
            f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_globus_input.json",
        log:
            "logs/{run}/{project}_{sample}_{run_uid}_create_globus_json.log",
        conda:
            "../envs/python.yaml"
        threads: 1
        script:
            "../scripts/create_globus_json_input.py"

    # NOTE: this step will only invoke the transfer but there is no guarantee that it
    # will be successful. Check the Globus dashboard for the status of the transfer.
    rule transfer:
        input:
            f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_globus_input.json",
        output:
            transfer_file=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_transfer.txt",
        log:
            "logs/{run}/{project}_{sample}_{run_uid}_transfer.log",
        conda:
            "../envs/globus_automate.yaml"
        threads: 1
        params:
            globus_flow_id=config["globus_flow_id"],
        shell:
            """
            globus-automate flow run \
                {params.globus_flow_id} \
                --flow-input {input} \
                --label "Transfer {wildcards.project}" > f"{data_dir}/{{project}}/{{sample}}/{{run}}/processing.success"

            touch f"{data_dir}/{{project}}/{{sample}}/{{run}}/processing.success"
            """

else:

    # NOTE: this step will only invoke the transfer but there is no guarantee that it
    # will be successful. Check the Globus dashboard for the status of the transfer.
    rule transfer:
        input:
            counts=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_file_counts.txt",
            checksums=f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/checksums/{{project}}_{{sample}}_{{run_uid}}_archives.sha1",
        output:
            f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}/{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_transfer.txt",
        log:
            "logs/{run}/{project}_{sample}_{run_uid}_transfer.log",
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
            globus transfer \
                {params.src_endpoint}:{params.data_dir}/{wildcards.project}/{params.transfer_dir} \
                {params.dest_endpoint}:{params.dest_path}/{wildcards.project}/{params.transfer_dir} \
                --recursive \
                --sync-level checksum \
                --verify-checksum \
                --fail-on-quota-errors \
                --notify on > {output}
            """
