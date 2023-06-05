# NOTE: this step will only invoke the transfer but there is no guarantee that it
# will be successful. Check the Globus dashboard for the status of the transfer.
rule transfer:
    input:
        f"{data_dir}/{{project}}/{transfer_dir}/system_file_counts.txt",
    output:
        f"{data_dir}/{{project}}/{transfer_dir}/transfer.txt",
    log:
        "logs/{project}_transfer.log",
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
            {params.dest_endpoint}:{params.dest_path}/{wildcards.project} \
            --recursive \
            --sync-level checksum \
            --verify-checksum \
            --fail-on-quota-errors \
            --notify on > {output}
        """
