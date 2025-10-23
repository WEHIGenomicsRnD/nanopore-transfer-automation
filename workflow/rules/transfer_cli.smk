rule transfer:
    input:
        counts = f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/logs/{{project}}_{{sample}}_{{run_uid}}_file_counts.txt",
        checksums = f"{data_dir}/{{project}}/{transfer_dir}_{{sample}}_{{run}}/checksums/{{project}}_{{sample}}_{{run_uid}}_archives.sha1",
    output:
        complete_file = f"{data_dir}/{{project}}/{{sample}}/{{run}}/{{run_uid}}.processing.success",
    log:
        "logs/{project}_{sample}_{run}_{run_uid}_transfer.log",
    conda:
        "../envs/globus.yaml"
    threads:
         1
    params:
         data_dir = data_dir,
         transfer_dir = transfer_dir,
         src_endpoint = config["src_endpoint"],
         dest_endpoint = config["dest_endpoint"],
         dest_path = config["dest_path"],
         delete_on_transfer = str(config["delete_on_transfer"]).lower()
    shell:
         """
         transfer_task_id=$(globus transfer \
               {params.src_endpoint}:{params.data_dir}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
               {params.dest_endpoint}:{params.dest_path}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
               --recursive \
               --label "Transfer archives for {wildcards.sample}-{wildcards.run} from {wildcards.project}" \
               -F json | jq '.task_id' -r)
         globus task wait "$transfer_task_id" --heartbeat

            
         if [ {params.delete_on_transfer} = "true"]; then

             delete_task_id=$(globus delete \
                  {params.src_endpoint}:{params.data_dir}/{wildcards.project}/{params.transfer_dir}_{wildcards.sample}_{wildcards.run} \
                  --label "Delete source archives for {wildcards.sample}-{wildcards.run} from {wildcards.project}" \
                  --recursive \
                  -F json | jq '.task_id' -r)
             globus task wait "$delete_task_id" --heartbeat
         fi

         touch {output.complete_file}
         """
