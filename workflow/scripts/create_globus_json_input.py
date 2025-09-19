"""
Creates json input file for Globus copy, delete and mark complete
data flow.
"""
import sys
import json
import os

sys.stderr = open(snakemake.log[0], "w")

src_endpoint = snakemake.config["src_endpoint"]
dest_endpoint = snakemake.config["dest_endpoint"]

data_dir = snakemake.config["data_dir"]
transfer_dir = snakemake.config["transfer_dir"]
project = snakemake.wildcards.project
sample = snakemake.wildcards.sample
run_uid = snakemake.wildcards.run_uid
run = snakemake.wildcards.run
td_smp="_".join([transfer_dir,sample,run])
src_path = f"{data_dir}/{project}/{transfer_dir}_{sample}_{run}"

dest_path = os.path.join(snakemake.config["dest_path"], project,td_smp)
print(f"Dest path : {dest_path}")

input = {
    "source": {
        "id": src_endpoint,
        "path": src_path
    },
    "destination": {
        "id": dest_endpoint,
        "path": dest_path
    },
    "transfer_label": f"Transfer archives for {sample}-{run} from {project}",
    "delete_label": f"Delete source archives for {sample}-{run} from {project}"
}

with open(snakemake.output[0], "w") as f:
    json.dump(input, f, indent=2)
