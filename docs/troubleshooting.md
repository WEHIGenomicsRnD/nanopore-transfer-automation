# Troubleshooting

Once the tool has been successfully setup and deployed, it is good practice to try running 1-2 small runs to test the functionality before moving to production usage.

## Detecting runs

Using the `--dry-run` flag with the snakemake command is useful to check which runs are being picked up and how they will be processed. If no runs are being picked up, check your `data_dir` and `proj_dir_regex` in your config file.

## Globus issues

Troubleshooting Globus transfer issues can be the most challenging. Checking your transfer in [app.globus.org](https://app.globus.org) may be useful, but for flows especially the transfer may fail to notify the user if it hasn't started the transfer, therefore the automated system does need to be checked regularly for successful transfers.

In cases of periodic transfer issues, you may find it useful to try running the `globus-automate flow` command manually outside of the pipeline to retry transfer issues, for example:

```
#!/bin/bash

run=$1
sample=$2
json_file=$3

globus-automate flow run 6336492e-e308-4a67-b78e-13684c747472 \
	    --flow-input /data/${run}/_transfer_${sample}/logs/${json_file} --label "Transfer $run"
```