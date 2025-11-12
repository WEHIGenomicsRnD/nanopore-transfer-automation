# Deployment

## Run script

A basic run script is included in the repository:

```bash
#!/bin/bash

cores=12

conda activate snakemake

snakemake \
        --use-conda \
        --conda-frontend mamba \
        --cores $cores > logs/`date '+%Y%m%d-%H%M%S'`_run.log 2>&1
```

If you want to use the log output redirection functionality of this script, make sure you have created the `log` directory in your deployment directory.

If you have installed conda on your instrument machine (e.g., PromethION), you may want to disable conda for the `prom` user in the `~/.bashrc` to avoid any potential interference with vendor software. In order to then run the automation, you will need to add this line (or the equivalent for your conda installation; note we have used `miniforge3` here) to allow conda to be invoked:

```bash
eval "$(/home/prom/miniforge3/bin/conda shell.bash hook)"
```

You may also want to use `--rerun-incomplete` in your snakemake command for rerunning incomplete runs where there has been some issue, and setting `--keep-going` to allow all other processing to continue if there is some premature error.

## Run automation

We use cron to schedule the task daily, for example:

```crontab
@daily /data/automations/nanopore-transfer-automation/run_on_prom.sh
```

Make sure your run script `cd`s into the deployment directory if you are calling the script like this.

## Globus

If you are planning to use the Globus transfer automation part of the workflow, your destination endpoint will need to have an active [Globus subscription](https://www.globus.org/subscriptions).

You will need to set up a [Globus personal endpoint](https://docs.globus.org/globus-connect-personal/install/linux/) on the source machine and keep it alive (we do this via a `screen` session, but there are better ways to handle this, such as [systemd](https://docs.globus.org/globus-connect-personal/install/linux/#running_globus_connect_personal_as_a_systemd_user_unit)).

Note, that you will also need to give Globus access to the `/data` directory (or wherever your runs are being stored). You will typically have to edit the file `~/.globusonline/lta/config-paths`:

```
~/,0,1
```

Please see the [Globus Documentation](https://docs.globus.org/globus-connect-personal/install/linux/#config-paths) for more details. You will need to add your data directory to this, enable sharing, read access and (AT YOUR OWN RISK) write access.

Note that you will need to authenticate into a Globus account that has permission to transfer to the destination end point. You may need to re-authenticate to Globus periodically to keep your session alive.
