# Getting started

Snakemake pipeline for automating data tarring and transfer for Oxford Nanopore
Technologies data.

## Installation

The only prerequisite is [snakemake](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html). To install snakemake, you will need to install a Conda-based Python3 distribution. For this, [Mambaforge](https://github.com/conda-forge/miniforge#mambaforge) is recommended. Once mamba is installed, snakemake can be installed like so:

```
mamba create -c conda-forge -c bioconda -n snakemake snakemake
```

Now activate the snakemake environment (you'll have to do this every time you want to run the pipeline):

```
conda activate snakemake
```

To reduce the possibility of conda interfering with the sequencer's software, it is safer to disable conda in the `~/.bashrc` and load it manually (you can put this in your run script): `source /home/prom/mambaforge/etc/profile.d/conda.sh`.

Now clone the repository:

```
git clone https://github.com/WEHIGenomicsRnD/nanopore-transfer-automation.git
cd nanopore-transfer-automation
```

## Testing

You can test the pipeline as follows:

```bash
cd .test && python make_test_data.py && cd ..
snakemake --cores 4 --directory .test --config data_dir=$PWD/.test/test_data
```

## Configuration

The configuration file is found under `config/config.yaml`. Make sure to revise these config values carefully.

| Parameter             | Description                                                                                                                     |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------|
| data_dir              | Directory containing runs (MUST be an absolute path; usually `/data`)                                                           |
| transfer_dir          | Directory prefix value (usually `_transfer`)                                                                                    |
| file_types            | File types to process in a list. Each item must match one of: 'fastq',   'fast5', 'pod5', 'bam', 'reports' and 'checksums'      |
| proj_dir_regex        | Project directory regex (set this based on your lab's naming convention)                                                        |
| end_of_run_file_regex | Regex of run file that signifies that the run has finished (do not change   unless you know what you are doing)                 |
| check_if_complete     | Check if run is complete before processing (setting this option to false is usually not recommended)                            |
| time_delay            | Minimum number of seconds a run needs to be finished before archiving                                                           |
| extra_dirs            | Directories exempt from the standard mask; use YAML lists for this arg                                                          |
| ignore_dirs           | Do not process these directories (must be a list)                                                                               |
| ignore_proj_regex     | Set true if you only want to run the script on only the extra_dirs set above                                                    |
| threads               | Number of threads to use                                                                                                        |
| transfer              | Whether to transfer data upon archiving completion using Globus                                                                 |
| delete_on_transfer    | Whether to delete data from source endpoint after transfer                                                                      |
| globus_flow_id        | Globus Flow ID to use for transfer (Move (copy and delete) files using Globus) - only used if delete_on_transfer flag is true   |
| src_endpoint          | This machine's Globus endpoint ID                                                                                               |
| dest_endpoint         | Globus destination endpoint ID                                                                                                  |
| dest_path             | Globus path on destination path (usually this will start with `/~/`                                                             |

