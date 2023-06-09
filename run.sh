#!/bin/bash

cores=12

conda activate snakemake

snakemake \
        --use-conda \
        --conda-frontend mamba \
        --cores $cores > logs/`date '+%Y%m%d-%H%M%S'`_run.log 2>&1