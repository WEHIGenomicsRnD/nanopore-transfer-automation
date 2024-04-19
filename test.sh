#!/bin/bash

rm -r .test/test_data/*/_transfer*

snakemake --directory .test \
        --use-conda \
        --conda-frontend mamba \
        --cores 2 \
        --config data_dir="$PWD/.test/test_data"
