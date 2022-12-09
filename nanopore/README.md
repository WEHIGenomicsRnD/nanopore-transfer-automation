# Nanopore sequencer automations

Automatically performs archiving of completed run on Nanopore machines.

Makes three archives: reports (containing metadata and all reports), fastq (tar file) and fast5 (tar.gz file).

## Installation

Only python 3 is required. [Pytest](https://pypi.org/project/pytest/) is required for testing.

## Configure

`DATADIR` must be configured in `auto_archive.py`. 

## How to run

Run via:

```bash
python auto_archive.py
```

## Testing

Make sure `DATADIR` is set to the execution directory, then run:

```bash
pytest test_auto_archive.py
```
