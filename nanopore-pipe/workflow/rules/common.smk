import pandas as pd
import numpy as np
import os
import re

# variables
DATA_FILES = ["reports", "fastq", "fast5"]
POSSIBLE_FILE_TYPES = DATA_FILES + ["checksums"]
STATES = ["pass", "fail"]

data_dir = config["data_dir"]
transfer_dir = config["transfer_dir"]
extra_dirs = config["extra_dirs"]
ignore_dirs = config["ignore_dirs"]
file_types = config["file_types"]
proj_dir_regex = re.compile(r"%s" % config["proj_dir_regex"])
end_of_run_file_regex = re.compile(r"%s" % config["end_of_run_file_regex"])
ignore_proj_regex = str(config["ignore_proj_regex"]).lower() == "true"
check_if_complete = str(config["check_if_complete"]).lower() == "true"

# error check input
if not os.path.exists(data_dir):
    print("Data directory does not exist, exiting.", file=sys.stderr)
    sys.exit()

if not os.path.isabs(data_dir):
    print("Data directory path is not absolute, exiting.", file=sys.stderr)
    sys.exit()

if not isinstance(extra_dirs, list) and extra_dirs:
    print("extra_dirs argument is not a list or empty, exiting.", file=sys.stderr)
    sys.exit()

if not isinstance(ignore_dirs, list) and ignore_dirs:
    print("ignore_dirs argument is not a list or empty, exiting.", file=sys.stderr)
    sys.exit()

if not isinstance(file_types, list) and file_types:
    print("file_types argument is not a list or empty, exiting.", file=sys.stderr)
    sys.exit()

for file_type in file_types:
    if file_type not in POSSIBLE_FILE_TYPES:
        print(f"Invalid file type {file_type} specified.", file=sys.stderr)
        sys.exit()

if ignore_proj_regex and not extra_dirs:
    print(
        "Invalid parameters: extra_dirs must be specified if ignoring project regex.",
        file=sys.stderr,
    )
    sys.exit()

# functions
def get_project_dirs(data_dir, proj_dir_regex):
    """
    Iterate through directories in data_dir,
    check whether each directory matches the
    naming schema, if so, add to a list and
    return.
    """
    project_dirs = []
    for proj_dir in os.listdir(data_dir):
        is_project_dir = re.match(proj_dir_regex, proj_dir)
        if is_project_dir:
            print(f"Found project directory {proj_dir}.", file=sys.stderr)
            project_dirs.append(proj_dir)
    return project_dirs


def is_run_complete(sample_dir):
    """
    Checks whether run is complete for a given sample,
    indicated by presence of sequencing_summary.txt file
    in at least one run directory
    """
    runs = next(os.walk(sample_dir))[1]
    run_complete = []
    for run in runs:
        run_contents = os.listdir(os.path.join(sample_dir, run))
        eor_files = list(filter(end_of_run_file_regex.match, run_contents))
        run_complete.append(len(eor_files) > 0)
    return any(run_complete)


# build list of projects and samples to archive
project_dirs = []
if ignore_proj_regex and extra_dirs:
    project_dirs = extra_dirs
else:
    project_dirs = get_project_dirs(data_dir, proj_dir_regex)
    project_dirs = project_dirs + extra_dirs if extra_dirs else project_dirs

project_dirs = filter(lambda project: project not in ignore_dirs, project_dirs)

projects, samples = [], []
for project in project_dirs:
    project_dir_full = os.path.join(data_dir, project)
    samples_in_project = next(os.walk(project_dir_full))[1]
    samples_in_project = filter(
        lambda sample: sample != transfer_dir, samples_in_project
    )

    # add both projects and sample to keep their association together
    for sample in samples_in_project:
        sample_dir = os.path.join(project_dir_full, sample)
        if not check_if_complete or is_run_complete(sample_dir):
            projects.append(project)
            samples.append(sample)


# input/output functions
def get_report_outputs():
    report_outputs = [
        f"{data_dir}/{project}/{{transfer_dir}}/reports/{project}_{sample}_reports.tar.gz"
        for project, sample in zip(projects, samples)
    ]
    report_outputs = expand(
        report_outputs,
        transfer_dir=transfer_dir,
    )
    return report_outputs


def get_checksum_outputs():
    checksum_outputs = [
        f"{data_dir}/{project}/{{transfer_dir}}/checksums/{project}_{sample}.sha1"
        for project, sample in zip(projects, samples)
    ]
    checksum_outputs = expand(
        checksum_outputs,
        transfer_dir=transfer_dir,
    )
    return checksum_outputs


def get_fastq_outputs():
    fastq_outputs = [
        f"{data_dir}/{project}/{{transfer_dir}}/fastq/{project}_{sample}_fastq_{{state}}.tar"
        for project, sample in zip(projects, samples)
    ]
    fastq_outputs = expand(
        fastq_outputs,
        transfer_dir=transfer_dir,
        state=STATES,
    )
    return fastq_outputs


def get_fast5_outputs():
    fast5_outputs = [
        f"{data_dir}/{project}/{{transfer_dir}}/fast5/{project}_{sample}_fast5_{{state}}.tar.gz"
        for project, sample in zip(projects, samples)
    ]
    fast5_outputs = expand(
        fast5_outputs,
        transfer_dir=transfer_dir,
        state=STATES,
    )
    return fast5_outputs


def get_outputs(file_types):
    outputs = []
    if "checksums" in file_types:
        outputs.extend(get_checksum_outputs())
    if "reports" in file_types:
        outputs.extend(get_report_outputs())
    if "fastq" in file_types:
        outputs.extend(get_fastq_outputs())
    if "fast5" in file_types:
        outputs.extend(get_fast5_outputs())
    return outputs


def get_final_checksum_outputs():
    final_checksum_outputs = expand(
        "{data_dir}/{project}/{transfer_dir}/checksums/final/{project}_archives.sha1",
        data_dir=data_dir,
        project=np.unique(projects),
        transfer_dir=transfer_dir,
    )
    return final_checksum_outputs


def get_validate_tars_outputs():
    validate_tars_outputs = [
        f"{data_dir}/{project}/{{transfer_dir}}/checksums/final/{project}_{sample}_{{file_type}}_list.txt"
        for project, sample in zip(projects, samples)
    ]

    validate_tars_outputs = expand(
        validate_tars_outputs,
        data_dir=data_dir,
        transfer_dir=transfer_dir,
        file_type=DATA_FILES,
    )
    return validate_tars_outputs
