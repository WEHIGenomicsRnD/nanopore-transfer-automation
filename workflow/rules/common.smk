import pandas as pd
import numpy as np
import os
import re

# variables
DATA_FILES = ["reports", "fastq", "fast5", "pod5"]
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
transfer = str(config["transfer"]).lower() == "true"

if "pod5" in file_types:
    raw_format = "pod5"
elif "fast5" in file_types:
    raw_format = "fast5"
else:
    raw_format = ""

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


def is_processing_complete(project_dir_full):
    """
    Checks whether run has already been archived;
    this is indicated by the presence of a file under
    the transfer directory called tar_file_counts
    (or archive.success for backwards compatibility).
    This is required because files from the _transfer
    directory may have been deleted due to transfer.
    """
    transfer_dir_full = os.path.join(project_dir_full, transfer_dir)
    if os.path.exists(transfer_dir_full):
        files_in_transfer_dir = next(os.walk(transfer_dir_full))[2]
        final_file = "transfer.txt" if transfer else "tar_file_counts.txt"

        project_name = os.path.basename(project_dir_full)
        final_file_with_projname = (
            f"{project_name}_transfer.txt"
            if transfer
            else f"{project_name}_tar_file_counts.txt"
        )

        log_dir = os.path.join(transfer_dir_full, "logs")
        files_in_log_dir = next(os.walk(log_dir))[2] if os.path.exists(log_dir) else []

        return (
            "archive.success" in files_in_transfer_dir
            or final_file in files_in_transfer_dir
            or final_file_with_projname in files_in_log_dir
        )
    else:
        return False


# build list of projects and samples to archive
project_dirs = []
if ignore_proj_regex and extra_dirs:
    project_dirs = extra_dirs
else:
    project_dirs = get_project_dirs(data_dir, proj_dir_regex)
    project_dirs = project_dirs + extra_dirs if extra_dirs else project_dirs

project_dirs = list(filter(lambda project: project not in ignore_dirs, project_dirs))
for proj_dir in project_dirs:
    print(f"Found project directory {proj_dir}.", file=sys.stderr)
projects_with_incomplete_runs = []


projects, samples = [], []
for project in project_dirs:
    project_dir_full = os.path.join(data_dir, project)
    if not os.path.exists(project_dir_full):
        print(
            f"Project directory {project} does not exist; skipping.",
            file=sys.stderr,
        )
        continue

    if is_processing_complete(project_dir_full):
        print(
            f"Processing of project {project} already complete; skipping.",
            file=sys.stdout,
        )
        continue

    samples_in_project = next(os.walk(project_dir_full))[1]
    samples_in_project = filter(
        lambda sample: sample != transfer_dir, samples_in_project
    )

    # add both projects and sample to keep their association together
    for sample in samples_in_project:
        sample_dir = os.path.join(project_dir_full, sample)
        if not check_if_complete or is_run_complete(sample_dir):
            print(
                f"Found {sample} in project {project} for processing.", file=sys.stderr
            )
            projects.append(project)
            samples.append(sample)
        elif check_if_complete and not is_run_complete(sample_dir):
            print(
                f"Skipping {sample} in project {project} (run incomplete).",
                file=sys.stderr,
            )
            projects_with_incomplete_runs.append(project)


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


def get_raw_outputs():
    raw_outputs = [
        f"{data_dir}/{project}/{{transfer_dir}}/{raw_format}/{project}_{sample}_{raw_format}_{{state}}.tar.gz"
        for project, sample in zip(projects, samples)
    ]
    raw_outputs = expand(
        raw_outputs,
        transfer_dir=transfer_dir,
        state=STATES,
    )
    return raw_outputs


def get_outputs(file_types):
    outputs = []
    if "checksums" in file_types:
        outputs.extend(get_checksum_outputs())
    if "reports" in file_types:
        outputs.extend(get_report_outputs())
    if "fastq" in file_types:
        outputs.extend(get_fastq_outputs())
    if "fast5" in file_types or "pod5" in file_types:
        outputs.extend(get_raw_outputs())
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
        f"{data_dir}/{project}/{transfer_dir}/{{file_type}}/{project}_{sample}_{{file_type}}_{{state}}_list.txt"
        for project, sample in zip(projects, samples)
    ]

    validate_tars_outputs = expand(
        validate_tars_outputs,
        file_type=[
            file_type
            for file_type in file_types
            if file_type not in ["checksums", "reports"]
        ],
        state=STATES,
    )
    return validate_tars_outputs


def get_validate_reports_outputs():
    validate_reports_outputs = [
        f"{data_dir}/{project}/{transfer_dir}/reports/{project}_{sample}_reports_list.txt"
        for project, sample in zip(projects, samples)
    ]
    return validate_reports_outputs


def get_archive_complete_outputs():
    archive_complete_outputs = [
        f"{data_dir}/{project}/{transfer_dir}/logs/{project}_tar_file_counts.txt"
        for project in np.unique(projects)
        if project not in projects_with_incomplete_runs
    ]
    return archive_complete_outputs


def get_transfer_outputs():
    if transfer:
        transfer_outputs = [
            f"{data_dir}/{project}/{transfer_dir}/logs/{project}_transfer.txt"
            for project in np.unique(projects)
            if project not in projects_with_incomplete_runs
        ]
        return transfer_outputs
    else:
        return []
