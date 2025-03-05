import os
import re
import sys
from glob import iglob

# variables
DATA_FILES = ["reports", "fastq", "fast5", "pod5", "bam"]
POSSIBLE_FILE_TYPES = DATA_FILES + ["checksums"]
STATES = ["pass", "fail", "skip"]

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
delete_on_transfer = str(config["delete_on_transfer"]).lower() == "true"

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


def is_run_complete(run_dir):
    """
    Checks whether run is complete for a given sample,
    indicated by presence of sequencing_summary.txt file
    """
#    runs = next(os.walk(sample_dir))[1]
    run_complete = []
#    for run in runs:
    run_contents = os.listdir(run_dir)
    eor_files = list(filter(end_of_run_file_regex.match, run_contents))
    run_complete.append(len(eor_files) > 0)
    return any(run_complete)


def is_run_processing_complete(run_dir,project_dir_full):
    """
    Checks whether run has already been processed
    through precense of processing.success file
    """
    sample=run_dir.split("/")[-2]
    run=run_dir.split("/")[-1]
    trans_dir="_".join([transfer_dir,sample])
    transfer_dir_full = os.path.join(project_dir_full,trans_dir,run)
    processing_complete_file = os.path.join(run_dir, "processing.success")
    if os.path.exists(transfer_dir_full):
        files_in_transfer_dir = next(os.walk(transfer_dir_full))[1]
        final_file = "transfer.txt" if transfer else "tar_file_counts.txt"

        project_name = os.path.basename(project_dir_full)
        final_file_with_projname = (
            f"{project_name}_transfer.txt"
            if transfer
            else f"{project_name}_file_counts.txt"
        )
        final_file_legacy = (
            f"{project_name}_transfer.txt"
            if transfer
            else f"{project_name}_tar_file_counts.txt"
        )

        log_dir = os.path.join(transfer_dir_full, run, "logs")
        files_in_log_dir = next(os.walk(log_dir))[2] if os.path.exists(log_dir) else []

        return (
            "archive.success" in files_in_transfer_dir
            or final_file in files_in_transfer_dir
            or final_file_with_projname in files_in_log_dir
            or final_file_legacy in files_in_log_dir
        )

    return os.path.exists(processing_complete_file)


def is_project_processing_complete(project_dir_full):
    """
    Checks whether run has already been archived;
    this is indicated by the presence of a file under
    the transfer directory called tar_file_counts
    (or archive.success for backwards compatibility).
    This is required because files from the _transfer
    directory may have been deleted due to transfer.
    """
#    transfer_dir_full = os.path.join(project_dir_full, transfer_dir,run)
    processing_complete_file = os.path.join(project_dir_full, "processing.success")
#    if os.path.exists(transfer_dir_full):
#        files_in_transfer_dir = next(os.walk(transfer_dir_full))[2]
#        final_file = "transfer.txt" if transfer else "tar_file_counts.txt"

#        project_name = os.path.basename(project_dir_full)
#        final_file_with_projname = (
#            f"{project_name}_transfer.txt"
#            if transfer
#            else f"{project_name}_file_counts.txt"
#        )
#        final_file_legacy = (
#            f"{project_name}_transfer.txt"
#            if transfer
#            else f"{project_name}_tar_file_counts.txt"
#        )

#        log_dir = os.path.join(transfer_dir_full, run, "logs")
#        files_in_log_dir = next(os.walk(log_dir))[2] if os.path.exists(log_dir) else []

#        return (
#            "archive.success" in files_in_transfer_dir
#            or final_file in files_in_transfer_dir
#            or final_file_with_projname in files_in_log_dir
#            or final_file_legacy in files_in_log_dir
#        )

    # if transfer directory does not exist, check for _complete directory
    return os.path.exists(processing_complete_file)


# build list of projects and samples to archive
project_dirs = []
if ignore_proj_regex and extra_dirs:
    project_dirs = extra_dirs
else:
    project_dirs = get_project_dirs(data_dir, proj_dir_regex)
    project_dirs = project_dirs + extra_dirs if extra_dirs else project_dirs

project_dirs = list(filter(lambda project: project not in ignore_dirs, project_dirs))
project_dirs = list(
    filter(lambda project: project is not None and project != "", project_dirs)
)
for proj_dir in project_dirs:
    print(f"Found project directory {proj_dir}.", file=sys.stdout)


projects, samples, runs ,runs_uid = [], [], [], []
for project in project_dirs:
    project_dir_full = os.path.join(data_dir, project)
    if not os.path.exists(project_dir_full):
        print(
            f"Project directory {project} does not exist; skipping.",
            file=sys.stdout,
        )
        continue


#    if is_project_processing_complete(project_dir_full):
#        print(
#            f"Processing of project {project} already complete; skipping.",
#            file=sys.stdout,
#        )
#        continue

    samples_in_project = next(os.walk(project_dir_full))[1]
    samples_in_project = filter(
        lambda sample: sample != transfer_dir, samples_in_project
    )

    # add both projects and sample to keep their association together
    for sample in samples_in_project:
        sample_dir = os.path.join(project_dir_full, sample)
        runs_in_samples = next(os.walk(sample_dir))[1]

        if is_project_processing_complete(sample_dir):
           print(
               f"Processing of project {project} already complete; skipping.",
               file=sys.stdout,
           )
           continue

        for run in runs_in_samples:
           run_dir=os.path.join(sample_dir, run )
           run_sample=os.path.join(sample,run)


           if is_run_processing_complete(run_dir,project_dir_full):
               print(
                   f"Processing of {run} for {sample} in project {project} already complete; skipping",
                   file=sys.stdout,
               )
           elif not check_if_complete or is_run_complete(run_dir):
               print(
                   f"Found {run_sample} in project {project} for processing.",
                   file=sys.stdout,
               )
               runs_uid.append(run.split("_")[-1])
               projects.append(project)
               samples.append(sample)
               runs.append(run)
           elif check_if_complete and not is_run_complete(run_dir):
               print(
                   f"Skipping {run_sample} in project {project} (run incomplete).",
                   file=sys.stdout,
               )
          
        print(f"rin sample - {runs_uid}")
        print(f" sample - {samples}")
        print(f" project - {projects}")

# input/output functions
def get_checksum_outputs():
    checksum_outputs = [
        f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/checksums/{project}_{sample}_{run_uid}_checksums.sha1"
        for project, sample, run, run_uid in zip(projects, samples,runs , runs_uid)
    ]
    return checksum_outputs


def get_report_outputs():
    report_outputs = []
    for project, sample, run, run_uid in zip(projects, samples,runs, runs_uid):
        report_outputs.append(
            f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/reports/{project}_{sample}_{run_uid}_reports.tar.gz"
        )
        report_outputs.append(
            f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/reports/{project}_{sample}_{run_uid}_reports_list.txt"
        )
    return report_outputs


def get_output_by_type(filetype):
    file_extension = "tar" if filetype in ["fastq", "bam"] else "tar.gz"

    outputs = []
    for project, sample, run, run_uid in zip(projects, samples, runs, runs_uid):
        files_under_sample = [
            os.path.basename(f) for f in iglob(f"{data_dir}/{project}/{sample}/{run}/*")
        ]
        out_prefix = f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/{filetype}/{project}_{sample}_{run_uid}_{filetype}"
        if filetype == "pod5":
            if f"{filetype}" in files_under_sample:
                outputs.append(f"{out_prefix}.{file_extension}")
                outputs.append(f"{out_prefix}_list.txt")
        for state in STATES:
            if f"{filetype}_{state}" in files_under_sample:
                outputs.append(f"{out_prefix}_{state}.{file_extension}")
                outputs.append(f"{out_prefix}_{state}_list.txt")

    return outputs


def get_outputs(file_types):
    outputs = []
    if "checksums" in file_types:
        outputs.extend(get_checksum_outputs())
    if "reports" in file_types:
        outputs.extend(get_report_outputs())
    if "fastq" in file_types:
        outputs.extend(get_output_by_type("fastq"))
    if "fast5" in file_types:
        outputs.extend(get_output_by_type("fast5"))
    if "pod5" in file_types:
        outputs.extend(get_output_by_type("pod5"))
    if "bam" in file_types:
        outputs.extend(get_output_by_type("bam"))
    return outputs


def get_final_checksum_outputs():
    final_checksum_outputs = [
        f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/checksums/{project}_{sample}_{run_uid}_archives.sha1"
        for project, sample, run, run_uid in zip(projects, samples, runs, runs_uid)
    ]
    return final_checksum_outputs


def get_validate_reports_outputs():
    validate_reports_outputs = [
        f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/reports/{project}_{sample}_{run_uid}_reports_list.txt"
        for project, sample, run, run_uid in zip(projects, samples, runs, runs_uid)
    ]
    return validate_reports_outputs


def get_archive_complete_outputs():
    archive_complete_outputs = [
        f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/logs/{project}_{sample}_{run_uid}_file_counts.txt"
        for project, sample, run, run_uid in zip(projects, samples, runs, runs_uid)
    ]
    return archive_complete_outputs


def get_transfer_outputs():
    if transfer:
        transfer_outputs = [
            f"{data_dir}/{project}/{transfer_dir}_{sample}/{run}/logs/{project}_{sample}_{run_uid}_transfer.txt"
            for project, sample, run, run_uid in zip(projects, samples, runs, runs_uid)
        ]
        return transfer_outputs
    else:
        return []
