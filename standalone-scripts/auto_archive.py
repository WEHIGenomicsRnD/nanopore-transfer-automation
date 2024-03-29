'''
Module      : auto_archiver
Description : Creates three archives (reports, fastq and
              fast5 files) on Nanopore sequencing machines.
Copyright   : (c) Marek Cmero, 8/12/2022
License     : TBD
Maintainer  : Marek Cmero
Portability : POSIX
Scans data directory for runs matching metadata schema,
YYYYMMDD_affiliation_labname_projname, then check whether
that run has finished and then  autonmatically archives
run into reports, fastq and fast5 archives, placing them
in a transfer directory for egress.
'''
import os
import sys
import re
import subprocess
import glob
import logging
import time
from argparse import ArgumentParser
from datetime import datetime
import yaml

POSSIBLE_FILE_TYPES = ['reports', 'fastq', 'fast5']

end_of_run_file_regex = re.compile(r'^sequencing_summary\w*\.txt')

def parse_args(args):
    '''
    Parse command line arguments
    '''
    description = '''
        Run auto-archiving with config file:

        python auto_archive.py config.yaml

        Config file should contain:

        data_dir: <path_to_data_directory>
        transfer_dir: <name_of_transfer_directories>
        '''
    parser = ArgumentParser(description=description)
    parser.add_argument('config',
                        metavar='CONFIG',
                        type=str,
                        help='Config file.')
    return parser.parse_args(args)

def init_logging(log_filename):
    '''
    Initiate logging
    '''
    logging.basicConfig(filename=log_filename,
                        level=logging.DEBUG,
                        filemode='w',
                        format='%(asctime)s %(levelname)s - %(message)s',
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    logging.info('Auto-archiver started')

def run_tar(tar_file, files_to_archive, run_dir_full, file_type, threads=1):
    '''
    Archive data using tar and pigz (if using multi-threading)
    '''
    # check that all files/folders exist
    for file in files_to_archive:
        full_file_path = os.path.join(run_dir_full, file)
        if not os.path.exists(full_file_path):
            logging.error('%s does not exist!', full_file_path)
            return

    # do not use gzip compression if multithreaded (handled by pigz)
    # or archiving fastqs (already in gz format)
    tar_args = '-cpvf' if file_type == 'fastq' or threads > 1 else '-czpvf'
    if threads > 1 and file_type != 'fastq':
        with open(tar_file, 'w') as tout:
            proc0 = subprocess.Popen(['tar', tar_args, '-'] + files_to_archive,
                                     cwd=run_dir_full,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
            proc1 = subprocess.Popen(['pigz', '-p', str(threads), '-k'],
                                     stdin=proc0.stdout,
                                     stdout=tout,
                                     stderr=subprocess.PIPE)
            for line in proc0.stderr:
                logging.info(bytes.decode(line).strip())

            for line in proc1.stderr:
                logging.info(bytes.decode(line).strip())
            return_code = proc1.wait()
    else:
        proc = subprocess.Popen(['tar', tar_args, tar_file] + files_to_archive,
                                 cwd=run_dir_full,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
        for line in proc.stdout:
            logging.info(bytes.decode(line).strip())
        return_code = proc.wait()

    run_dir = os.path.split(run_dir_full)[1]
    success_file = os.path.join(run_dir_full, f'{run_dir}_{file_type}_archive.success')
    if return_code == 0:
        open(success_file, 'w').close()
        logging.info('Successfully archived %s files for %s.', file_type, run_dir)
    else:
        logging.error('An error occured archiving %s for %s.', file_type, run_dir)

def make_archive(run_dir_full, transfer_dir_full, file_type, threads):
    '''
    Given directories for project, sample and run dirs,
    make tar.gz of report files and fast5 files, and
    make tar file for fastq files
    '''
    tmp, run_dir = os.path.split(run_dir_full)
    sample_dir = os.path.split(tmp)[1]

    dest_dir = os.path.join(transfer_dir_full, file_type, sample_dir)
    os.makedirs(dest_dir, exist_ok=True)

    success_file = os.path.join(run_dir_full, f'{run_dir}_{file_type}_archive.success')
    if os.path.exists(success_file):
        # nothing to do as archiving already done
        logging.info('Skipped %s for run %s due to presence of success file.', file_type, run_dir)
        return

    ext = 'tar' if file_type == 'fastq' else 'tar.gz'
    tar_file = os.path.join(dest_dir, f'{run_dir}_{file_type}.{ext}')
    tar_file = os.path.abspath(tar_file) # need absolute path for tar to get put in right place
    files_to_archive = []

    if file_type == 'reports':
        report_files = glob.glob(os.path.join(run_dir_full, '*.*'))
        report_files = [os.path.basename(file) for file in report_files]
        files_to_archive = report_files + ['other_reports']

    elif file_type == 'fast5':
        files_to_archive = ['fast5_pass', 'fast5_fail']

    elif file_type == 'fastq':
        files_to_archive = ['fastq_pass', 'fastq_fail']

    run_tar(tar_file, files_to_archive, run_dir_full, file_type, threads=threads)

def get_files(directory):
    '''
    Get relative path of all files in directory
    Adapted from
    https://stackoverflow.com/questions/9816816/get-absolute-paths-of-all-files-in-a-directory
    '''
    for dirpath,_,filenames in os.walk(directory):
        for file in filenames:
            yield os.path.join(dirpath, file)

def calculate_checksums(run_dir_full, transfer_dir_full, checksum_filename):
    '''
    Calculate sha1sum for all files in run directory
    '''
    dest_dir = os.path.join(transfer_dir_full, 'checksums')
    os.makedirs(dest_dir, exist_ok=True)
    checksum_file = os.path.join(dest_dir, checksum_filename)

    run_dir = os.path.split(run_dir_full)[1]
    success_file = os.path.join(run_dir_full, f'{run_dir}_checksums.success')
    if os.path.exists(success_file):
        # nothing to do as checksums were already calculated
        logging.info('Skipped checksums for run %s due to presence of success file.', run_dir)
        return

    error = 0
    with open(checksum_file, 'a') as cfile:
        for file in get_files(run_dir_full):
            if os.path.splitext(file)[1] == '.success':
                continue
            proc = subprocess.Popen(['shasum', '-a', '1', file],
                                     stdout=cfile,
                                     stderr=subprocess.PIPE)
            for line in proc.stderr:
                logging.error(line)

            # check for errors
            return_code = proc.wait()
            error = return_code if return_code != 0 else error

    if error == 0:
        open(success_file, 'w').close()

def get_project_dirs(data_dir, proj_dir_regex):
    '''
    Iterate through directories in data_dir,
    check whether each directory matches the
    naming schema, if so, add to a list and
    return.
    '''
    project_dirs = []
    for proj_dir in os.listdir(data_dir):
        is_project_dir = re.match(proj_dir_regex, proj_dir)
        if is_project_dir:
            logging.info('Found project directory %s.', proj_dir)
            project_dirs.append(proj_dir)
    return project_dirs

def archive_runs_if_complete(data_dir, proj_dir, file_types, config):
    '''
    Checks whether run is complete, if so,
    create one archive each for reports,
    fast5 and fastq files.
    '''
    logging.info('Processing %s...', proj_dir)

    transfer_dir = config['transfer_dir']
    time_delay = config['time_delay']
    calc_checksums = bool(config['calculate_checksums'])
    threads = int(config['threads'])

    sample_dirs = os.listdir(os.path.join(data_dir, proj_dir))
    transfer_dir_full = os.path.join(data_dir, proj_dir, transfer_dir)

    # iterate through sample dirs
    for sample_dir in sample_dirs:
        full_sample_dir = os.path.join(data_dir, proj_dir, sample_dir)
        if not os.path.isdir(full_sample_dir):
            continue
        run_dirs = os.listdir(full_sample_dir)

        # check whether each run is finished
        for run_dir in run_dirs:
            run_dir_full = os.path.join(data_dir, proj_dir, sample_dir, run_dir)
            if not os.path.isdir(run_dir_full):
                continue
            run_contents = os.listdir(run_dir_full)

            eor_files = list(filter(end_of_run_file_regex.match, run_contents))
            if len(eor_files) > 0:
                logging.info('Run %s finished! Checking time delay...', run_dir)
                run_file = os.path.join(run_dir_full, eor_files[0])
                if time.time() - os.path.getctime(run_file) > time_delay:
                    logging.info('Making archives...')
                    if calc_checksums:
                        logging.info('Calculating checksums for run %s', run_dir)
                        checksum_filename = f'{run_dir}_checksums.sha1'
                        calculate_checksums(run_dir_full, transfer_dir_full, checksum_filename)
                    for file_type in file_types:
                        make_archive(run_dir_full, transfer_dir_full, file_type, threads)
                else:
                    logging.info('Run %s has not been complete for %f seconds yet, skipping.',
                                 run_dir, time_delay)

def main():
    '''
    Main function
    '''
    os.makedirs('logs', exist_ok=True)
    init_logging('logs/auto_archive_{:%Y-%m-%d_%H%M}.log'.format(datetime.now()))

    args = parse_args(sys.argv[1:])
    if not os.path.exists(args.config):
        logging.error('Config file does not exist, exiting.')
        sys.exit()

    with open(args.config, 'r') as stream:
        config = yaml.load(stream, yaml.SafeLoader)

    data_dir = config['data_dir']
    extra_dirs = config['extra_dirs']
    file_types = config['file_types']
    proj_dir_regex = re.compile(r'%s' % config['proj_dir_regex'])

    if not os.path.exists(data_dir):
        logging.error('Data directory does not exist, exiting.')
        sys.exit()

    if not isinstance(extra_dirs, list) and extra_dirs:
        logging.error('extra_dirs argument is not a list or empty, exiting.')
        sys.exit()

    if not isinstance(file_types, list) and file_types:
        logging.error('file_types argument is not a list or empty, exiting.')
        sys.exit()

    for file_type in file_types:
        if file_type not in POSSIBLE_FILE_TYPES:
            logging.error('Invalid file type %s specified.', file_type)
            sys.exit()

    project_dirs = get_project_dirs(data_dir, proj_dir_regex)
    project_dirs = project_dirs + extra_dirs if extra_dirs else project_dirs
    for proj_dir in project_dirs:
        archive_runs_if_complete(data_dir, proj_dir, file_types, config)

    logging.info('Done!')

if __name__ == '__main__':
    main()
