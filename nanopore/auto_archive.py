'''
Module      : auto_archiver
Description : Creates three archives (reports, fastq and
              fast5 files) on Nanopore sequencing machines.
Copyright   : (c) Marek Cmero, 8/12/2022
License     : TBD
Maintainer  : Marek Cmero
Portability : POSIX
Scans data directory for runs matching metadata schema,
YYYYMMDD_affiliation_labname_runname, then check whether
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
import yaml
from argparse import ArgumentParser
from datetime import datetime

file_types = ['reports', 'fastq', 'fast5']
proj_dir_regex = re.compile(r'^(\d{8})_([a-zA-Z0-9\-]+)_([a-zA-Z0-9\-]+)_([a-zA-Z0-9\-]+)$')

def parse_args(args):
    description = '''
        Run auto-archiving with config file:

        python auto_archive.py config.yaml

        Config file should contain:

        data_dir: <path_to_data_directory>
        transfer_dir_name: <name_of_transfer_directories>
        '''
    parser = ArgumentParser(description=description)
    parser.add_argument('config',
                        metavar='CONFIG',
                        type=str,
                        help='Config file.')
    return parser.parse_args(args)


def init_logging(log_filename):
    logging.basicConfig(filename=log_filename,
                        level=logging.DEBUG,
                        filemode='w',
                        format='%(asctime)s %(levelname)s - %(message)s',
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    logging.info('Auto-archiver started')

def make_archive(data_dir, proj_dir, sample_dir, run_dir, transfer_dir_name, file_type):
    '''
    Given directories for project, sample and run dirs,
    make tar.gz of report files and fast5 files, and
    make tar file for fastq files
    '''
    assert file_type in file_types
    
    run_dir_full = os.path.join(data_dir, proj_dir, sample_dir, run_dir)
    transfer_dir = os.path.join(data_dir, proj_dir, transfer_dir_name)
    dest_dir = os.path.join(transfer_dir, file_type, sample_dir)
    os.makedirs(dest_dir, exist_ok=True)

    success_file = os.path.join(run_dir_full, f'{run_dir}_{file_type}_archive.success')
    if os.path.exists(success_file):
        # nothing to do as archiving already done
        logging.info(f'Skipped {file_type} for run {run_dir} due to presence of success file.')
        return
    
    tar_args = '-cpvf' if file_type == 'fastq' else '-czpvf' # don't need to zip fastqs
    ext = 'tar' if file_type == 'fastq' else 'tar.gz'
    tar_file = os.path.join(dest_dir, f'{run_dir}_{file_type}.{ext}')
    tar_file = os.path.abspath(tar_file) # need absolute path for tar to get put in right place
    files_to_archive = []
    
    if file_type == 'reports':        
        report_files = [os.path.basename(file) for file in glob.glob(os.path.join(run_dir_full, '*.*'))]
        files_to_archive = report_files + ['other_reports']

    elif file_type == 'fast5':
        files_to_archive = ['fast5_pass', 'fast5_fail']

    elif file_type == 'fastq':
        files_to_archive = ['fastq_pass', 'fastq_fail']

    for file in files_to_archive:
        if not os.path.exists(os.path.join(run_dir_full, file)):
            logging.error(f'{file} does not exist!')
            return
        
    proc = subprocess.Popen(['tar', tar_args, tar_file] + files_to_archive,
                             cwd=run_dir_full,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
    for line in proc.stdout:
        logging.info(bytes.decode(line).strip())

    return_code = proc.wait()
    if return_code == 0:
        open(success_file, 'w').close()
        logging.info(f'Successfully archived {file_type} files for {run_dir}.')
    else:
        logging.error(f'An error occured archiving {file_type} for {run_dir}.')

def get_project_dirs(data_dir):
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
            logging.info(f'Found project directory {proj_dir}.')
            project_dirs.append(proj_dir)
    return project_dirs
            
def archive_runs_if_complete(data_dir, proj_dir, transfer_dir_name):
    '''    
    Checks whether run is complete, if so,
    create one archive each for reports,
    fast5 and fastq files.
    '''
    logging.info(f'Processing {proj_dir}...')
    sample_dirs = os.listdir(os.path.join(data_dir, proj_dir))

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

            run_finished = any([bool(re.match(r'^sequencing_summary\w*\.txt', rc)) for rc in run_contents])
            if run_finished:
                logging.info(f'Run {run_dir} finished! Making archives...')
                for file_type in file_types:
                    make_archive(data_dir, proj_dir, sample_dir, run_dir, transfer_dir_name, file_type)
            
def main():
    init_logging('auto_archive_{:%Y-%m-%d_%H%M}.log'.format(datetime.now()))
    
    args = parse_args(sys.argv[1:])
    if not os.path.exists(args.config):
        logging.error('Config file does not exist, exiting.')
        sys.exit()
    
    stream = open(args.config, 'r')
    config = yaml.load(stream, yaml.SafeLoader)

    data_dir = config['data_dir']
    transfer_dir_name = config['transfer_dir_name']

    if not os.path.exists(data_dir):
        logging.error('Data directory does not exist, exiting.')
        sys.exit()

    project_dirs = get_project_dirs(data_dir)
    for proj_dir in project_dirs:
        archive_runs_if_complete(data_dir, proj_dir, transfer_dir_name)

    logging.info('Done!')

if __name__ == '__main__':
    main()