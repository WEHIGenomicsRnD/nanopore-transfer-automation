'''
Pytest script for testing auto_archive program.
Generates a new set of test files for each run.
'''
import pytest
import auto_archive as aa
import os
import random
import shutil
import glob

def get_random_hexstring(magnitude):
    return hex(round(random.random() * magnitude))[2:]

def make_run(basedir, subdirs, flowcellid, runhex, is_finished):
    os.makedirs(basedir)
    for subdir in subdirs:
        os.makedirs(os.path.join(basedir, subdir))
    if is_finished:
        sequencing_summary = f'sequencing_summary_{flowcellid}_{runhex}_{get_random_hexstring(1e8)}.txt'
        sequencing_summary = os.path.join(basedir, sequencing_summary)
        open(sequencing_summary, 'a').close()
    random_fastq_pass = os.path.join(basedir, 'fastq_pass', f'{get_random_hexstring(1e8)}.fastq')
    random_fastq_fail = os.path.join(basedir, 'fastq_fail', f'{get_random_hexstring(1e8)}.fastq')
    random_fast5_pass = os.path.join(basedir, 'fast5_pass', f'{get_random_hexstring(1e8)}.fast5')
    random_fast5_fail = os.path.join(basedir, 'fast5_fail', f'{get_random_hexstring(1e8)}.fast5')
    random_report = os.path.join(basedir, f'report_{get_random_hexstring(1e8)}.html')
    open(random_fastq_pass, 'a').close()
    open(random_fastq_fail, 'a').close()
    open(random_fast5_pass, 'a').close()
    open(random_fast5_fail, 'a').close()
    open(random_report, 'a').close()

### create test data ###

# directories found under every promethION run
subdirs = ['fast5_pass', 'fast5_fail', 'fastq_pass', 'fastq_fail', 'other_reports']

# some example values
date = '20221208'
affiliation = 'wehi'
lab = 'bowden'
flowcellid = 'PAK1234'

# specify runs and whether they have finished
runs = {'runa': True, 'runb': True, 'runc': False}
samples = ['sample_a', 'sample_b']

# remove test dir and remake test directories
if os.path.exists('test'):
    shutil.rmtree('test')
for run in runs:
    runhex = get_random_hexstring(1e8)
    for sample in samples:
        basedir = f'test/{date}_{affiliation}_{lab}_{run}/{sample}/{date}_1111_2F_{flowcellid}_{runhex}'
        make_run(basedir, subdirs, flowcellid, runhex, runs[run])

# make a test dirs that is meant to be ignored
os.makedirs('test/TEST_a')

# make a test dir that doesn't follow the pattern but still needs to be processed
os.makedirs('test/TEST_b')
runhex = get_random_hexstring(1e8)
sample = samples[0]
basedir = f'test/TEST_b/{sample}/{date}_1111_2F_{flowcellid}_{runhex}'
make_run(basedir, subdirs, flowcellid, runhex, True)

file_types = ['reports', 'fastq', 'fast5']
proj_dir_regex = r'^(\d{6,8})_([a-zA-Z0-9\-]+)_([a-zA-Z0-9\-]+)_([a-zA-Z0-9\-_]+)$'

### end test data creation ###
def test_calculate_checksums():
    run_dir = os.listdir('test/20221208_wehi_bowden_runa/sample_a')[0]
    aa.calculate_checksums(f'test/20221208_wehi_bowden_runa/sample_a/{run_dir}',
                           'test/20221208_wehi_bowden_runa/_transfer',
                           '20221208_wehi_bowden_runa_checksums.sha1')
    checksum_file = 'test/20221208_wehi_bowden_runa/_transfer/checksums/20221208_wehi_bowden_runa_checksums.sha1'
    checksum_contents = []
    for line in open(checksum_file, 'r'):
        checksum_contents.append(line)
    len(checksum_contents) == 6

def test_make_archive():
    project_dirs = aa.get_project_dirs('test', proj_dir_regex)
    project_dirs_truth = ['20221208_wehi_bowden_runa',
                          '20221208_wehi_bowden_runb',
                          '20221208_wehi_bowden_runc']
    assert all([proj_dir in project_dirs_truth for proj_dir in project_dirs])
    assert len(project_dirs) == len(project_dirs_truth)

def test_archive_runs_if_complete():
    config = {'transfer_dir': '_transfer',
              'time_delay': 0,
              'calculate_checksums': False,
              'threads': 1,
              'proj_dir_regex': proj_dir_regex}

    # complete run test
    aa.archive_runs_if_complete('test', '20221208_wehi_bowden_runa', file_types, config)

    rundir = 'test/20221208_wehi_bowden_runa'
    fast5_tar = glob.glob(f'{rundir}/_transfer/fast5/sample_a/*_fast5.tar.gz')
    fastq_tar = glob.glob(f'{rundir}/_transfer/fastq/sample_a/*_fastq.tar')
    report_tar = glob.glob(f'{rundir}/_transfer/reports/sample_a/*_reports.tar.gz')

    assert len(fast5_tar) == 1
    assert len(fastq_tar) == 1
    assert len(report_tar) == 1

    # incomplete run test
    aa.archive_runs_if_complete('test', '20221208_wehi_bowden_runc', file_types, config)

    rundir = 'test/20221208_wehi_bowden_runc'
    fast5_tar = glob.glob(f'{rundir}/_transfer/fast5/sample_a/*_fast5.tar.gz')
    fastq_tar = glob.glob(f'{rundir}/_transfer/fastq/sample_a/*_fastq.tar')
    report_tar = glob.glob(f'{rundir}/_transfer/reports/sample_a/*_reports.tar.gz')

    assert len(fast5_tar) == 0
    assert len(fastq_tar) == 0
    assert len(report_tar) == 0

    # complete run not within time delay
    config['time_delay'] = 600
    aa.archive_runs_if_complete('test', '20221208_wehi_bowden_runb', file_types, config)

    rundir = 'test/20221208_wehi_bowden_runb'
    fast5_tar = glob.glob(f'{rundir}/_transfer/fast5/sample_a/*_fast5.tar.gz')
    fastq_tar = glob.glob(f'{rundir}/_transfer/fastq/sample_a/*_fastq.tar')
    report_tar = glob.glob(f'{rundir}/_transfer/reports/sample_a/*_reports.tar.gz')

    assert len(fast5_tar) == 0
    assert len(fastq_tar) == 0
    assert len(report_tar) == 0

    # test multiple threads
    config['time_delay'] = 0
    config['threads'] = 2
    aa.archive_runs_if_complete('test', '20221208_wehi_bowden_runb', file_types, config)

    rundir = 'test/20221208_wehi_bowden_runb'
    fast5_tar = glob.glob(f'{rundir}/_transfer/fast5/sample_a/*_fast5.tar.gz')
    fastq_tar = glob.glob(f'{rundir}/_transfer/fastq/sample_a/*_fastq.tar')
    report_tar = glob.glob(f'{rundir}/_transfer/reports/sample_a/*_reports.tar.gz')

    assert len(fast5_tar) == 1
    assert len(fastq_tar) == 1
    assert len(report_tar) == 1



