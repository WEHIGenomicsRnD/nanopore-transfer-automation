import os
import random
import shutil

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
    random_fastq_pass = os.path.join(basedir, 'fastq_pass', f'{get_random_hexstring(1e8)}.fastq.gz')
    random_fastq_fail = os.path.join(basedir, 'fastq_fail', f'{get_random_hexstring(1e8)}.fastq.gz')
    random_pod5 = os.path.join(basedir, 'pod5', f'{get_random_hexstring(1e8)}.pod5')
    report_hexstring = get_random_hexstring(1e8)
    random_report = os.path.join(basedir, f'report_{report_hexstring}.html')
    random_json = os.path.join(basedir, f'report_{report_hexstring}.json')
    open(random_fastq_pass, 'a').close()
    open(random_fastq_fail, 'a').close()
    open(random_pod5, 'a').close()
    open(random_report, 'a').close()
    open(random_json, 'a').close()

### create test data ###

# directories found under every promethION run
subdirs = ['pod5', 'fastq_pass', 'fastq_fail', 'other_reports']

# some example values
date = '20221208'
affiliation = 'wehi'
lab = 'bowden'
flowcellid = 'PAK1234'

# specify runs and whether they have finished
runs = {'runa': True, 'runb': True, 'runc': False}
samples = ['sample_a', 'sample_b']

# remake test directories
#if os.path.exists('test_data'):
#    shutil.rmtree('test_data')
for run in runs:
    for sample in samples:
        runhex = get_random_hexstring(1e8)
        basedir = f'test_data/{date}_{affiliation}_{lab}_{run}/{sample}/{date}_1111_2F_{flowcellid}_{runhex}'
        make_run(basedir, subdirs, flowcellid, runhex, runs[run])

        # make a second run
        runhex = get_random_hexstring(1e8)
        basedir = f'test_data/{date}_{affiliation}_{lab}_{run}/{sample}/{date}_1111_2F_{flowcellid}_{runhex}'
        make_run(basedir, subdirs, flowcellid, runhex, runs[run])

# make a test dirs that is meant to be ignored
os.makedirs('test_data/TEST_a')

# make a test dir that doesn't follow the pattern but still needs to be processed
os.makedirs('test_data/TEST_b')
runhex = get_random_hexstring(1e8)
sample = samples[0]
basedir = f'test_data/TEST_b/{sample}/{date}_1111_2F_{flowcellid}_{runhex}'
make_run(basedir, subdirs, flowcellid, runhex, True)
