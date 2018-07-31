#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import bz2
import glob
import argparse
import subprocess
import multiprocessing
from gensim.utils import grouper
from helpers import NextFile, OutputSplitter, mkdir_if_not_exists, JobsReporter

job_batch_size = 10

class PtWikiSentences(object):
    def __init__(self, dirname):
        self.dirname = dirname

    def __iter__(self):
        filepaths = glob.glob(self.dirname)
        filepaths.sort()
        for fp in filepaths:
            with bz2.BZ2File(fp, 'r') as input:
                for line in input:
                    yield line.decode('utf-8')

def run_parse(sentence):
    process = subprocess.Popen(['/opt/palavras/por.pl'],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate(
        input=sentence.encode('utf-8'))
    if stderrdata != b'PALAVRAS revision 12687, compiled on 2018-03-14\n':
        print(stderrdata)
    return stdoutdata

def split_result(result):
    current = ''
    for line in result.split('\n'):
        current += line + '\n'
        if (line.startswith('<ß>')):
            current = line + '\n'
        elif line.startswith('</ß>'):
            print (current)
            yield current

def worker_palavras(job):
    sentences = ''
    for sentence in job:
        sentences += sentence

    result = run_parse(sentences).decode('utf-8')
    return split_result(result)

def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument(
        "input", help="ptwiki-compressed-preprocessed-text-folder")
    parser.add_argument("-o", "--output", default="./data/palavras/",
                        help="directory for extracted files")
    args = parser.parse_args()
    input_dirname = args.input
    output_dirname = args.output
    mkdir_if_not_exists(output_dirname)

    output = OutputSplitter(NextFile(output_dirname),  1024, False)

    num_threds = multiprocessing.cpu_count()
    pool = multiprocessing.pool.ThreadPool(num_threds)
    print('Running with {0} threads ...'.format(num_threds))
    reporter = JobsReporter(batch_size=job_batch_size)
    sentences = PtWikiSentences(input_dirname+'/**/*.bz2')

    jobs = grouper(sentences, job_batch_size)

    for result in pool.imap(worker_palavras, jobs):
        for st in result:
            output.write(st.encode('utf-8'))
        reporter.complete_job(report=True)
    output.close()
    print('\n')

if __name__ == '__main__':
    main()
