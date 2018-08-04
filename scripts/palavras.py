#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import bz2
import glob
import sqlite3
import argparse
import subprocess
import multiprocessing
from gensim.utils import grouper
from helpers import NextFile, OutputSplitter, mkdir_if_not_exists, JobsReporter

sentence_separator_token = '\n\nNull.\n\n'
sentence_separator_result = '<ß>\nNull \t[Null] <hum> <*> PROP M/F S @NPHR  #1->0\n$. #2->0\n</ß>'


def run_parse(sentence):
    process = subprocess.Popen(['/opt/palavras/por.pl'],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate(
        input=sentence.encode('utf-8'))
    if stderrdata != b'PALAVRAS revision 12687, compiled on 2018-03-14\n':
        print(stderrdata)
        return b'null'
    return stdoutdata


def create_worker_method(sqlfile_path, job_batch_size):
    def worker_palavras(job):
        toparse = [id for (id,) in job]
        data = []
        with sqlite3.connect(sqlfile_path) as conn:
            c = conn.cursor()
            query = "SELECT id, text FROM sentences where id IN ({0})".format(
                ','.join(['?']*len(toparse)))
            data = [row for row in c.execute(query, toparse)]

        sentences = sentence_separator_token.join(
            [sentence for (_, sentence) in data])
        result = run_parse(sentences).decode('utf-8')
        parsed_list = result.split(sentence_separator_result)
        if (len(data) != len(parsed_list)):
            print('error')
            return []
        return [(id, parsed_list[idx]) for idx, (id, text) in enumerate(data)]

    def worker_palavras_one(job):
        parsed_list = []
        with sqlite3.connect(sqlfile_path) as conn:
            for (id,) in job:
                c = conn.cursor()
                c.execute("SELECT id, text FROM sentences where id = ?", (id,))
                (_, text) = c.fetchone()
                result = run_parse(text).decode('utf-8')
                parsed_list.append((id, result))
        return parsed_list

    if job_batch_size == 1:
        return worker_palavras_one
    return worker_palavras


def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument(
        "input", help="sqlfile_path")
    parser.add_argument("-b", "--batchsize", type=int, default=50,
                        help="The number of sentences to be sended to the parser in each iteration.")
    args = parser.parse_args()
    sqlfile_path = args.input
    job_batch_size = args.batchsize

    num_threds = multiprocessing.cpu_count()
    pool = multiprocessing.pool.ThreadPool(num_threds)
    print('Running with {0} threads ...'.format(num_threds))
    print('Batch size: {0}'.format(job_batch_size))

    reporter = JobsReporter(batch_size=job_batch_size)

    with sqlite3.connect(sqlfile_path) as conn:
        sentences = [row for row in conn.cursor().execute(
            'SELECT id FROM sentences where palavras IS NULL')]
        print('Sentences to be parsed: {0}'.format(len(sentences)))
        jobs = grouper(sentences, job_batch_size)
        c = conn.cursor()
        reporter.reset()
        for result in pool.imap(create_worker_method(sqlfile_path, job_batch_size), jobs):
            for (id, parsed_text) in result:
                u = (parsed_text, id)
                c.execute('UPDATE sentences SET palavras = ? WHERE id = ?', u)
            conn.commit()
            reporter.complete_job(report=True)
    print('\n')


if __name__ == '__main__':
    main()
