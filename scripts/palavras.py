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

job_batch_size = 1


class PtWikiSqliteSentences(object):
    def __init__(self, filepath):
        self.filepath = filepath

    def __iter__(self):
        toparse = []
        with sqlite3.connect(self.filepath) as conn:
            c = conn.cursor()
            toparse = [row for row in c.execute(
                'SELECT id FROM sentences where palavras IS NULL')]
        for row in toparse:
            yield row


def run_parse(sentence):
    process = subprocess.Popen(['/opt/palavras/por.pl'],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate(
        input=sentence.encode('utf-8'))
    if stderrdata != b'PALAVRAS revision 12687, compiled on 2018-03-14\n':
        print(stderrdata)
        return 'null'
    return stdoutdata


# def split_result(result):
#     current = ''
#     previous = ''
#     for line in result.split('\n'):
#         current += line + '\n'
#         if (line.startswith('<ß>')):
#             current = line + '\n'
#         elif line.startswith('</ß>') and not previous.startswith('$;'):
#             yield current
#         previous = line


# def create_worker_method(sqlfile_path):
#     def worker_palavras(job):
#         toparse = [id for (id,) in job]
#         data = []
#         with sqlite3.connect(sqlfile_path) as conn:
#             c = conn.cursor()
#             query = "SELECT id, text FROM sentences where id IN ({0})".format(
#                 ','.join(['?']*len(toparse)))
#             data = [row for row in c.execute(query, toparse)]

#         sentences = ''
#         for (_, sentence) in data:
#             sentences += sentence

#         result = run_parse(sentences).decode('utf-8')
#         parsed_list = [x for x in split_result(result)]

#         return [(id, parsed_list[idx]) for idx, (id, text) in enumerate(data)]
#     return worker_palavras

def create_worker_method(sqlfile_path):
    def worker_palavras(job):
        parsed_list = []
        with sqlite3.connect(sqlfile_path) as conn:
            for (id,) in job:
                c = conn.cursor()
                c.execute("SELECT id, text FROM sentences where id = ?", (id,))
                (_, text) = c.fetchone()
                result = run_parse(text).decode('utf-8')
                parsed_list.append((id, result))
        return parsed_list
    return worker_palavras


def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument(
        "input", help="sqlfile_path")
    args = parser.parse_args()
    sqlfile_path = args.input

    num_threds = multiprocessing.cpu_count()
    pool = multiprocessing.pool.ThreadPool(num_threds)
    print('Running with {0} threads ...'.format(num_threds))
    reporter = JobsReporter(batch_size=job_batch_size)
    sentences = PtWikiSqliteSentences(sqlfile_path)

    jobs = grouper(sentences, job_batch_size)

    with sqlite3.connect(sqlfile_path) as conn:
        c = conn.cursor()
        for (total,) in c.execute('SELECT COUNT(*) FROM sentences where palavras IS NULL'):
            print('Sentences to be parsed: {0}'.format(total))
        for result in pool.imap(create_worker_method(sqlfile_path), jobs):
            for (id, parsed_text) in result:
                u = (parsed_text, id)
                c.execute('UPDATE sentences SET palavras = ? WHERE id = ?', u)
            conn.commit()
            reporter.complete_job(report=True)
    print('\n')


if __name__ == '__main__':
    main()
