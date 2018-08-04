#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import bz2
import glob
import math
import pickle
import sqlite3
import argparse
import subprocess
import multiprocessing
import multiprocessing.pool
from itertools import dropwhile
from collections import Counter
from helpers import NextFile, OutputSplitter, mkdir_if_not_exists, JobsReporter


re_extract_context = re.compile(
    r'^(.*?) +.*?(@.*?)\s+#(\d+)->(\d+)', re.MULTILINE | re.UNICODE)
re_remove_especial = re.compile(r'^\$.*\s', re.MULTILINE | re.UNICODE)
remove_doc_tags = re.compile(r'<\/?ß>\s+', re.MULTILINE | re.UNICODE)


def regex_to_contexts(text):
    text = text.lower()
    text = remove_doc_tags.sub(r'', text)
    text = re_remove_especial.sub(r'', text)
    text = re_extract_context.sub(r'\3|\1|\2|\4', text)
    return text


def extract_context_from_doc_text(doc):
    words = {}
    lastid = -1
    for line in regex_to_contexts(doc).split('\n'):
        tokens = line.split('|')
        if len(tokens) == 4:
            num = tokens[0]
            if int(num) < lastid:
                words = {}
                lastid = -1
            else:
                lastid = int(num)
            word = tokens[1]
            subwords = word.split('=')
            if len(subwords) > 1:
                subwords.append(word)
            contexts = tokens[2].replace('@', '').split(' ')
            ref = tokens[3]
            words[num] = (num, subwords, contexts, ref)
    for key in words:
        (num, subwords, contexts, ref) = words[key]
        if (ref in words):
            (_, ref_words, _, _) = words[ref]
            for word in subwords:
                for context in contexts:
                    for ref_word in ref_words:
                        yield (word, '{}_{}'.format(context, ref_word))
                        # print('{} {}_{}'.format(word, context, ref_word))


def update_counters_and_format(word, context, wv, cv):
    wv.update([word])
    cv.update([context])
    return '{} {}'.format(word, context)


def create_worker_method(sqlfile_path, job_batch_size, word_filter):
    def contexts_worker(page_id):
        cv = Counter()
        wv = Counter()
        query = '''
        SELECT palavras
        FROM sentences
        WHERE palavras IS NOT NULL AND id > ?
        ORDER BY id
        LIMIT ?;
        '''
        with sqlite3.connect(sqlfile_path) as conn:
            c = conn.cursor()
            params = (page_id * job_batch_size, job_batch_size)
            batch_result = []
            for (parsed_sentence,) in c.execute(query, params):
                single_result = [update_counters_and_format(
                    word, ctx, wv, cv) for (word, ctx) in extract_context_from_doc_text(parsed_sentence) if word_filter == None or word_filter[word] > 0]
                batch_result = batch_result+single_result
            return (cv, wv, batch_result)

    return contexts_worker


def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument(
        "input", help="sqlfile_path")
    parser.add_argument("-b", "--batchsize", type=int, default=50,
                        help="The number of sentences to be sended to the parser in each iteration.")
    parser.add_argument("-mc", "--mincount", type=int,
                        help="ignores all contexts that apears less then")
    parser.add_argument("-wv", "--wordvocabcount", type=int,
                        help="generates the word vocab and ignores all words that apears less then")
    parser.add_argument("-o", "--output", default="./data/contexts/",
                        help="directory for extracted files")
    args = parser.parse_args()
    sqlfile_path = args.input
    job_batch_size = args.batchsize
    output_dirname = args.output
    wordvocabcount = args.wordvocabcount
    mincount = args.mincount
    mkdir_if_not_exists(output_dirname)
    word_vocab_file = os.path.join(output_dirname, 'wordvocabcount')

    cv_all = Counter()
    wv_all = Counter()
    word_filter = None
    output = None
    if wordvocabcount == None:
        with open(word_vocab_file, 'rb') as f:
            word_filter = wv_all = pickle.load(f)
        output = open(os.path.join(output_dirname, 'dep.contexts'), 'wb')
    else:
        mincount = None
    num_threds = multiprocessing.cpu_count()
    pool = multiprocessing.pool.ThreadPool(num_threds)
    print('Running with {0} threads ...'.format(num_threds))
    print('Batch size: {0}'.format(job_batch_size))

    reporter = JobsReporter(batch_size=job_batch_size, report_period=10)

    with sqlite3.connect(sqlfile_path) as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM sentences where palavras IS NOT NULL')
        (total,) = c.fetchone()
        jobs_number = math.ceil(total/job_batch_size)
        jobs = range(1, jobs_number)
        print('Sentences to be parsed: {0}'.format(total))

        reporter.reset()
        for (cv, wv, batch_result) in pool.imap(create_worker_method(sqlfile_path, job_batch_size, word_filter), jobs):
            cv_all = cv_all + cv
            if wordvocabcount != None:
                wv_all = wv_all + wv
            else:
                output.write('\n'.join(batch_result).encode('utf-8'))
            reporter.complete_job(report=True)
    print('\n')

    if wordvocabcount != None:
        for key, count in dropwhile(lambda key_count: key_count[1] >= wordvocabcount, wv_all.most_common()):
            del wv_all[key]
        with open(word_vocab_file, 'wb') as f:
            pickle.dump(wv_all, f)

        with open(os.path.join(output_dirname, 'wv'), encoding='utf-8', mode='w') as f:
            for w, count in wv_all.items():
                f.write('{} {}\n'.format(w, count))
        wv_all = None

    if wordvocabcount == None:
        for key, count in dropwhile(lambda key_count: key_count[1] >= mincount, cv_all.most_common()):
            del cv_all[key]
        with open(os.path.join(output_dirname, 'cv'), encoding='utf-8', mode='w') as f:
            for w, count in cv_all.items():
                f.write('{} {}\n'.format(w, count))
        cv_all = None


if __name__ == '__main__':
    main()

