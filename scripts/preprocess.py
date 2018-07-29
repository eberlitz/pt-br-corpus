#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script used for cleaning corpus in order to train word embeddings.

All emails are mapped to a EMAIL token.
All numbers are mapped to 0 token.
All urls are mapped to URL token.
Different quotes are standardized.
Different hiphen are standardized.
HTML strings are removed.
All text between brackets are removed.

Script adapted from https://github.com/nathanshartmann/portuguese_word_embeddings/blob/master/preprocessing.py
Modifications:

- Do not change the case (original was transforming everything to lowercase, but PALAVRAS parser has problems identifying the sentence end without this. You should lowercase afterward.)
- Do not remove sentences with less than 5 words (the original script does that).
- Modified to allow abbreviations, like 'Dr.'.
- Keep words with '-', like 'guarda-chuva'.
- Breaks into multiple sentences using nltk.data.load('tokenizers/punkt/portuguese.pickle').
...
"""
from sys import stdout
import os
import re
import nltk
import logging
import bz2
import glob
import argparse
import sys
from helpers import NextFile, OutputSplitter, mkdir_if_not_exists, JobsReporter
import multiprocessing
from multiprocessing.pool import ThreadPool
from gensim.utils import grouper


# nltk.download('punkt')
sent_tokenizer = nltk.data.load('tokenizers/punkt/portuguese.pickle')

# ##### #
# Regex #
# ##### #
punctuations = re.escape('!"#%\'()*+,./:;<=>?@[\\]^_`{|}~')
re_remove_brackets = re.compile(r'\{.*\}')
re_remove_html = re.compile(r'<(\/|\\)?.+?>', re.UNICODE)
re_transform_numbers = re.compile(r'\d', re.UNICODE)
re_transform_emails = re.compile(r'[^\s]+@[^\s]+', re.UNICODE)
re_transform_url = re.compile(r'(http|https)://[^\s]+', re.UNICODE)
# Different quotes are used.
re_quotes_1 = re.compile(r"(?u)(^|\W)[‘’′`']", re.UNICODE)
re_quotes_2 = re.compile(r"(?u)[‘’`′'](\W|$)", re.UNICODE)
re_quotes_3 = re.compile(r'(?u)[‘’`′“”]', re.UNICODE)
re_dots = re.compile(r'(?<!\.)\.\.(?!\.)', re.UNICODE)
re_punctuation = re.compile(r'([,";:]){2},', re.UNICODE)
re_hiphen = re.compile(r' -(?=[^\W\d_])', re.UNICODE)
re_tree_dots = re.compile(u'…', re.UNICODE)
# Differents punctuation patterns are used.
re_punkts = re.compile(r'(\w+)([%s])([ %s])' %
                       (punctuations, punctuations), re.UNICODE)
re_punkts_b = re.compile(r'([ %s])([%s])(\w+)' %
                         (punctuations, punctuations), re.UNICODE)
re_punkts_c = re.compile(r'(\w+)([%s])$' % (punctuations), re.UNICODE)
re_changehyphen = re.compile(u'–')
re_doublequotes_1 = re.compile(r'(\"\")')
re_doublequotes_2 = re.compile(r'(\'\')')
re_trim = re.compile(r' +', re.UNICODE)

def clean_single_sentence(text):
    """Apply all regex above to a given string."""
    # text = text.lower() # Nao passa para caixa baixa pois o parser palavras tem problemas para detectar as frases sem.
    text = re_tree_dots.sub('...', text)
    text = re.sub(r'\.\.\.', '', text)
    text = re_remove_brackets.sub('', text)
    text = re_changehyphen.sub('-', text)
    text = re_remove_html.sub(' ', text)
    text = re_transform_numbers.sub('0', text)
    text = re_transform_url.sub('URL', text)
    text = re_transform_emails.sub('EMAIL', text)
    text = re_quotes_1.sub(r'\1"', text)
    text = re_quotes_2.sub(r'"\1', text)
    text = re_quotes_3.sub('"', text)
    text = re.sub('"', '', text)
    text = re_dots.sub('.', text)
    text = re_punctuation.sub(r'\1', text)
    text = re_hiphen.sub(' - ', text)
    text = re_punkts.sub(r'\1 \2 \3', text)
    text = re_punkts_b.sub(r'\1 \2 \3', text)
    text = re_punkts_c.sub(r'\1 \2', text)
    text = re_doublequotes_1.sub('\"', text)
    text = re_doublequotes_2.sub('\'', text)
    text = re_trim.sub(' ', text)
    return text.strip()

def clean_document(document):
    '''
    Returns a list of sentences for the given document.
    Process the document line by line:
    - Split into multiple sentences using the NLTK Punkt Tokenizer.
    - Remove any senteces with less then 4 words.
    '''
    for line in document.split('\n'):
        for sent in sent_tokenizer.tokenize(line):
            sent = clean_single_sentence(sent)
            if sent.count(' ') >= 3 and sent[-1] in ['.', '!', '?', ';']:
                if sent[0:2] == '- ':
                    sent = sent[2:]
                elif sent[0] == ' ' or sent[0] == '-':
                    sent = sent[1:]
                yield sent

def read_wiki_documents_compressed(dirname):
    '''
    Reads all /**/*.bz2 files in the given dirname and returns each document. 
    '''
    filepaths = glob.glob(dirname+'/**/*.bz2')
    filepaths.sort()
    for fp in filepaths:
        with bz2.BZ2File(fp, 'r') as input:
            doc = b''
            for line in input:
                if (line.startswith(b'<doc ')):
                    doc = b''
                elif line.startswith(b'</doc>'):
                    yield doc
                else:
                    doc += line + b'\n'

def worker_clean_document(jobs):
    return [clean_document(document.decode('utf-8')) for document in jobs]

def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input", help="ptwiki-compressed-text-folder")
    parser.add_argument("-o", "--output", default="./data/cleaned/",
                        help="directory for extracted files")
    args = parser.parse_args()
    input_dirname = args.input
    output_dirname = args.output
    mkdir_if_not_exists(output_dirname)

    vocab, tokens = set(), 0
    output = OutputSplitter(NextFile(output_dirname), 10*1024*1024, True)
    
    num_threds = multiprocessing.cpu_count()
    pool = ThreadPool(num_threds)
    print('Running with {0} threads ...'.format(num_threds))
    job_batch_size = 1000
    reporter = JobsReporter(report_period=1000)
    documents = read_wiki_documents_compressed(input_dirname)
    
    jobs = grouper(documents, job_batch_size)

    for job in pool.imap(worker_clean_document, jobs):
        for sentences in job:
            for sentence in sentences:
                output.write((sentence+'\n').encode('utf-8'))
                tokens += sentence.count(' ') + 1
                for w in sentence.split():
                    vocab.add(w)
                reporter.complete_job(report=True)
    output.close()
    print('\n')
    print('Tokens: ', tokens)
    print('Vocabulary: ', len(vocab))

if __name__ == '__main__':
    main()